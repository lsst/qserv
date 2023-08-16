// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "ccontrol/UserQueryQservManager.h"

// System headers
#include <list>
#include <stdexcept>

// Third party headers
#include <nlohmann/json.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qdisp/CzarStats.h"
#include "qdisp/MessageStore.h"
#include "sql/SqlBulkInsert.h"
#include "sql/SqlConnection.h"
#include "util/StringHelper.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryQservManager");

}

namespace lsst::qserv::ccontrol {

UserQueryQservManager::UserQueryQservManager(shared_ptr<UserQueryResources> const& queryResources,
                                             string const& value)
        : _value(value),
          _resultTableName("qserv_manager_" + queryResources->userQueryId),
          _messageStore(make_shared<qdisp::MessageStore>()),
          _resultDbConn(queryResources->resultDbConn),
          _resultDb(queryResources->resultDb) {}

void UserQueryQservManager::submit() {
    LOGS(_log, LOG_LVL_TRACE, "processing command: " << _value);

    // Remove quotes around a value of the input parameter. Also parse the command.
    // Some commands may have optional parameters.
    // Note that (single or double) quotes are required by SQL when calling
    // the stored procedure. The quotes  are preserved AS-IS by the Qserv query parser.
    string command;
    vector<string> params;
    if (_value.size() > 2) {
        string const space = " ";
        string const quotesRemoved = _value.substr(1, _value.size() - 2);
        for (auto&& str : util::StringHelper::splitString(quotesRemoved, space)) {
            // This is just in case if the splitter won't recognise consequtive spaces.
            if (str.empty() || (str == space)) continue;
            if (command.empty()) {
                command = str;
            } else {
                params.push_back(str);
            }
        }
    }

    // Create the table as per the command.
    string createTable;
    vector<string> resColumns;  // This must match the schema in the CREATE TABLE statement.
    if (command == "query_proc_stats") {
        createTable = "CREATE TABLE " + _resultTableName + "(`stats` BLOB)";
        resColumns.push_back("stats");
    } else if (command == "query_info") {
        createTable = "CREATE TABLE " + _resultTableName +
                      "(`queryId` BIGINT NOT NULL, `timestamp_ms` BIGINT NOT NULL, `num_jobs` INT NOT NULL)";
        resColumns.push_back("queryId");
        resColumns.push_back("timestamp_ms");
        resColumns.push_back("num_jobs");
    } else {
        createTable = "CREATE TABLE " + _resultTableName + "(`result` BLOB)";
        resColumns.push_back("result");
    }
    LOGS(_log, LOG_LVL_TRACE, "creating result table: " << createTable);
    sql::SqlErrorObject errObj;
    if (!_resultDbConn->runQuery(createTable, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "failed to create result table: " << errObj.errMsg());
        string const message = "Internal failure, failed to create result table: " + errObj.errMsg();
        _messageStore->addMessage(-1, "SQL", 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // Prepare data for the command.
    // note that the output string(s) should be quoted.
    auto const stats = qdisp::CzarStats::get();
    list<vector<string>> rows;
    if (command == "query_proc_stats") {
        json const result = json::object({{"qdisp_stats", stats->getQdispStatsJson()},
                                          {"transmit_stats", stats->getTransmitStatsJson()}});
        vector<string> row = {"'" + result.dump() + "'"};
        rows.push_back(move(row));
    } else if (command == "query_info") {
        // The optonal query identifier and the number of the last seconds in a history
        // of queries may be provided to narrow a scope of the operation:
        //
        //   query_info
        //   query_info <qid>
        //   query_info <qid> <seconds>
        //
        // Where any value may be set to 0 to indicate the default behavior. Any extra
        // parameters will be ignored.
        //
        QueryId selectQueryId = 0;     // any query
        unsigned int lastSeconds = 0;  // any timestamps
        try {
            if (params.size() > 0) selectQueryId = stoull(params[0]);
            if (params.size() > 1) lastSeconds = stoul(params[1]);
        } catch (exception const& ex) {
            string const message =
                    "failed to parse values of parameter from " + _value + ", ex: " + string(ex.what());
            LOGS(_log, LOG_LVL_ERROR, message);
            _messageStore->addMessage(-1, "SQL", 1051, message, MessageSeverity::MSG_ERROR);
            _qState = ERROR;
            return;
        }

        // The original order of timestams within queries will be preserved as if
        // the following query was issued:
        //
        //   SELECT
        //     `queryId`,
        //     `timestamp_ms`,
        //     `num_jobs`
        //   FROM
        //     `table`
        //   ORDER BY
        //     `queryId`,
        //     `timestamp_ms` ASC
        //
        for (auto&& [queryId, history] : stats->getQueryProgress(selectQueryId, lastSeconds)) {
            string const queryIdStr = to_string(queryId);
            for (auto&& point : history) {
                vector<string> row = {queryIdStr, to_string(point.timestampMs), to_string(point.numJobs)};
                rows.push_back(move(row));
            }
        }
    } else {
        // Return a value of the original command (which includeds quotes).
        vector<string> row = {_value};
        rows.push_back(move(row));
    }

    // Ingest row(s) into the table.
    bool success = true;
    sql::SqlBulkInsert bulkInsert(_resultDbConn.get(), _resultTableName, resColumns);
    for (auto const& row : rows) {
        success = success && bulkInsert.addRow(row, errObj);
        if (!success) break;
    }
    if (success) success = bulkInsert.flush(errObj);
    if (!success) {
        LOGS(_log, LOG_LVL_ERROR, "error updating result table: " << errObj.errMsg());
        string const message = "Internal failure, error updating result table: " + errObj.errMsg();
        _messageStore->addMessage(-1, "SQL", 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }
    _qState = SUCCESS;
}

string UserQueryQservManager::getResultQuery() const {
    string ret = "SELECT * FROM " + _resultDb + "." + _resultTableName;
    return ret;
}

}  // namespace lsst::qserv::ccontrol
