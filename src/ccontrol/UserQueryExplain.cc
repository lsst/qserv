// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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
#include "ccontrol/UserQueryExplain.h"

// System headers
#include <sstream>

// Third-party headers
#include "nlohmann/json.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "qdisp/MessageStore.h"
#include "qproc/QuerySession.h"
#include "query/AreaRestrictor.h"
#include "query/QueryTemplate.h"
#include "query/ScanTableInfo.h"
#include "query/SecIdxRestrictor.h"
#include "query/SelectStmt.h"
#include "query/typedefs.h"
#include "sql/SqlBulkInsert.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlErrorObject.h"
#include "util/IterableFormatter.h"

using namespace lsst::qserv;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryExplain");

/// Concatenate restrictors for display
template <typename VecPtr>
std::string restrictorsToString(VecPtr const& restrictors) {
    if (restrictors == nullptr || restrictors->empty()) return "none";
    std::ostringstream os;
    os << util::ptrPrintable(restrictors, "", "");
    return os.str();
}

/// Convert JSON values for use in MariaDB-style EXPLAIN table
std::string jsonValueToString(json const& value) {
    if (value.is_string()) return value.get<std::string>();
    if (value.is_boolean()) return value.get<bool>() ? "yes" : "no";
    if (value.is_number()) return value.dump();
    return value.dump(2);
}

}  // namespace

namespace lsst::qserv::ccontrol {

UserQueryExplain::UserQueryExplain(std::shared_ptr<qproc::QuerySession> const& qSession,
                                   std::string const& userQueryId, std::string const& resultDb,
                                   bool jsonFormat)
        : _qSession(qSession),
          _jsonFormat(jsonFormat),
          _messageStore(std::make_shared<qdisp::MessageStore>()),
          _resultTableName("qserv_result_explain_" + userQueryId),
          _resultDb(resultDb) {}

std::string UserQueryExplain::getError() const { return _qSession->getError(); }

json UserQueryExplain::_getQueryInfo() const {
    auto const& qs = *_qSession;
    json qInfo = json::object();

    qInfo["parsed_sql"] = qs.getStmt().getQueryTemplate().sqlFragment();

    std::ostringstream irStream;
    irStream << qs.getStmt();
    qInfo["parsed_ir"] = irStream.str();

    qInfo["needs_merge"] = qs.needsMerge();
    auto const mergeStmt = qs.getMergeStmt();
    qInfo["merge_sql"] = mergeStmt != nullptr ? mergeStmt->getQueryTemplate().sqlFragment() : "none";

    qInfo["is_chunked"] = qs.hasChunks();
    qInfo["chunks_matched"] = qs.isDummy() ? 0 : qs.getChunksSize();

    qInfo["area_restrictors"] = restrictorsToString(qs.getAreaRestrictors());
    qInfo["secidx_restrictors"] = restrictorsToString(qs.getSecIdxRestrictors());

    qInfo["scan_interactive"] = qs.getScanInteractive();
    auto const scanInfo = qs.getScanInfo();
    qInfo["scan_rating"] = scanInfo.scanRating;
    json scanTables = json::array();
    for (auto const& tbl : scanInfo.infoTables) {
        scanTables.push_back(json::object({{"db", tbl.db},
                                           {"table", tbl.table},
                                           {"rating", tbl.scanRating},
                                           {"lock_in_memory", tbl.lockInMemory}}));
    }
    qInfo["scan_tables"] = scanTables;

    std::ostringstream workerStream;
    bool first = true;
    for (auto const& stmt : qs.getStmtParallel()) {
        if (stmt == nullptr) continue;
        if (!first) workerStream << " /*QSEPARATOR*/; ";
        workerStream << stmt->getQueryTemplate().sqlFragment();
        first = false;
    }
    qInfo["worker_sql"] = first ? "none" : workerStream.str();

    return qInfo;
}

std::string UserQueryExplain::getResultQuery() const {
    return "SELECT * FROM " + _resultDb + "." + _resultTableName + " ORDER BY 1";
}

void UserQueryExplain::submit() {
    try {
        _populateResultTable();
    } catch (std::exception const& exc) {
        std::string const message = std::string("Internal failure, EXPLAIN failed: ") + exc.what();
        LOGS(_log, LOG_LVL_ERROR, message);
        _messageStore->addMessage(-1, "EXPLAIN", 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
    }
}

void UserQueryExplain::_populateResultTable() {
    auto qInfo = _getQueryInfo();

    auto const czarConfig = cconfig::CzarConfig::instance();
    auto const resultDbConn = sql::SqlConnectionFactory::make(czarConfig->getMySqlResultConfig());
    sql::SqlErrorObject errObj;

    auto fail = [this](int code, std::string const& message) {
        LOGS(_log, LOG_LVL_ERROR, message);
        _messageStore->addMessage(-1, "EXPLAIN", code, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
    };

    std::string createTable;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;

    if (_jsonFormat) {
        // Single-column, single-row JSON document (like MariaDB EXPLAIN FORMAT=JSON).
        createTable = "CREATE TABLE " + _resultTableName + " (`EXPLAIN` MEDIUMTEXT)";
        columns = {"EXPLAIN"};
        rows.push_back({qInfo.dump(2)});
    } else {
        // Attribute/value table, one row per entry
        createTable = "CREATE TABLE " + _resultTableName + " (`attribute` VARCHAR(255), `value` MEDIUMTEXT)";
        columns = {"attribute", "value"};
        for (auto const& [key, value] : qInfo.items()) {
            rows.push_back({key, jsonValueToString(value)});
        }
    }

    LOGS(_log, LOG_LVL_DEBUG, "creating result table: " << createTable);
    if (!resultDbConn->runQuery(createTable, errObj)) {
        fail(1051, "Internal failure, failed to create result table: " + errObj.errMsg());
        return;
    }

    sql::SqlBulkInsert bulkInsert(resultDbConn.get(), _resultTableName, columns);
    for (auto const& row : rows) {
        std::vector<std::string> values;
        values.reserve(row.size());
        for (auto const& value : row) {
            values.push_back("'" + resultDbConn->escapeString(value) + "'");
        }
        if (!bulkInsert.addRow(values, errObj)) {
            fail(1051, "Internal failure, error updating result table: " + errObj.errMsg());
            return;
        }
    }
    if (!bulkInsert.flush(errObj)) {
        fail(1051, "Internal failure, error updating result table: " + errObj.errMsg());
        return;
    }

    _qState = SUCCESS;
}

QueryState UserQueryExplain::join() { return _qState; }

void UserQueryExplain::kill() {}

void UserQueryExplain::discard() {}

}  // namespace lsst::qserv::ccontrol
