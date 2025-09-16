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
#include "ccontrol/UserQueryResultDelete.h"

// System headers
#include <list>
#include <stdexcept>

// Third party headers
#include "boost/lexical_cast.hpp"
#include <nlohmann/json.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "qmeta/Exceptions.h"
#include "qmeta/QMeta.h"
#include "qmeta/MessageStore.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlErrorObject.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryResultDelete");

}

namespace lsst::qserv::ccontrol {

UserQueryResultDelete::UserQueryResultDelete(shared_ptr<UserQueryResources> const& queryResources,
                                             string const& value)
        : _value(value), _queryResources(queryResources), _messageStore(make_shared<qmeta::MessageStore>()) {}

void UserQueryResultDelete::submit() {
    LOGS(_log, LOG_LVL_DEBUG, "UserQueryResultDelete::submit: " << _value);

    // The current implementation requires exactly one numeric argument
    // which is the query ID of a query whose result needs to be deleted.
    QueryId queryId;
    try {
        queryId = boost::lexical_cast<QueryId>(_value);
    } catch (boost::bad_lexical_cast const& ex) {
        string const message = "failed to convert queryId: " + _value;
        LOGS(_log, LOG_LVL_ERROR, message);
        _messageStore->addMessage(-1, "SQL", 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // Get query info from QMeta
    qmeta::QInfo qInfo;
    try {
        qInfo = _queryResources->queryMetadata->getQueryInfo(queryId);
        LOGS(_log, LOG_LVL_DEBUG,
             "found QMeta record: czar=" << qInfo.czarId() << " queryId=" << queryId << " status="
                                         << qInfo.queryStatus() << " resultLoc=" << qInfo.resultLocation()
                                         << " msgTableName=" << qInfo.msgTableName());
    } catch (qmeta::QueryIdError const& exc) {
        string message = "No query found for ID=" + to_string(queryId);
        LOGS(_log, LOG_LVL_DEBUG, message);
        _messageStore->addErrorMessage("SYSTEM", message);
        _qState = ERROR;
        return;
    } catch (exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "error in querying QMeta: " << exc.what());
        string message = "Internal failure, error in querying QMeta: ";
        message += exc.what();
        _messageStore->addErrorMessage("SYSTEM", message);
        _qState = ERROR;
        return;
    }

    // If query has not finished yet return error
    if (qInfo.queryStatus() != qmeta::QInfo::COMPLETED) {
        string message = "Query is still executing (or FAILED)";
        LOGS(_log, LOG_LVL_DEBUG, message);
        _messageStore->addErrorMessage("SYSTEM", message);
        _qState = ERROR;
        return;
    }

    // Can only return results from mysql tables
    if (qInfo.resultLocation().compare(0, 6, "table:") != 0) {
        string message = "Cannot delete result as it is not stored in table.";
        LOGS(_log, LOG_LVL_DEBUG, message);
        _messageStore->addErrorMessage("SYSTEM", message);
        return;
    }
    string const resultTableName = qInfo.resultLocation().substr(6);

    // IMPORTANT: make a new connection each time since a state of the database service
    // is not deterministic and the SQL library available to Czar is not terribly reliable
    // (not able to properly handle disconnects).
    auto const czarConfig = cconfig::CzarConfig::instance();
    auto const resultDbConn = sql::SqlConnectionFactory::make(czarConfig->getMySqlResultConfig());

    // Make the best effort to drop both tables, log and ignore errors (if any).
    // Users do not care about this error, so don't bother sending it upstream.
    sql::SqlErrorObject sqlErrObj;
    for (auto const& tableName : {qInfo.msgTableName(), resultTableName}) {
        string const query = "DROP TABLE " + tableName;
        if (!resultDbConn->runQuery(query, sqlErrObj)) {
            LOGS(_log, LOG_LVL_ERROR,
                 "QID=" << queryId << " Failed to delete table: " << tableName
                        << ", error: " << sqlErrObj.errMsg());
        } else {
            LOGS(_log, LOG_LVL_DEBUG, "QID=" << queryId << " Deleted table: " << tableName);
        }
    }
    _qState = SUCCESS;
}

}  // namespace lsst::qserv::ccontrol
