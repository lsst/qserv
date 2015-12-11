// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
#include "ccontrol/UserQueryDropTable.h"

// System headers
#include <ctime>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "qdisp/MessageStore.h"
#include "qmeta/Exceptions.h"
#include "qmeta/QMeta.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER getLogger() {
    static LOG_LOGGER logger = LOG_GET("lsst.qserv.ccontrol.UserQueryDropTable");
    return logger;
}

}

namespace lsst {
namespace qserv {
namespace ccontrol {

// Constructor
UserQueryDropTable::UserQueryDropTable(std::shared_ptr<css::CssAccess> const& css,
                                       std::string const& dbName,
                                       std::string const& tableName,
                                       sql::SqlConnection* resultDbConn,
                                       std::string const& resultTable,
                                       std::shared_ptr<qmeta::QMeta> const& queryMetadata,
                                       qmeta::CzarId qMetaCzarId)
    : _css(css), _dbName(dbName), _tableName(tableName),
      _resultDbConn(resultDbConn), _resultTable(resultTable),
      _queryMetadata(queryMetadata), _qMetaCzarId(qMetaCzarId),
      _qState(UNKNOWN), _messageStore(std::make_shared<qdisp::MessageStore>()),
      _sessionId(0) {
}

std::string UserQueryDropTable::getError() const {
    return std::string();
}

// Attempt to kill in progress.
void UserQueryDropTable::kill() {
}

// Submit or execute the query.
void UserQueryDropTable::submit() {
    // Just mark this table in CSS with special status, watcher
    // will take care of the actual delete process

    LOGF(getLogger(), LOG_LVL_INFO, "going to drop table - %s.%s" % _dbName % _tableName);

    // create result table first, exact schema does not matter but mysql
    // needs at least one column in table DDL
    LOGF(getLogger(), LOG_LVL_DEBUG, "creating result table: %s" % _resultTable);
    std::string sql = "CREATE TABLE " + _resultTable + " (CODE INT)";
    sql::SqlErrorObject sqlErr;
    if (not _resultDbConn->runQuery(sql, sqlErr)) {
        // There is no way to return success if we cannot create result table so just stop here
        std::string message = "Failed to create result table: " + sqlErr.errMsg();
        _messageStore->addMessage(-1, 1005, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // check current table status, if not READY then fail
    try {
        auto statusMap = _css->getTableStatus(_dbName);
        LOGF(getLogger(), LOG_LVL_DEBUG, "all table status: %s" % util::printable(statusMap));
        if (statusMap.count(_tableName) != 1) {
            std::string message = "Unknown table " + _dbName + "." + _tableName;
            _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
            _qState = ERROR;
            return;
        }
        LOGF(getLogger(), LOG_LVL_DEBUG, "table status: %s" % statusMap[_tableName]);
        if (statusMap[_tableName] != css::KEY_STATUS_READY) {
            std::string message = "Unexpected status for table: " + _dbName + "."
                            + _tableName + ": " + statusMap[_tableName];
            _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
            _qState = ERROR;
            return;
        }
    } catch (css::CssError const& exc) {
        LOGF(getLogger(), LOG_LVL_ERROR, "css failure: %s" % exc.what());
        std::string message = "CSS error: " + std::string(exc.what());
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // Add this query to QMeta so that progress can be tracked,
    // QMeta needs to be updated by watcher when it finishes with the table
    // so we embed query id into CSS table status below
    qmeta::QInfo::QType qType = qmeta::QInfo::ASYNC;
    std::string user = "anonymous";    // we do not have access to that info yet
    std::string query = "DROP TABLE " + _dbName  + "." + _tableName;
    qmeta::QInfo qInfo(qType, _qMetaCzarId, user, query, "", "", "");
    qmeta::QMeta::TableNames tableNames;
    qmeta::QueryId qMetaQueryId = 0;
    try {
        qMetaQueryId = _queryMetadata->registerQuery(qInfo, tableNames);
    } catch (qmeta::Exception const& exc) {
        // not fatal, just print error message and continue
        LOGF(getLogger(), LOG_LVL_WARN, "QMeta failure (non-fatal): %s" % exc.what());
    }

    // update status to trigger watcher
    std::string newStatus = css::KEY_STATUS_DROP_PFX + std::to_string(::time(nullptr)) +
            ":qid=" + std::to_string(qMetaQueryId);
    LOGF(getLogger(), LOG_LVL_DEBUG, "new table status: %s" % newStatus);
    try {
        // TODO: it's better to do it in one atomic operation with
        // getTableStatus, but CSS API does not have this option yet
        _css->setTableStatus(_dbName, _tableName, newStatus);
        _qState = SUCCESS;
    } catch (css::NoSuchTable const& exc) {
        // Has it disappeared already?
        LOGF(getLogger(), LOG_LVL_ERROR, "table disappeared from CSS");
        std::string message = "Unknown table " + _dbName + "." + _tableName;
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
    } catch (css::CssError const& exc) {
        LOGF(getLogger(), LOG_LVL_ERROR, "CSS failure: %s" % exc.what());
        std::string message = "CSS error: " + std::string(exc.what());
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
    }

    // if failed then update status in QMeta
    if (_qState == ERROR) {
        if (qMetaQueryId) {
            try {
                _queryMetadata->completeQuery(qMetaQueryId, qmeta::QInfo::FAILED);
            } catch (qmeta::Exception const& exc) {
                // not fatal, just print error message and continue
                LOGF(getLogger(), LOG_LVL_WARN, "QMeta failure (non-fatal): %s" % exc.what());
            }
        }
    }
}

// Block until a submit()'ed query completes.
QueryState UserQueryDropTable::join() {
    // everything should be done in submit()
    return _qState;
}

// Release resources.
void UserQueryDropTable::discard() {
    // no resources
}

}}} // lsst::qserv::ccontrol
