// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
#include "ccontrol/UserQueryProcessList.h"

// System headers
#include <atomic>
#include <ctime>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "qdisp/MessageStore.h"
#include "qmeta/Exceptions.h"
#include "qmeta/QMetaSelect.h"
#include "query/FromList.h"
#include "query/SelectStmt.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"
#include "sql/SqlBulkInsert.h"
#include "sql/statement.h"
#include "util/IterableFormatter.h"

using namespace lsst::qserv;


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryProcessList");

std::string g_nextResultTableId(std::string const& userQueryId) {
    return "qserv_result_processlist_" + userQueryId;
}

}

namespace lsst {
namespace qserv {
namespace ccontrol {

// Constructor
UserQueryProcessList::UserQueryProcessList(std::shared_ptr<query::SelectStmt> const& statement,
        sql::SqlConnection* resultDbConn,
        std::shared_ptr<qmeta::QMetaSelect> const& qMetaSelect,
        qmeta::CzarId qMetaCzarId,
        std::string const& userQueryId)
    : _resultDbConn(resultDbConn),
      _qMetaSelect(qMetaSelect),
      _qMetaCzarId(qMetaCzarId),
      _messageStore(std::make_shared<qdisp::MessageStore>()),
      _resultTableName(::g_nextResultTableId(userQueryId)) {

    // The SQL statement should be mostly OK alredy but we need to change
    // table name, instead of INFORMATION_SCHEMA.PROCESSLIST we use special
    // Qmeta view with the name InfoSchemaProcessList
    auto stmt = statement->clone();
    for (auto& tblRef: stmt->getFromList().getTableRefList()) {
        // assume all table refs have to be replaced
        // (in practice we accept only one table in FROM
        tblRef->setDb("");
        tblRef->setTable("InfoSchemaProcessList");
    }

    auto qtempl = stmt->getQueryTemplate();
    _query = qtempl.sqlFragment();

    // we also do not want to return too many results by default
    // as QMeta can contain a lot of records, limit ourselves
    // to some reasonable number, users can override with different LIMIT
    if (! stmt->hasLimit()) {
        _query += " LIMIT 1000";
    }

    if (stmt->hasOrderBy()) {
        _orderBy = stmt->getOrderBy().sqlFragment();
    }
}

UserQueryProcessList::UserQueryProcessList(bool full,
        sql::SqlConnection* resultDbConn,
        std::shared_ptr<qmeta::QMetaSelect> const& qMetaSelect,
        qmeta::CzarId qMetaCzarId,
        std::string const& userQueryId)
    : _resultDbConn(resultDbConn),
      _qMetaSelect(qMetaSelect),
      _qMetaCzarId(qMetaCzarId),
      _messageStore(std::make_shared<qdisp::MessageStore>()),
      _resultTableName(::g_nextResultTableId(userQueryId)) {

    // use ShowProcessList view
    _query = "SELECT Id, User, Host, db, Command, Time, State, ";
    _query += full ? "Info, " : "SUBSTRING(Info FROM 1 FOR 100) Info, ";
    // These are non-standard but they need to be there because they appear in WHERE
    _query += "CzarId, Submitted, Completed, ResultLocation";
    _query += " FROM ShowProcessList";

    // only show stuff for current czar and not too old
    _query += " WHERE CzarId = " + std::to_string(qMetaCzarId) + " AND "
            "(Completed IS NULL OR Completed > NOW() - INTERVAL 3 DAY)";

    _orderBy = "ORDER BY Submitted";
}

std::string UserQueryProcessList::getError() const {
    return std::string();
}

// Attempt to kill in progress.
void UserQueryProcessList::kill() {
}

// Submit or execute the query.
void UserQueryProcessList::submit() {

    // query database
    std::unique_ptr<sql::SqlResults> results;
    try {
        results = _qMetaSelect->select(_query);
    } catch (std::exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "error in querying QMeta: " << exc.what());
        std::string message = "Internal failure, error in querying QMeta: ";
        message += exc.what();
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // get result schema
    sql::SqlErrorObject errObj;
    auto schema = results->makeSchema(errObj);
    if (errObj.isSet()) {
        LOGS(_log, LOG_LVL_ERROR, "failed to extract schema from result: " << errObj.errMsg());
        std::string message = "Internal failure, failed to extract schema from result: " + errObj.errMsg();
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // create result table, one could use formCreateTable() method
    // to build statement but it does not set NULL flag on TIMESTAMP columns
    std::string createTable = "CREATE TABLE " + _resultTableName;
    char sep = '(';
    for (auto& col: schema.columns) {
        createTable += sep;
        sep = ',';
        createTable += "`" + col.name +"`";
        createTable += " ";
        createTable += col.colType.sqlType;
        if (col.colType.sqlType == "TIMESTAMP") createTable += " NULL";
        if (col.hasDefault) createTable += " DEFAULT '" + col.defaultValue + "'";
    }
    createTable += ')';
    LOGS(_log, LOG_LVL_DEBUG, "creating result table: " << createTable);
    if (!_resultDbConn->runQuery(createTable, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "failed to create result table: " << errObj.errMsg());
        std::string message = "Internal failure, failed to create result table: " + errObj.errMsg();
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // list of column names
    std::vector<std::string> resColumns;
    for (auto& col: schema.columns) {
        resColumns.push_back(col.name);
    }

    // copy stuff over to result table
    sql::SqlBulkInsert bulkInsert(_resultDbConn, _resultTableName, resColumns);
    for (auto& row: *results) {

        std::vector<std::string> values;
        for (unsigned i = 0; i != row.size(); ++i) {
            auto ptr = row[i].first;
            auto len = row[i].second;

            if (ptr == nullptr) {
                values.push_back("NULL");
            } else if (IS_NUM(schema.columns[i].colType.mysqlType)) {
                // numeric types do not need quoting (IS_NUM is mysql macro)
                values.push_back(std::string(ptr, ptr+len));
            } else {
                // everything else should be quoted
                values.push_back("'" + _resultDbConn->escapeString(std::string(ptr, ptr+len)) + "'");
            }
        }

        if (!bulkInsert.addRow(values, errObj)) {
            LOGS(_log, LOG_LVL_ERROR, "error updating result table: " << errObj.errMsg());
            std::string message = "Internal failure, error updating result table: " + errObj.errMsg();
            _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
            _qState = ERROR;
            return;
        }
    }
    if (!bulkInsert.flush(errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "error updating result table: " << errObj.errMsg());
        std::string message = "Internal failure, error updating result table: " + errObj.errMsg();
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    _qState = SUCCESS;
}

// Block until a submit()'ed query completes.
QueryState UserQueryProcessList::join() {
    // everything should be done in submit()
    return _qState;
}

// Release resources.
void UserQueryProcessList::discard() {
    // no resources
}

}}} // lsst::qserv::ccontrol
