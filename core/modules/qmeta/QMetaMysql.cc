/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "qmeta/QMetaMysql.h"

// System headers
#include <algorithm>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "Exceptions.h"
#include "QMetaTransaction.h"
#include "sql/SqlResults.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QMetaMysql");

using lsst::qserv::qmeta::QInfo;

char const* status2string(QInfo::QStatus qStatus) {
    switch (qStatus) {
    case QInfo::EXECUTING:
        return "'EXECUTING'";
    case QInfo::COMPLETED:
        return "'COMPLETED'";
    case QInfo::FAILED:
        return "'FAILED'";
    case QInfo::ABORTED:
        return "'ABORTED'";
    default:
        return "NULL";
    }
}

QInfo::QStatus string2status(char const* statusStr) {
    if (not statusStr) {
        // we do not have enum for that (and it should not really happen),
        // just pretend we are executing
        return QInfo::EXECUTING;
    } else if (strcmp(statusStr, "EXECUTING") == 0) {
        return QInfo::EXECUTING;
    } else if (strcmp(statusStr, "COMPLETED") == 0) {
        return QInfo::COMPLETED;
    } else if (strcmp(statusStr, "FAILED") == 0) {
        return QInfo::FAILED;
    } else if (strcmp(statusStr, "ABORTED") == 0) {
        return QInfo::ABORTED;
    } else {
        // some unexpected string, say we are still executing
        return QInfo::EXECUTING;
    }
}


}

namespace lsst {
namespace qserv {
namespace qmeta {

// Constructors
QMetaMysql::QMetaMysql(mysql::MySqlConfig const& mysqlConf)
  : QMeta(), _conn(mysqlConf) {
    // Check that database is in consistent state
    _checkDb();
}

// Destructor
QMetaMysql::~QMetaMysql() {
}

// Return czar ID given czar "name".
CzarId
QMetaMysql::getCzarID(std::string const& name) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string const query = "SELECT czarId FROM QCzar WHERE czar = '" + name +"'";
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to extract czar ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // check number of results and convert to integer
    if (ids.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Result set is empty");
        return 0;
    } else if (ids.size() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one czar ID found for czar name " + name +
                               ": " + boost::lexical_cast<std::string>(ids.size()));
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Found czar ID: " << ids[0]);
        return boost::lexical_cast<CzarId>(ids[0]);
    }
}

// Register new czar, return czar ID.
CzarId
QMetaMysql::registerCzar(std::string const& name) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // check if czar is already defined
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT czarId FROM QCzar WHERE czar = '" + name +"'";
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to extract czar ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    // check number of results and convert to integer
    CzarId czarId = 0;
    if (ids.size() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one czar ID found for czar name " + name +
                               ": " + boost::lexical_cast<std::string>(ids.size()));
    } else if (ids.empty()) {

        // no such czar, make new one
        LOGS(_log, LOG_LVL_DEBUG, "Create new czar with name: " << name);
        results.freeResults();
        query = "INSERT INTO QCzar (czar, active) VALUES ('" + name +"', b'1')";
        LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
        if (not _conn.runQuery(query, results, errObj)) {
            LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
            throw SqlError(ERR_LOC, errObj);
        }
        auto newId = _conn.getInsertId();
        LOGS(_log, LOG_LVL_DEBUG, "Created czar ID: " << newId);
        czarId = static_cast<CzarId>(newId);

    } else {

        // its exists, get its ID
        czarId = boost::lexical_cast<CzarId>(ids[0]);
        LOGS(_log, LOG_LVL_DEBUG, "Use existing czar with ID: " << czarId);

        // make sure it's active
        results.freeResults();
        query = "UPDATE QCzar SET active = b'1' WHERE czarId = " + ids[0];
        LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
        if (not _conn.runQuery(query, results, errObj)) {
            LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
            throw SqlError(ERR_LOC, errObj);
        }
    }

    trans.commit();

    return czarId;
}

// Mark specified czar as active or inactive.
void
QMetaMysql::setCzarActive(CzarId czarId, bool active) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string const query = "UPDATE QCzar SET active = b'" +
            std::string(active ? "1" : "0") +
            "' WHERE czarId = " + boost::lexical_cast<std::string>(czarId);
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // check number of rows updated, expect exactly one
    if (results.getAffectedRows() == 0) {
        throw CzarIdError(ERR_LOC, czarId);
    } else if (results.getAffectedRows() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one row updated for czar ID " +
                               boost::lexical_cast<std::string>(czarId) + ": " +
                               boost::lexical_cast<std::string>(results.getAffectedRows()));
    }

    trans.commit();
}

// Register new query.
QueryId
QMetaMysql::registerQuery(QInfo const& qInfo,
                          TableNames const& tables) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // build query
    sql::SqlErrorObject errObj;
    std::string const qType(qInfo.queryType() == QInfo::SYNC ? "'SYNC'" : "'ASYNC'");
    std::string const user = "'" + _conn.escapeString(qInfo.user()) + "'";
    std::string const queryText = "'" + _conn.escapeString(qInfo.queryText()) + "'";
    std::string const queryTemplate = "'" + _conn.escapeString(qInfo.queryTemplate()) + "'";
    std::string qMerge = "NULL";
    if (not qInfo.mergeQuery().empty()) {
        qMerge = "'" + _conn.escapeString(qInfo.mergeQuery()) + "'";
    }
    std::string proxyOrderBy = "NULL";
    if (not qInfo.proxyOrderBy().empty()) {
        proxyOrderBy = "'" + _conn.escapeString(qInfo.proxyOrderBy()) + "'";
    }
    std::string query = "INSERT INTO QInfo (qType, czarId, user, query, qTemplate, qMerge, "
                        "proxyOrderBy, status) VALUES (";
    query += qType;
    query += ", ";
    query += boost::lexical_cast<std::string>(qInfo.czarId());
    query += ", ";
    query += user;
    query += ", ";
    query += queryText;
    query += ", ";
    query += queryTemplate;
    query += ", ";
    query += qMerge;
    query += ", ";
    query += proxyOrderBy;
    query += ", 'EXECUTING')";

    // run query
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // return value of the auto-increment column
    QueryId queryId = static_cast<QueryId>(_conn.getInsertId());
    std::string qIdStr = QueryIdHelper::makeIdStr(queryId);

    // register all tables, first remove all duplicates from a list
    TableNames uniqueTables = tables;
    auto end = std::unique(uniqueTables.begin(), uniqueTables.end());
    for (auto itr = uniqueTables.begin(); itr != end; ++ itr) {
        query = "INSERT INTO QTable (queryId, dbName, tblName) VALUES (";
        query += boost::lexical_cast<std::string>(queryId);
        query += ", '";
        query += _conn.escapeString(itr->first);
        query += "', '";
        query += _conn.escapeString(itr->second);
        query += "')";

        LOGS(_log, LOG_LVL_DEBUG, qIdStr << " Executing query: " << query);
        if (not _conn.runQuery(query, errObj)) {
            LOGS(_log, LOG_LVL_ERROR, qIdStr << " SQL query failed: " << query);
            throw SqlError(ERR_LOC, errObj);
        }
    }

    trans.commit();
    LOGS(_log, LOG_LVL_DEBUG, qIdStr << " assigned to UserQuery:" << qInfo.queryText());

    return queryId;
}

// Add list of chunks to query.
void
QMetaMysql::addChunks(QueryId queryId, std::vector<int> const& chunks) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // register all tables
    sql::SqlErrorObject errObj;
    for (std::vector<int>::const_iterator itr = chunks.begin(); itr != chunks.end(); ++ itr) {
        std::string query = "INSERT INTO QWorker (queryId, chunk) VALUES (";
        query += boost::lexical_cast<std::string>(queryId);
        query += ", ";
        query += boost::lexical_cast<std::string>(*itr);
        query += ")";

        LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
        if (not _conn.runQuery(query, errObj)) {
            LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
            throw SqlError(ERR_LOC, errObj);
        }
    }

    trans.commit();
}

// Assign or re-assign chunk to a worker.
void
QMetaMysql::assignChunk(QueryId queryId,
                        int chunk,
                        std::string const& xrdEndpoint) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // find and update chunk info
    sql::SqlErrorObject errObj;
    std::string query = "UPDATE QWorker SET wxrd = '";
    query += _conn.escapeString(xrdEndpoint);
    query += "', submitted = NOW() WHERE queryId = ";
    query += boost::lexical_cast<std::string>(queryId);
    query += " AND chunk = ";
    query += boost::lexical_cast<std::string>(chunk);

    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // check number of rows updated, expect exactly one
    if (results.getAffectedRows() == 0) {
        throw ChunkIdError(ERR_LOC, queryId, chunk);
    } else if (results.getAffectedRows() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one row updated for query/chunk ID " +
                               boost::lexical_cast<std::string>(queryId) + "/" +
                               boost::lexical_cast<std::string>(chunk) + ": " +
                               boost::lexical_cast<std::string>(results.getAffectedRows()));
    }

    trans.commit();
}

// Mark chunk as completed.
void
QMetaMysql::finishChunk(QueryId queryId, int chunk) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // find and update query info
    sql::SqlErrorObject errObj;
    std::string query = "UPDATE QWorker SET completed = NOW() WHERE queryId = ";
    query += boost::lexical_cast<std::string>(queryId);
    query += " AND chunk = ";
    query += boost::lexical_cast<std::string>(chunk);

    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // check number of rows updated, expect exactly one
    if (results.getAffectedRows() == 0) {
        throw ChunkIdError(ERR_LOC, queryId, chunk);
    } else if (results.getAffectedRows() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one row updated for query/chunk ID " +
                               boost::lexical_cast<std::string>(queryId) + "/" +
                               boost::lexical_cast<std::string>(chunk) + ": " +
                               boost::lexical_cast<std::string>(results.getAffectedRows()));
    }

    trans.commit();
}

// Mark query as completed or failed.
void
QMetaMysql::completeQuery(QueryId queryId, QInfo::QStatus qStatus) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // find and update query info
    std::string query = "UPDATE QInfo SET completed = NOW(), status = ";
    query += ::status2string(qStatus);
    query += " WHERE queryId = ";
    query += boost::lexical_cast<std::string>(queryId);

    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // check number of rows updated, expect exactly one
    if (results.getAffectedRows() == 0) {
        throw QueryIdError(ERR_LOC, queryId);
    } else if (results.getAffectedRows() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one row updated for query ID " +
                               boost::lexical_cast<std::string>(queryId) + ": " +
                               boost::lexical_cast<std::string>(results.getAffectedRows()));
    }

    trans.commit();
}

// Mark query as finished and returned to client.
void
QMetaMysql::finishQuery(QueryId queryId) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // find and update chunk info
    std::string query = "UPDATE QInfo SET returned = NOW() WHERE queryId = ";
    query += boost::lexical_cast<std::string>(queryId);

    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // check number of rows updated, expect exactly one
    if (results.getAffectedRows() == 0) {
        throw QueryIdError(ERR_LOC, queryId);
    } else if (results.getAffectedRows() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one row updated for query ID " +
                               boost::lexical_cast<std::string>(queryId) + ": " +
                               boost::lexical_cast<std::string>(results.getAffectedRows()));
    }

    trans.commit();
}

// Generic interface for finding queries.
std::vector<QueryId>
QMetaMysql::findQueries(CzarId czarId,
                        QInfo::QType qType,
                        std::string const& user,
                        std::vector<QInfo::QStatus> const& status,
                        int completed,
                        int returned) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    std::vector<QueryId> result;

    QMetaTransaction trans(_conn);

    // all conditions for query
    std::vector<std::string> cond;
    if (czarId != 0) {
        cond.push_back("czarId = " + boost::lexical_cast<std::string>(czarId));
    }
    if (qType != QInfo::ANY) {
        std::string const qTypeStr(qType == QInfo::SYNC ? "SYNC" : "ASYNC");
        cond.push_back("qType = '" + qTypeStr + "'");
    }
    if (not user.empty()) {
        cond.push_back("user = '" + _conn.escapeString(user) + "'");
    }
    if (not status.empty()) {
        std::string condStr = "status IN (";
        for (auto itr = status.begin(), end = status.end(); itr != end; ++ itr) {
            if (itr != status.begin()) condStr += ", ";
            condStr += status2string(*itr);
        }
        condStr += ")";
        cond.push_back(condStr);
    }
    if (completed >= 0) {
        cond.push_back(completed ? "completed IS NOT NULL" : "completed IS NULL");
    }
    if (returned >= 0) {
        cond.push_back(returned ? "returned IS NOT NULL" : "returned IS NULL");
    }

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT queryId FROM QInfo";
    for (auto itr = cond.begin(), end = cond.end(); itr != end; ++ itr) {
        if (itr == cond.begin()) {
            query += " WHERE ";
        } else {
            query += " AND ";
        }
        query += *itr;
    }
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to extract query ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // convert strings to numbers
    result.reserve(ids.size());
    std::transform(ids.begin(), ids.end(), std::back_inserter(result),
                   boost::lexical_cast<QueryId, std::string>);

    return result;
}

// Find all pending queries.
std::vector<QueryId>
QMetaMysql::getPendingQueries(CzarId czarId) {

    std::vector<QueryId> result;

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT queryId FROM QInfo WHERE czarId = ";
    query += boost::lexical_cast<std::string>(czarId);
    query += " AND returned IS NULL";
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to extract query ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // convert strings to numbers
    result.reserve(ids.size());
    std::transform(ids.begin(), ids.end(), std::back_inserter(result),
                   boost::lexical_cast<QueryId, std::string>);

    return result;
}

// Get full query information.
QInfo
QMetaMysql::getQueryInfo(QueryId queryId) {

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT qType, czarId, user, query, qTemplate, qMerge, proxyOrderBy, status,"
            " UNIX_TIMESTAMP(submitted), UNIX_TIMESTAMP(completed), UNIX_TIMESTAMP(returned)"
            " FROM QInfo WHERE queryId = ";
    query += boost::lexical_cast<std::string>(queryId);
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    sql::SqlResults::iterator rowIter = results.begin();
    if (rowIter == results.end()) {
        // no records found
        throw QueryIdError(ERR_LOC, queryId);
    }

    // make sure that iterator does not move until we are done with row
    sql::SqlResults::value_type const& row = *rowIter;

    // convert query result into QInfo instance
    QInfo::QType qType = QInfo::SYNC;
    if (strcmp(row[0].first, "ASYNC") == 0) qType = QInfo::ASYNC;
    CzarId czarId = boost::lexical_cast<CzarId>(row[1].first);
    std::string user(row[2].first);
    std::string rQuery(row[3].first);
    std::string qTemplate(row[4].first);
    std::string qMerge(row[5].first ? row[5].first : "");
    std::string proxyOrderBy(row[6].first ? row[6].first : "");
    QInfo::QStatus qStatus = ::string2status(row[7].first);
    std::time_t submitted(row[8].first ? boost::lexical_cast<std::time_t>(row[8].first) : std::time_t(0));
    std::time_t completed(row[9].first ? boost::lexical_cast<std::time_t>(row[9].first) : std::time_t(0));
    std::time_t returned(row[10].first ? boost::lexical_cast<std::time_t>(row[10].first) : std::time_t(0));

    if (++ rowIter != results.end()) {
        // something else found
        throw ConsistencyError(ERR_LOC, "More than one row returned for query ID " +
                               boost::lexical_cast<std::string>(queryId));
    }

    trans.commit();

    return QInfo(qType, czarId, user, rQuery, qTemplate, qMerge, proxyOrderBy, qStatus,
                 submitted, completed, returned);
}

// Get queries which use specified database.
std::vector<QueryId>
QMetaMysql::getQueriesForDb(std::string const& dbName) {

    std::vector<QueryId> result;

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT QInfo.queryId FROM QInfo NATURAL JOIN QTable WHERE QTable.dbName = '";
    query += _conn.escapeString(dbName);
    query += "' AND QInfo.completed IS NULL";
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to extract query ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // convert strings to numbers
    result.reserve(ids.size());
    std::transform(ids.begin(), ids.end(), std::back_inserter(result),
                   boost::lexical_cast<QueryId, std::string>);

    return result;
}

// Get queries which use specified table.
std::vector<QueryId>
QMetaMysql::getQueriesForTable(std::string const& dbName,
                               std::string const& tableName) {

    std::vector<QueryId> result;

    std::lock_guard<std::mutex> sync(_dbMutex);

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT QInfo.queryId FROM QInfo NATURAL JOIN QTable WHERE QTable.dbName = '";
    query += _conn.escapeString(dbName);
    query += "' AND QTable.tblName = '";
    query += _conn.escapeString(tableName);
    query += "' AND QInfo.completed IS NULL";
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to extract query ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // convert strings to numbers
    result.reserve(ids.size());
    std::transform(ids.begin(), ids.end(), std::back_inserter(result),
                   boost::lexical_cast<QueryId, std::string>);

    return result;
}

// Check that all necessary tables exist or create them
void
QMetaMysql::_checkDb() {

    // this is only called from constructor, no locking is needed here

    std::vector<std::string> tables;
    sql::SqlErrorObject errObj;
    if (not _conn.listTables(tables, errObj)) {
        // likely failed to connect to server or database is missing
        LOGS(_log, LOG_LVL_ERROR, "Failed to connect to query metadata database, check that "
             "server is running and database " << _conn.getActiveDbName() << " exists");
        throw SqlError(ERR_LOC, errObj);
    }

    // check that all tables are there
    char const* requiredTables[] = {"QCzar", "QInfo", "QTable", "QWorker"};
    int const nTables = sizeof requiredTables / sizeof requiredTables[0];
    for (int i = 0; i != nTables; ++ i) {
        char const* const table = requiredTables[i];
        if (std::find(tables.begin(), tables.end(), table) == tables.end()) {
            LOGS(_log, LOG_LVL_ERROR, "Query metadata table is missing: " << table);
            throw MissingTableError(ERR_LOC, table);
        }
    }
}

}}} // namespace lsst::qserv::qmeta
