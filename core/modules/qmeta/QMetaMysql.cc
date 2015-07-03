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

// logger instance for this module
LOG_LOGGER _logger = LOG_GET("lsst.qserv.qmeta.QMetaMysql");

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

/**
 *  @brief Return czar ID given czar "name".
 *
 *  Negative number is returned if czar does not exist.
 *
 *  @param name:  Czar name, arbitrary string.
 *  @return: Car ID, negative if czar does not exist.
 */
int
QMetaMysql::getCzarID(std::string const& name) {

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string const query = "SELECT cid from QCzar where czar = '" + name +"'";
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "Failed to extract czar ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // check number of results and convert to integer
    if (ids.empty()) {
        return -1;
    } else if (ids.size() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one czar ID found for czar name " + name +
                               ": " + boost::lexical_cast<std::string>(ids.size()));
    } else {
        return boost::lexical_cast<int>(ids[0]);
    }
}

// Register new czar, return czar ID.
int
QMetaMysql::registerCzar(std::string const& name) {

    QMetaTransaction trans(_conn);

    // check if czar is already defined
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT cid from QCzar where czar = '" + name +"'";
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "Failed to extract czar ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    // check number of results and convert to integer
    int cid = -1;
    if (ids.size() > 1) {
        throw ConsistencyError(ERR_LOC, "More than one czar ID found for czar name " + name +
                               ": " + boost::lexical_cast<std::string>(ids.size()));
    } else if (ids.empty()) {

        // no such czar, make new one
        LOGF(_logger, LOG_LVL_DEBUG, "Create new czar with name: %1%" % name);
        results.freeResults();
        query = "INSERT INTO QCzar (czar, active) VALUES ('" + name +"', b'1')";
        LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
        if (not _conn.runQuery(query, results, errObj)) {
            LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
            throw SqlError(ERR_LOC, errObj);
        }
        cid = _conn.getInsertId();

    } else {

        // its exists, get its ID
        cid = boost::lexical_cast<int>(ids[0]);
        LOGF(_logger, LOG_LVL_DEBUG, "Use existing czar with ID: %1%" % cid);

        // make sure it's active
        results.freeResults();
        query = "UPDATE QCzar SET active = b'1' WHERE cid = " + ids[0];
        LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
        if (not _conn.runQuery(query, results, errObj)) {
            LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
            throw SqlError(ERR_LOC, errObj);
        }
    }

    trans.commit();

    return cid;
}

// Mark specified czar as active or inactive.
void
QMetaMysql::setCzarActive(int czarId, bool active) {

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string const query = "UPDATE QCzar SET active = b'" +
            std::string(active ? "1" : "0") +
            "' WHERE cid = " + boost::lexical_cast<std::string>(czarId);
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
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
int
QMetaMysql::registerQuery(QInfo const& qInfo,
                          TableNames const& tables) {

    QMetaTransaction trans(_conn);

    // build query
    sql::SqlErrorObject errObj;
    std::string const qType(qInfo.queryType() == QInfo::INTERACTIVE ? "INTER" : "LONG");
    std::string qResult = "NULL";
    if (not qInfo.resultQuery().empty()) {
        qResult = "'" + qInfo.resultQuery() + "'";
    }
    std::string query = "INSERT INTO QInfo (qtype, cid, user, query, qtemplate, qresult) VALUES ('";
    query += qType;
    query += "', ";
    query += boost::lexical_cast<std::string>(qInfo.czarId());
    query += ", '";
    query += _conn.escapeString(qInfo.user());
    query += "', '";
    query += _conn.escapeString(qInfo.queryText());
    query += "', '";
    query += _conn.escapeString(qInfo.queryTemplate());
    query += "', ";
    query += qResult;
    query += ")";

    // run query
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
        throw SqlError(ERR_LOC, errObj);
    }

    // return value of the auto-increment column
    unsigned long long qid = _conn.getInsertId();

    // register all tables
    for (TableNames::const_iterator itr = tables.begin(); itr != tables.end(); ++ itr) {
        query = "INSERT INTO QTable (qid, dbname, tblname) VALUES (";
        query += boost::lexical_cast<std::string>(qid);
        query += ", '";
        query += _conn.escapeString(itr->first);
        query += "', '";
        query += _conn.escapeString(itr->second);
        query += "')";

        LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
        if (not _conn.runQuery(query, errObj)) {
            LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
            throw SqlError(ERR_LOC, errObj);
        }
    }

    trans.commit();

    return qid;
}

// Add list of chunks to query.
void
QMetaMysql::addChunks(int queryId, std::vector<int> const& chunks) {

    QMetaTransaction trans(_conn);

    // register all tables
    sql::SqlErrorObject errObj;
    for (std::vector<int>::const_iterator itr = chunks.begin(); itr != chunks.end(); ++ itr) {
        std::string query = "INSERT INTO QWorker (qid, chunk) VALUES (";
        query += boost::lexical_cast<std::string>(queryId);
        query += ", ";
        query += boost::lexical_cast<std::string>(*itr);
        query += ")";

        LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
        if (not _conn.runQuery(query, errObj)) {
            LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
            throw SqlError(ERR_LOC, errObj);
        }
    }

    trans.commit();
}

// Assign or re-assign chunk to a worker.
void
QMetaMysql::assignChunk(int queryId,
                        int chunk,
                        std::string const& xrdEndpoint) {
    QMetaTransaction trans(_conn);

    // find and update chunk info
    sql::SqlErrorObject errObj;
    std::string query = "UPDATE QWorker SET wxrd = '";
    query += _conn.escapeString(xrdEndpoint);
    query += "', submitted = NOW() WHERE qid = ";
    query += boost::lexical_cast<std::string>(queryId);
    query += " AND chunk = ";
    query += boost::lexical_cast<std::string>(chunk);

    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
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
QMetaMysql::finishChunk(int queryId, int chunk) {

    QMetaTransaction trans(_conn);

    // find and update query info
    sql::SqlErrorObject errObj;
    std::string query = "UPDATE QWorker SET completed = NOW() WHERE qid = ";
    query += boost::lexical_cast<std::string>(queryId);
    query += " AND chunk = ";
    query += boost::lexical_cast<std::string>(chunk);

    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
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

// Mark query as collected.
void
QMetaMysql::markQueryCollected(int queryId) {

    QMetaTransaction trans(_conn);

    // find and update query info
    std::string query = "UPDATE QInfo SET collected = NOW() WHERE qid = ";
    query += boost::lexical_cast<std::string>(queryId);

    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
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

// Mark query as completed.
void
QMetaMysql::finishQuery(int queryId) {

    QMetaTransaction trans(_conn);

    // find and update chunk info
    std::string query = "UPDATE QInfo SET completed = NOW() WHERE qid = ";
    query += boost::lexical_cast<std::string>(queryId);

    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
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

// Find all queries currently executing.
std::vector<int>
QMetaMysql::getExecutingQueries(int czarId) {

    std::vector<int> result;

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT qid from QInfo where cid = ";
    query += boost::lexical_cast<std::string>(czarId);
    query += " AND completed IS NULL";
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "Failed to extract query ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // convert strings to numbers
    result.reserve(ids.size());
    std::transform(ids.begin(), ids.end(), std::back_inserter(result),
                   boost::lexical_cast<int, std::string>);

    return result;
}

// Get full query information.
QInfo
QMetaMysql::getQueryInfo(int queryId) {

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT qtype, cid, user, query, qtemplate, qresult,"
            " UNIX_TIMESTAMP(submitted), UNIX_TIMESTAMP(collected), UNIX_TIMESTAMP(completed)"
            " FROM QInfo WHERE qid = ";
    query += boost::lexical_cast<std::string>(queryId);
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
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
    QInfo::QType qType = QInfo::INTERACTIVE;
    if (strcmp(row[0].first, "LONG") == 0) qType = QInfo::LONG_RUNNING;
    int cid = boost::lexical_cast<int>(row[1].first);
    std::string user(row[2].first);
    std::string rQuery(row[3].first);
    std::string qtemplate(row[4].first);
    std::string qresult(row[5].first ? row[5].first : "");
    std::time_t submitted(row[6].first ? boost::lexical_cast<std::time_t>(row[6].first) : std::time_t(0));
    std::time_t collected(row[7].first ? boost::lexical_cast<std::time_t>(row[7].first) : std::time_t(0));
    std::time_t completed(row[8].first ? boost::lexical_cast<std::time_t>(row[8].first) : std::time_t(0));

    if (++ rowIter != results.end()) {
        // something else found
        throw ConsistencyError(ERR_LOC, "More than one row returned for query ID " +
                               boost::lexical_cast<std::string>(queryId));
    }

    trans.commit();

    return QInfo(qType, cid, user, rQuery, qtemplate, qresult, submitted, collected, completed);
}

// Get queries which use specified table.
std::vector<int>
QMetaMysql::getQueriesForTable(std::string const& dbName,
                               std::string const& tableName) {

    std::vector<int> result;

    QMetaTransaction trans(_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    std::string query = "SELECT QInfo.qid from QInfo NATURAL JOIN QTable WHERE QTable.dbname = '";
    query += _conn.escapeString(dbName);
    query += "' AND QTable.tblname = '";
    query += _conn.escapeString(tableName);
    query += "' and QInfo.completed IS NULL";
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
        throw SqlError(ERR_LOC, errObj);
    }

    // get results of the query
    std::vector<std::string> ids;
    if (not results.extractFirstColumn(ids, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "Failed to extract query ID from query result");
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();

    // convert strings to numbers
    result.reserve(ids.size());
    std::transform(ids.begin(), ids.end(), std::back_inserter(result),
                   boost::lexical_cast<int, std::string>);

    return result;
}

// Check that all necessary tables exist or create them
void
QMetaMysql::_checkDb() {

    std::vector<std::string> tables;
    sql::SqlErrorObject errObj;
    if (not _conn.listTables(tables, errObj)) {
        // likely failed to connect to server or database is missing
        LOGF(_logger, LOG_LVL_ERROR, "Failed to connect to query metadata database, check that "
             "server is running and database %1% exists" % _conn.getActiveDbName());
        throw SqlError(ERR_LOC, errObj);
    }

    // check that all tables are there
    char const* requiredTables[] = {"QCzar", "QInfo", "QTable", "QWorker"};
    int const nTables = sizeof requiredTables / sizeof requiredTables[0];
    for (int i = 0; i != nTables; ++ i) {
        char const* const table = requiredTables[i];
        if (std::find(tables.begin(), tables.end(), table) == tables.end()) {
            LOGF(_logger, LOG_LVL_ERROR, "Query metadata table is missing: %1%" % table);
            throw MissingTableError(ERR_LOC, table);
        }
    }
}

}}} // namespace lsst::qserv::qmeta
