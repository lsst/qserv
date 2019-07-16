/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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
#include "qmeta/QStatusMysql.h"

// System headers
#include <algorithm>

// Third-party headers
#include "boost/lexical_cast.hpp"
#include <boost/algorithm/string/replace.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qmeta/Exceptions.h"
#include "qmeta/QMetaTransaction.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"


using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QStatusMysql");

}

namespace lsst {
namespace qserv {
namespace qmeta {


QStatusMysql::QStatusMysql(mysql::MySqlConfig const& mysqlConf)
  : QStatus(), _conn(sql::SqlConnectionFactory::make(mysqlConf)) {
}


void QStatusMysql::queryStatsTmpRegister(QueryId queryId, int totalChunks) {
    lock_guard<mutex> sync(_dbMutex);
    QMetaTransaction trans(*_conn);
    sql::SqlErrorObject errObj;
    string query = "INSERT INTO QStatsTmp (queryId, totalChunks, completedChunks, queryBegin, lastUpdate) "
                   "VALUES ( " + to_string(queryId) + ", " + to_string(totalChunks) +
                   ", 0, NOW(), NOW())";

    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        // If this doesn't work, it is not vital to qserv functionality.
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();
}


void QStatusMysql::queryStatsTmpChunkUpdate(QueryId queryId, int completedChunks) {
    lock_guard<mutex> sync(_dbMutex);
    QMetaTransaction trans(*_conn);
    sql::SqlErrorObject errObj;
    string query = "UPDATE QStatsTmp SET completedChunks = " + to_string(completedChunks) +
                    ", lastUpdate = NOW() WHERE queryId =" + to_string(queryId);

    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        // If this doesn't work, it is not at all vital to qserv functionality.
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();
}


QStats QStatusMysql::queryStatsTmpGet(QueryId queryId) {
    lock_guard<mutex> sync(_dbMutex);
    QMetaTransaction trans(*_conn);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    string query = "SELECT queryId, totalChunks, completedChunks, "
                   "UNIX_TIMESTAMP(queryBegin), UNIX_TIMESTAMP(lastUpdate) "
                   "FROM QStatsTmp WHERE queryId= " +  to_string(queryId);
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn->runQuery(query, results, errObj)) {
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

    QueryId qId         = boost::lexical_cast<QueryId>(row[0].first);
    int totalChunks     = boost::lexical_cast<int>(row[1].first);
    int completedChunks = boost::lexical_cast<int>(row[2].first);
    time_t begin        = boost::lexical_cast<time_t>(row[3].first);
    time_t lastUpdate   = boost::lexical_cast<time_t>(row[4].first);

    trans.commit();
    return QStats(qId, totalChunks, completedChunks, begin, lastUpdate);
}


void QStatusMysql::queryStatsTmpRemove(QueryId queryId) {
    lock_guard<mutex> sync(_dbMutex);
    QMetaTransaction trans(*_conn);
    sql::SqlErrorObject errObj;
    string query = "DELETE FROM QStatsTmp WHERE queryId =" + to_string(queryId);

    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        // If this doesn't work, it is not vital to qserv functionality. It's an in memory table, and,
        // if needed, there could be a check added to remove any rows more than a couple of weeks old.
        throw SqlError(ERR_LOC, errObj);
    }

    trans.commit();
}

}}} // namespace lsst::qserv::qmeta
