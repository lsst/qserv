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
#include "qmeta/QProgress.h"

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
#include "qmeta/QProgressData.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QProgress");
}

namespace lsst::qserv::qmeta {

QProgress::QProgress(mysql::MySqlConfig const& mysqlConf)
        : _conn(sql::SqlConnectionFactory::make(mysqlConf)) {}

void QProgress::insert(QueryId queryId, int totalChunks) const {
    lock_guard<mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    sql::SqlErrorObject errObj;
    string const query =
            "INSERT INTO `QProgress` (`queryId`,`totalChunks`,`completedChunks`,`queryBegin`,`lastUpdate`) "
            "VALUES ( " +
            to_string(queryId) + ", " + to_string(totalChunks) + ",0,NOW(),NOW())";
    LOGS(_log, LOG_LVL_TRACE, "Executing query: " << query);
    if (!_conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    trans->commit();
}

void QProgress::update(QueryId queryId, int completedChunks) const {
    lock_guard<mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    sql::SqlErrorObject errObj;
    string const query = "UPDATE `QProgress` SET `completedChunks`=" + to_string(completedChunks) +
                         ", `lastUpdate`=NOW() WHERE `queryId`=" + to_string(queryId);
    LOGS(_log, LOG_LVL_TRACE, "Executing query: " << query);
    if (!_conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }
    trans->commit();
}

QProgressData QProgress::get(QueryId queryId) const {
    lock_guard<mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    string const query =
            "SELECT "
            "`queryId`,`totalChunks`,`completedChunks`,UNIX_TIMESTAMP(`queryBegin`),UNIX_TIMESTAMP(`"
            "lastUpdate`) "
            "FROM `QProgress` WHERE `queryId`=" +
            to_string(queryId);
    LOGS(_log, LOG_LVL_TRACE, "Executing query: " << query);
    if (!_conn->runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }
    sql::SqlResults::iterator rowIter = results.begin();
    if (rowIter == results.end()) {
        LOGS(_log, LOG_LVL_ERROR, "Unknown query: " << queryId);
        throw QueryIdError(ERR_LOC, queryId);
    }
    sql::SqlResults::value_type const& row = *rowIter;
    QueryId qId = boost::lexical_cast<QueryId>(row[0].first);
    int totalChunks = boost::lexical_cast<int>(row[1].first);
    int completedChunks = boost::lexical_cast<int>(row[2].first);
    time_t const begin = boost::lexical_cast<time_t>(row[3].first);
    time_t const lastUpdate = boost::lexical_cast<time_t>(row[4].first);
    trans->commit();
    return QProgressData(qId, totalChunks, completedChunks, begin, lastUpdate);
}

void QProgress::remove(QueryId queryId) const {
    lock_guard<mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    sql::SqlErrorObject errObj;
    string const query = "DELETE FROM `QProgress` WHERE `queryId`=" + to_string(queryId);
    LOGS(_log, LOG_LVL_TRACE, "Executing query: " << query);
    if (!_conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }
    trans->commit();
}

}  // namespace lsst::qserv::qmeta
