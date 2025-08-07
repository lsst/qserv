/*
 * LSST Data Management System
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
#include "qmeta/QProgressHistory.h"

// System headers
#include <stdexcept>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qmeta/Exceptions.h"
#include "qmeta/QMetaTransaction.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"
#include "util/Issue.h"
#include "util/TimeUtils.h"

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QProgressHistory");
}

namespace lsst::qserv::qmeta {

shared_ptr<QProgressHistory> QProgressHistory::_instance;
mutex QProgressHistory::_instanceMutex;

shared_ptr<QProgressHistory> QProgressHistory::create(mysql::MySqlConfig const& connConfig) {
    lock_guard<mutex> lock(QProgressHistory::_instanceMutex);
    if (_instance == nullptr) {
        _instance = shared_ptr<QProgressHistory>(new QProgressHistory(connConfig));
    }
    return _instance;
}

shared_ptr<QProgressHistory> QProgressHistory::get() {
    lock_guard<mutex> lock(QProgressHistory::_instanceMutex);
    return _instance;
}

QProgressHistory::QProgressHistory(mysql::MySqlConfig const& connConfig)
        : _createdTimeMs(util::TimeUtils::now()), _conn(sql::SqlConnectionFactory::make(connConfig)) {}

void QProgressHistory::track(QueryId queryId) {
    string const queryIdStr = to_string(queryId);
    lock_guard<mutex> const lock(_mtx);
    if (_executing.count(queryIdStr) == 0) {
        _executing[queryIdStr].push_back(json::array({util::TimeUtils::now(), 0}));
    }
}

void QProgressHistory::untrack(QueryId queryId) { _writeToDatabase(queryId, _removeFromMemory(queryId)); }

void QProgressHistory::update(QueryId queryId, int numUnfinishedChunks) {
    string const queryIdStr = to_string(queryId);
    lock_guard<mutex> const lock(_mtx);
    auto itr = _executing.find(queryIdStr);
    if (itr == _executing.end()) {
        throw ConsistencyError(ERR_LOC, "The query ID: " + queryIdStr + " not found in the collection");
    }
    json& history = *itr;
    if (history.empty() || history.back()[1] != numUnfinishedChunks) {
        history.push_back(json::array({util::TimeUtils::now(), numUnfinishedChunks}));
    }
}

json QProgressHistory::findOne(QueryId queryId) const {
    json result = _readFromMemory(queryId);
    if (result.empty()) result = _readFromDatabase(queryId);
    return result;
}

json QProgressHistory::findMany(unsigned int lastSeconds, string const& queryStatus) const {
    if (lastSeconds == 0) {
        throw util::Issue(ERR_LOC, "The cut-off time must be specified");
    }
    uint64_t const minTimeMs = util::TimeUtils::now() - 1000 * lastSeconds;
    json result = json::array();
    if (queryStatus.empty() || queryStatus == "EXECUTING" || queryStatus == "!COMPLETED") {
        _readFromMemory(result, minTimeMs);
    }
    if (queryStatus != "EXECUTING") {
        string statusRestrictor;
        if (queryStatus.empty() || queryStatus == "!EXECUTING") {
            statusRestrictor = "`qi`.`status` NOT IN ('EXECUTING')";
        } else if (queryStatus == "!COMPLETED") {
            statusRestrictor = "`qi`.`status` NOT IN ('EXECUTING','COMPLETED')";
        } else {
            statusRestrictor = "`qi`.`status` IN ('" + queryStatus + "')";
        }
        _readFromDatabase(result, minTimeMs, statusRestrictor);
    }
    return result;
}

json QProgressHistory::_readFromMemory(QueryId queryId) const {
    string const queryIdStr = to_string(queryId);
    lock_guard<mutex> const lock(_mtx);
    auto const itr = _executing.find(queryIdStr);
    if (itr == _executing.end()) return json::object();
    return json::object({{"queryId", queryIdStr}, {"status", "EXECUTING"}, {"history", *itr}});
}

void QProgressHistory::_readFromMemory(json& result, uint64_t minTimeMs) const {
    lock_guard<mutex> const lock(_mtx);
    for (auto const& [queryIdStr, historyIn] : _executing.items()) {
        json historyOut = json::array();
        for (json const& point : historyIn) {
            if (point[0] >= minTimeMs) {
                historyOut.push_back(point);
            }
        }
        if (!historyOut.empty()) {
            result.push_back({{"queryId", queryIdStr}, {"status", "EXECUTING"}, {"history", historyOut}});
        }
    }
}

json QProgressHistory::_removeFromMemory(QueryId queryId) {
    string const queryIdStr = to_string(queryId);
    lock_guard<mutex> const lock(_mtx);
    auto const itr = _executing.find(queryIdStr);
    if (itr == _executing.end()) {
        throw ConsistencyError(ERR_LOC, "The query ID: " + queryIdStr + " not found in the collection");
    }
    json history = move(*itr);
    _executing.erase(itr);
    return history;
}

json QProgressHistory::_readFromDatabase(QueryId queryId) const {
    string const queryIdStr = to_string(queryId);
    lock_guard<mutex> const lock(_connMtx);
    string const query =
            "SELECT `qp`.`history`,`qi`.`status` FROM `QProgressHistory` `qp`"
            " INNER JOIN `QInfo` `qi` ON `qp`.`queryId`=`qi`.`queryId`"
            " WHERE `qp`.`queryId`=" +
            queryIdStr;
    sql::SqlResults results;
    sql::SqlErrorObject errObj;
    if (!_conn->runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }
    for (sql::SqlResults::iterator rowIter = results.begin(); rowIter != results.end(); ++rowIter) {
        sql::SqlResults::value_type const& row = *rowIter;
        json const history = json::parse(row[0].first);
        string const status = row[1].first;
        if (!history.is_array()) {
            throw ConsistencyError(ERR_LOC,
                                   "Invalid history for query ID: " + queryIdStr +
                                           ", expected an array, got: " + string(history.type_name()));
        }
        return json::object({{"queryId", queryIdStr}, {"status", status}, {"history", history}});
    }
    return json::object();
}

void QProgressHistory::_readFromDatabase(json& result, uint64_t minTimeMs,
                                         string const& statusRestrictor) const {
    lock_guard<mutex> const lock(_connMtx);
    string const query =
            "SELECT `qp`.`queryId`,`qp`.`history`,`qi`.`status` FROM `QProgressHistory` `qp`"
            " INNER JOIN `QInfo` `qi` ON `qp`.`queryId`=`qi`.`queryId`"
            " WHERE `qp`.`end`>=" +
            to_string(minTimeMs) + " AND " + statusRestrictor;
    sql::SqlResults results;
    sql::SqlErrorObject errObj;
    if (!_conn->runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }
    for (sql::SqlResults::iterator rowIter = results.begin(); rowIter != results.end(); ++rowIter) {
        sql::SqlResults::value_type const& row = *rowIter;
        string const queryIdStr = row[0].first;
        json const historyIn = json::parse(row[1].first);
        if (!historyIn.is_array()) {
            throw ConsistencyError(ERR_LOC,
                                   "Invalid history for query ID: " + queryIdStr +
                                           ", expected an array, got: " + string(historyIn.type_name()));
        }
        string const status = row[2].first;
        json historyOut = json::array();
        for (json const& point : historyIn) {
            if (point[0] >= minTimeMs) {
                historyOut.push_back(point);
            }
        }
        if (!historyOut.empty()) {
            result.push_back(
                    json::object({{"queryId", queryIdStr}, {"status", status}, {"history", historyOut}}));
        }
    }
}

void QProgressHistory::_writeToDatabase(QueryId queryId, json const& history) {
    if (history.empty()) return;
    string const queryIdStr = to_string(queryId);
    uint64_t const beginTimeMs = history.front()[0];
    uint64_t const endTimeMs = history.back()[0];
    unsigned int const totalPoints = history.size();
    string const historyStr = history.dump();
    lock_guard<mutex> const lock(_connMtx);
    auto trans = QMetaTransaction::create(*_conn);
    string const query =
            "INSERT INTO `QProgressHistory` (`queryId`,`history`,`begin`,`end`,`totalPoints`) "
            "VALUES (" +
            queryIdStr + ",'" + _conn->escapeString(historyStr) + "'," + to_string(beginTimeMs) + "," +
            to_string(endTimeMs) + "," + to_string(totalPoints) + ")";
    sql::SqlErrorObject errObj;
    if (!_conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR,
             "SQL query failed to store the history of query ID: "
                     << queryIdStr << ",  beginTimeMs: " << beginTimeMs << ", endTimeMs: " << endTimeMs
                     << ", totalPoints: " << totalPoints);
        throw SqlError(ERR_LOC, errObj);
    }
    trans->commit();
}

}  // namespace lsst::qserv::qmeta
