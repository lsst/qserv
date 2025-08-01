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
#include "qmeta/QProgressPlot.h"

// System headers
#include <stdexcept>

// Third-party headers
#include "boost/lexical_cast.hpp"

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

namespace lsst::qserv::qmeta {

shared_ptr<QProgressPlot> QProgressPlot::_instance;
mutex QProgressPlot::_instanceMutex;

shared_ptr<QProgressPlot> QProgressPlot::create(mysql::MySqlConfig const& mysqlConf) {
    lock_guard<mutex> lock(QProgressPlot::_instanceMutex);
    if (_instance == nullptr) {
        _instance = shared_ptr<QProgressPlot>(new QProgressPlot(mysqlConf));
    }
    return _instance;
}

shared_ptr<QProgressPlot> QProgressPlot::get() {
    lock_guard<mutex> lock(QProgressPlot::_instanceMutex);
    return _instance;
}

QProgressPlot::QProgressPlot(mysql::MySqlConfig const& mysqlConf)
        : _createdTimeMs(util::TimeUtils::now()), _conn(sql::SqlConnectionFactory::make(mysqlConf)) {}

void QProgressPlot::track(QueryId qid) {
    lock_guard<mutex> const lock(_mtx);
    if (_executing.count(qid) == 0) {
        _executing[qid].emplace_back(util::TimeUtils::now(), 0);
    }
}

void QProgressPlot::untrack(QueryId qid) {
    list<HistoryPoint> history;
    {
        lock_guard<mutex> const lock(_mtx);
        auto const itr = _executing.find(qid);
        if (itr == _executing.end()) {
            throw ConsistencyError(ERR_LOC, "query ID " + to_string(qid) + " not found in the collection");
        }
        history = move(itr->second);
        _executing.erase(itr);
    }
    _archive(qid, history);
}

void QProgressPlot::update(QueryId qid, int numUnfinishedJobs) {
    lock_guard<mutex> const lock(_mtx);
    auto itr = _executing.find(qid);
    if (itr == _executing.end()) {
        throw ConsistencyError(ERR_LOC, "The query ID " + to_string(qid) + " not found in the collection");
    }
    list<HistoryPoint>& history = itr->second;
    if (history.empty() || history.back().numJobs != numUnfinishedJobs) {
        history.emplace_back(util::TimeUtils::now(), numUnfinishedJobs);
    }
}

json QProgressPlot::get(QueryId qid) const {
    // Find the query in the in-memory collection.
    {
        lock_guard<mutex> const lock(_mtx);
        auto itr = _executing.find(qid);
        if (itr != _executing.end()) {
            list<HistoryPoint> const& history = itr->second;
            json result = json::object();
            result["queryId"] = qid;
            result["status"] = "EXECUTING";
            result["history"] = json::array();
            json& historyArray = result["history"];
            for (HistoryPoint const& point : history) {
                historyArray.push_back({point.timeMs, point.numJobs});
            }
            return result;
        }
    }

    // Otherwise, look for the query in the database.
    json result = json::object();
    return result;
}

json QProgressPlot::get(unsigned int lastSeconds, bool includeFinished) const {
    if (lastSeconds == 0 && includeFinished) {
        throw util::Issue(ERR_LOC, "The cut-off time must be specified if including finished queries");
    }
    uint64_t const minTimeMs = util::TimeUtils::now() - 1000 * lastSeconds;
    json result = json::array();

    // Collect the histories of all executing queries.
    {
        lock_guard<mutex> const lock(_mtx);
        for (auto const& [qid, history] : _executing) {
            json queryResult = json::object();
            queryResult["queryId"] = qid;
            queryResult["status"] = "EXECUTING";
            queryResult["history"] = json::array();
            json& historyArray = queryResult["history"];
            for (HistoryPoint const& point : history) {
                if (point.timeMs >= minTimeMs) historyArray.push_back({point.timeMs, point.numJobs});
            }
            if (!historyArray.empty()) result.push_back(queryResult);
        }
    }

    // Collect the histories of all archived queries.
    if (includeFinished) {
        lock_guard<mutex> const lock(_connMtx);
    }
    return result;
}

void QProgressPlot::_archive(QueryId qid, list<HistoryPoint> const& history) {
    if (history.empty()) return;
    lock_guard<mutex> const lock(_connMtx);
    // TBC...
}

}  // namespace lsst::qserv::qmeta
