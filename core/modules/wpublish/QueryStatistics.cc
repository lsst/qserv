// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#include "QueryStatistics.h"

#include "lsst/log/Log.h"

// Qserv headers


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.QueryStatistics");
}

namespace lsst {
namespace qserv {
namespace wpublish {

void Queries::addTask(wbase::Task::Ptr const& task) {
    auto qid = task->getQueryId();
    auto ent = std::pair<QueryId, QueryStatistics::Ptr>(qid, nullptr);

    std::unique_lock<std::mutex> guardStats(_qStatsMtx);
    auto res = _queryStats.insert(ent);
    if (res.second) {
        res.first->second = std::make_shared<QueryStatistics>(qid);
    }
    QueryStatistics::Ptr stats = res.first->second;
    guardStats.unlock();
    stats->addTask(task);
}


void Queries::queuedTask(wbase::Task::Ptr const& task) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    task->queued(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_mx);
        stats->_touched = now;
    }
}


void Queries::startedTask(wbase::Task::Ptr const& task) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    task->started(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_mx);
        stats->_touched = now;
        stats->_tasksRunning += 1;
    }
}


void Queries::finishedTask(wbase::Task::Ptr const& task) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    double taskDuration = (double)(task->finished(now).count());
    taskDuration /= 1000.0;

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_mx);
        stats->_touched = now;
        stats->_tasksRunning -= 1;
        stats->_tasksCompleted += 1;
        stats->_totalCompletionTime += taskDuration;
    }

    // &&& add statistics to SchedulerChunkStatistics.
}


QueryStatistics::Ptr Queries::getStats(QueryId const& qId) const {
    std::lock_guard<std::mutex> g(_qStatsMtx);
    auto iter = _queryStats.find(qId);
    if (iter != _queryStats.end()) {
        return iter->second;
    }
    return nullptr;
}


void QueryStatistics::addTask(wbase::Task::Ptr const& task) {
    std::lock_guard<std::mutex> guard(_mx);
    std::pair<int, wbase::Task::Ptr> ent(task->getJobId(), task);
    _taskMap.insert(ent);
}


}}} // namespace lsst:qserv:wsched
