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
    taskDuration /= 60000.0; // convert to minutes.

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_mx);
        stats->_touched = now;
        stats->_tasksRunning -= 1;
        stats->_tasksCompleted += 1;
        stats->_totalCompletionTime += taskDuration;
    }

    _finishedTaskForChunk(task, taskDuration);
}


/// &&& doc
void Queries::_finishedTaskForChunk(wbase::Task::Ptr const& task, double taskDuration) {
    std::unique_lock<std::mutex> ul(_chunkMtx);
    std::pair<int, ChunkTaskStatistics::Ptr> ele(task->getChunkId(), nullptr);
    auto res = _chunkStats.insert(ele);
    if (res.second) {
        res.first->second = std::make_shared<ChunkTaskStatistics>(task->getChunkId());
    }
    ul.unlock();
    auto iter = res.first->second;
    proto::ScanInfo& scanInfo = task->getScanInfo();
    std::string tblName = "";
    if (!scanInfo.infoTables.empty()) {
        proto::ScanTableInfo &sti = scanInfo.infoTables.at(0);
        tblName = ChunkTableStatistics::makeTableName(sti.db, sti.table);
    }
    ChunkTableStatistics::Ptr tableStats = iter->add(tblName, taskDuration);
}


/// &&& doc
QueryStatistics::Ptr Queries::getStats(QueryId const& qId) const {
    std::lock_guard<std::mutex> g(_qStatsMtx);
    auto iter = _queryStats.find(qId);
    if (iter != _queryStats.end()) {
        return iter->second;
    }
    return nullptr;
}


/// &&& doc
void QueryStatistics::addTask(wbase::Task::Ptr const& task) {
    std::lock_guard<std::mutex> guard(_mx);
    std::pair<int, wbase::Task::Ptr> ent(task->getJobId(), task);
    _taskMap.insert(ent);
}


/// &&& doc
ChunkTableStatistics::Ptr ChunkTaskStatistics::add(std::string const& scanTableName, double duration) {
    std::pair<std::string, ChunkTableStatistics::Ptr> ele(scanTableName, nullptr);
    std::unique_lock<std::mutex> ul(_tStatsMtx);
    auto res = _tableStats.insert(ele);
    auto iter = res.first;
    if (res.second) {
        iter->second = std::make_shared<ChunkTableStatistics>(scanTableName);
    }
    ul.unlock();
    iter->second->addTaskFinished(duration);
    return iter->second;
}


/// &&& doc
ChunkTableStatistics::Ptr ChunkTaskStatistics::getStats(std::string const& scanTableName) const {
    std::lock_guard<std::mutex> g(_tStatsMtx);
    auto iter = _tableStats.find(scanTableName);
    if (iter != _tableStats.end()) {
        return iter->second;
    }
    return nullptr;
}


/// &&& doc
void ChunkTableStatistics::addTaskFinished(double duration) {
    std::lock_guard<std::mutex> g(_mtx);
    ++_tasksCompleted;
    if (_tasksCompleted > 1) {
        _avgCompletionTime = (_avgCompletionTime*_weightAvg + duration*_weightDur)/_weightSum;
    } else {
        _avgCompletionTime = duration;
    }
}


}}} // namespace lsst:qserv:wsched
