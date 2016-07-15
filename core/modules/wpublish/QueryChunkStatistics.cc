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
#include "lsst/log/Log.h"
#include "QueryChunkStatistics.h"

// Qserv headers


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.QueryStatistics");
}

namespace lsst {
namespace qserv {
namespace wpublish {


/// Add statistics for the Task, creating a QueryStatistics object if needed.
void QueryChunkStatistics::addTask(wbase::Task::Ptr const& task) {
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


/// Update statistics for the Task that was just queued.
void QueryChunkStatistics::queuedTask(wbase::Task::Ptr const& task) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    task->queued(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_mx);
        stats->_touched = now;
        stats->_size += 1;
    }
}


/// Update statistics for the Task that just started.
void QueryChunkStatistics::startedTask(wbase::Task::Ptr const& task) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    task->started(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_mx);
        stats->_touched = now;
        stats->_tasksRunning += 1;
    }
}


/// Update statistics for the Task that finished an the chunk it was querying on.
void QueryChunkStatistics::finishedTask(wbase::Task::Ptr const& task) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    double taskDuration = (double)(task->finished(now).count());
    taskDuration /= 60000.0; // convert to minutes.

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex> gs(stats->_mx);
        stats->_touched = now;
        stats->_tasksRunning -= 1;
        stats->_tasksCompleted += 1;
        stats->_totalCompletionTime += taskDuration;
        if (stats->_isMostlyDead()) {
            LOGS(_log, LOG_LVL_DEBUG, "&&& deadList adding task");
            std::lock_guard<std::mutex> gd(_deadMtx);
            _deadList.push_back(stats);
        }
    }

    _finishedTaskForChunk(task, taskDuration);
}


/// Update statistics for the Task that finished an the chunk it was querying on.
void QueryChunkStatistics::_finishedTaskForChunk(wbase::Task::Ptr const& task, double taskDuration) {
    std::unique_lock<std::mutex> ul(_chunkMtx);
    std::pair<int, ChunkStats::Ptr> ele(task->getChunkId(), nullptr);
    auto res = _chunkStats.insert(ele);
    if (res.second) {
        res.first->second = std::make_shared<ChunkStats>(task->getChunkId());
    }
    ul.unlock();
    auto iter = res.first->second;
    proto::ScanInfo& scanInfo = task->getScanInfo();
    std::string tblName = "";
    if (!scanInfo.infoTables.empty()) {
        proto::ScanTableInfo &sti = scanInfo.infoTables.at(0);
        tblName = ChunkStatsTable::makeTableName(sti.db, sti.table);
    }
    ChunkStatsTable::Ptr tableStats = iter->add(tblName, taskDuration);
}


/// Go through the list of possibly dead queries and remove those that are too old.
void QueryChunkStatistics::removeDead() {
    LOGS(_log, LOG_LVL_DEBUG, "QueryChunkStatistics::removeDead");
    std::vector<QueryStatistics::Ptr> dList;
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    {
        std::lock_guard<std::mutex> g(_deadMtx);
        LOGS(_log, LOG_LVL_DEBUG, "QueryChunkStatistics::removeDead deadList size=" << _deadList.size());
        auto iter = _deadList.begin();
        auto end = _deadList.end();
        for (;iter != end; ++iter) {
            if ((*iter)->isDead(_deadAfter, now)) {
                dList.push_back(*iter);
                iter = _deadList.erase(iter);
            }
        }
    }

    LOGS(_log, LOG_LVL_DEBUG, "QueryChunkStatistics::removeDead mid &&&"); // temporary
    for (auto const& dead:dList) {
        removeDead(dead);
    }
    LOGS(_log, LOG_LVL_DEBUG, "QueryChunkStatistics::removeDead end &&&"); // temporary
}


/// Remove a statistics for a user query.
void QueryChunkStatistics::removeDead(QueryStatistics::Ptr const& queryStats) {
    LOGS(_log, LOG_LVL_DEBUG, " Queries::removeDead " << queryStats);

    std::unique_lock<std::mutex> gS(queryStats->_mx);
    QueryId qId = queryStats->_queryId;
    gS.unlock();

    std::lock_guard<std::mutex> gQ(_qStatsMtx);
    _queryStats.erase(qId);
}

/// @return the statistics for a user query.
QueryStatistics::Ptr QueryChunkStatistics::getStats(QueryId const& qId) const {
    std::lock_guard<std::mutex> g(_qStatsMtx);
    auto iter = _queryStats.find(qId);
    if (iter != _queryStats.end()) {
        return iter->second;
    }
    return nullptr;
}


/// Add a Task to the user query statistics.
void QueryStatistics::addTask(wbase::Task::Ptr const& task) {
    std::lock_guard<std::mutex> guard(_mx);
    std::pair<int, wbase::Task::Ptr> ent(task->getJobId(), task);
    _taskMap.insert(ent);
}


/// @return true if this query is don and has not been touched for deadTime.
bool QueryStatistics::isDead(std::chrono::seconds deadTime, std::chrono::system_clock::time_point now) {
    std::lock_guard<std::mutex> guard(_mx);
    if (_isMostlyDead()) {
        if (now - _touched > deadTime) {
            return true;
        }
    }
    return false;
}


/// @return true if all Tasks for this query are complete.
/// Precondition, _mx must be locked.
bool QueryStatistics::_isMostlyDead() {
    return _tasksCompleted >= _size;
}

std::ostream& operator<<(std::ostream& os, QueryStatistics const& q) {
    std::lock_guard<std::mutex> gd(q._mx);
    os << QueryIdHelper::makeIdStr(q._queryId)
       << " time="           << q._totalCompletionTime
       << " size="           << q._size
       << " tasksCompleted=" << q._tasksCompleted
       << " tasksRunning="   << q._tasksRunning
       << " tasksBooted="    << q._tasksBooted;
    return os;
}


/// Add the duration to the statistics for the table. Create a statistics object if needed.
/// @return the statistics for the table.
ChunkStatsTable::Ptr ChunkStats::add(std::string const& scanTableName, double duration) {
    std::pair<std::string, ChunkStatsTable::Ptr> ele(scanTableName, nullptr);
    std::unique_lock<std::mutex> ul(_tStatsMtx);
    auto res = _tableStats.insert(ele);
    auto iter = res.first;
    if (res.second) {
        iter->second = std::make_shared<ChunkStatsTable>(_chunkId, scanTableName);
    }
    ul.unlock();
    iter->second->addTaskFinished(duration);
    return iter->second;
}


/// @return the statistics for a table. nullptr if the table is not found.
ChunkStatsTable::Ptr ChunkStats::getStats(std::string const& scanTableName) const {
    std::lock_guard<std::mutex> g(_tStatsMtx);
    auto iter = _tableStats.find(scanTableName);
    if (iter != _tableStats.end()) {
        return iter->second;
    }
    return nullptr;
}


/// Use the duration of the last Task completed to adjust the average completion time.
void ChunkStatsTable::addTaskFinished(double duration) {
    std::lock_guard<std::mutex> g(_mtx);
    ++_tasksCompleted;
    if (_tasksCompleted > 1) {
        _avgCompletionTime = (_avgCompletionTime*_weightAvg + duration*_weightDur)/_weightSum;
    } else {
        _avgCompletionTime = duration;
    }
    LOGS(_log, LOG_LVL_DEBUG, "ChkId=" << _chunkId << ":tbl=" << _scanTableName
         << " completed=" << _tasksCompleted
         << " avgCompletionTime=" << _avgCompletionTime);
}


}}} // namespace lsst:qserv:wsched
