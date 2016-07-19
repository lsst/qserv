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
#include "wpublish/QueryChunkStatistics.h"
#include "wsched/SchedulerBase.h"

// LSST headers
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.QueryChunkStatistics");
}

namespace lsst {
namespace qserv {
namespace wpublish {


/// Add statistics for the Task, creating a QueryStatistics object if needed.
void QueryChunkStatistics::addTask(wbase::Task::Ptr const& task) {
    auto qid = task->getQueryId();
    auto ent = std::pair<QueryId, QueryStatistics::Ptr>(qid, nullptr);

    std::unique_lock<std::mutex> guardStats(_queryStatsMtx);
    auto res = _queryStats.insert(ent);
    LOGS(_log, LOG_LVL_DEBUG, "&&& _queryStats.insert qid=" << qid);
    if (res.second) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& _queryStats.insert adding stats qid=" << qid);
        res.first->second = std::make_shared<QueryStatistics>(qid);
    }
    QueryStatistics::Ptr stats = res.first->second;
    guardStats.unlock();
    stats->addTask(task);
}


/// Update statistics for the Task that was just queued.
void QueryChunkStatistics::queuedTask(wbase::Task::Ptr const& task) {
    auto now = std::chrono::system_clock::now();
    task->queued(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_qStatsMtx);
        stats->_touched = now;
        stats->_size += 1;
    }
}


/// Update statistics for the Task that just started.
void QueryChunkStatistics::startedTask(wbase::Task::Ptr const& task) {
    auto now = std::chrono::system_clock::now();
    task->started(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex>(stats->_qStatsMtx);
        stats->_touched = now;
        stats->_tasksRunning += 1;
    }
}


/// Update statistics for the Task that finished an the chunk it was querying on.
void QueryChunkStatistics::finishedTask(wbase::Task::Ptr const& task) {
    auto now = std::chrono::system_clock::now();
    double taskDuration = (double)(task->finished(now).count());
    taskDuration /= 60000.0; // convert to minutes.

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        std::lock_guard<std::mutex> gs(stats->_qStatsMtx);
        stats->_touched = now;
        stats->_tasksRunning -= 1;
        stats->_tasksCompleted += 1;
        stats->_totalTimeMinutes += taskDuration;
        if (stats->_isMostlyDead()) {
            std::lock_guard<std::mutex> gd(_deadMtx);
            _deadList.push_back(stats);
        }
    }

    _finishedTaskForChunk(task, taskDuration);
}


/// Update statistics for the Task that finished an the chunk it was querying on.
void QueryChunkStatistics::_finishedTaskForChunk(wbase::Task::Ptr const& task, double minutes) {
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
    ChunkStatsTable::Ptr tableStats = iter->add(tblName, minutes);
}


/// Go through the list of possibly dead queries and remove those that are too old.
void QueryChunkStatistics::removeDead() {
    std::vector<QueryStatistics::Ptr> dList;
    auto now = std::chrono::system_clock::now();
    {
        std::lock_guard<std::mutex> g(_deadMtx);
        LOGS(_log, LOG_LVL_DEBUG, "QueryChunkStatistics::removeDead deadList size=" << _deadList.size());
        auto iter = _deadList.begin();
        while (iter != _deadList.end()) {
            if ((*iter)->isDead(_deadAfter, now)) {
                LOGS(_log, LOG_LVL_DEBUG, QueryIdHelper::makeIdStr((*iter)->_queryId)
                     << " QueryChunkStatistics::removeDead added to list");
                dList.push_back(*iter);
                iter = _deadList.erase(iter);
            } else {
                ++iter;
            }
        }
    }

    for (auto const& dead : dList) {
        removeDead(dead);
    }
}


/// Remove a statistics for a user query.
void QueryChunkStatistics::removeDead(QueryStatistics::Ptr const& queryStats) {
    std::unique_lock<std::mutex> gS(queryStats->_qStatsMtx);
    QueryId qId = queryStats->_queryId;
    gS.unlock();
    LOGS(_log, LOG_LVL_DEBUG, QueryIdHelper::makeIdStr(qId) << " Queries::removeDead");

    std::lock_guard<std::mutex> gQ(_queryStatsMtx);
    _queryStats.erase(qId);
}

/// @return the statistics for a user query.
QueryStatistics::Ptr QueryChunkStatistics::getStats(QueryId const& qId) const {
    std::lock_guard<std::mutex> g(_queryStatsMtx);
    auto iter = _queryStats.find(qId);
    if (iter != _queryStats.end()) {
        return iter->second;
    }
    return nullptr;
}


/// Add a Task to the user query statistics.
void QueryStatistics::addTask(wbase::Task::Ptr const& task) {
    LOGS(_log, LOG_LVL_DEBUG, "&&& QueryStatistics::addTask " << task->getIdStr());
    std::lock_guard<std::mutex> guard(_qStatsMtx);
    std::pair<int, wbase::Task::Ptr> ent(task->getJobId(), task);
    _taskMap.insert(ent);
}


/// @return true if this query is don and has not been touched for deadTime.
bool QueryStatistics::isDead(std::chrono::seconds deadTime, std::chrono::system_clock::time_point now) {
    std::lock_guard<std::mutex> guard(_qStatsMtx);
    if (_isMostlyDead()) {
        if (now - _touched > deadTime) {
            return true;
        }
    }
    return false;
}


/// @return true if all Tasks for this query are complete.
/// Precondition, _mx must be locked.
bool QueryStatistics::_isMostlyDead() const {
    return _tasksCompleted >= _size;
}

std::ostream& operator<<(std::ostream& os, QueryStatistics const& q) {
    std::lock_guard<std::mutex> gd(q._qStatsMtx);
    os << QueryIdHelper::makeIdStr(q._queryId)
       << " time="           << q._totalTimeMinutes
       << " size="           << q._size
       << " tasksCompleted=" << q._tasksCompleted
       << " tasksRunning="   << q._tasksRunning
       << " tasksBooted="    << q._tasksBooted;
    return os;
}


/// Remove all Tasks belonging to the user query 'qId' from the queue of 'sched'.
/// If 'sched' is null, then Tasks will be removed from the queue of whatever scheduler
/// they are queued on.
/// Tasks that are already running continue, but are marked as complete on their
/// current scheduler. Stopping and rescheduling them would be difficult at best, and
/// probably not very helpful.
std::vector<wbase::Task::Ptr>
QueryChunkStatistics::removeQueryFrom(QueryId const& qId,
                                      std::shared_ptr<wsched::SchedulerBase> const& sched) {

    std::vector<wbase::Task::Ptr> removedList; // Return value;

    // Find the user query.
    std::unique_lock<std::mutex> lock(_queryStatsMtx);
    auto query = _queryStats.find(qId);
    LOGS(_log, LOG_LVL_DEBUG, "&&& _queryStats.size=" << _queryStats.size());
    if (query == _queryStats.end()) {
        LOGS(_log, LOG_LVL_DEBUG, QueryIdHelper::makeIdStr(qId) << " was not found by removeQueryFrom");
        return removedList;;
    }
    lock.unlock();

    // Remove Tasks from their scheduler put them on 'removedList', but only if their Scheduler is the same
    // as 'sched'or if sched == nullptr.
    auto &taskMap = query->second->_taskMap;
    std::vector<wbase::Task::Ptr> taskList;
    {
        std::lock_guard<std::mutex> taskLock(query->second->_qStatsMtx);
        for (auto const& elem : taskMap) {
            taskList.push_back(elem.second);
        }
    }
    /* &&& delete
    for(auto iter = taskMap.begin(); iter != taskMap.end(); ++iter) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& removeQueryFrom loop ");
        auto task = iter->second;
        auto baseTaskSched = std::dynamic_pointer_cast<wsched::SchedulerBase>(task->getTaskScheduler());
        if (baseTaskSched == sched || sched == nullptr) {
            LOGS(_log, LOG_LVL_DEBUG, "&&& removeQueryFrom loop - removing ");
            // removeTask will only return the task if it was found on the
            // queue for its scheduler and removed.
            auto removedTask = baseTaskSched->removeTask(task);
            if (removedTask != nullptr) {
                LOGS(_log, LOG_LVL_DEBUG, "&&& removeQueryFrom loop - removing - removed");
                removedList.push_back(removedTask);
            }
        }
    }
    */
    for(auto const& task : taskList) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& removeQueryFrom loop ");
        auto baseTaskSched = std::dynamic_pointer_cast<wsched::SchedulerBase>(task->getTaskScheduler());
        if (baseTaskSched == sched || sched == nullptr) {
            LOGS(_log, LOG_LVL_DEBUG, "&&& removeQueryFrom loop - removing ");
            // removeTask will only return the task if it was found on the
            // queue for its scheduler and removed.
            auto removedTask = baseTaskSched->removeTask(task);
            if (removedTask != nullptr) {
                LOGS(_log, LOG_LVL_DEBUG, "&&& removeQueryFrom loop - removing - removed");
                removedList.push_back(removedTask);
            }
        }
    }
    return removedList;
}


/// Add the duration to the statistics for the table. Create a statistics object if needed.
/// @return the statistics for the table.
ChunkStatsTable::Ptr ChunkStats::add(std::string const& scanTableName, double minutes) {
    std::pair<std::string, ChunkStatsTable::Ptr> ele(scanTableName, nullptr);
    std::unique_lock<std::mutex> ul(_tStatsMtx);
    auto res = _tableStats.insert(ele);
    auto iter = res.first;
    if (res.second) {
        iter->second = std::make_shared<ChunkStatsTable>(_chunkId, scanTableName);
    }
    ul.unlock();
    iter->second->addTaskFinished(minutes);
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
void ChunkStatsTable::addTaskFinished(double minutes) {
    std::lock_guard<std::mutex> g(_cStatsMtx);
    ++_tasksCompleted;
    if (_tasksCompleted > 1) {
        _avgCompletionTime = (_avgCompletionTime*_weightAvg + minutes*_weightDur)/_weightSum;
    } else {
        _avgCompletionTime = minutes;
    }
    LOGS(_log, LOG_LVL_DEBUG, "ChkId=" << _chunkId << ":tbl=" << _scanTableName
         << " completed=" << _tasksCompleted
         << " avgCompletionTime=" << _avgCompletionTime);
}


}}} // namespace lsst:qserv:wpublish
