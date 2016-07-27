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
#include "wsched/ScanScheduler.h"

// LSST headers
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.QueryChunkStatistics");
}

namespace lsst {
namespace qserv {
namespace wpublish {

QueryChunkStatistics::QueryChunkStatistics(
    std::chrono::seconds deadAfter,
    std::chrono::seconds examineAfter)
     :_deadAfter{deadAfter}, _examineAfter{examineAfter} {
    auto rDead = [this](){
        while (_loopRemoval) {
            removeDead();
            std::this_thread::sleep_for(_deadAfter);
        }
    };
    std::thread td(rDead);
    _removalThread = move(td);

    LOGS(_log, LOG_LVL_DEBUG, "&&&_examineAfter.count()=" << _examineAfter.count());
    if (_examineAfter.count() == 0) {
        LOGS(_log, LOG_LVL_DEBUG, "QueryChunkStatistics turning off examineThread");
        _loopExamine = false;
    }

    auto rExamine = [this](){
        while (_loopExamine) {
            std::this_thread::sleep_for(_examineAfter);
            if (_loopExamine) examineAll();
        }
    };
    std::thread te(rExamine);
    _examineThread = move(te);
}

/// Add statistics for the Task, creating a QueryStatistics object if needed.
void QueryChunkStatistics::addTask(wbase::Task::Ptr const& task) {
    auto qid = task->getQueryId();
    auto ent = std::pair<QueryId, QueryStatistics::Ptr>(qid, nullptr);

    std::unique_lock<std::mutex> guardStats(_queryStatsMtx);
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


/// Update statistics for the Task that finished and the chunk it was querying.
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
    std::pair<int, ChunkStatistics::Ptr> ele(task->getChunkId(), nullptr);
    auto res = _chunkStats.insert(ele);
    if (res.second) {
        res.first->second = std::make_shared<ChunkStatistics>(task->getChunkId());
    }
    ul.unlock();
    auto iter = res.first->second;
    proto::ScanInfo& scanInfo = task->getScanInfo();
    std::string tblName = "";
    if (!scanInfo.infoTables.empty()) {
        proto::ScanTableInfo &sti = scanInfo.infoTables.at(0);
        tblName = ChunkTableStats::makeTableName(sti.db, sti.table);
    }
    ChunkTableStats::Ptr tableStats = iter->add(tblName, minutes);
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


/// Examine all running Tasks and boot Tasks that are taking too long and
/// move user queries that are too slow to the snail scan.
void QueryChunkStatistics::examineAll() {
    // Need to know how long it takes to complete tasks on each table
    // in each chunk, and their percentage total of the whole.
    auto scanTblSums = _calcScanTableSums();

    // Copy a vector of the Queries in the map and work with the copy
    // to free up the mutex.
    std::vector<QueryStatistics::Ptr> uqs;
    {
        std::lock_guard<std::mutex> g(_queryStatsMtx);
        for (auto const& ele : _queryStats) {
            auto const& q = ele.second;
            LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll push_back=" << q->_queryId);
            uqs.push_back(q);
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll uqs.size=" << uqs.size());

    // Go through all tasks in each query and examine the running ones.
    // If the running queries are taking longer than their percent of total time, boot them.
    // If the booted query makes it more than 5% of Tasks for the user query have been booted,
    //    then boot the user query to the snailScan.
    for (auto const& uq : uqs) {
        // Copy all the running tasks that are on ScanSchedulers.
        std::vector<wbase::Task::Ptr> runningTasks;
        {
            std::lock_guard<std::mutex> lock(uq->_qStatsMtx);
            for (auto const& ele : uq->_taskMap) {
                auto const& task = ele.second;
                if (task->getState() == wbase::Task::State::RUNNING) {
                    auto const& sched = std::dynamic_pointer_cast<wsched::ScanScheduler>(task->getTaskScheduler());
                    if (sched != nullptr) {
                        runningTasks.push_back(task);
                    }
                }
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll runningTasks.size=" << runningTasks.size());

        // For each running task, check if it is taking too long, or if the query is taking too long.
        for (auto const& task : runningTasks) {
            auto const& sched = std::dynamic_pointer_cast<wsched::ScanScheduler>(task->getTaskScheduler());
            LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll checking scheduler");
            if (sched == nullptr) {
                LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll scheduler == nullptr");
                continue;
            }
            double schedMaxTime = sched->getMaxTimeMinutes(); // Get max time for scheduler
            LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll " << sched->getName() << " schedMaxTime=" << schedMaxTime);
            // Get the slowest scan table in task.
            auto begin = task->getScanInfo().infoTables.begin();
            if (begin == task->getScanInfo().infoTables.end()) {
                LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll table name not found");
                continue;
            }
            std::string const& slowestTable = begin->db + ":" + begin->table;
            LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll table name=" << slowestTable);
            for (auto const& tt:scanTblSums) { // &&&
                LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll first=" << tt.first );
            }     // &&&
            auto iterTbl = scanTblSums.find(slowestTable);
            if (iterTbl != scanTblSums.end()) {
                LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll chunkId=" << task->getChunkId());
                ScanTableSums &tblSums = iterTbl->second;
                auto iterChunk = tblSums.chunkPercentages.find(task->getChunkId());
                if (iterChunk != tblSums.chunkPercentages.end()) {
                    // We can only make the check if there's data on past chunks/tables.
                    double percent = iterChunk->second.percent;
                    double maxTimeChunk = percent * schedMaxTime;
                    LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll percent=" << percent
                            << " schedMaxTime=" << schedMaxTime << " maxTimeChunk=" << maxTimeChunk);
                    auto runTimeMilli = task->getRunTime();
                    LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll runTimeMilli=" << runTimeMilli.count());
                    double runTimeMinutes = (double)runTimeMilli.count()/60000.0;
                    LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll runTimeMinutes=" << runTimeMinutes);
                    LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll runTimeMinutes > maxTimeChunk=" << (runTimeMinutes > maxTimeChunk));
                    if (runTimeMinutes > maxTimeChunk) {
                        LOGS(_log, LOG_LVL_DEBUG, "&&& examineAll booting task " << task->getIdStr());
                        _bootTask(uq, task, sched);
                    }
                }
            }
        }
    }
}


/// @return a map that contains time totals for all chunks for tasks running on specific
/// tables. The map is sorted by table name and contains sub-maps ordered by chunk id.
/// The sub-maps contain information about how long tasks take to complete on that table
/// on that chunk. These are then used to see what the percentage total of the time it took
/// for tasks on each chunk.
/// The table names are based on the slowest scan table in each task.
QueryChunkStatistics::ScanTableSumsMap QueryChunkStatistics::_calcScanTableSums() {
    LOGS(_log, LOG_LVL_DEBUG, "&&& _calcScanTableSums start");
    // Copy a vector of all the chunks in the map;
    std::vector<ChunkStatistics::Ptr> chks;
    {
        std::lock_guard<std::mutex> g(_chunkMtx);
        for (auto const& ele : _chunkStats) {
            auto const& chk = ele.second;
            LOGS(_log, LOG_LVL_DEBUG, "&&& _calc chk " << chk->_chunkId);
            chks.push_back(chk);
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "&&& _calcScanTableSums chks size=" << chks.size());

    QueryChunkStatistics::ScanTableSumsMap scanTblSums;
    for (auto const& chunkStats : chks) {
        auto chunkId = chunkStats->_chunkId;
        std::lock_guard<std::mutex> lock(chunkStats->_tStatsMtx);
        for (auto const& ele : chunkStats->_tableStats) {
            auto const& tblName = ele.first;
            LOGS(_log, LOG_LVL_DEBUG, "&&& tblName=" << tblName);
            if (tblName != "") {
                auto &sTSums = scanTblSums[tblName];
                sTSums.totalTime  += ele.second->getAvgCompletionTime();
                ChunkTimePercent &ctp = sTSums.chunkPercentages[chunkId];
                ctp.shardTime = ele.second->getAvgCompletionTime();
                LOGS(_log, LOG_LVL_DEBUG, "&&& chunkId=" << chunkId << " ctp.shardTime=" << ctp.shardTime);
            }
        }
    }

    // Calculate percentage totals.
    for (auto &eleTbl : scanTblSums) {
        auto &scanTbl = eleTbl.second;
        auto totalTime = scanTbl.totalTime;
        for (auto &eleChunk : scanTbl.chunkPercentages) {
            auto &tPercent = eleChunk.second;
            tPercent.percent = tPercent.shardTime / totalTime;
        }
    }
    return scanTblSums;
}


/// &&& doc
void QueryChunkStatistics::_bootTask(QueryStatistics::Ptr const& uq, wbase::Task::Ptr const& task,
                                     wsched::SchedulerBase::Ptr const& sched) {
    LOGS(_log, LOG_LVL_INFO, task->getIdStr() << " taking too long, booting from " << sched->getName());
    sched->removeTask(task);
    uq->_tasksBooted += 1;
    if (uq->_tasksBooted > _maxBooted) {
        LOGS(_log, LOG_LVL_INFO, task->getIdStr() << " entire UserQuery booting from " << sched->getName());
        removeQueryFrom(uq->_queryId, sched);
    }
}


/// Add a Task to the user query statistics.
void QueryStatistics::addTask(wbase::Task::Ptr const& task) {
    std::lock_guard<std::mutex> guard(_qStatsMtx);
    std::pair<int, wbase::Task::Ptr> ent(task->getJobId(), task);
    _taskMap.insert(ent);
}


/// @return the number of Tasks that have been booted for this user query.
int QueryStatistics::getTasksBooted() {
    std::lock_guard<std::mutex> guard(_qStatsMtx);
    return _tasksBooted;
}


/// @return true if this query is done and has not been touched for deadTime.
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
    if (query == _queryStats.end()) {
        LOGS(_log, LOG_LVL_DEBUG, QueryIdHelper::makeIdStr(qId) << " was not found by removeQueryFrom");
        return removedList;
    }
    lock.unlock();

    // Remove Tasks from their scheduler put them on 'removedList', but only if their Scheduler is the same
    // as 'sched' or if sched == nullptr.
    auto &taskMap = query->second->_taskMap;
    std::vector<wbase::Task::Ptr> taskList;
    {
        std::lock_guard<std::mutex> taskLock(query->second->_qStatsMtx);
        for (auto const& elem : taskMap) {
            taskList.push_back(elem.second);
        }
    }

    for(auto const& task : taskList) {
        auto taskSched = task->getTaskScheduler();
        if (taskSched != nullptr && (taskSched == sched || sched == nullptr)) {
            // removeTask will only return the task if it was found on the
            // queue for its scheduler and removed.
            auto removedTask = taskSched->removeTask(task);
            if (removedTask != nullptr) {
                removedList.push_back(removedTask);
            }
        }
    }
    return removedList;
}


/// Add the duration to the statistics for the table. Create a statistics object if needed.
/// @return the statistics for the table.
ChunkTableStats::Ptr ChunkStatistics::add(std::string const& scanTableName, double minutes) {
    std::pair<std::string, ChunkTableStats::Ptr> ele(scanTableName, nullptr);
    std::unique_lock<std::mutex> ul(_tStatsMtx);
    auto res = _tableStats.insert(ele);
    auto iter = res.first;
    if (res.second) {
        iter->second = std::make_shared<ChunkTableStats>(_chunkId, scanTableName);
    }
    ul.unlock();
    iter->second->addTaskFinished(minutes);
    return iter->second;
}


/// @return the statistics for a table. nullptr if the table is not found.
ChunkTableStats::Ptr ChunkStatistics::getStats(std::string const& scanTableName) const {
    std::lock_guard<std::mutex> g(_tStatsMtx);
    auto iter = _tableStats.find(scanTableName);
    if (iter != _tableStats.end()) {
        return iter->second;
    }
    return nullptr;
}


/// Use the duration of the last Task completed to adjust the average completion time.
void ChunkTableStats::addTaskFinished(double minutes) {
    std::lock_guard<std::mutex> g(_cStatsMtx);
    ++_tasksCompleted;
    if (_tasksCompleted > 1) {
        _avgCompletionTime = (_avgCompletionTime*_weightAvg + minutes*_weightNew)/_weightSum;
    } else {
        _avgCompletionTime = minutes;
    }
    LOGS(_log, LOG_LVL_DEBUG, "ChkId=" << _chunkId << ":tbl=" << _scanTableName
         << " completed=" << _tasksCompleted
         << " avgCompletionTime=" << _avgCompletionTime);
}


double ChunkTableStats::getAvgCompletionTime() {
    std::lock_guard<std::mutex> g(_cStatsMtx);
    return _avgCompletionTime;
}

}}} // namespace lsst:qserv:wpublish
