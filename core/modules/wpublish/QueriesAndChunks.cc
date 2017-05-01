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
#include "wpublish/QueriesAndChunks.h"
// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wsched/BlendScheduler.h"
#include "wsched/SchedulerBase.h"
#include "wsched/ScanScheduler.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.QueriesAndChunks");
}

namespace lsst {
namespace qserv {
namespace wpublish {


QueriesAndChunks::QueriesAndChunks(std::chrono::seconds deadAfter,
                                   std::chrono::seconds examineAfter, int maxTasksBooted)
     : _deadAfter{deadAfter}, _examineAfter{examineAfter}, _maxTasksBooted{maxTasksBooted} {
    auto rDead = [this](){
        while (_loopRemoval) {
            removeDead();
            std::this_thread::sleep_for(_deadAfter);
        }
    };
    std::thread td(rDead);
    _removalThread = std::move(td);

    if (_examineAfter.count() == 0) {
        LOGS(_log, LOG_LVL_DEBUG, "QueriesAndChunks turning off examineThread");
        _loopExamine = false;
    }

    auto rExamine = [this](){
        while (_loopExamine) {
            std::this_thread::sleep_for(_examineAfter);
            if (_loopExamine) examineAll();
        }
    };
    std::thread te(rExamine);
    _examineThread = std::move(te);
}


QueriesAndChunks::~QueriesAndChunks() {
    _loopRemoval = false;
    _loopExamine = false;
    try {
        _removalThread.join();
        _examineThread.join();
    } catch (std::system_error const& e) {
        LOGS(_log, LOG_LVL_ERROR, "~QueriesAndChunks " << e.what());
    }
}


void QueriesAndChunks::setBlendScheduler(std::shared_ptr<wsched::BlendScheduler> const& blendSched) {
    _blendSched = blendSched;
}


void QueriesAndChunks::setRequiredTasksCompleted(unsigned int value) {
    _requiredTasksCompleted = value;
}

/// Add statistics for the Task, creating a QueryStatistics object if needed.
void QueriesAndChunks::addTask(wbase::Task::Ptr const& task) {
    auto qid = task->getQueryId();
    std::unique_lock<std::mutex> guardStats(_queryStatsMtx);
    QueryStatistics::Ptr& stats = _queryStats[qid];
    if (stats == nullptr) {
        stats = std::make_shared<QueryStatistics>(qid);
    }
    guardStats.unlock();
    stats->addTask(task);
}


/// Update statistics for the Task that was just queued.
void QueriesAndChunks::queuedTask(wbase::Task::Ptr const& task) {
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
void QueriesAndChunks::startedTask(wbase::Task::Ptr const& task) {
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
void QueriesAndChunks::finishedTask(wbase::Task::Ptr const& task) {
    auto now = std::chrono::system_clock::now();
    double taskDuration = (double)(task->finished(now).count());
    taskDuration /= 60000.0; // convert to minutes.

    QueryId qId = task->getQueryId();
    QueryStatistics::Ptr stats = getStats(qId);
    if (stats != nullptr) {
        bool mostlyDead = false;
        {
            std::lock_guard<std::mutex> gs(stats->_qStatsMtx);
            stats->_touched = now;
            stats->_tasksRunning -= 1;
            stats->_tasksCompleted += 1;
            stats->_totalTimeMinutes += taskDuration;
            mostlyDead = stats->_isMostlyDead();
        }
        if (mostlyDead) {
            std::lock_guard<std::mutex> gd(_newlyDeadMtx);
            (*_newlyDeadQueries)[qId] = stats;
        }
    }

    _finishedTaskForChunk(task, taskDuration);
}


/// Update statistics for the Task that finished and the chunk it was querying.
void QueriesAndChunks::_finishedTaskForChunk(wbase::Task::Ptr const& task, double minutes) {
    std::unique_lock<std::mutex> ul(_chunkMtx);
    std::pair<int, ChunkStatistics::Ptr> ele(task->getChunkId(), nullptr);
    auto res = _chunkStats.insert(ele);
    if (res.second) {
        res.first->second = std::make_shared<ChunkStatistics>(task->getChunkId());
    }
    ul.unlock();
    auto iter = res.first->second;
    proto::ScanInfo& scanInfo = task->getScanInfo();
    std::string tblName;
    if (!scanInfo.infoTables.empty()) {
        proto::ScanTableInfo& sti = scanInfo.infoTables.at(0);
        tblName = ChunkTableStats::makeTableName(sti.db, sti.table);
    }
    ChunkTableStats::Ptr tableStats = iter->add(tblName, minutes);
}


/// Go through the list of possibly dead queries and remove those that are too old.
void QueriesAndChunks::removeDead() {
    std::vector<QueryStatistics::Ptr> dList;
    auto now = std::chrono::system_clock::now();
    {
        std::shared_ptr<DeadQueriesType> newlyDead;
        {
            std::lock_guard<std::mutex> gnd(_newlyDeadMtx);
            newlyDead = _newlyDeadQueries;
            _newlyDeadQueries.reset(new DeadQueriesType);
        }

        std::lock_guard<std::mutex> gd(_deadMtx);
        // Copy newlyDead into dead.
        for(auto const& elem : *newlyDead) {
            _deadQueries[elem.first] = elem.second;
        }
        LOGS(_log, LOG_LVL_DEBUG, "QueriesAndChunks::removeDead deadQueries size=" << _deadQueries.size());
        auto iter = _deadQueries.begin();
        while (iter != _deadQueries.end()) {
            auto const& statPtr = iter->second;
            if (statPtr->isDead(_deadAfter, now)) {
                LOGS(_log, LOG_LVL_DEBUG, QueryIdHelper::makeIdStr(statPtr->_queryId)
                     << " QueriesAndChunks::removeDead added to list");
                dList.push_back(statPtr);
                iter = _deadQueries.erase(iter);
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
/// Query Ids should be unique for the life of the system, so erasing
/// a qId multiple times from _queryStats should be harmless.
void QueriesAndChunks::removeDead(QueryStatistics::Ptr const& queryStats) {
    std::unique_lock<std::mutex> gS(queryStats->_qStatsMtx);
    QueryId qId = queryStats->_queryId;
    gS.unlock();
    LOGS(_log, LOG_LVL_DEBUG, QueryIdHelper::makeIdStr(qId) << " Queries::removeDead");

    std::lock_guard<std::mutex> gQ(_queryStatsMtx);
    _queryStats.erase(qId);
}

/// @return the statistics for a user query.
QueryStatistics::Ptr QueriesAndChunks::getStats(QueryId const& qId) const {
    std::lock_guard<std::mutex> g(_queryStatsMtx);
    auto iter = _queryStats.find(qId);
    if (iter != _queryStats.end()) {
        return iter->second;
    }
    return nullptr;
}


/// Examine all running Tasks and boot Tasks that are taking too long and
/// move user queries that are too slow to the snail scan.
/// This is expected to be called maybe once every 5 minutes.
void QueriesAndChunks::examineAll() {
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
            uqs.push_back(q);
        }
    }

    // Go through all Tasks in each query and examine the running ones.
    // If a running Task is taking longer than its percent of total time, boot it.
    // Booting a Task may result in the entire user query it belongs to being moved
    // to the snail scan.
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

        // For each running task, check if it is taking too long, or if the query is taking too long.
        for (auto const& task : runningTasks) {
            auto const& sched = std::dynamic_pointer_cast<wsched::ScanScheduler>(task->getTaskScheduler());
            if (sched == nullptr) {
                continue;
            }
            double schedMaxTime = sched->getMaxTimeMinutes(); // Get max time for scheduler
            // Get the slowest scan table in task.
            auto begin = task->getScanInfo().infoTables.begin();
            if (begin == task->getScanInfo().infoTables.end()) {
                continue;
            }
            std::string const& slowestTable = begin->db + ":" + begin->table;
            auto iterTbl = scanTblSums.find(slowestTable);
            if (iterTbl != scanTblSums.end()) {
                LOGS(_log, LOG_LVL_DEBUG, "examineAll " << slowestTable
                                       << " chunkId=" << task->getChunkId());
                ScanTableSums& tblSums = iterTbl->second;
                auto iterChunk = tblSums.chunkPercentages.find(task->getChunkId());
                if (iterChunk != tblSums.chunkPercentages.end()) {
                    // We can only make the check if there's data on past chunks/tables.
                    double percent = iterChunk->second.percent;
                    bool valid = iterChunk->second.valid;
                    double maxTimeChunk = percent * schedMaxTime;
                    auto runTimeMilli = task->getRunTime();
                    double runTimeMinutes = (double)runTimeMilli.count()/60000.0;
                    bool booting = runTimeMinutes > maxTimeChunk && valid;
                    auto lvl = booting ? LOG_LVL_INFO : LOG_LVL_DEBUG;
                    LOGS(_log, lvl, "examineAll " << (booting ? "booting" : "keeping") << " task " << task->getIdStr()
                            << "maxTimeChunk(" << maxTimeChunk << ")=percent(" << percent << ")*schedMaxTime(" << schedMaxTime << ")"
                            << " runTimeMinutes=" << runTimeMinutes << " valid=" << valid);
                    if (booting) {
                        _bootTask(uq, task, sched);
                    }
                }
            }
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "QueriesAndChunks::examineAll end");
}


/// @return a map that contains time totals for all chunks for tasks running on specific
/// tables. The map is sorted by table name and contains sub-maps ordered by chunk id.
/// The sub-maps contain information about how long tasks take to complete on that table
/// on that chunk. These are then used to see what the percentage total of the time it took
/// for tasks on each chunk.
/// The table names are based on the slowest scan table in each task.
QueriesAndChunks::ScanTableSumsMap QueriesAndChunks::_calcScanTableSums() {
    // Copy a vector of all the chunks in the map;
    std::vector<ChunkStatistics::Ptr> chks;
    {
        std::lock_guard<std::mutex> g(_chunkMtx);
        for (auto const& ele : _chunkStats) {
            auto const& chk = ele.second;
            chks.push_back(chk);
        }
    }

    QueriesAndChunks::ScanTableSumsMap scanTblSums;
    for (auto const& chunkStats : chks) {
        auto chunkId = chunkStats->_chunkId;
        std::lock_guard<std::mutex> lock(chunkStats->_tStatsMtx);
        for (auto const& ele : chunkStats->_tableStats) {
            auto const& tblName = ele.first;
            if (!tblName.empty()) {
                auto& sTSums = scanTblSums[tblName];
                auto data = ele.second->getData();
                sTSums.totalTime  += data.avgCompletionTime;
                ChunkTimePercent& ctp = sTSums.chunkPercentages[chunkId];
                ctp.shardTime = data.avgCompletionTime;
                ctp.valid = data.tasksCompleted >= _requiredTasksCompleted;
            }
        }
    }

    // Calculate percentage totals.
    for (auto& eleTbl : scanTblSums) {
        auto& scanTbl = eleTbl.second;
        auto totalTime = scanTbl.totalTime;
        for (auto& eleChunk : scanTbl.chunkPercentages) {
            auto& tPercent = eleChunk.second;
            tPercent.percent = tPercent.shardTime / totalTime;
        }
    }
    return scanTblSums;
}


/// Remove the running 'task' from a scheduler and possibly move all Tasks that belong to its user query
/// to the snail scheduler. 'task' continues to run in its thread, but the scheduler is told 'task' is
/// finished, which allows the scheduler to move on to another Task.
/// If too many Tasks from the same user query are booted, all remaining tasks are moved to the snail
/// scheduler in an attempt to keep a single user query from jamming up a scheduler.
void QueriesAndChunks::_bootTask(QueryStatistics::Ptr const& uq, wbase::Task::Ptr const& task,
                                     wsched::SchedulerBase::Ptr const& sched) {
    LOGS(_log, LOG_LVL_INFO, task->getIdStr() << " taking too long, booting from " << sched->getName());
    sched->removeTask(task, true);
    uq->_tasksBooted += 1;

    auto bSched = _blendSched.lock();
    if (bSched == nullptr) {
        LOGS(_log, LOG_LVL_WARN, task->getIdStr()
                             << " blendSched undefined, can't check user query");
        return;
    }
    if (bSched->isScanSnail(sched)) {
        // If it's already on the snail scan, it has already been booted from another scan.
        if (uq->_tasksBooted > _maxTasksBooted + 1) {
            LOGS(_log, LOG_LVL_WARN, task->getIdStr() <<
                 "User Query taking excessive amount of time on snail scan and should be cancelled");
            // TODO: Add code to send message back to czar to cancel this user query.
        }
    } else {
        if (uq->_tasksBooted > _maxTasksBooted) {
            LOGS(_log, LOG_LVL_INFO, task->getIdStr()
                 << " entire UserQuery booting from " << sched->getName());
            uq->_queryBooted = true;
            bSched->moveUserQueryToSnail(uq->_queryId, sched);
        }
    }
}


/// Add a Task to the user query statistics.
void QueryStatistics::addTask(wbase::Task::Ptr const& task) {
    std::lock_guard<std::mutex> guard(_qStatsMtx);
    _taskMap.insert(std::make_pair(task->getJobId(), task));
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
/// Precondition, _qStatsMtx must be locked.
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
QueriesAndChunks::removeQueryFrom(QueryId const& qId, wsched::SchedulerBase::Ptr const& sched) {
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
    auto& taskMap = query->second->_taskMap;
    std::vector<wbase::Task::Ptr> taskList;
    {
        std::lock_guard<std::mutex> taskLock(query->second->_qStatsMtx);
        for (auto const& elem : taskMap) {
            taskList.push_back(elem.second);
        }
    }

    auto moveTasks = [&sched, &taskList, &removedList](bool moveRunning) {
        std::vector<wbase::Task::Ptr> taskListNotRemoved;
        for(auto const& task : taskList) {
            auto taskSched = task->getTaskScheduler();
            if (taskSched != nullptr && (taskSched == sched || sched == nullptr)) {
                // Returns true only if the task still needs to be scheduled.
                if (taskSched->removeTask(task, moveRunning)) {
                    removedList.push_back(task);
                } else {
                    taskListNotRemoved.push_back(task);
                }
            }
        }
        taskList = taskListNotRemoved;
    };

    // Remove as many non-running tasks as possible from the scheduler queue. This
    // needs to be done before removing running tasks to avoid a race condition where
    // tasks are pulled off the scheduler queue every time a running one is removed.
    moveTasks(false);

    // Remove all remaining tasks. Most likely, all will be running.
    moveTasks(true);

    return removedList;
}


std::ostream& operator<<(std::ostream& os, QueriesAndChunks const& qc) {
    std::lock_guard<std::mutex> g(qc._chunkMtx);
    os << "Chunks(";
    for (auto const& ele : qc._chunkStats) {
        os << *(ele.second) << ";";
    }
    os << ")";
    return os;
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


std::ostream& operator<<(std::ostream& os, ChunkStatistics const& cs) {
    std::lock_guard<std::mutex> g(cs._tStatsMtx);
    os << "ChunkStatsistics(" << cs._chunkId << "(";
    for (auto const& ele : cs._tableStats) {
        os << *(ele.second) << ";";
    }
    os << ")";
    return os;
}


/// Use the duration of the last Task completed to adjust the average completion time.
void ChunkTableStats::addTaskFinished(double minutes) {
    std::lock_guard<std::mutex> g(_dataMtx);
    ++_data.tasksCompleted;
    if (_data.tasksCompleted > 1) {
        _data.avgCompletionTime = (_data.avgCompletionTime*_weightAvg + minutes*_weightNew)/_weightSum;
    } else {
        _data.avgCompletionTime = minutes;
    }
    LOGS(_log, LOG_LVL_DEBUG, "ChkId=" << _chunkId << ":tbl=" << _scanTableName
         << " completed=" << _data.tasksCompleted
         << " avgCompletionTime=" << _data.avgCompletionTime);
}


std::ostream& operator<<(std::ostream& os, ChunkTableStats const& cts) {
    std::lock_guard<std::mutex> g(cts._dataMtx);
    os << "ChunkTableStats(" << cts._chunkId << ":" << cts._scanTableName
       << " tasks(completed=" << cts._data.tasksCompleted << ",avgTime=" << cts._data.avgCompletionTime
       << ",booted=" << cts._data.tasksBooted << "))";
    return os;
}


}}} // namespace lsst:qserv:wpublish
