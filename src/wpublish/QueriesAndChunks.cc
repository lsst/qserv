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

// System headers
#include <algorithm>

// Qserv headers
#include "util/Bug.h"
#include "util/HoldTrack.h"
#include "util/TimeUtils.h"
#include "wbase/TaskState.h"
#include "wbase/UserQueryInfo.h"
#include "wsched/BlendScheduler.h"
#include "wsched/SchedulerBase.h"
#include "wsched/ScanScheduler.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace wbase = lsst::qserv::wbase;
namespace wpublish = lsst::qserv::wpublish;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.QueriesAndChunks");
}  // namespace

namespace lsst::qserv::wpublish {

QueryStatistics::QueryStatistics(QueryId const& qId_) : creationTime(CLOCK::now()), queryId(qId_) {
    /// For all of the histograms, all entries should be kept at least until the work is finished.
    string qidStr = to_string(queryId);
    _histSizePerTask = util::Histogram::Ptr(new util::Histogram(
            string("SizePerTask_") + qidStr, {1'000, 10'0000, 1'000'000, 10'000'000, 100'000'000}));
    _histRowsPerTask = util::Histogram::Ptr(new util::Histogram(string("RowsPerChunk_") + qidStr,
                                                                {1, 100, 1'000, 10'000, 100'000, 1'000'000}));
    _histTimeRunningPerTask = util::Histogram::Ptr(new util::Histogram(
            string("TimeRunningPerTask_") + qidStr, {0.1, 1, 10, 30, 60, 120, 300, 600, 1200, 10000}));
    _histTimeSubchunkPerTask = util::Histogram::Ptr(new util::Histogram(
            string("TimeSubchunkPerTask_") + qidStr, {0.1, 1, 10, 30, 60, 120, 300, 600, 1200, 10000}));
    _histTimeTransmittingPerTask = util::Histogram::Ptr(new util::Histogram(
            string("TimeTransmittingPerTask_") + qidStr, {0.1, 1, 10, 30, 60, 120, 300, 600, 1200, 10000}));
    _histTimeBufferFillPerTask = util::Histogram::Ptr(new util::Histogram(
            string("TimeFillingBuffersPerTask_") + qidStr, {0.1, 1, 10, 30, 60, 120, 300, 600, 1200, 10000}));
}

/// Return a json object containing high level data, such as histograms.
nlohmann::json QueryStatistics::getJsonHist() const {
    nlohmann::json js = nlohmann::json::object();
    js["timeRunningPerTask"] = _histTimeRunningPerTask->getJson();
    js["timeSubchunkPerTask"] = _histTimeSubchunkPerTask->getJson();
    js["timeTransmittingPerTask"] = _histTimeTransmittingPerTask->getJson();
    js["timeBufferFillPerTask"] = _histTimeBufferFillPerTask->getJson();
    js["sizePerTask"] = _histSizePerTask->getJson();
    js["rowsPerTask"] = _histRowsPerTask->getJson();
    return js;
}

nlohmann::json QueryStatistics::getJsonTasks(wbase::TaskSelector const& taskSelector) const {
    lock_guard<mutex> const guard(_qStatsMtx);
    nlohmann::json result = nlohmann::json::object();
    result["snapshotTime_msec"] = util::TimeUtils::now();
    result["entries"] = nlohmann::json::array();
    auto& taskEntriesJson = result["entries"];
    uint32_t numTasksSelected = 0;
    if (taskSelector.includeTasks) {
        auto const& ids = taskSelector.queryIds;
        auto const& states = taskSelector.taskStates;
        for (auto&& task : _tasks) {
            bool const selectedByQueryId =
                    ids.empty() || find(ids.begin(), ids.end(), task->getQueryId()) != ids.end();
            bool const selectedByTaskState =
                    states.empty() || find(states.begin(), states.end(), task->state()) != states.end();
            if (selectedByQueryId && selectedByTaskState) {
                ++numTasksSelected;
                // Stop reporting tasks if the limit has been reached. Just keep
                // counting the number of tasks selected by the filter.
                if ((taskSelector.maxTasks == 0) || (numTasksSelected < taskSelector.maxTasks)) {
                    taskEntriesJson.push_back(task->getJson());
                }
            }
        }
    }
    result["total"] = _tasks.size();
    result["selected"] = numTasksSelected;
    return result;
}

QueriesAndChunks::Ptr QueriesAndChunks::_globalQueriesAndChunks;

QueriesAndChunks::Ptr QueriesAndChunks::get(bool noThrow) {
    if (_globalQueriesAndChunks == nullptr && !noThrow) {
        throw util::Bug(ERR_LOC, "QueriesAndChunks::get() called before QueriesAndChunks::setupGlobal");
    }
    return _globalQueriesAndChunks;
}

QueriesAndChunks::Ptr QueriesAndChunks::setupGlobal(chrono::seconds deadAfter, chrono::seconds examineAfter,
                                                    int maxTasksBooted, bool resetForTesting) {
    if (resetForTesting) {
        _globalQueriesAndChunks.reset();
    }
    if (_globalQueriesAndChunks != nullptr) {
        throw util::Bug(ERR_LOC, "QueriesAndChunks::setupGlobal called twice");
    }
    _globalQueriesAndChunks = Ptr(new QueriesAndChunks(deadAfter, examineAfter, maxTasksBooted));
    return _globalQueriesAndChunks;
}

QueriesAndChunks::QueriesAndChunks(chrono::seconds deadAfter, chrono::seconds examineAfter,
                                   int maxTasksBooted)
        : _deadAfter{deadAfter}, _examineAfter{examineAfter}, _maxTasksBooted{maxTasksBooted} {
    auto rDead = [this]() {
        while (_loopRemoval) {
            removeDead();
            this_thread::sleep_for(_deadAfter);
        }
    };
    thread td(rDead);
    _removalThread = move(td);

    if (_examineAfter.count() == 0) {
        LOGS(_log, LOG_LVL_DEBUG, "QueriesAndChunks turning off examineThread");
        _loopExamine = false;
    }

    auto rExamine = [this]() {
        while (_loopExamine) {
            this_thread::sleep_for(_examineAfter);
            if (_loopExamine) examineAll();
        }
    };
    thread te(rExamine);
    _examineThread = move(te);
}

QueriesAndChunks::~QueriesAndChunks() {
    _loopRemoval = false;
    _loopExamine = false;
    try {
        _removalThread.join();
        _examineThread.join();
    } catch (system_error const& e) {
        LOGS(_log, LOG_LVL_ERROR, "~QueriesAndChunks " << e.what());
    }
}

void QueriesAndChunks::setBlendScheduler(shared_ptr<wsched::BlendScheduler> const& blendSched) {
    _blendSched = blendSched;
}

void QueriesAndChunks::setRequiredTasksCompleted(unsigned int value) { _requiredTasksCompleted = value; }

/// Add statistics for the Task, creating a QueryStatistics object if needed.
void QueriesAndChunks::addTask(wbase::Task::Ptr const& task) {
    auto qid = task->getQueryId();
    unique_lock<mutex> guardStats(_queryStatsMtx);
    auto itr = _queryStats.find(qid);
    QueryStatistics::Ptr stats;
    if (_queryStats.end() == itr) {
        stats = QueryStatistics::Ptr(new QueryStatistics(qid));
        _queryStats[qid] = stats;
    } else {
        stats = itr->second;
    }
    guardStats.unlock();
    stats->addTask(task);
    task->setQueryStatistics(stats);
}

/// Update statistics for the Task that was just queued.
void QueriesAndChunks::queuedTask(wbase::Task::Ptr const& task) {
    auto now = chrono::system_clock::now();
    task->queued(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        lock_guard<mutex>(stats->_qStatsMtx);
        stats->_touched = now;
        stats->_size += 1;
    }
}

/// Update statistics for the Task that just started.
void QueriesAndChunks::startedTask(wbase::Task::Ptr const& task) {
    auto now = chrono::system_clock::now();
    task->started(now);

    QueryStatistics::Ptr stats = getStats(task->getQueryId());
    if (stats != nullptr) {
        lock_guard<mutex>(stats->_qStatsMtx);
        stats->_touched = now;
        stats->_tasksRunning += 1;
    }
}

/// Update statistics for the Task that finished and the chunk it was querying.
void QueriesAndChunks::finishedTask(wbase::Task::Ptr const& task) {
    auto now = chrono::system_clock::now();
    double taskDuration = (double)(task->finished(now).count());
    taskDuration /= 60000.0;  // convert to minutes.

    QueryId qId = task->getQueryId();
    QueryStatistics::Ptr stats = getStats(qId);
    if (stats != nullptr) {
        bool mostlyDead = false;
        {
            lock_guard<mutex> gs(stats->_qStatsMtx);
            stats->_touched = now;
            stats->_tasksRunning -= 1;
            stats->_tasksCompleted += 1;
            stats->_totalTimeMinutes += taskDuration;
            mostlyDead = stats->_isMostlyDead();
        }
        if (mostlyDead) {
            lock_guard<mutex> gd(_newlyDeadMtx);
            (*_newlyDeadQueries)[qId] = stats;
        }
    }

    _finishedTaskForChunk(task, taskDuration);
}

/// Update statistics for the Task that finished and the chunk it was querying.
void QueriesAndChunks::_finishedTaskForChunk(wbase::Task::Ptr const& task, double minutes) {
    unique_lock<mutex> ul(_chunkMtx);
    pair<int, ChunkStatistics::Ptr> ele(task->getChunkId(), nullptr);
    auto res = _chunkStats.insert(ele);
    if (res.second) {
        res.first->second = make_shared<ChunkStatistics>(task->getChunkId());
    }
    ul.unlock();
    auto iter = res.first->second;
    proto::ScanInfo& scanInfo = task->getScanInfo();
    string tblName;
    if (!scanInfo.infoTables.empty()) {
        proto::ScanTableInfo& sti = scanInfo.infoTables.at(0);
        tblName = ChunkTableStats::makeTableName(sti.db, sti.table);
    }
    ChunkTableStats::Ptr tableStats = iter->add(tblName, minutes);
}

/// Go through the list of possibly dead queries and remove those that are too old.
void QueriesAndChunks::removeDead() {
    vector<QueryStatistics::Ptr> dList;
    auto now = chrono::system_clock::now();
    {
        shared_ptr<DeadQueriesType> newlyDead;
        {
            lock_guard<mutex> gnd(_newlyDeadMtx);
            newlyDead = _newlyDeadQueries;
            _newlyDeadQueries.reset(new DeadQueriesType);
        }

        lock_guard<mutex> gd(_deadMtx);
        // Copy newlyDead into dead.
        for (auto const& elem : *newlyDead) {
            _deadQueries[elem.first] = elem.second;
        }
        LOGS(_log, LOG_LVL_DEBUG, "QueriesAndChunks::removeDead deadQueries size=" << _deadQueries.size());
        auto iter = _deadQueries.begin();
        while (iter != _deadQueries.end()) {
            auto const& statPtr = iter->second;
            if (statPtr->isDead(_deadAfter, now)) {
                LOGS(_log, LOG_LVL_TRACE, "QueriesAndChunks::removeDead added to list");
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
    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        lock_guard<mutex> gdend(_deadMtx);
        LOGS(_log, LOG_LVL_DEBUG,
             "QueriesAndChunks::removeDead end deadQueries size=" << _deadQueries.size());
    }
}

/// Remove a statistics for a user query.
/// Query Ids should be unique for the life of the system, so erasing
/// a qId multiple times from _queryStats should be harmless.
void QueriesAndChunks::removeDead(QueryStatistics::Ptr const& queryStats) {
    unique_lock<mutex> gS(queryStats->_qStatsMtx);
    QueryId qId = queryStats->queryId;
    gS.unlock();
    LOGS(_log, LOG_LVL_TRACE, "Queries::removeDead");

    lock_guard<mutex> gQ(_queryStatsMtx);
    _queryStats.erase(qId);
}

/// @return the statistics for a user query.
QueryStatistics::Ptr QueriesAndChunks::getStats(QueryId const& qId) const {
    lock_guard<mutex> g(_queryStatsMtx);
    auto iter = _queryStats.find(qId);
    if (iter != _queryStats.end()) {
        return iter->second;
    }
    LOGS(_log, LOG_LVL_WARN, "QueriesAndChunks::getStats could not find qId=" << qId);
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
    vector<QueryStatistics::Ptr> uqs;
    {
        lock_guard<mutex> g(_queryStatsMtx);
        for (auto const& ele : _queryStats) {
            auto const q = ele.second;
            uqs.push_back(q);
        }
    }

    // Go through all Tasks in each query and examine the running ones.
    // If a running Task is taking longer than its percent of total time, boot it.
    // Booting a Task may result in the entire user query it belongs to being moved
    // to the snail scan.
    for (auto const& uq : uqs) {
        // Copy all the running tasks that are on ScanSchedulers.
        vector<wbase::Task::Ptr> runningTasks;
        {
            lock_guard<mutex> lock(uq->_qStatsMtx);
            for (auto&& task : uq->_tasks) {
                if (task->isRunning()) {
                    auto const& sched = dynamic_pointer_cast<wsched::ScanScheduler>(task->getTaskScheduler());
                    if (sched != nullptr) {
                        runningTasks.push_back(task);
                    }
                }
            };
        }

        // For each running task, check if it is taking too long, or if the query is taking too long.
        for (auto const& task : runningTasks) {
            auto const& sched = dynamic_pointer_cast<wsched::ScanScheduler>(task->getTaskScheduler());
            if (sched == nullptr) {
                continue;
            }
            double schedMaxTime = sched->getMaxTimeMinutes();  // Get max time for scheduler
            // Get the slowest scan table in task.
            auto begin = task->getScanInfo().infoTables.begin();
            if (begin == task->getScanInfo().infoTables.end()) {
                continue;
            }
            string const& slowestTable = begin->db + ":" + begin->table;
            auto iterTbl = scanTblSums.find(slowestTable);
            if (iterTbl != scanTblSums.end()) {
                LOGS(_log, LOG_LVL_DEBUG, "examineAll " << slowestTable << " chunkId=" << task->getChunkId());
                ScanTableSums& tblSums = iterTbl->second;
                auto iterChunk = tblSums.chunkPercentages.find(task->getChunkId());
                if (iterChunk != tblSums.chunkPercentages.end()) {
                    // We can only make the check if there's data on past chunks/tables.
                    double percent = iterChunk->second.percent;
                    bool valid = iterChunk->second.valid;
                    double maxTimeChunk = percent * schedMaxTime;
                    auto runTimeMilli = task->getRunTime();
                    double runTimeMinutes = (double)runTimeMilli.count() / 60000.0;
                    bool booting = runTimeMinutes > maxTimeChunk && valid;
                    auto lvl = booting ? LOG_LVL_INFO : LOG_LVL_DEBUG;
                    LOGS(_log, lvl,
                         "examineAll " << (booting ? "booting" : "keeping") << " task " << task->getIdStr()
                                       << "maxTimeChunk(" << maxTimeChunk << ")=percent(" << percent
                                       << ")*schedMaxTime(" << schedMaxTime << ")"
                                       << " runTimeMinutes=" << runTimeMinutes << " valid=" << valid);
                    if (booting && !task->atMaxThreadCount()) {
                        _bootTask(uq, task, sched);
                    }
                }
            }
        }
    }

    LOGS(_log, LOG_LVL_WARN, util::HoldTrack::CheckKeySet());
    LOGS(_log, LOG_LVL_DEBUG, "QueriesAndChunks::examineAll end");
}

nlohmann::json QueriesAndChunks::statusToJson(wbase::TaskSelector const& taskSelector) const {
    nlohmann::json status = nlohmann::json::object();
    {
        auto bSched = _blendSched.lock();
        if (bSched == nullptr) {
            LOGS(_log, LOG_LVL_WARN, "blendSched undefined, can't check user query");
            status["blend_scheduler"] = nlohmann::json::object();
        } else {
            status["blend_scheduler"] = bSched->statusToJson();
        }
    }
    status["query_stats"] = nlohmann::json::object();
    lock_guard<mutex> g(_queryStatsMtx);
    for (auto&& itr : _queryStats) {
        string const qId = to_string(itr.first);  // forcing string type for the json object key
        QueryStatistics::Ptr const& qStats = itr.second;
        status["query_stats"][qId]["histograms"] = qStats->getJsonHist();
        status["query_stats"][qId]["tasks"] = qStats->getJsonTasks(taskSelector);
    }
    return status;
}

nlohmann::json QueriesAndChunks::mySqlThread2task(set<unsigned long> const& activeMySqlThreadIds) const {
    nlohmann::json result = nlohmann::json::object();
    lock_guard<mutex> g(_queryStatsMtx);
    for (auto&& itr : _queryStats) {
        QueryStatistics::Ptr const& qStats = itr.second;
        for (auto&& task : qStats->_tasks) {
            auto const threadId = task->getMySqlThreadId();
            if ((threadId != 0) && activeMySqlThreadIds.contains(threadId)) {
                // Force the identifier to be converted into a string because the JSON library
                // doesn't support numeric keys in its dictionary class.
                result[to_string(threadId)] =
                        nlohmann::json::object({{"query_id", task->getQueryId()},
                                                {"job_id", task->getJobId()},
                                                {"chunk_id", task->getChunkId()},
                                                {"subchunk_id", task->getSubchunkId()},
                                                {"template_id", task->getTemplateId()},
                                                {"state", wbase::taskState2str(task->state())}});
            }
        }
    }
    return result;
}

/// @return a map that contains time totals for all chunks for tasks running on specific
/// tables. The map is sorted by table name and contains sub-maps ordered by chunk id.
/// The sub-maps contain information about how long tasks take to complete on that table
/// on that chunk. These are then used to see what the percentage total of the time it took
/// for tasks on each chunk.
/// The table names are based on the slowest scan table in each task.
QueriesAndChunks::ScanTableSumsMap QueriesAndChunks::_calcScanTableSums() {
    // Copy a vector of all the chunks in the map;
    vector<ChunkStatistics::Ptr> chks;
    {
        lock_guard<mutex> g(_chunkMtx);
        for (auto const& ele : _chunkStats) {
            auto const& chk = ele.second;
            chks.push_back(chk);
        }
    }

    QueriesAndChunks::ScanTableSumsMap scanTblSums;
    for (auto const& chunkStats : chks) {
        auto chunkId = chunkStats->_chunkId;
        lock_guard<mutex> lock(chunkStats->_tStatsMtx);
        for (auto const& ele : chunkStats->_tableStats) {
            auto const& tblName = ele.first;
            if (!tblName.empty()) {
                auto& sTSums = scanTblSums[tblName];
                auto data = ele.second->getData();
                sTSums.totalTime += data.avgCompletionTime;
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
    LOGS(_log, LOG_LVL_INFO, "taking too long, booting from " << sched->getName());
    sched->removeTask(task, true);
    uq->_tasksBooted += 1;

    auto bSched = _blendSched.lock();
    if (bSched == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "blendSched undefined, can't check user query");
        return;
    }
    if (bSched->isScanSnail(sched)) {
        // If it's already on the snail scan, it has already been booted from another scan.
        if (uq->_tasksBooted > _maxTasksBooted + 1) {
            LOGS(_log, LOG_LVL_WARN,
                 "User Query taking excessive amount of time on snail scan and should be cancelled");
            // TODO: Add code to send message back to czar to cancel this user query.
        }
    } else {
        // Disabled as too aggressive and vulnerable to bad statistics. DM-11526
        if (false && uq->_tasksBooted > _maxTasksBooted) {
            LOGS(_log, LOG_LVL_INFO,
                 "entire UserQuery booting from " << sched->getName() << " tasksBooted=" << uq->_tasksBooted
                                                  << " maxTasksBooted=" << _maxTasksBooted);
            uq->_queryBooted = true;
            bSched->moveUserQueryToSnail(uq->queryId, sched);
        }
    }
}

/// Add a Task to the user query statistics.
void QueryStatistics::addTask(wbase::Task::Ptr const& task) {
    lock_guard<mutex> guard(_qStatsMtx);
    if (queryId != task->getQueryId()) {
        string const msg = "QueryStatistics::" + string(__func__) +
                           ": the task has queryId=" + to_string(task->getQueryId()) +
                           " where queryId=" + to_string(queryId) + " was expected.";
        throw util::Bug(ERR_LOC, msg);
    }
    _tasks.push_back(task);
}

/// @return the number of Tasks that have been booted for this user query.
int QueryStatistics::getTasksBooted() {
    lock_guard<mutex> guard(_qStatsMtx);
    return _tasksBooted;
}

/// @return true if this query is done and has not been touched for deadTime.
bool QueryStatistics::isDead(chrono::seconds deadTime, chrono::system_clock::time_point now) {
    lock_guard<mutex> guard(_qStatsMtx);
    if (_isMostlyDead()) {
        if (now - _touched > deadTime) {
            return true;
        }
    }
    return false;
}

/// @return true if all Tasks for this query are complete.
/// Precondition, _qStatsMtx must be locked.
bool QueryStatistics::_isMostlyDead() const { return _tasksCompleted >= _size; }

ostream& operator<<(ostream& os, QueryStatistics const& q) {
    lock_guard<mutex> gd(q._qStatsMtx);
    os << QueryIdHelper::makeIdStr(q.queryId) << " time=" << q._totalTimeMinutes << " size=" << q._size
       << " tasksCompleted=" << q._tasksCompleted << " tasksRunning=" << q._tasksRunning
       << " tasksBooted=" << q._tasksBooted;
    return os;
}

void QueryStatistics::addTaskTransmit(double timeSeconds, int64_t bytesTransmitted, int64_t rowsTransmitted,
                                      double bufferFillSecs) {
    _histTimeTransmittingPerTask->addEntry(timeSeconds);
    _histRowsPerTask->addEntry(rowsTransmitted);
    _histSizePerTask->addEntry(bytesTransmitted);
    _histTimeBufferFillPerTask->addEntry(bufferFillSecs);
}

void QueryStatistics::addTaskRunQuery(double runTimeSeconds, double subchunkRunTimeSeconds) {
    _histTimeRunningPerTask->addEntry(runTimeSeconds);
    _histTimeSubchunkPerTask->addEntry(subchunkRunTimeSeconds);
}

/// Remove all Tasks belonging to the user query 'qId' from the queue of 'sched'.
/// If 'sched' is null, then Tasks will be removed from the queue of whatever scheduler
/// they are queued on.
/// Tasks that are already running continue, but are marked as complete on their
/// current scheduler. Stopping and rescheduling them would be difficult at best, and
/// probably not very helpful.
vector<wbase::Task::Ptr> QueriesAndChunks::removeQueryFrom(QueryId const& qId,
                                                           wsched::SchedulerBase::Ptr const& sched) {
    vector<wbase::Task::Ptr> removedList;  // Return value;

    // Find the user query.
    unique_lock<mutex> lock(_queryStatsMtx);
    auto query = _queryStats.find(qId);
    if (query == _queryStats.end()) {
        LOGS(_log, LOG_LVL_DEBUG, "was not found by removeQueryFrom");
        return removedList;
    }
    lock.unlock();

    // Remove Tasks from their scheduler put them on 'removedList', but only if their Scheduler is the same
    // as 'sched' or if sched == nullptr.
    vector<wbase::Task::Ptr> taskList;
    {
        lock_guard<mutex> taskLock(query->second->_qStatsMtx);
        taskList = query->second->_tasks;
    }
    auto moveTasks = [&sched, &taskList, &removedList](bool moveRunning) {
        vector<wbase::Task::Ptr> taskListNotRemoved;
        for (auto const& task : taskList) {
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

ostream& operator<<(ostream& os, QueriesAndChunks const& qc) {
    lock_guard<mutex> g(qc._chunkMtx);
    os << "Chunks(";
    for (auto const& ele : qc._chunkStats) {
        os << *(ele.second) << ";";
    }
    os << ")";
    return os;
}

/// Add the duration to the statistics for the table. Create a statistics object if needed.
/// @return the statistics for the table.
ChunkTableStats::Ptr ChunkStatistics::add(string const& scanTableName, double minutes) {
    pair<string, ChunkTableStats::Ptr> ele(scanTableName, nullptr);
    unique_lock<mutex> ul(_tStatsMtx);
    auto res = _tableStats.insert(ele);
    auto iter = res.first;
    if (res.second) {
        iter->second = make_shared<ChunkTableStats>(_chunkId, scanTableName);
    }
    ul.unlock();
    iter->second->addTaskFinished(minutes);
    return iter->second;
}

/// @return the statistics for a table. nullptr if the table is not found.
ChunkTableStats::Ptr ChunkStatistics::getStats(string const& scanTableName) const {
    lock_guard<mutex> g(_tStatsMtx);
    auto iter = _tableStats.find(scanTableName);
    if (iter != _tableStats.end()) {
        return iter->second;
    }
    return nullptr;
}

ostream& operator<<(ostream& os, ChunkStatistics const& cs) {
    lock_guard<mutex> g(cs._tStatsMtx);
    os << "ChunkStatsistics(" << cs._chunkId << "(";
    for (auto const& ele : cs._tableStats) {
        os << *(ele.second) << ";";
    }
    os << ")";
    return os;
}

/// Use the duration of the last Task completed to adjust the average completion time.
void ChunkTableStats::addTaskFinished(double minutes) {
    lock_guard<mutex> g(_dataMtx);
    ++_data.tasksCompleted;
    if (_data.tasksCompleted > 1) {
        _data.avgCompletionTime = (_data.avgCompletionTime * _weightAvg + minutes * _weightNew) / _weightSum;
    } else {
        _data.avgCompletionTime = minutes;
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "ChkId=" << _chunkId << ":tbl=" << _scanTableName << " completed=" << _data.tasksCompleted
                  << " avgCompletionTime=" << _data.avgCompletionTime);
}

ostream& operator<<(ostream& os, ChunkTableStats const& cts) {
    lock_guard<mutex> g(cts._dataMtx);
    os << "ChunkTableStats(" << cts._chunkId << ":" << cts._scanTableName
       << " tasks(completed=" << cts._data.tasksCompleted << ",avgTime=" << cts._data.avgCompletionTime
       << ",booted=" << cts._data.tasksBooted << "))";
    return os;
}

}  // namespace lsst::qserv::wpublish
