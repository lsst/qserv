// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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
/**
 * @file
 *
 * @brief Task is a bundle of query task fields
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "wbase/Task.h"

// System headers
#include <ctime>
#include <stdexcept>

// Third-party headers
#include <boost/algorithm/string/replace.hpp>
#include "boost/filesystem.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "global/LogContext.h"
#include "global/UnsupportedError.h"
#include "mysql/MySqlConfig.h"
#include "proto/worker.pb.h"
#include "util/Bug.h"
#include "util/common.h"
#include "util/HoldTrack.h"
#include "util/IterableFormatter.h"
#include "util/ResultFileName.h"
#include "util/TimeUtils.h"
#include "wbase/Base.h"
#include "wbase/FileChannelShared.h"
#include "wbase/UserQueryInfo.h"
#include "wconfig/WorkerConfig.h"
#include "wdb/QueryRunner.h"
#include "wpublish/QueriesAndChunks.h"

using namespace std;
using namespace std::chrono_literals;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.Task");

string buildResultFileName(shared_ptr<lsst::qserv::proto::TaskMsg> const& taskMsg) {
    auto const resultFileName =
            lsst::qserv::util::ResultFileName(taskMsg->czarid(), taskMsg->queryid(), taskMsg->jobid(),
                                              taskMsg->chunkid(), taskMsg->attemptcount());
    return resultFileName.fileName();
}

string buildResultFilePath(string const& resultFileName, string const& resultsDirname) {
    if (resultsDirname.empty()) return resultsDirname;
    return fs::weakly_canonical(fs::path(resultsDirname) / resultFileName).string();
}

size_t const MB_SIZE_BYTES = 1024 * 1024;

}  // namespace

namespace lsst::qserv::wbase {

string const Task::_fqdn = util::get_current_host_fqdn();

// Task::ChunkEqual functor
bool Task::ChunkEqual::operator()(Task::Ptr const& x, Task::Ptr const& y) {
    if (!x || !y) {
        return false;
    }
    return x->_chunkId == y->_chunkId;
}

// Task::PtrChunkIdGreater functor
bool Task::ChunkIdGreater::operator()(Task::Ptr const& x, Task::Ptr const& y) {
    if (!x || !y) {
        return false;
    }
    return x->_chunkId > y->_chunkId;
}

string const Task::defaultUser = "qsmaster";
IdSet Task::allIds{};

TaskScheduler::TaskScheduler() {
    auto hour = chrono::milliseconds(1h);
    histTimeOfRunningTasks = util::HistogramRolling::Ptr(
            new util::HistogramRolling("RunningTaskTimes", {0.1, 1.0, 10.0, 100.0, 200.0}, hour, 10'000));
    histTimeOfTransmittingTasks = util::HistogramRolling::Ptr(new util::HistogramRolling(
            "TransmittingTaskTime", {0.1, 1.0, 10.0, 60.0, 600.0, 1200.0}, hour, 10'000));
}

atomic<uint32_t> taskSequence{0};  ///< Unique identifier source for Task.

/// When the constructor is called, there is not enough information
/// available to define the action to take when this task is run, so
/// Command::setFunc() is used set the action later. This is why
/// the util::CommandThreadPool is not called here.
Task::Task(TaskMsgPtr const& t, int fragmentNumber, shared_ptr<UserQueryInfo> const& userQueryInfo,
           size_t templateId, int subchunkId, shared_ptr<FileChannelShared> const& sc,
           uint16_t resultsHttpPort)
        : _userQueryInfo(userQueryInfo),
          _sendChannel(sc),
          _tSeq(++taskSequence),
          _qId(t->queryid()),
          _templateId(templateId),
          _hasChunkId(t->has_chunkid()),
          _chunkId(t->has_chunkid() ? t->chunkid() : -1),
          _subchunkId(subchunkId),
          _jId(t->jobid()),
          _attemptCount(t->attemptcount()),
          _queryFragmentNum(fragmentNumber),
          _fragmentHasSubchunks(t->fragment(fragmentNumber).has_subchunks()),
          _hasDb(t->has_db()),
          _db(t->has_db() ? t->db() : ""),
          _czarId(t->has_czarid() ? t->czarid() : -1) {
    // These attributes will be passed back to Czar in the Protobuf response
    // to advice which result delivery channel to use.
    auto const workerConfig = wconfig::WorkerConfig::instance();
    _resultFileName = ::buildResultFileName(t);
    _resultFileAbsPath = ::buildResultFilePath(_resultFileName, workerConfig->resultsDirname());
    _resultFileHttpUrl = "http://" + _fqdn + ":" + to_string(resultsHttpPort) + "/" + _resultFileName;
    if (t->has_user()) {
        user = t->user();
    } else {
        user = defaultUser;
    }

    allIds.add(to_string(_qId) + "_" + to_string(_jId));
    LOGS(_log, LOG_LVL_DEBUG, "Task(...) " << "this=" << this << " : " << allIds);

    // Determine which major tables this task will use.
    int const size = t->scantable_size();
    for (int j = 0; j < size; ++j) {
        _scanInfo.infoTables.push_back(proto::ScanTableInfo(t->scantable(j)));
    }
    _scanInfo.scanRating = t->scanpriority();
    _scanInfo.sortTablesSlowestFirst();
    _scanInteractive = t->scaninteractive();
    _maxTableSize = t->maxtablesize_mb() * ::MB_SIZE_BYTES;

    // Create sets and vectors for 'aquiring' subchunk temporary tables.
    proto::TaskMsg_Fragment const& fragment(t->fragment(_queryFragmentNum));
    DbTableSet dbTbls_;
    IntVector subchunksVect_;
    if (!_fragmentHasSubchunks) {
        /// FUTURE: Why acquire anything if there are no subchunks in the fragment?
        ///   This branch never seems to happen, but this needs to be proven beyond any doubt.
        LOGS(_log, LOG_LVL_WARN, "Task::Task not _fragmentHasSubchunks");
        for (auto const& scanTbl : t->scantable()) {
            dbTbls_.emplace(scanTbl.db(), scanTbl.table());
            LOGS(_log, LOG_LVL_INFO,
                 "Task::Task scanTbl.db()=" << scanTbl.db() << " scanTbl.table()=" << scanTbl.table());
        }
        LOGS(_log, LOG_LVL_INFO,
             "fragment a db=" << _db << ":" << _chunkId << " dbTbls=" << util::printable(dbTbls_));
    } else {
        proto::TaskMsg_Subchunk const& sc = fragment.subchunks();
        for (int j = 0; j < sc.dbtbl_size(); j++) {
            /// Different subchunk fragments can require different tables.
            /// FUTURE: It may save space to store these in UserQueryInfo as it seems
            ///         database and table names are consistent across chunks.
            dbTbls_.emplace(sc.dbtbl(j).db(), sc.dbtbl(j).tbl());
            LOGS(_log, LOG_LVL_TRACE,
                 "Task::Task subchunk j=" << j << " sc.dbtbl(j).db()=" << sc.dbtbl(j).db()
                                          << " sc.dbtbl(j).tbl()=" << sc.dbtbl(j).tbl());
        }
        IntVector sVect(sc.id().begin(), sc.id().end());
        subchunksVect_ = sVect;
        if (sc.has_database()) {
            _db = sc.database();
        } else {
            _db = t->db();
        }
        LOGS(_log, LOG_LVL_DEBUG,
             "fragment b db=" << _db << ":" << _chunkId << " dbTableSet" << util::printable(dbTbls_)
                              << " subChunks=" << util::printable(subchunksVect_));
    }
    _dbTblsAndSubchunks = make_unique<DbTblsAndSubchunks>(dbTbls_, subchunksVect_);
    if (_sendChannel == nullptr) {
        throw util::Bug(ERR_LOC, "Task::Task _sendChannel==null " + getIdStr());
    }
}

Task::~Task() {
    allIds.remove(to_string(_qId) + "_" + to_string(_jId));
    LOGS(_log, LOG_LVL_TRACE, "~Task() : " << allIds);

    _userQueryInfo.reset();
    UserQueryInfo::uqMapErase(_qId);
    if (UserQueryInfo::uqMapGet(_qId) == nullptr) {
        LOGS(_log, LOG_LVL_TRACE, "~Task Cleared uqMap entry for _qId=" << _qId);
    }
}

vector<Task::Ptr> Task::createTasks(shared_ptr<proto::TaskMsg> const& taskMsg,
                                    shared_ptr<wbase::FileChannelShared> const& sendChannel,
                                    shared_ptr<wdb::ChunkResourceMgr> const& chunkResourceMgr,
                                    mysql::MySqlConfig const& mySqlConfig,
                                    shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
                                    shared_ptr<wpublish::QueriesAndChunks> const& queriesAndChunks,
                                    uint16_t resultsHttpPort) {
    QueryId qId = taskMsg->queryid();
    QSERV_LOGCONTEXT_QUERY_JOB(qId, taskMsg->jobid());
    vector<Task::Ptr> vect;

    UserQueryInfo::Ptr userQueryInfo = UserQueryInfo::uqMapInsert(qId);

    /// Make one task for each fragment.
    int fragmentCount = taskMsg->fragment_size();
    if (fragmentCount < 1) {
        throw util::Bug(ERR_LOC, "Task::createTasks No fragments to execute in TaskMsg");
    }

    string const chunkIdStr = to_string(taskMsg->chunkid());
    for (int fragNum = 0; fragNum < fragmentCount; ++fragNum) {
        proto::TaskMsg_Fragment const& fragment = taskMsg->fragment(fragNum);
        for (string queryStr : fragment.query()) {
            size_t templateId = userQueryInfo->addTemplate(queryStr);
            if (fragment.has_subchunks() && not fragment.subchunks().id().empty()) {
                for (auto subchunkId : fragment.subchunks().id()) {
                    auto task = make_shared<wbase::Task>(taskMsg, fragNum, userQueryInfo, templateId,
                                                         subchunkId, sendChannel, resultsHttpPort);
                    vect.push_back(task);
                }
            } else {
                int subchunkId = -1;  // there are no subchunks.
                auto task = make_shared<wbase::Task>(taskMsg, fragNum, userQueryInfo, templateId, subchunkId,
                                                     sendChannel, resultsHttpPort);
                vect.push_back(task);
            }
        }
    }
    for (auto task : vect) {
        // newQueryRunner sets the `_taskQueryRunner` pointer in `task`.
        task->setTaskQueryRunner(wdb::QueryRunner::newQueryRunner(task, chunkResourceMgr, mySqlConfig,
                                                                  sqlConnMgr, queriesAndChunks));
    }
    sendChannel->setTaskCount(vect.size());

    return vect;
}

void Task::action(util::CmdData* data) {
    string tIdStr = getIdStr();
    if (_queryStarted.exchange(true)) {
        LOGS(_log, LOG_LVL_WARN, "task was already started " << tIdStr);
        return;
    }

    if (_unitTest) {
        LOGS(_log, LOG_LVL_ERROR,
             __func__ << " Command::_func has been set, this should only happen in unit tests.");
        _func(data);
        return;
    }

    // Get a local copy for safety.
    auto qr = _taskQueryRunner;
    bool success = false;
    try {
        success = qr->runQuery();
    } catch (UnsupportedError const& e) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " runQuery threw UnsupportedError " << e.what() << tIdStr);
    }
    if (not success) {
        LOGS(_log, LOG_LVL_ERROR, "runQuery failed " << tIdStr);
        if (not getSendChannel()->kill("Foreman::_setRunFunc")) {
            LOGS(_log, LOG_LVL_WARN, "runQuery sendChannel already killed " << tIdStr);
        }
    }

    // The QueryRunner class access to sendChannel for results is over by this point.
    // 'task' contains statistics that are still useful. However, the resources used
    // by sendChannel need to be freed quickly.
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " calling resetSendChannel() for " << tIdStr);
    resetSendChannel();  // Frees its xrdsvc::SsiRequest object.
}

string Task::getQueryString() const {
    string qs = _userQueryInfo->getTemplate(_templateId);
    boost::algorithm::replace_all(qs, CHUNK_TAG, to_string(_chunkId));
    boost::algorithm::replace_all(qs, SUBCHUNK_TAG, to_string(_subchunkId));
    return qs;
}

void Task::setQueryStatistics(wpublish::QueryStatistics::Ptr const& qStats) { _queryStats = qStats; }

wpublish::QueryStatistics::Ptr Task::getQueryStats() const {
    auto qStats = _queryStats.lock();
    if (qStats == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "Task::getQueryStats() _queryStats==null " << getIdStr());
    }
    return qStats;
}

/// Flag the Task as cancelled, try to stop the SQL query, and try to remove it from the schedule.
void Task::cancel() {
    if (_cancelled.exchange(true)) {
        // Was already cancelled.
        return;
    }

    util::HoldTrack::Mark markA(ERR_LOC, "Task::cancel");
    LOGS(_log, LOG_LVL_DEBUG, "Task::cancel " << getIdStr());
    auto qr = _taskQueryRunner;  // Need a copy in case _taskQueryRunner is reset.
    if (qr != nullptr) {
        qr->cancel();
    }

    // At this point, this code doesn't do anything. It may be
    // useful to remove this task from the scheduler, but it
    // seems doubtful that that would improve performance.
    auto sched = _taskScheduler.lock();
    if (sched != nullptr) {
        sched->taskCancelled(this);
    }
}

bool Task::checkCancelled() {
    // A czar doesn't directly tell the worker the query is dead.
    // A czar has XrdSsi kill the SsiRequest, which kills the
    // sendChannel used by this task. sendChannel can be killed
    // in other ways, however, without the sendChannel, this task
    // has no way to return anything to the originating czar and
    // may as well give up now.
    if (_sendChannel == nullptr || _sendChannel->isDead()) {
        // The sendChannel is dead, probably squashed by the czar.
        cancel();
    }
    return _cancelled;
}

/// @return true if task has already been cancelled.
bool Task::setTaskQueryRunner(TaskQueryRunner::Ptr const& taskQueryRunner) {
    _taskQueryRunner = taskQueryRunner;
    return checkCancelled();
}

void Task::freeTaskQueryRunner(TaskQueryRunner* tqr) {
    if (_taskQueryRunner.get() == tqr) {
        _taskQueryRunner.reset();
    } else {
        LOGS(_log, LOG_LVL_WARN, "Task::freeTaskQueryRunner pointer didn't match!");
    }
}

/// Set values associated with the Task being put on the queue.
void Task::queued(chrono::system_clock::time_point const& now) {
    lock_guard<mutex> guard(_stateMtx);
    _state = TaskState::QUEUED;
    _queueTime = now;
}

bool Task::isRunning() const {
    lock_guard<mutex> lock(_stateMtx);
    switch (_state) {
        case TaskState::STARTED:
        case TaskState::EXECUTING_QUERY:
        case TaskState::READING_DATA:
            return true;
        default:
            return false;
    }
}

void Task::started(chrono::system_clock::time_point const& now) {
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " " << getIdStr() << " started");
    lock_guard<mutex> guard(_stateMtx);
    _state = TaskState::STARTED;
    _startTime = now;
}

void Task::queryExecutionStarted() {
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " " << getIdStr() << " executing");
    lock_guard<mutex> guard(_stateMtx);
    _state = TaskState::EXECUTING_QUERY;
    _queryExecTime = chrono::system_clock::now();
}

void Task::queried() {
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " " << getIdStr() << " reading");
    lock_guard<mutex> guard(_stateMtx);
    _state = TaskState::READING_DATA;
    _queryTime = chrono::system_clock::now();
    // Reset finish time as it might be already set when the task got booted off
    // a scheduler.
    _finishTime = chrono::system_clock::time_point();
}

/// Set values associated with the Task being finished.
/// @return milliseconds to complete the Task, system clock time.
chrono::milliseconds Task::finished(chrono::system_clock::time_point const& now) {
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " " << getIdStr() << " finished");
    chrono::milliseconds duration;
    {
        lock_guard<mutex> guard(_stateMtx);
        _finishTime = now;
        _state = TaskState::FINISHED;
        duration = chrono::duration_cast<chrono::milliseconds>(_finishTime - _startTime);
    }
    // Ensure that the duration is greater than 0.
    if (duration.count() < 1) {
        duration = chrono::milliseconds{1};
    }
    LOGS(_log, LOG_LVL_DEBUG, "processing millisecs=" << duration.count());
    return duration;
}

chrono::milliseconds Task::getRunTime() const {
    lock_guard<mutex> guard(_stateMtx);
    switch (_state) {
        case TaskState::FINISHED:
            return chrono::duration_cast<chrono::milliseconds>(_finishTime - _startTime);
        case TaskState::STARTED:
        case TaskState::EXECUTING_QUERY:
        case TaskState::READING_DATA:
            return chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now() - _startTime);
        default:
            return chrono::milliseconds(0);
    }
}

bool Task::setBooted() {
    bool alreadyBooted = _booted.exchange(true);
    if (!alreadyBooted) {
        _bootedTime = CLOCK::now();
    }
    return alreadyBooted;
}

nlohmann::json Task::getJson() const {
    // It would be nice to have the _queryString in this, but that could make the results very large.
    nlohmann::json js;
    js["czarId"] = _czarId;
    js["queryId"] = _qId;
    js["templateId"] = getTemplateId();
    js["jobId"] = _jId;
    js["chunkId"] = getChunkId();
    js["subChunkId"] = getSubchunkId();
    js["fragmentId"] = _queryFragmentNum;
    js["attemptId"] = _attemptCount;
    js["sequenceId"] = _tSeq;
    js["scanInteractive"] = _scanInteractive;
    js["maxTableSize"] = _maxTableSize;
    js["cancelled"] = to_string(_cancelled);
    js["state"] = static_cast<uint64_t>(_state.load());
    js["createTime_msec"] = util::TimeUtils::tp2ms(_createTime);
    js["queueTime_msec"] = util::TimeUtils::tp2ms(_queueTime);
    js["startTime_msec"] = util::TimeUtils::tp2ms(_startTime);
    js["queryExecTime_msec"] = util::TimeUtils::tp2ms(_queryExecTime);
    js["queryTime_msec"] = util::TimeUtils::tp2ms(_queryTime);
    js["finishTime_msec"] = util::TimeUtils::tp2ms(_finishTime);
    js["sizeSoFar"] = _totalSize;
    js["mysqlThreadId"] = _mysqlThreadId.load();
    js["booted"] = _booted.load() ? 1 : 0;
    js["bootedTime_msec"] = util::TimeUtils::tp2ms(_bootedTime);
    auto const scheduler = getTaskScheduler();
    js["scheduler"] = scheduler == nullptr ? "" : scheduler->getName();
    return js;
}

ostream& operator<<(ostream& os, Task const& t) {
    os << "Task: "
       << "msg: " << t.getIdStr() << " chunk=" << t._chunkId << " db=" << t._db << " " << t.getQueryString();

    return os;
}

ostream& operator<<(ostream& os, IdSet const& idSet) {
    // Limiting output as number of entries can be very large.
    int maxDisp = idSet.maxDisp;  // only affects the amount of data printed.
    lock_guard<mutex> lock(idSet.mx);
    os << "showing " << maxDisp << " of count=" << idSet._ids.size() << " ";
    bool first = true;
    int i = 0;
    for (auto id : idSet._ids) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << id;
        if (++i >= maxDisp) break;
    }
    return os;
}

}  // namespace lsst::qserv::wbase
