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

// Third-party headers
#include "boost/regex.hpp"
#include <boost/algorithm/string/replace.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "global/LogContext.h"
#include "proto/TaskMsgDigest.h"
#include "proto/worker.pb.h"
#include "util/Bug.h"
#include "util/IterableFormatter.h"
#include "wbase/Base.h"
#include "wbase/SendChannelShared.h"
#include "wbase/UserQueryInfo.h"
#include "wpublish/QueriesAndChunks.h"

using namespace std;
using namespace std::chrono_literals;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.Task");

/**
 * @param tp The timepoint to be converted.
 * @return The number of milliseconds since UNIX Epoch
 */
uint64_t tp2ms(std::chrono::system_clock::time_point const& tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

}  // namespace

namespace lsst::qserv::wbase {

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

std::string const Task::defaultUser = "qsmaster";
IdSet Task::allIds{};

TaskScheduler::TaskScheduler() {
    auto hour = std::chrono::milliseconds(1h);
    histTimeOfRunningTasks = util::HistogramRolling::Ptr(
            new util::HistogramRolling("RunningTaskTimes", {0.1, 1.0, 10.0, 100.0, 200.0}, hour, 10'000));
    histTimeOfTransmittingTasks = util::HistogramRolling::Ptr(new util::HistogramRolling(
            "TransmittingTaskTime", {0.1, 1.0, 10.0, 60.0, 600.0, 1200.0}, hour, 10'000));
}

std::atomic<uint32_t> taskSequence{0};

/// When the constructor is called, there is not enough information
/// available to define the action to take when this task is run, so
/// Command::setFunc() is used set the action later. This is why
/// the util::CommandThreadPool is not called here.
Task::Task(TaskMsgPtr const& t, int fragmentNumber, std::shared_ptr<UserQueryInfo> const& userQueryInfo,
           size_t templateId, int subchunkId, std::shared_ptr<SendChannelShared> const& sc)
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
          _session(t->has_session() ? t->session() : -1),
          _hasDb(t->has_db()),
          _db(t->has_db() ? t->db() : ""),
          _protocol(t->has_protocol() ? t->protocol() : -1),
          _czarId(t->has_czarid() ? t->czarid() : -1) {
    if (t->has_user()) {
        user = t->user();
    } else {
        user = defaultUser;
    }

    allIds.add(std::to_string(_qId) + "_" + std::to_string(_jId));
    LOGS(_log, LOG_LVL_DEBUG,
         "Task(...) "
                 << "this=" << this << " : " << allIds);

    // Determine which major tables this task will use.
    int const size = t->scantable_size();
    for (int j = 0; j < size; ++j) {
        _scanInfo.infoTables.push_back(proto::ScanTableInfo(t->scantable(j)));
    }
    _scanInfo.scanRating = t->scanpriority();
    _scanInfo.sortTablesSlowestFirst();
    _scanInteractive = t->scaninteractive();

    // Create sets and vectors for 'aquiring' subchunk temporary tables.
    proto::TaskMsg_Fragment const& fragment(t->fragment(_queryFragmentNum));
    if (!_fragmentHasSubchunks) {
        /// FUTURE: Why acquire anything if there are no subchunks in the fragment?
        ///   This branch never seems to happen, but this needs to be proven beyond any doubt.
        LOGS(_log, LOG_LVL_WARN, "Task::Task not _fragmentHasSubchunks");
        for (auto const& scanTbl : t->scantable()) {
            _dbTbls.emplace(scanTbl.db(), scanTbl.table());
            LOGS(_log, LOG_LVL_INFO,
                 "Task::Task scanTbl.db()=" << scanTbl.db() << " scanTbl.table()=" << scanTbl.table());
        }
        LOGS(_log, LOG_LVL_INFO,
             "fragment a db=" << _db << ":" << _chunkId << " dbTbls=" << util::printable(_dbTbls));
    } else {
        proto::TaskMsg_Subchunk const& sc = fragment.subchunks();
        for (int j = 0; j < sc.dbtbl_size(); j++) {
            /// Different subchunk fragments can require different tables.
            /// FUTURE: It may save space to store these in UserQueryInfo as it seems
            ///         database and table names are consistent across chunks.
            _dbTbls.emplace(sc.dbtbl(j).db(), sc.dbtbl(j).tbl());
            LOGS(_log, LOG_LVL_TRACE,
                 "Task::Task subchunk j=" << j << " sc.dbtbl(j).db()=" << sc.dbtbl(j).db()
                                          << " sc.dbtbl(j).tbl()=" << sc.dbtbl(j).tbl());
        }
        IntVector sVect(sc.id().begin(), sc.id().end());
        _subchunksVect = sVect;
        if (sc.has_database()) {
            _db = sc.database();
        } else {
            _db = t->db();
        }
        LOGS(_log, LOG_LVL_DEBUG,
             "fragment b db=" << _db << ":" << _chunkId << " dbTableSet" << util::printable(_dbTbls)
                              << " subChunks=" << util::printable(_subchunksVect));
    }
}

Task::~Task() {
    allIds.remove(std::to_string(_qId) + "_" + std::to_string(_jId));
    LOGS(_log, LOG_LVL_TRACE, "~Task() : " << allIds);

    _userQueryInfo.reset();
    UserQueryInfo::uqMapErase(_qId);
    if (UserQueryInfo::uqMapGet(_qId) == nullptr) {
        LOGS(_log, LOG_LVL_TRACE, "~Task Cleared uqMap entry for _qId=" << _qId);
    }
}

std::vector<Task::Ptr> Task::createTasks(std::shared_ptr<proto::TaskMsg> const& taskMsg,
                                         std::shared_ptr<wbase::SendChannelShared> const& sendChannel) {
    QueryId qId = taskMsg->queryid();
    QSERV_LOGCONTEXT_QUERY_JOB(qId, taskMsg->jobid());
    std::vector<Task::Ptr> vect;

    UserQueryInfo::Ptr userQueryInfo = UserQueryInfo::uqMapInsert(qId);

    /// Make one task for each fragment.
    int fragmentCount = taskMsg->fragment_size();
    if (fragmentCount < 1) {
        throw util::Bug(ERR_LOC, "Task::createTasks No fragments to execute in TaskMsg");
    }

    string const chunkIdStr = to_string(taskMsg->chunkid());
    for (int fragNum = 0; fragNum < fragmentCount; ++fragNum) {
        proto::TaskMsg_Fragment const& fragment = taskMsg->fragment(fragNum);
        for (std::string queryStr : fragment.query()) {
            size_t templateId = userQueryInfo->addTemplate(queryStr);
            if (fragment.has_subchunks() && not fragment.subchunks().id().empty()) {
                for (auto subchunkId : fragment.subchunks().id()) {
                    auto task = std::make_shared<wbase::Task>(taskMsg, fragNum, userQueryInfo, templateId,
                                                              subchunkId, sendChannel);
                    vect.push_back(task);
                }
            } else {
                int subchunkId = -1;  // there are no subchunks.
                auto task = std::make_shared<wbase::Task>(taskMsg, fragNum, userQueryInfo, templateId,
                                                          subchunkId, sendChannel);
                vect.push_back(task);
            }
        }
    }
    sendChannel->setTaskCount(vect.size());

    return vect;
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

/// @return the chunkId for this task. If the task has no chunkId, return -1.
int Task::getChunkId() const { return _chunkId; }

/// Flag the Task as cancelled, try to stop the SQL query, and try to remove it from the schedule.
void Task::cancel() {
    if (_cancelled.exchange(true)) {
        // Was already cancelled.
        return;
    }

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
void Task::queued(std::chrono::system_clock::time_point const& now) {
    std::lock_guard<std::mutex> guard(_stateMtx);
    _state = State::QUEUED;
    _queueTime = now;
}

bool Task::isRunning() const {
    std::lock_guard<std::mutex> lock(_stateMtx);
    switch (_state) {
        case State::EXECUTING_QUERY:
        case State::READING_DATA:
            return true;
        default:
            return false;
    }
}

/// Set values associated with the Task being started.
void Task::started(std::chrono::system_clock::time_point const& now) {
    std::lock_guard<std::mutex> guard(_stateMtx);
    _state = State::EXECUTING_QUERY;
    _startTime = now;
}

void Task::queried() {
    std::lock_guard<std::mutex> guard(_stateMtx);
    _state = State::READING_DATA;
    _queryTime = std::chrono::system_clock::now();
    // Reset finish time as it might be already set when the task got booted off
    // a scheduler.
    _finishTime = std::chrono::system_clock::time_point();
}

/// Set values associated with the Task being finished.
/// @return milliseconds to complete the Task, system clock time.
std::chrono::milliseconds Task::finished(std::chrono::system_clock::time_point const& now) {
    std::chrono::milliseconds duration;
    {
        std::lock_guard<std::mutex> guard(_stateMtx);
        _finishTime = now;
        _state = State::FINISHED;
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(_finishTime - _startTime);
    }
    // Ensure that the duration is greater than 0.
    if (duration.count() < 1) {
        duration = std::chrono::milliseconds{1};
    }
    LOGS(_log, LOG_LVL_DEBUG, "processing millisecs=" << duration.count());
    return duration;
}

std::chrono::milliseconds Task::getRunTime() const {
    std::lock_guard<std::mutex> guard(_stateMtx);
    switch (_state) {
        case State::FINISHED:
            return std::chrono::duration_cast<std::chrono::milliseconds>(_finishTime - _startTime);
        case State::EXECUTING_QUERY:
        case State::READING_DATA:
            return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() -
                                                                         _startTime);
        default:
            return std::chrono::milliseconds(0);
    }
}

/// Wait for MemMan to finish reserving resources. The mlock call can take several seconds
/// and only one mlock call can be running at a time. Further, queries finish slightly faster
/// if they are mlock'ed in the same order they were scheduled, hence the ulockEvents
/// EventThread and CommandMlock class.
void Task::waitForMemMan() {
    if (_memMan != nullptr) {
        if (_memMan->lock(_memHandle, true)) {
            int errorCode = (errno == EAGAIN ? ENOMEM : errno);
            LOGS(_log, LOG_LVL_WARN,
                 "mlock err=" << errorCode << " " << _memMan->getStatistics().logString() << " "
                              << _memMan->getStatus(_memHandle).logString());
        }
        LOGS(_log, LOG_LVL_DEBUG,
             "waitForMemMan " << _memMan->getStatistics().logString() << " "
                              << _memMan->getStatus(_memHandle).logString());
    }
    _safeToMoveRunning = true;
}

memman::MemMan::Status Task::getMemHandleStatus() {
    if (_memMan == nullptr || !hasMemHandle()) {
        return memman::MemMan::Status();
    }
    return _memMan->getStatus(_memHandle);
}

nlohmann::json Task::getJson() const {
    // It would be nice to have the _queryString in this, but that could make the results very large.
    nlohmann::json js;
    js["queryId"] = _qId;
    js["jobId"] = _jId;
    js["chunkId"] = getChunkId();
    js["fragmentId"] = _queryFragmentNum;
    js["attemptId"] = _attemptCount;
    js["sequenceId"] = _tSeq;
    js["scanInteractive"] = _scanInteractive;
    js["cancelled"] = to_string(_cancelled);
    js["state"] = _state;
    js["createTime_msec"] = ::tp2ms(_createTime);
    js["queueTime_msec"] = ::tp2ms(_queueTime);
    js["startTime_msec"] = ::tp2ms(_startTime);
    js["queryTime_msec"] = ::tp2ms(_queryTime);
    js["finishTime_msec"] = ::tp2ms(_finishTime);
    js["sizeSoFar"] = _totalSize;
    // LOGS(_log, LOG_LVL_WARN, "&&& task::getJson " << js);
    return js;
}

std::ostream& operator<<(std::ostream& os, Task const& t) {
    os << "Task: "
       << "msg: " << t.getIdStr() << " session=" << t._session << " chunk=" << t._chunkId << " db=" << t._db
       << " " << t.getQueryString();

    return os;
}

std::ostream& operator<<(std::ostream& os, IdSet const& idSet) {
    // Limiting output as number of entries can be very large.
    int maxDisp = idSet.maxDisp;  // only affects the amount of data printed.
    std::lock_guard<std::mutex> lock(idSet.mx);
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
