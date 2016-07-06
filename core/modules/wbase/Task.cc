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

// Third-party headers
#include "boost/regex.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/TaskMsgDigest.h"
#include "proto/worker.pb.h"
#include "wbase/Base.h"
#include "wbase/SendChannel.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.Task");

std::ostream&
dump(std::ostream& os,
    lsst::qserv::proto::TaskMsg_Fragment const& f) {
    os << "frag: " << "q=";
    for(int i=0; i < f.query_size(); ++i) {
        os << f.query(i) << ",";
    }
    if (f.has_subchunks()) {
        os << " sc=";
        for(int i=0; i < f.subchunks().id_size(); ++i) {
            os << f.subchunks().id(i) << ",";
        }
    }
    os << " rt=" << f.resulttable();
    return os;
}

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wbase {

// Task::ChunkEqual functor
bool Task::ChunkEqual::operator()(Task::Ptr const& x, Task::Ptr const& y) {
    if (!x || !y) { return false; }
    if ((!x->msg) || (!y->msg)) { return false; }
    return x->msg->has_chunkid() && y->msg->has_chunkid()
        && x->msg->chunkid()  == y->msg->chunkid();
}

// Task::PtrChunkIdGreater functor
bool Task::ChunkIdGreater::operator()(Task::Ptr const& x, Task::Ptr const& y) {
    if (!x || !y) { return false; }
    if ((!x->msg) || (!y->msg)) { return false; }
    return x->msg->chunkid()  > y->msg->chunkid();
}


std::string const Task::defaultUser = "qsmaster";
IdSet Task::allIds{};


/// When the constructor is called, there is not enough information
/// available to define the action to take when this task is run, so
/// Command::setFunc() is used set the action later. This is why
/// the util::CommandThreadPool is not called here.
Task::Task(Task::TaskMsgPtr const& t, SendChannel::Ptr const& sc)
    : msg{t}, sendChannel{sc},
      _qId{t->queryid()}, _jId{t->jobid()},
      _idStr{QueryIdHelper::makeIdStr(_qId, _jId)} {
    hash = hashTaskMsg(*t);

    if (t->has_user()) {
        user = t->user();
    } else {
        user = defaultUser;
    }
    timestr[0] = '\0';

    allIds.add(std::to_string(_qId) + "_" + std::to_string(_jId));
    LOGS(_log, LOG_LVL_DEBUG, "Task(...) " << _idStr << " : " << allIds);

    // Determine which major tables this task will use.
    int const size = msg->scantable_size();
    for(int j=0; j < size; ++j) {
        _scanInfo.infoTables.push_back(proto::ScanTableInfo(msg->scantable(j)));
    }
    _scanInfo.scanRating = msg->scanpriority();
    _scanInfo.sortTablesSlowestFirst();
}

Task::~Task() {
    allIds.remove(std::to_string(_qId) + "_" + std::to_string(_jId));
    LOGS(_log, LOG_LVL_DEBUG, "~Task() " << _idStr << ": " << allIds);
}


/// @return the chunkId for this task. If the task has no chunkId, return -1.
int Task::getChunkId() {
    if (msg->has_chunkid()) {
        return msg->chunkid();
    }
    return -1;
}


/// Flag the Task as cancelled, try to stop the SQL query, and try to remove it from the schedule.
void Task::cancel() {
    if (_cancelled.exchange(true)) {
        // Was already cancelled.
        return;
    }
    auto qr = _taskQueryRunner; // Want a copy in case _taskQueryRunner is reset.
    if (qr != nullptr) {
        qr->cancel();
    }

    auto sched = _taskScheduler.lock();
    if (sched != nullptr) {
        sched->taskCancelled(this);
    }
}


/// @return true if task has already been cancelled.
bool Task::setTaskQueryRunner(TaskQueryRunner::Ptr const& taskQueryRunner) {
    _taskQueryRunner = taskQueryRunner;
    return getCancelled();
}

void Task::freeTaskQueryRunner(TaskQueryRunner *tqr){
    if (_taskQueryRunner.get() == tqr) {
        _taskQueryRunner.reset();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Task::freeTaskQueryRunner pointer didn't match!");
    }
}


/// Set values associated with the Task being put on the queue.
void Task::queued(std::chrono::system_clock::time_point const& now) {
    std::lock_guard<std::mutex> guard(_stateMtx);
    _state = State::QUEUED;
    _queueTime = now;
}


/// Set values associated with the Task being started.
void Task::started(std::chrono::system_clock::time_point const& now) {
    std::lock_guard<std::mutex> guard(_stateMtx);
    _state = State::RUNNING;
    _startTime = now;
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
    LOGS(_log, LOG_LVL_DEBUG, _idStr << " processing millisecs=" << duration.count());
    return duration;
}


/// Wait for MemMan to finish reserving resources.
void Task::waitForMemMan() {
    static std::mutex mx;
    LOGS(_log,LOG_LVL_DEBUG, _idStr << " waitForMemMan begin");
    if (_memMan != nullptr) {
        std::lock_guard<std::mutex> lck(mx);
        auto err = _memMan->lock(_memHandle, true);
        if (err) {
            LOGS(_log, LOG_LVL_WARN, _idStr << " mlock err=" << err);
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, _idStr << " waitForMemMan end");
}

std::ostream& operator<<(std::ostream& os, Task const& t) {
    proto::TaskMsg& m = *t.msg;
    os << "Task: "
       << "msg: " << t._idStr
       << " session=" << m.session()
       << " chunk=" << m.chunkid()
       << " db=" << m.db()
       << " entry time=" << t.timestr
       << " ";
    for(int i=0; i < m.fragment_size(); ++i) {
        dump(os, m.fragment(i));
        os << " ";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, IdSet const& idSet) {
    // Limiting output as number of entries can be very large.
    os << "showing " << idSet.maxDisp << " of count=" << idSet._ids.size() << " ";
    bool first = true;
    int i = 0;
    int maxDisp = idSet.maxDisp; // idSet.maxDisp is atomic
    for(auto id: idSet._ids) {
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

}}} // namespace lsst::qserv::wbase
