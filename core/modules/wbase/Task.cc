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

util::Sequential<int> Task::sequence{0};
IdSet Task::allTSeq{};

Task::Task() {
    tSeq = sequence.incr();
    allTSeq.add(tSeq);
    LOGS(_log, LOG_LVL_DEBUG, "Task tSeq=" << tSeq << ": " << allTSeq);
}

Task::Task(Task::TaskMsgPtr const& t, SendChannel::Ptr const& sc)
    : msg{t}, sendChannel{sc} {
    hash = hashTaskMsg(*t);
    dbName = "q_" + hash;
    if (t->has_user()) {
        user = t->user();
    } else {
        user = defaultUser;
    }
    timestr[0] = '\0';

    tSeq = sequence.incr();
    allTSeq.add(tSeq);
    LOGS(_log, LOG_LVL_DEBUG, "Task(...) tSeq=" << tSeq << ": " << allTSeq);

    // Determine which major tables this task will use.
    int size = msg->scantable_size();

    for(int j=0; j < size; ++j) {
        proto::TaskMsg_ScanTable const& scanTbl = msg->scantable(j);
        _scanInfo.infoTables.push_back(proto::ScanTableInfo(scanTbl.db(), scanTbl.table(),
                                       scanTbl.lockinmemory(), scanTbl.scanspeed()));
    }
    _scanInfo.priority = msg->scanpriority();
}

Task::~Task() {
    allTSeq.remove(tSeq);
    LOGS(_log, LOG_LVL_DEBUG, "~Task() tSeq=" << tSeq << ": " << allTSeq);
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

std::ostream& operator<<(std::ostream& os, Task const& t) {
    proto::TaskMsg& m = *t.msg;
    os << "Task: "
       << "msg: session=" << m.session()
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
    os << "count=" << idSet._ids.size() << " ";
    bool first = true;
    for(auto j: idSet._ids) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << j;
    }
    return os;
}

}}} // namespace lsst::qserv::wbase
