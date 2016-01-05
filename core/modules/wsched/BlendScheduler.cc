// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 LSST Corporation.
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
  * @brief A scheduler implementation that limits disk scans to one at
  * a time, but allows multiple queries to share I/O.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "wsched/BlendScheduler.h"

// System headers
#include <cstddef>
#include <iostream>
#include <mutex>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
#include "proto/worker.pb.h"
#include "util/EventThread.h"
#include "wcontrol/Foreman.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"

template <class Sched>
inline Sched* other(Sched* notThis, Sched* a, Sched* b) {
    return (notThis == a) ? b : a;
}

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.BlendScheduler");
}

namespace lsst {
namespace qserv {
namespace wsched {

BlendScheduler* dbgBlendScheduler=nullptr; ///< A symbol for gdb

////////////////////////////////////////////////////////////////////////
// class BlendScheduler
////////////////////////////////////////////////////////////////////////
BlendScheduler::BlendScheduler(std::shared_ptr<GroupScheduler> group,
                               std::shared_ptr<ScanScheduler> scan)
    : _group{group}, _scan{scan}
{
    dbgBlendScheduler = this;
    if(!group || !scan) { throw Bug("BlendScheduler: missing scheduler"); }
}

void BlendScheduler::queCmd(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if(task == nullptr || task->msg == nullptr) {
        throw Bug("BlendScheduler::queueTaskAct: null task");
    }
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::queCmd tSeq=" << task->tSeq);
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    // Check for scan tables
    assert(_group);
    assert(_scan);
    wcontrol::Scheduler* s = nullptr;
    if(task->msg->scantables_size() > 0) {
        if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
            std::ostringstream ss;
            int size = task->msg->scantables_size();
            ss << "Blend chose scan for:";
            for(int i=0; i < size; ++i) {
                ss << i << " " << task->msg->scantables(i);
            }
            LOGS(_log, LOG_LVL_DEBUG, ss.str());
        }
        s = _scan.get();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Blend chose group");
        s = _group.get();
    }
    {
        std::lock_guard<std::mutex> guard(_mapMutex);
        _map[task.get()] = s;
    }
    LOGS(_log, LOG_LVL_DEBUG, "Blend queCmd tSeq=" << task->tSeq);
    s->queCmd(task);
    notify(true);
}

void BlendScheduler::commandStart(util::Command::Ptr const& cmd) {
    auto t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "BlendScheduler::commandStart cmd failed conversion");
        return;
    }
    wcontrol::Scheduler* s = lookup(t);
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::commandStart tSeq=" << t->tSeq);
    if(s == _group.get()) {
        _group->commandStart(t);
    } else {
        _scan->commandStart(t);
    }
}

void BlendScheduler::commandFinish(util::Command::Ptr const& cmd) {
    auto t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "BlendScheduler::commandFinish cmd failed conversion");
        return;
    }
    wcontrol::Scheduler* s = lookup(t);
    if(s == _group.get()) {
        _group->commandFinish(t);
    } else {
        _scan->commandFinish(t);
    }
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::commandFinish tSeq=" << t->tSeq);
}

/// @return ptr to scheduler that is tracking p
wcontrol::Scheduler* BlendScheduler::lookup(wbase::Task::Ptr p) {
    std::lock_guard<std::mutex> guard(_mapMutex);
    auto i = _map.find(p.get());
    return i->second;
}


bool BlendScheduler::ready() {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    return _ready();
}

/// Returns true when either scheduler has a command ready.
/// Precondition util::CommandQueue::_mx must be locked when this is called.
bool BlendScheduler::_ready() {
    auto groupReady = _group->ready();
    auto scanReady = _scan->ready();
    auto ready = groupReady || scanReady;
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::_ready() groups("
         << "r=" << groupReady
         << ", q=" << _group->getSize()
         << ", flight=" << _group->getInFlight() << ") "
         << "scan(r=" << scanReady
         << ", q=" << _scan->getSize()
         << ", flight=" <<_scan->getInFlight() << ")");
    return ready;
}

util::Command::Ptr BlendScheduler::getCmd(bool wait) {
    std::unique_lock<std::mutex> lock(util::CommandQueue::_mx);
    if (wait) {
        util::CommandQueue::_cv.wait(lock, [this](){return _ready();});
    }

    // Figure out which scheduler to draw from.
    auto scanReady = _scan->ready();
    auto groupReady = _group->ready();

    util::Command::Ptr cmd;
    // Alternate priority between schedulers. TODO: There is probably a better way, but this will work for now.
    if (_lastCmdFromScan) {
        if (groupReady) {
            cmd = _group->getCmd(false); // no wait
            _lastCmdFromScan = false;
        } else  if (scanReady) {
            cmd = _scan->getCmd(false); // no wait
            _lastCmdFromScan = true;
        }
    } else {
        if (scanReady) {
            cmd = _scan->getCmd(false); // no wait
            _lastCmdFromScan = true;
        } else if (groupReady) {
            cmd = _group->getCmd(false); // no wait
            _lastCmdFromScan = false;
        }
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "BlendScheduler::getCmd: "
         << "groupReady=" << groupReady
         << " scanReady=" << scanReady
         << " lastCmdFromScan=" << _lastCmdFromScan);
    return cmd;
}

}}} // namespace lsst::qserv::wsched
