// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
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
namespace lsst {
namespace qserv {
namespace wsched {

BlendScheduler* dbgBlendScheduler=nullptr; ///< A symbol for gdb

////////////////////////////////////////////////////////////////////////
// class BlendScheduler
////////////////////////////////////////////////////////////////////////
BlendScheduler::BlendScheduler(std::shared_ptr<GroupScheduler> group,
                               std::shared_ptr<ScanScheduler> scan)
    : _group{group}, _scan{scan}, _logger{LOG_GET(getName())}
{
    dbgBlendScheduler = this;
    if(!group || !scan) { throw Bug("BlendScheduler: missing scheduler"); }
}

void BlendScheduler::queCmd(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if(task == nullptr || task->msg == nullptr) {
        throw Bug("BlendScheduler::queueTaskAct: null task");
    }
    LOGF_DEBUG("BlendScheduler::queCmd tSeq=%1%" % task->tSeq);
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    // Check for scan tables
    assert(_group);
    assert(_scan);
    wcontrol::Scheduler* s = nullptr;
    if(task->msg->scantables_size() > 0) {
        if (LOG_CHECK_LVL(_logger, LOG_LVL_DEBUG)) {
            std::ostringstream ss;
            int size = task->msg->scantables_size();
            ss << "Blend chose scan for:";
            for(int i=0; i < size; ++i) {
                ss << i << " " << task->msg->scantables(i);
            }
            LOGF(_logger, LOG_LVL_DEBUG, "%1%" % ss.str());
        }
        s = _scan.get();
    } else {
        LOGF(_logger, LOG_LVL_DEBUG, "Blend chose group");
        s = _group.get();
    }
    {
        std::lock_guard<std::mutex> guard(_mapMutex);
        _map[task.get()] = s;
    }
    LOGF(_logger, LOG_LVL_DEBUG, "Blend queCmd tSeq=%1%" % task->tSeq);
    s->queCmd(task);
    notify(true);
}

void BlendScheduler::commandStart(util::Command::Ptr const& cmd) {
    auto t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGF(_logger, LOG_LVL_WARN, "BlendScheduler::commandStart cmd failed conversion");
        return;
    }
    wcontrol::Scheduler* s = lookup(t);
    LOGF(_logger, LOG_LVL_DEBUG, "BlendScheduler::commandStart tSeq=%1%" % t->tSeq);
    if(s == _group.get()) {
        _group->commandStart(t);
    } else {
        _scan->commandStart(t);
    }
}

void BlendScheduler::commandFinish(util::Command::Ptr const& cmd) {
    auto t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGF(_logger, LOG_LVL_WARN, "BlendScheduler::commandFinish cmd failed conversion");
        return;
    }
    wcontrol::Scheduler* s = lookup(t);
    if(s == _group.get()) {
        _group->commandFinish(t);
    } else {
        _scan->commandFinish(t);
    }
    LOGF(_logger, LOG_LVL_DEBUG, "BlendScheduler::commandFinish tSeq=%1%" % t->tSeq);
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
    LOGF(_logger, LOG_LVL_DEBUG, "BlendScheduler::_ready() groups(r=%1%, q=%2%, flight=%3%) scan(r=%4%, q=%5%, flight=%6%)"
            % groupReady % _group->getSize() % _group->getInFlight()
            % scanReady %_scan->getSize() % _scan->getInFlight());
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
    LOGF(_logger, LOG_LVL_DEBUG,
         "BlendScheduler::getCmd: groupReady=%1% scanReady=%2% lastCmdFromScan=%3%"
         % groupReady % scanReady % _lastCmdFromScan);
    return cmd;
}

}}} // namespace lsst::qserv::wsched
