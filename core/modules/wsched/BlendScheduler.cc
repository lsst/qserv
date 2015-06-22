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
    : _group(group),
      _scan(scan),
      _logger(LOG_GET(getName()))
{
    dbgBlendScheduler = this;
    if(!group || !scan) { throw Bug("BlendScheduler: missing scheduler"); }
}

void
BlendScheduler::queueTaskAct(wbase::Task::Ptr incoming) {
    // Check for scan tables
    if(!incoming || !incoming->msg) {
        throw Bug("BlendScheduler::queueTaskAct: null task");
    }
    assert(_group);
    assert(_scan);
    wcontrol::Foreman::Scheduler* s = nullptr;
    if(incoming->msg->scantables_size() > 0) {
        if (LOG_CHECK_LVL(_logger, LOG_LVL_DEBUG)) {
            std::ostringstream ss;
            int size = incoming->msg->scantables_size();
            ss << "Blend chose scan for:";
            for(int i=0; i < size; ++i) {
                ss << i << " " << incoming->msg->scantables(i);
            }
            LOGF(_logger, LOG_LVL_DEBUG, "%1%" % ss.str());
        }
        s = _scan.get();
    } else {
        s = _group.get();
    }
    {
        std::lock_guard<std::mutex> guard(_mapMutex);
        _map[incoming.get()] = s;
    }
    s->queueTaskAct(incoming);
}

wbase::TaskQueuePtr
BlendScheduler::nopAct(wbase::TaskQueuePtr running) {
    // For now, do nothing when there is no event.

    // Perhaps better: Check to see how many are running, and schedule
    // a task if the number of running jobs is below a threshold.
    return wbase::TaskQueuePtr();
}

/// @return a queue of all tasks ready to run.
///
wbase::TaskQueuePtr
BlendScheduler::newTaskAct(wbase::Task::Ptr incoming,
                           wbase::TaskQueuePtr running) {
    queueTaskAct(incoming);
    assert(checkIntegrity());
    assert(running.get());
    return _getNextIfAvail(running);
}

wbase::TaskQueuePtr
BlendScheduler::taskFinishAct(wbase::Task::Ptr finished,
                              wbase::TaskQueuePtr running) {
    wcontrol::Foreman::Scheduler* s = nullptr;
    {
        std::lock_guard<std::mutex> guard(_mapMutex);
        assert(_integrityHelper());
        Map::iterator i = _map.find(finished.get());
        if(i == _map.end()) {
            throw Bug("BlendScheduler::taskFinishAct: Finished untracked task");
        }
        s = i->second;
        _map.erase(i);
    }
    LOGF(_logger, LOG_LVL_DEBUG, "Completed: (%1%) %2%" %
            finished->msg->chunkid() % finished->msg->fragment(0).query(0));
    wbase::TaskQueuePtr t = s->taskFinishAct(finished, running);
    if(!t) { // Try other scheduler.
        LOG(_logger, LOG_LVL_DEBUG, "Blend trying other sched.");
        return other<wcontrol::Foreman::Scheduler>(s, _group.get(),
                                                   _scan.get())->nopAct(running);
    }
    return t;
}

void
BlendScheduler::markStarted(wbase::Task::Ptr t) {
    wcontrol::Foreman::Scheduler* s = lookup(t);
    if(s == _group.get()) {
        _group->markStarted(t);
    } else {
        _scan->markStarted(t);
    }
}

void
BlendScheduler::markFinished(wbase::Task::Ptr t) {
    wcontrol::Foreman::Scheduler* s = lookup(t);
    if(s == _group.get()) {
        _group->markFinished(t);
    } else {
        _scan->markFinished(t);
    }
}

/// @return true if data is okay.
bool
BlendScheduler::checkIntegrity() {
    std::lock_guard<std::mutex> guard(_mapMutex);
    return _integrityHelper();
}

/// @return ptr to scheduler that is tracking p
wcontrol::Foreman::Scheduler*
BlendScheduler::lookup(wbase::Task::Ptr p) {
    std::lock_guard<std::mutex> guard(_mapMutex);
    Map::iterator i = _map.find(p.get());
    return i->second;
}

/// @return true if data is okay
/// precondition: _mapMutex is locked.
bool
BlendScheduler::_integrityHelper() const {
    if(!_group) { return false; }
    else if(!_group->checkIntegrity()) { return false; }

    if(!_scan) { return false; }
    else if(!_scan->checkIntegrity()) { return false; }

    Map::const_iterator i, e;
    // Make sure each map entry points at one of the schedulers.
    for(i=_map.begin(), e=_map.end(); i != e; ++i) {
        if(i->second == _group.get()) continue;
        if(i->second == _scan.get()) continue;
        return false;
    }
    return true;
}

/// @return new tasks to run
wbase::TaskQueuePtr
BlendScheduler::_getNextIfAvail(wbase::TaskQueuePtr running) {
    // Get from interactive queue first
    wbase::TaskQueuePtr tg = _group->nopAct(running);
    wbase::TaskQueuePtr ts = _scan->nopAct(running);
    // Merge
    if(tg) {
        if(ts) { tg->insert(tg->end(), ts->begin(), ts->end()); }
        return tg;
    } else {
        if(!ts) LOG(_logger, LOG_LVL_DEBUG, "BlendScheduler: no tasks available");
        return ts;
    }
}

}}} // namespace lsst::qserv::wsched
