/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file BlendScheduler.cc
  *
  * @brief A scheduler implementation that limits disk scans to one at
  * a time, but allows multiple queries to share I/O.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iostream>
#include <sstream>

// Third-party headers
#include <boost/thread.hpp>

// Local headers
#include "proto/worker.pb.h"
#include "wcontrol/Foreman.h"
#include "wlog/WLogger.h"
#include "wsched/BlendScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"

template <class Sched>
inline Sched* other(Sched* notThis, Sched* a, Sched* b) {
    return (notThis == a) ? b : a;
}
namespace lsst {
namespace qserv {
namespace wsched {

BlendScheduler* dbgBlendScheduler=0; //< A symbol for gdb

////////////////////////////////////////////////////////////////////////
// class BlendScheduler
////////////////////////////////////////////////////////////////////////
BlendScheduler::BlendScheduler(boost::shared_ptr<wlog::WLogger> logger,
                               boost::shared_ptr<GroupScheduler> group,
                               boost::shared_ptr<ScanScheduler> scan)
    : _group(group),
      _scan(scan),
      _logger(logger)
{
    dbgBlendScheduler = this;
    if(!group || !scan) { throw std::invalid_argument("missing scheduler"); }
}

void
BlendScheduler::queueTaskAct(wcontrol::Task::Ptr incoming) {
    // Check for scan tables
    if(!incoming || !incoming->msg) {
        throw std::invalid_argument("null task");
    }
    assert(_group);
    assert(_scan);
    wcontrol::Foreman::Scheduler* s = 0;
    if(incoming->msg->scantables_size() > 0) {
        std::ostringstream ss;
        int size = incoming->msg->scantables_size();
        ss << "Blend chose scan for:";
        for(int i=0; i < size; ++i) {
            ss << i << " " << incoming->msg->scantables(i);
        }
        _logger->debug(ss.str());
        s = _scan.get();
    } else {
        s = _group.get();
    }
    {
        boost::lock_guard<boost::mutex> guard(_mapMutex);
        _map[incoming.get()] = s;
    }
    s->queueTaskAct(incoming);
}

wcontrol::TaskQueuePtr
BlendScheduler::nopAct(wcontrol::TaskQueuePtr running) {
    // For now, do nothing when there is no event.

    // Perhaps better: Check to see how many are running, and schedule
    // a task if the number of running jobs is below a threshold.
    return wcontrol::TaskQueuePtr();
}

/// @return a queue of all tasks ready to run.
///
wcontrol::TaskQueuePtr
BlendScheduler::newTaskAct(wcontrol::Task::Ptr incoming,
                           wcontrol::TaskQueuePtr running) {
    queueTaskAct(incoming);
    assert(_integrityHelper());
    assert(running.get());
    return _getNextIfAvail(running);
}

wcontrol::TaskQueuePtr
BlendScheduler::taskFinishAct(wcontrol::Task::Ptr finished,
                              wcontrol::TaskQueuePtr running) {
    assert(_integrityHelper());
    wcontrol::Foreman::Scheduler* s = 0;
    {
        boost::lock_guard<boost::mutex> guard(_mapMutex);
        Map::iterator i = _map.find(finished.get());
        if(i == _map.end()) {
            throw std::logic_error("Finished untracked task");
        }
        s = i->second;
        _map.erase(i);
    }
    std::ostringstream os;
    os << "Completed: " << "(" << finished->msg->chunkid()
       << ")" << finished->msg->fragment(0).query(0);
    _logger->debug(os.str());
    wcontrol::TaskQueuePtr t = s->taskFinishAct(finished, running);
    if(!t) { // Try other scheduler.
        _logger->debug("Blend trying other sched.");
        return other<wcontrol::Foreman::Scheduler>(s, _group.get(),
                                                   _scan.get())->nopAct(running);
    }
    return t;
}

void
BlendScheduler::markStarted(wcontrol::Task::Ptr t) {
    wcontrol::Foreman::Scheduler* s = lookup(t);
    if(s == _group.get()) {
        _group->markStarted(t);
    } else {
        _scan->markStarted(t);
    }
}

void
BlendScheduler::markFinished(wcontrol::Task::Ptr t) {
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
    boost::lock_guard<boost::mutex> guard(_mapMutex);
    return _integrityHelper();
}

/// @return ptr to scheduler that is tracking p
wcontrol::Foreman::Scheduler*
BlendScheduler::lookup(wcontrol::Task::Ptr p) {
    boost::lock_guard<boost::mutex> guard(_mapMutex);
    Map::iterator i = _map.find(p.get());
    return i->second;
}

/// @return true if data is okay
/// precondition: _mutex is locked.
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

/// Precondition: _mutex is already locked.
/// @return new tasks to run
wcontrol::TaskQueuePtr
BlendScheduler::_getNextIfAvail(wcontrol::TaskQueuePtr running) {
    // Get from interactive queue first
    wcontrol::TaskQueuePtr tg = _group->nopAct(running);
    wcontrol::TaskQueuePtr ts = _scan->nopAct(running);
    // Merge
    if(tg) {
        if(ts) { tg->insert(tg->end(), ts->begin(), ts->end()); }
        return tg;
    } else {
        if(!ts) _logger->debug("BlendScheduler: no tasks available");
        return ts;
    }
}

}}} // namespace lsst::qserv::wsched
