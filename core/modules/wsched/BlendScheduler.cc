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
#include <algorithm>
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

BlendScheduler::BlendScheduler(std::string const& name,
                               int schedMaxThreads,
                               std::shared_ptr<GroupScheduler> const& group,
                               std::vector<std::shared_ptr<ScanScheduler>> const& scanSchedulers)
    : SchedulerBase{name, 0, 0, 0}, _schedMaxThreads{schedMaxThreads},
      _group{group}, _scanFast{scanSchedulers.at(0)} {
    dbgBlendScheduler = this;
    // If these are not defined, there is no point in continuing.
    assert(_group);
    assert(_scanFast);
    _schedulers.push_back(_group); // _group scheduler must be first in the list.
    for (auto const& sched : scanSchedulers) {
        _schedulers.push_back(sched);
        sched->setBlendScheduler(this);
    }
    assert(_schedulers.size() >= 2); // Must have at least _group and _scanFast in the list.
    _sortScanSchedulers();
    for (auto sched : _schedulers) {
        LOGS(_log, LOG_LVL_DEBUG, "Scheduler " << _name << " found scheduler " << sched->getName());
    }
}


BlendScheduler::~BlendScheduler() {
    /// Cleanup pointers.
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    for (auto const& sched : _schedulers) {
        auto const& scanSched = std::dynamic_pointer_cast<ScanScheduler>(sched);
        if (scanSched != nullptr) {
            scanSched->setBlendScheduler(nullptr);
        }
    }
}


void BlendScheduler::_sortScanSchedulers() {
    auto greaterThan = [](SchedulerBase::Ptr const& a, SchedulerBase::Ptr const& b)->bool {
        // Experiment of sorts, priority depends on number of Tasks in each scheduler.
        auto aVal = a->getPriority() * (1 + a->getUserQueriesInQ());
        auto bVal = b->getPriority() * (1 + b->getUserQueriesInQ());
        return aVal > bVal;
    };
    // The first scheduler should always be _group (for interactive queries).
    if (_schedulers.size() >= 2) {
        std::sort(_schedulers.begin()+1, _schedulers.end(), greaterThan);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "not enough schedulers, _schedulers.size=" << _schedulers.size());
    }
}


void BlendScheduler::queCmd(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (task == nullptr || task->msg == nullptr) {
        throw Bug("BlendScheduler::queueTaskAct: null task");
    }
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::queCmd " << task->getIdStr());

    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    // Check for scan tables
    SchedulerBase* s = nullptr;
    auto const& scanTables = task->getScanInfo().infoTables;
    if (scanTables.size() > 0) {
        int scanPriority = task->getScanInfo().scanRating;
        if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
            std::ostringstream ss;
            ss << "Blend chose scan for priority=" << scanPriority << " : ";
            for (auto scanTbl : scanTables) {
                ss << scanTbl.db + "." + scanTbl.table + " ";
            }
            LOGS(_log, LOG_LVL_DEBUG, ss.str());
        }

        for (auto const& sched : _schedulers) {
            ScanScheduler *scan = dynamic_cast<ScanScheduler*>(sched.get());
            if (scan != nullptr) {
                if (scan->isRatingInRange(scanPriority)) {
                    s = scan;
                    break;
                }
            }
        }
        if (s == nullptr) {
            // Task wasn't assigned with a scheduler, assuming it is simple and fast.
            // TODO: This is probably not a good assumption for the long term, but fine for our
            // integration test data and the like.
            LOGS_WARN("Task had unexpected scanRating=" << scanPriority << " " << task);
            s = _scanFast.get();
        }
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Blend chose group");
        s = _group.get();
    }
    {
        std::lock_guard<std::mutex> guard(_mapMutex);
        _map[task.get()] = s;
    }
    LOGS(_log, LOG_LVL_DEBUG, "Blend queCmd " << task->getIdStr());
    s->queCmd(task);
    _infoChanged = true;
    notify(true);
}

void BlendScheduler::commandStart(util::Command::Ptr const& cmd) {
    auto t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "BlendScheduler::commandStart cmd failed conversion");
        return;
    }

    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::commandStart " << t->getIdStr());
    wcontrol::Scheduler* s = lookup(t);
    if (s != nullptr) {
        s->commandStart(t);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "BlendScheduler::commandStart scheduler not found " << t ->getIdStr());
    }
    _infoChanged = true;
}

void BlendScheduler::commandFinish(util::Command::Ptr const& cmd) {
    auto t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "BlendScheduler::commandFinish cmd failed conversion");
        return;
    }
    wcontrol::Scheduler* s = lookup(t, true); // erase entry in map

    if (s != nullptr) {
        s->commandFinish(t);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "BlendScheduler::commandFinish scheduler not found " << t->getIdStr());
    }
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::commandFinish " << t->getIdStr());
    _infoChanged = true;
    _logChunkStatus();

    // TODO: DM-4943 Add check to only call notify if resources were actually freed by commandFinish()
    notify(true);

}

/// @return ptr to scheduler that is tracking p
wcontrol::Scheduler* BlendScheduler::lookup(wbase::Task::Ptr p, bool erase) {
    std::lock_guard<std::mutex> guard(_mapMutex);
    auto i = _map.find(p.get());
    if (i == _map.end()) {
        LOGS(_log, LOG_LVL_ERROR, "lookup failed to find scheduler " << p->getIdStr());
        return nullptr;
    }
    auto val = i->second;
    //if (erase) _map.erase(i); // &&& why does this break unit tests???
    return val;
}


bool BlendScheduler::ready() {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    return _ready();
}


/// Returns true when any sub-scheduler has a command ready.
/// Precondition util::CommandQueue::_mx must be locked when this is called.
bool BlendScheduler::_ready() {
    std::ostringstream os;
    bool ready = false;

    if (_flagReorderScans) {
        _flagReorderScans = false;
        _sortScanSchedulers();
    }

    // Get the total number of threads schedulers want reserved
    int availableThreads = calcAvailableTheads();
    bool changed = _infoChanged.exchange(false);
    for (auto sched : _schedulers) {
        availableThreads = sched->applyAvailableThreads(availableThreads);
        ready = sched->ready();
        if (changed && LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
            os << sched->getName() << "(r=" << ready << " sz=" << sched->getSize()
               << " fl=" << sched-> getInFlight() << " avail=" << availableThreads << ") ";
        }
        if (ready) break;
    }
    if (changed) {
        LOGS(_log, LOG_LVL_DEBUG, getName() << "_ready() " << os.str());
    }
    return ready;
}

util::Command::Ptr BlendScheduler::getCmd(bool wait) {
    std::unique_lock<std::mutex> lock(util::CommandQueue::_mx);
    if (wait) {
        util::CommandQueue::_cv.wait(lock, [this](){return _ready();});
    }

    // Try to get a command from the schedulers
    util::Command::Ptr cmd;
    int availableThreads = calcAvailableTheads();
    for (auto const& sched : _schedulers) {
        availableThreads = sched->applyAvailableThreads(availableThreads);
        cmd = sched->getCmd(false); // no wait
        if (cmd != nullptr) {
            LOGS(_log, LOG_LVL_DEBUG, "Blend getCmd() using cmd from " << sched->getName());
            wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
            break;
        }
        // adjMax = _getAdjustedMaxThreads(adjMax, sched->getInFlight()); // DM-4943 possible alternate method
        LOGS(_log, LOG_LVL_DEBUG, "Blend getCmd() nothing from " << sched->getName() << " avail=" << availableThreads);
    }
    if (cmd != nullptr) {
        _infoChanged = true;
        _logChunkStatus();
    }
    // returning nullptr is acceptable.
    return cmd;
}

/// Method A - maybe use with MemManReal
int BlendScheduler::_getAdjustedMaxThreads(int oldAdjMax, int inFlight) {
    int newAdjMax = oldAdjMax - std::max(inFlight - 1, 0);
    if (newAdjMax < 1) {
        LOGS(_log, LOG_LVL_ERROR,
             "_getAdjustedMaxThreadsgetCmd() too low newAdjMax=" << newAdjMax);
        newAdjMax = 1;
    }
    return newAdjMax;
}

/// @return the number of threads that are not reserved by any sub-scheduler.
int BlendScheduler::calcAvailableTheads() {
    int reserve = 0;
    for (auto sched : _schedulers) {
        reserve += sched->desiredThreadReserve();
    }
    int available = _schedMaxThreads - reserve;
    if (available < 0) {
        LOGS(_log, LOG_LVL_DEBUG, "calcAvailableTheads negative available=" << available);
    }
    return available;
}

/// Returns the number of Tasks queued in all sub-schedulers.
std::size_t BlendScheduler::getSize() const {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    std::size_t sz = 0;
    for (auto sched : _schedulers) {
        sz += sched->getSize();
    }
    return sz;
}

/// Returns the number of Tasks inFlight.
int BlendScheduler::getInFlight() const {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    int inFlight = 0;
    for (auto sched : _schedulers) {
        inFlight += sched->getInFlight();
    }
    return inFlight;
}

void BlendScheduler::_logChunkStatus() {
    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        std::string str;
        for (auto const& sched : _schedulers) {
            if (sched != nullptr) str += sched->chunkStatusStr() + "\n";
        }
        LOGS(_log, LOG_LVL_DEBUG, str);
    }
}

}}} // namespace lsst::qserv::wsched
