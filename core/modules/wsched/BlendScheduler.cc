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
#include "util/Timer.h"
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
                               wpublish::QueriesAndChunks::Ptr const& queries,
                               int schedMaxThreads,
                               std::shared_ptr<GroupScheduler> const& group,
                               std::shared_ptr<ScanScheduler> const& snailScheduler,
                               std::vector<std::shared_ptr<ScanScheduler>> const& scanSchedulers)
    : SchedulerBase{name, 0, 0, 0, 0}, _schedMaxThreads{schedMaxThreads},
      _group{group}, _scanSnail{snailScheduler}, _queries{queries} {
    dbgBlendScheduler = this;
    // If these are not defined, there is no point in continuing.
    assert(_group);
    assert(_scanSnail);
    int position = 0;
    _schedulers.push_back(_group); // _group scheduler must be first in the list.
    _group->setDefaultPosition(position++);
    for (auto const& sched : scanSchedulers) {
        _schedulers.push_back(sched);
        sched->setDefaultPosition(position++);
        sched->setBlendScheduler(this);
    }
    _schedulers.push_back(_scanSnail);
    _scanSnail->setDefaultPosition(position++);
    assert(_schedulers.size() >= 2); // Must have at least _group and _scanSnail in the list.
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
    auto lessThan = [this](SchedulerBase::Ptr const& a, SchedulerBase::Ptr const& b) -> bool {
        // group scheduler is always first
        if (a == _group) return true;
        if (b == _group) return false;

        // snail scheduler is always last
        if (a == _scanSnail) return false;
        if (b == _scanSnail) return true;

        // base on the number of scans in flight.
        if (_prioritizeByInFlight) {
            auto aInFlight = a->getInFlight() - a->getPriority();
            auto bInFlight = b->getInFlight() - b->getPriority();
            if (aInFlight < bInFlight) return true;
            if (bInFlight < aInFlight) return false;
        }

        /// Order by original position in the list
        return (a->getDefaultPosition() < b->getDefaultPosition());
    };

    std::sort(_schedulers.begin(), _schedulers.end(), lessThan);

    std::string str = "sort:";
    for (auto& sched : _schedulers) {
        str += sched->getName() + ", ";
    }
    LOGS(_log, LOG_LVL_DEBUG, str);
}


void BlendScheduler::queCmd(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (task == nullptr) {
        LOGS(_log, LOG_LVL_INFO, "BlendScheduler::queCmd got control command");
        {
            util::LockGuardTimed guard(util::CommandQueue::_mx, "BlendScheduler::queCmd a");
            _ctrlCmdQueue.queCmd(cmd);
        }
        notify(true);
        return;
    }
    if (task->msg == nullptr) {
        throw Bug("BlendScheduler::queCmd task with null message!");
    }
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::queCmd " << task->getIdStr());

    util::LockGuardTimed guard(util::CommandQueue::_mx, "BlendScheduler::queCmd b");
    // Check for scan tables
    SchedulerBase::Ptr s{nullptr};
    auto const& scanTables = task->getScanInfo().infoTables;
    bool interactive = task->getScanInteractive();
    if (scanTables.size() <= 0 || interactive) {
        // If there are no scan tables, no point in putting on a shared scan.
        LOGS(_log, LOG_LVL_DEBUG, "Blend chose group scanTables.size=" << scanTables.size()
             << " interactive=" << interactive);
        task->setOnInteractive(true);
        s = _group;
    } else {
        task->setOnInteractive(false);
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
            ScanScheduler::Ptr scan = std::dynamic_pointer_cast<ScanScheduler>(sched);
            if (scan != nullptr) {
                if (scan->isRatingInRange(scanPriority)) {
                    s = scan;
                    break;
                }
            }
        }
        // If the user query for this task has been booted, put this task on the snail scheduler.
        auto queryStats = _queries->getStats(task->getQueryId());
        if (queryStats && queryStats->getQueryBooted()) {
            s = _scanSnail;
        }
        if (s == nullptr) {
            // Task wasn't assigned with a scheduler, assuming it is terribly slow.
            // Assign it to the slowest scheduler so it does the least damage to other queries.
            LOGS_WARN(task->getIdStr() << " Task had unexpected scanRating="
                      << scanPriority << " adding to scanSnail");
            s = _scanSnail;
        }
    }
    task->setTaskScheduler(s);

    LOGS(_log, LOG_LVL_DEBUG, "Blend queCmd " << task->getIdStr());
    s->queCmd(task);
    _queries->queuedTask(task);
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
    wcontrol::Scheduler::Ptr s = std::dynamic_pointer_cast<wcontrol::Scheduler>(t->getTaskScheduler());
    if (s != nullptr) {
        s->commandStart(t);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "BlendScheduler::commandStart scheduler not found " << t ->getIdStr());
    }

    _queries->startedTask(t);
    _infoChanged = true;
}

void BlendScheduler::commandFinish(util::Command::Ptr const& cmd) {
    auto t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "BlendScheduler::commandFinish cmd failed conversion");
        return;
    }
    wcontrol::Scheduler::Ptr s = std::dynamic_pointer_cast<wcontrol::Scheduler>(t->getTaskScheduler());
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduler::commandFinish " << t->getIdStr());
    if (s != nullptr) {
        s->commandFinish(t);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "BlendScheduler::commandFinish scheduler not found " << t->getIdStr());
    }
    _infoChanged = true;
    _logChunkStatus();
    _queries->finishedTask(t);
    notify(true);
}


bool BlendScheduler::ready() {
    bool ready = false;
    {
        util::LockGuardTimed guard(util::CommandQueue::_mx, "BlendScheduler::ready");
        ready = _ready();
    }
    if (ready) {
        notify(false);
    }
    return ready;
}


/// Returns true when any sub-scheduler has a command ready.
/// Precondition util::CommandQueue::_mx must be locked when this is called.
bool BlendScheduler::_ready() {
    std::ostringstream os;
    bool ready = false;

    // _readSched points to the scheduler with a ready task until that
    // task has been retrieved by getCmd().
    if (_readySched != nullptr) {
        ready = true;
    }

    // Get the total number of threads schedulers want reserved
    int availableThreads = calcAvailableTheads();
    bool changed = _infoChanged.exchange(false);

    if (!ready) {
        for (auto&& sched : _schedulers) {
            availableThreads = sched->applyAvailableThreads(availableThreads);
            ready = sched->ready();
            if (changed && LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
                os << sched->getName() << "(r=" << ready << " sz=" << sched->getSize()
                       << " fl=" << sched-> getInFlight() << " avail=" << availableThreads << ") ";
            }
            if (ready) {
                _readySched = sched;
                break;
            }
        }
    }

    if (!ready) {
        ready = _ctrlCmdQueue.ready();
    }
    if (changed) {
        LOGS(_log, LOG_LVL_DEBUG, getName() << "_ready() " << os.str());
    }
    return ready;
}


util::Command::Ptr BlendScheduler::getCmd(bool wait) {
    util::Timer timeToLock;
    util::Timer timeHeld;
    util::Command::Ptr cmd;
    double totalTimeHeld = 0.0;
    bool ready = false;
    {
        timeToLock.start();
        std::unique_lock<std::mutex> lock(util::CommandQueue::_mx);
        timeToLock.stop();
        timeHeld.start();
        if (wait) {
            // util::CommandQueue::_cv.wait(lock, [this](){return _ready();});
            while (!_ready()) {
                timeHeld.stop();
                totalTimeHeld += timeHeld.getElapsed();
                util::CommandQueue::_cv.wait(lock);
                timeHeld.start();
            }
            ready = true;
        } else {
            ready = _ready();
        }

        // Try to get a command from the schedulers
        if (ready && (_readySched != nullptr)) {
            cmd = _readySched->getCmd(false);
            if (cmd != nullptr) {
                LOGS(_log, LOG_LVL_DEBUG, "Blend getCmd() using cmd from " << _readySched->getName());
                wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
            }
            _readySched.reset();
            _sortScanSchedulers();
        }

        if (cmd == nullptr) {
            // The scheduler didn't have anything, see if there's anything on the control queue,
            // which could change the size of the pool.
            cmd = _ctrlCmdQueue.getCmd();
        }
    }
    if (cmd != nullptr) {
        _infoChanged = true;
        _logChunkStatus();
        notify(true);
    }
    // returning nullptr is acceptable.
    timeHeld.stop();
    totalTimeHeld += timeHeld.getElapsed();
    LOGS(_log, LOG_LVL_DEBUG, "lockTime BlendScheduler::getCmd ready toLock=" << timeToLock.getElapsed() <<
                              " held=" << timeHeld.getElapsed() << " totalHeld=" << totalTimeHeld);
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
    for (auto const& sched : _schedulers) {
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


nlohmann::json BlendScheduler::statusToJson() {
    nlohmann::json status;
    status["name"] = getName();
    status["priority"] = getPriority();
    status["num_tasks_in_queue"] = getSize();
    status["num_tasks_in_flight"] = getInFlight();
    nlohmann::json schedulers = nlohmann::json::array();
    for (auto&& sched: _schedulers) {
        schedulers.push_back(sched->statusToJson());
    }
    status["schedulers"] = schedulers;
    return status;
}


bool BlendScheduler::isScanSnail(SchedulerBase::Ptr const& scan) {
    return scan == _scanSnail;
}


int BlendScheduler::moveUserQueryToSnail(QueryId qId, SchedulerBase::Ptr const& source) {
    if (source == _scanSnail) {
        LOGS(_log, LOG_LVL_INFO, QueryIdHelper::makeIdStr(qId)
             << " moveUserQueryToSnail can't move, query is already on snail.");
        // TODO: send a message back to czar asking to cancel query
        return 0;
    }
    return moveUserQuery(qId, source, _scanSnail);
}


int BlendScheduler::moveUserQuery(QueryId qId, SchedulerBase::Ptr const& source,
                                  SchedulerBase::Ptr const& destination) {
    LOGS(_log, LOG_LVL_DEBUG, "moveUserQuery " << QueryIdHelper::makeIdStr(qId)
         << " source=" << ((source == nullptr) ? "NULL" : source->getName())
         << " dest=" << ((destination == nullptr) ? "NULL" : destination->getName()));
    int count = 0; // Number of Tasks that were moved.
    if (destination == nullptr) {
        LOGS(_log, LOG_LVL_WARN, QueryIdHelper::makeIdStr(qId) << " moveUserQuery destination can not be nullptr");
        return count;
    }
    // Go through the Tasks in the query and remove any that are not already on the 'destination'.
    auto taskList = _queries->removeQueryFrom(qId, source);
    // Add the tasks in taskList to 'destination'. taskList only contains tasks that were on the queue,
    // not tasks that were running.
    for (auto const& task : taskList) {
        // Change the scheduler to the new scheduler as normally this is done in BlendScheduler::queCmd
        LOGS(_log, LOG_LVL_DEBUG, task->getIdStr() << " moving to " << destination->getName());
        task->setTaskScheduler(destination);
        destination->queCmd(task);
        ++count;
    }
    return count;
}


void ControlCommandQueue::queCmd(util::Command::Ptr const& cmd) {
    std::lock_guard<std::mutex> lock{_mx};
    _qu.push_back(cmd);
}


util::Command::Ptr ControlCommandQueue::getCmd() {
    std::lock_guard<std::mutex> lock{_mx};
    if (_qu.empty()) {
        return nullptr;
    }
    auto cmd = _qu.front();
    _qu.pop_front();
    return cmd;
}


bool ControlCommandQueue::ready() {
    std::lock_guard<std::mutex> lock{_mx};
    return !_qu.empty();
}

}}} // namespace lsst::qserv::wsched
