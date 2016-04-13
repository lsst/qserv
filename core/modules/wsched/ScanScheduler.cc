// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
#include "wsched/ScanScheduler.h"

// System headers
#include <cstddef>
#include <iostream>
#include <mutex>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
#include "wcontrol/Foreman.h"
#include "wsched/BlendScheduler.h"
#include "wsched/ChunkDisk.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ScanScheduler");
}

namespace lsst {
namespace qserv {
namespace wsched {

ScanScheduler::ScanScheduler(std::string const& name, int maxThreads, int maxReserve, int priority,
                             memman::MemMan::Ptr const& memMan, int minRating, int maxRating)
    : SchedulerBase{name, maxThreads, maxReserve, priority},
      _memMan{memMan}, _minRating{minRating}, _maxRating{maxRating} {
    _disk = std::make_shared<ChunkDisk>(_memMan);
    assert(_minRating <= _maxRating);
}


void ScanScheduler::commandStart(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
    _infoChanged = true;
    if (task == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "ScanScheduler::commandStart cmd failed conversion");
        return;
    }
    LOGS(_log, LOG_LVL_DEBUG, "ScanScheduler::commandStart " << task->getIdStr());
    // task was registered Inflight when getCmd() was called.
}

void ScanScheduler::commandFinish(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    _infoChanged = true;
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "ScanScheduler::commandFinish cmd failed conversion");
        return;
    }
    std::lock_guard<std::mutex> guard(util::CommandQueue::_mx);
    --_inFlight;

    if (_memManHandleToUnlock != memman::MemMan::HandleType::INVALID) {
        _memMan->unlock(_memManHandleToUnlock);
    }
    if (_disk->getSize() != 0) {
        _memManHandleToUnlock = t->getMemHandle();
    } else {
        _memMan->unlock(t->getMemHandle());
    }

    _decrChunkTaskCount(t->getChunkId());
    LOGS(_log, LOG_LVL_DEBUG, "ScanScheduler::commandFinish inFlight=" << _inFlight);
    if (_disk->nextTaskDifferentChunkId()) {
        applyPriority();
    }
    // Whenever a Task finishes, all sleeping threads need to check if resources
    // are available to run new Tasks.
    _cv.notify_all();
}


/// Returns true if there is a Task ready to go and we aren't up against any limits.
bool ScanScheduler::ready() {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    return _ready();
}

/// Precondition: _mx is locked
/// Returns true if there is a Task ready to go and we aren't up against any limits.
bool ScanScheduler::_ready() {
    if (_infoChanged) {
        _infoChanged = false;
        LOGS(_log, LOG_LVL_DEBUG, "ScanScheduler::_ready name="<< getName() << " inFlight="
             << _inFlight << " maxThreads=" << _maxThreads << " adj=" << _maxThreadsAdj
             << " activeChunks=" << getActiveChunkCount());
    }
    if (_inFlight >= maxInFlight()) {
        return false;
    }
    /* &&& re-enable
    if (_disk->nextTaskDifferentChunkId() && getActiveChunkCount() >= 3) {   // &&& replace magic number or delete entire if block
        return false;
    }
    */
    bool useFlexibleLock = (_inFlight < 1);
    auto rdy = _disk->ready(useFlexibleLock); // Only returns true if MemMan grants resources.
    if (_memManHandleToUnlock != memman::MemMan::HandleType::INVALID) {
        _memMan->unlock(_memManHandleToUnlock);
        _memManHandleToUnlock = memman::MemMan::HandleType::INVALID;
    }
    return rdy;
}


std::size_t ScanScheduler::getSize() const {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    return _disk->getSize();
}


util::Command::Ptr ScanScheduler::getCmd(bool wait)  {
    std::unique_lock<std::mutex> lock(util::CommandQueue::_mx);
    if (wait) {
        util::CommandQueue::_cv.wait(lock, [this](){return _ready();});
    } else if (!_ready()) {
        return nullptr;
    }
    bool useFlexibleLock = (_inFlight < 1);
    auto task = _disk->getTask(useFlexibleLock);
    ++_inFlight; // in flight as soon as it is off the queue.
    _infoChanged = true;
    _decrCountForUserQuery(task->getQueryId());
    _incrChunkTaskCount(task->getChunkId());
    return task;
}


void ScanScheduler::queCmd(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, getName() << " queCmd could not be converted to Task or was nullptr");
        return;
    }
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    auto uqCount = _incrCountForUserQuery(t->getQueryId());
    LOGS(_log, LOG_LVL_DEBUG, getName() << " queCmd " << t->getIdStr()
         << " uqCount=" << uqCount);
    _disk->enqueue(t);
    _infoChanged = true;
    util::CommandQueue::_cv.notify_all();
}

}}} // namespace lsst::qserv::wsched
