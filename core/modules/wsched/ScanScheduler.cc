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
#include "wsched/ChunkDisk.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ScanScheduler");
}

namespace lsst {
namespace qserv {
namespace wsched {


ScanScheduler::ScanScheduler(std::string name, int maxThreads, memman::MemMan::Ptr const& memMan)
    : _name(name), _maxThreads(maxThreads), _maxThreadsAdj(maxThreads), _memMan(memMan)
{
    _disk = std::make_shared<ChunkDisk>(_memMan);
}

void ScanScheduler::commandStart(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr task = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (task == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "ScanScheduler::commandStart cmd failed conversion");
        return;
    }
    LOGS(_log, LOG_LVL_DEBUG, "ScanScheduler::commandStart tSeq=" << task->tSeq);
    // task was registeredInflight on its disk when getCmd() was called.
}

void ScanScheduler::commandFinish(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "ScanScheduler::commandFinish cmd failed conversion");
        return;
    }
    std::lock_guard<std::mutex> guard(util::CommandQueue::_mx);
    // If removeInflight finishes a scan, need to notify threads that
    // they might be able to advance to the next chunk.
    bool needNotify = _disk->removeInflight(t);
    --_inFlight;
    LOGS(_log, LOG_LVL_DEBUG, "ScanScheduler::commandFinish inFlight= " << _inFlight);
    if (needNotify) {
        _cv.notify_all();
    }
}


/// Returns true if there is a Task ready to go and we aren't up against any limits.
bool ScanScheduler::ready() {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    return _ready();
}

/// Precondition: _mx is locked
/// Returns true if there is a Task ready to go and we aren't up against any limits.
bool ScanScheduler::_ready() {
    bool useFlexibleLock = (_inFlight < 1);
    auto rdy = _disk->ready(useFlexibleLock);
    return rdy && _inFlight < _maxInFlight();

    // Check with resource manager if resources are available for the task
    /* &&&
    auto task = _queue.front()->peekTask();
    if (!task->hasMemHandle) {
        TableInfo::LockOptions lckOptTbl = TableInfo::LockOptions::MUSTLOCK;
        TableInfo::LockOPtions lckOptIdx = TableInfo::LockOptions::NOLOCK;
        if (_inFlight < 1) lckOptTbl = TableInfo::FLEXIBLE;
        auto tblVect = task->getTableVector(lckOptTbl, lckOptIdx); // &&& no so thrilled with putting this in Task
        auto chunkId = task->getChunkId();
        Handle handle = _memMgr->lock(tblVect, chunkId); // &&& what happens if tblVect is empty?
        if (handle == 0) {
            switch (_memMgr->errno) {
            case MemMan::ENOMEM:
                _resourceStarved = true;
                return false;
            case MemMan::ENOENT:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock errno=ENOENT chunk not found");
                // &&& is the correct course of action to go ahead and run the query.
                // &&& ??? It should get a mysql error, causing a msg to czar to run it somewhere else.
                // &&& ??? or cancel the task and then continue on ???
                break;
            case MemMan::EFAULT:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock errno=EFAULT");
                // &&& appropropriate course of action with file system fault is ???
                // &&& throw error or
                return false;
            }
        }
        task->setMemHandle(handle);
    }
    */
}


/// Sets the value of _maxThreadsAdj and must be set while in the
/// same critical region as the _ready() calls that use it.
/// The BlendScheduler must maintain the lock on its mutex to use this.
// &&& basically duplicate of same thing in GroupScheduler. Move to wcontrol::Scheduler along with maxThreads, maxThreadsAdj, and _maxInFlight()?
void ScanScheduler::maxThreadAdjust(int tempMax) {
    _maxThreadsAdj = tempMax;
}


std::size_t ScanScheduler::getSize() const {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    return _disk->getSize();
}

util::Command::Ptr ScanScheduler::getCmd(bool wait)  {
    std::unique_lock<std::mutex> lock(util::CommandQueue::_mx);
    if (wait) {
        util::CommandQueue::_cv.wait(lock, [this](){return _ready();});
    } else {
        if (!_ready()) {
            return nullptr;
        }
    }
    bool useFlexibleLock = (_inFlight < 1);
    auto task = _disk->getTask(useFlexibleLock);
    ++_inFlight; // in flight as soon as it is off the queue.
    return task;
}

void ScanScheduler::queCmd(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, getName() << " queCmd could not be converted to Task or was nullptr");
        return;
    }
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    LOGS(_log, LOG_LVL_DEBUG, getName() << " queCmd tSeq=" << t->tSeq);
    _disk->enqueue(t);
    util::CommandQueue::_cv.notify_all();
}

}}} // namespace lsst::qserv::wsched
