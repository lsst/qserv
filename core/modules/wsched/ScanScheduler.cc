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

ChunkDisk* dbgChunkDisk1 = nullptr; ///< A symbol for gdb


ScanScheduler::ScanScheduler(int maxThreads)
    : _maxThreads{maxThreads},
      _disks{}
{
    _disks.push_back(std::make_shared<ChunkDisk>());
    dbgChunkDisk1 = _disks.front().get();
    assert(!_disks.empty());
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
    assert(!_disks.empty());
    // If removeInflight finishes a scan, need to notify threads that
    // they might be able to advance to the next chunk.
    bool needNotify = _disks.front()->removeInflight(t);
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
    // FIXME: Select disk based on chunk location. Currently only ever 1 element in _disks.
    // FIXME: Pass most appropriate disk to getCmd().
    // Check disks for candidate ones.
    // Pick one. Prefer a less-loaded disk: want to make use of i/o
    // from both disks. (for multi-disk support)
    assert(!_disks.empty());
    assert(_disks.front());
    auto rdy = _disks.front()->ready();
    return rdy && _inFlight < _maxThreads;
}

std::size_t ScanScheduler::getSize() {
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    // When more disks are available, the total will need to be found.
    return _disks.front()->getSize();
}

util::Command::Ptr ScanScheduler::getCmd(bool wait)  {
    std::unique_lock<std::mutex> lock(util::CommandQueue::_mx);
    if (wait) {
        util::CommandQueue::_cv.wait(lock, [this](){return _ready();});
    }
    auto task =  _disks.front()->getTask();;
    ++_inFlight; // in flight as soon as it is off the queue.
    return task;
}

void ScanScheduler::queCmd(util::Command::Ptr const& cmd) {
    wbase::Task::Ptr t = std::dynamic_pointer_cast<wbase::Task>(cmd);
    if (t == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "ScanScheduler::queCmd could not be converted to Task or was nullptr");
        return;
    }
    std::lock_guard<std::mutex> lock(util::CommandQueue::_mx);
    assert(!_disks.empty());
    assert(_disks.front());
    LOGS(_log, LOG_LVL_DEBUG, "ScanScheduler::queCmd tSeq=" << t->tSeq);
    _disks.front()->enqueue(t);
    util::CommandQueue::_cv.notify_all();
}

}}} // namespace lsst::qserv::wsched
