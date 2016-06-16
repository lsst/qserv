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

// Class header
#include "wsched/ChunkDisk.h"

// System headers
#include <ctime>
#include <errno.h>
#include <exception>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/IterableFormatter.h"

/// ChunkDisk is a data structure that tracks a queue of pending tasks
/// for a disk, and the state of a chunkId-ordered scan on a disk
/// (current chunkId, tasks in flight).
///
/// It tracks the queue in two priority queues. Each queue is sorted
/// so according to chunkId, where the top element is has the lowest
/// chunkId. Two queues are used so that new incoming queries do not
/// "cut in front" of the queue when the scan (repeated scans of
/// monotonically increasing chunkId tables). If the chunkId is lower
/// than the current chunkId, the task is placed in the pending
/// queue. Also, when the time available for a single chunk has
/// passed, no more tasks should attach to that chunk, and thus the
/// queue should move onto another chunk (prevent starvation of other
/// chunks if new queries for the current chunk keep coming in). In
/// that case the incoming task is passed to the pending queue as
/// well.
///
/// _currentChunkIds tracks the chunkIds that should be
/// in-flight. _inflight is insufficient for this because it is
/// populated as queries execute. There is a delay between the time that
/// the scheduler returns task elements for execution and the starting
/// of those tasks. Within that delay, the chunkdisk may get a request
/// for more tasks. This happens infrequently, but can be reproduced
/// with high probability (>50%) while testing with a shared-scan load:
/// multiple chunks are launched and some queries complete much earlier
/// than others.

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ChunkDisk");
}

namespace lsst {
namespace qserv {
namespace wsched {
////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

void ChunkDisk::MinHeap::push(wbase::Task::Ptr const& task) {
    _tasks.push_back(task);
    std::push_heap(_tasks.begin(), _tasks.end(), compareFunc);
}
wbase::Task::Ptr ChunkDisk::MinHeap::pop() {
    if (_tasks.empty()) {
        return nullptr;
    }
    auto task = _tasks.front();
    std::pop_heap(_tasks.begin(), _tasks.end(), compareFunc);
    _tasks.pop_back();
    return task;
}


void ChunkDisk::queueTask(wbase::Task::Ptr const& a) {
    std::lock_guard<std::mutex> lock(_queueMutex);
    int chunkId = a->getChunkId();
    time(&a->entryTime);
    /// Compute entry time to reduce spurious valgrind errors
    ::ctime_r(&a->entryTime, a->timestr);

    const char* state = "";
    // To keep from getting stuck  on this chunkId, put new requests for this chunkId on pending.
    if (chunkId <= _lastChunk) {
        _pendingTasks.push(a);
        state = "PENDING";
    } else { // Ok to be part of scan. chunk not yet started
        _activeTasks.push(a);
        state = "ACTIVE";
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "ChunkDisk enqueue "
         << a->getIdStr()
         << " chunkId=" << chunkId
         << " state=" << state
         << " lastChunk=" << _lastChunk
         << " active.sz=" << _activeTasks._tasks.size()
         << " pend.sz=" << _pendingTasks._tasks.size());
    if (_activeTasks.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: (empty)");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: " << _activeTasks.top()->getChunkId());
    }
}


/// @return true if the next Task has a different chunkId than the current Task.
/// The purpose here is that the best time to change priority or switch to doing
/// something else is when all the Tasks for the current chunkId have finished.
bool ChunkDisk::nextTaskDifferentChunkId() {
    auto const& topTask = _activeTasks.top();
    if (topTask == nullptr) return true; // going to switch to pending, new chunkId
    return topTask->getChunkId() != _lastChunk;
}


/// Return true if this disk is ready to provide a Task from its queue.
bool ChunkDisk::ready(bool useFlexibleLock) {
    std::lock_guard<std::mutex> lock(_queueMutex);
    return _ready(useFlexibleLock);
}

/// Precondition: _queueMutex must be locked
/// Return true if this disk is ready to provide a Task from its queue.
bool ChunkDisk::_ready(bool useFlexibleLock) {
    auto logMemManRes =
        [this](bool newVal, std::string const& msg, std::vector<memman::TableInfo> const& tblVect) {
        if (setResourceStarved(newVal)) {
            std::string str;
            for (auto const& tblInfo:tblVect) {
                str += tblInfo.tableName + " ";
            }
            LOGS(_log, LOG_LVL_DEBUG, "ready memMan " << msg << " - " << str);
        }
    };

    // If the current queue is empty and the pending is not,
    // Switch to the pending queue.
    if (_activeTasks.empty() && !_pendingTasks.empty()) {
        std::swap(_activeTasks, _pendingTasks);
        LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk active-pending swap");
    }
    // If _pendingTasks was empty too, nothing to do.
    if(_activeTasks.empty()) { return false; }

    wbase::Task::Ptr task = _activeTasks.top();
    // Try to get memHandle for the task if doesn't have one.
    if (!task->hasMemHandle()) {
        memman::TableInfo::LockType lckOptTbl = memman::TableInfo::LockType::MUSTLOCK;
        memman::TableInfo::LockType lckOptIdx = memman::TableInfo::LockType::NOLOCK;
        if (useFlexibleLock) lckOptTbl = memman::TableInfo::LockType::FLEXIBLE;
        auto scanInfo = task-> getScanInfo();
        auto chunkId = task->getChunkId();
        std::vector<memman::TableInfo> tblVect;
        for (auto const& tbl : scanInfo.infoTables) {
            memman::TableInfo ti(tbl.db + "/" + tbl.table, lckOptTbl, lckOptIdx);
            tblVect.push_back(ti);
        }
        // If tblVect is empty, we should get the empty handle
        memman::MemMan::Handle handle = _memMan->lock(tblVect, chunkId);
        if (handle == 0) {
            switch (errno) {
            case ENOMEM:
                logMemManRes(true, "ENOMEM", tblVect);
                return false;
            case ENOENT:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock errno=ENOENT chunk not found " << task->getIdStr());
                // Not sure if this is the best course of action, but it should just need one
                // logic path. The query should fail from the missing tables
                // and the czar must be able to handle that with appropriate retries.
                handle = memman::MemMan::HandleType::ISEMPTY;
                break;
            default:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock file system error " << task->getIdStr());
                // Any error reading the file system is probably fatal for the worker.
                throw std::bad_exception();
                return false;
            }
        }
        task->setMemHandle(handle);
        logMemManRes(false, "got handle", tblVect);
        // Once the chunk has been granted, everything equal and below must go on pending.
        // Otherwise there's a risk of a Task with lower or same chunkId getting in front
        // of this one and needing the resources this Task has been promised.
        _lastChunk = chunkId;
    }
    return true;
}

/// Return a Task that is ready to run, if available.
wbase::Task::Ptr ChunkDisk::getTask(bool useFlexibleLock) {
    LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk::getTask start");
    std::lock_guard<std::mutex> lock(_queueMutex);
    if (!_ready(useFlexibleLock)) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk denying task");
        return nullptr;
    }
    // Check the chunkId.
    auto task = _activeTasks.pop();
    int chunkId = task->getChunkId();
    LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk getTask: current=" << _lastChunk
         << " candidate=" << chunkId << " " << task->getIdStr());
    return task;
}

// @return true if value changed.
// TODO: DM-4943 add statistics.
bool ChunkDisk::setResourceStarved(bool starved) {
    if (starved != _resourceStarved) {
        _resourceStarved = starved;
        LOGS(_log, LOG_LVL_DEBUG, "resourceStarved changed to " << _resourceStarved);
        return true;
    }
    return false;
}


bool ChunkDisk::empty() const {
    std::lock_guard<std::mutex> lock(_queueMutex);
    return _empty();
}

/// Precondition: _queueMutex must be locked
bool ChunkDisk::_empty() const {
    return _activeTasks.empty() && _pendingTasks.empty();
}

std::size_t ChunkDisk::getSize() const {
    std::lock_guard<std::mutex> lock(_queueMutex);
    return _activeTasks._tasks.size() + _pendingTasks._tasks.size();
}

}}} // namespace lsst::qserv::wsched
