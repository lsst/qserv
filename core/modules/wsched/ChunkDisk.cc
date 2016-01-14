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

/// @return chunkId of task
inline int taskChunkId(wbase::Task const& e) {
    assert(e.msg);
    assert(e.msg->has_chunkid());
    return e.msg->chunkid();
}

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

ChunkDisk::TaskSet ChunkDisk::getInflight() const {
    std::lock_guard<std::mutex> lock(_inflightMutex);
    return TaskSet(_inflight);
}

void ChunkDisk::enqueue(wbase::Task::Ptr const& a) {
    std::lock_guard<std::mutex> lock(_queueMutex);
    int chunkId = taskChunkId(*a);
    time(&a->entryTime);
    /// Compute entry time to reduce spurious valgrind errors
    ::ctime_r(&a->entryTime, a->timestr);

    const char* state = "";
    if (_chunkState.empty()) {
        _activeTasks.push(a);
        state = "EMPTY";
    } else {
        // To keep from getting stuck  on this chunkId, put new requests for this chunk on pending.
        if (chunkId <= _chunkState.lastScan()) {
            _pendingTasks.push(a);
            state = "PENDING";
        } else { // Ok to be part of scan. chunk not yet started
            _activeTasks.push(a);
            state = "ACTIVE";
        }
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "ChunkDisk enqueue "
         << "chunkId=" << chunkId
         << " state=" << state
         << " tSeq=" << a->tSeq
         << " lastScan=" << _chunkState.lastScan()
         << " active.sz=" << _activeTasks._tasks.size()
         << " pend.sz=" << _pendingTasks._tasks.size());
    if (_activeTasks.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: (empty)");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: " << taskChunkId(*_activeTasks.top()));
    }
}

/// Return true if this disk is ready to provide a Task from its queue.
bool ChunkDisk::ready(bool useFlexibleLock) {
    std::lock_guard<std::mutex> lock(_queueMutex);
    return _ready(useFlexibleLock);
}

/// Precondition: _queueMutex must be locked
/// Return true if this disk is ready to provide a Task from its queue.
bool ChunkDisk::_ready(bool useFlexibleLock) {
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
        // auto tblVect = task->getTableVector(lckOptTbl, lckOptIdx); // &&& no so thrilled with putting this in Task &&& delete
        auto scanInfo = task-> getScanInfo();
        auto chunkId = task->getChunkId();
        std::vector<memman::TableInfo> tblVect;
        for (auto const& tbl : scanInfo.infoTables) {
            memman::TableInfo ti(tbl.db + "." + tbl.table, lckOptTbl, lckOptIdx);
            tblVect.push_back(ti);
        }
        // If tblVect is empty, we should get the empty handle
        memman::MemMan::Handle handle = _memMan->lock(tblVect, chunkId);
        if (handle == 0) {
            switch (errno) {
            case ENOMEM:
                setResourceStarved(true);
                return false;
            case ENOENT:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock errno=ENOENT chunk not found tSeq=" << task->tSeq);
                // Not sure if this is the best course of action, but it should just need one
                // logic path. The query should fail from the missing tables
                // and the czar must be able to handle that with appropriate retries.
                handle = memman::MemMan::HandleType::ISEMPTY;
                break;
            default:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock file system error tSeq=" << task->tSeq);
                // Any error reading the file system is probably fatal for the worker.
                throw std::bad_exception();
                return false;
            }
        }
        task->setMemHandle(handle);
        setResourceStarved(false);
    }
    return true;

    // &&& ready if we've got a handle from MemMan for the top task
    // &&& add argument to _ready() and getTask() to indicate FLEXIBLE lock should be used.

    // &&& delete from here to end of function
    /* &&& delete
    // TODO: A timeout should be added such that we can advance to the next chunkId if too much time has
    // been spent on this chunkId, or we need a better method of determining when a chunk has been read than
    // waiting for the a query on the chunk to finish.
    wbase::Task::Ptr top = _activeTasks.top();
    int chunkId = taskChunkId(*top);
    bool allowAdvance = !_busy() && !_empty();
    bool idle = !_chunkState.hasScan();
    bool inScan = _chunkState.isScan(chunkId);
    LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk::_ready() " << "allowAdvance=" << allowAdvance
         << " idle=" << idle << " inScan=" << inScan);
    return allowAdvance || idle || inScan;
    */
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
    int chunkId = taskChunkId(*task);
    LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk getTask: current= " << _chunkState
         << " candidate=" << chunkId << " tSeq=" << task->tSeq);
    _chunkState.addScan(chunkId);
    registerInflight(task); // consider the task inflight as soon as it's off the queue
    return task;
    // If next chunk is of a different chunk, only continue if current
    // chunk has completed a scan already.

    // FIXME: If time for chunk has expired, advance to next chunk
    // Get the next chunk from the queue.
}

// TODO: DM-4943 add statistics.
void ChunkDisk::setResourceStarved(bool starved) {
    if (starved != _resourceStarved) {
        _resourceStarved = starved;
        LOGS(_log, LOG_LVL_DEBUG, "resourceStarved changed to " << _resourceStarved);
    }
}


bool ChunkDisk::busy() const {
    std::lock_guard<std::mutex> lock(_queueMutex);
    return _busy();
}

/// Precondition: _queueMutex must be locked
bool ChunkDisk::_busy() const {
    // Simplistic view, only one chunk in flight.
    // We are busy if the inflight list is non-empty
    bool busy = _chunkState.hasScan();
    LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk busyness: " << (busy ? "yes" : "no"));
    return busy;

    // More advanced:
    // If we have finished one task on the current chunk, we are
    // non-busy. We infer that the resource is non-busy, assuming that
    // the chunk is now cached.

    // Should track which tables are loaded.
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

void ChunkDisk::registerInflight(wbase::Task::Ptr const& e) {
    std::lock_guard<std::mutex> lock(_inflightMutex);
    LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk registering for "
         << e->msg->chunkid() << ": "
         << e->msg->fragment(0).query(0)
         << " p=" << (void*) e.get());
    _inflight.insert(e.get());
}


/// Remove the Task from the set of inflight Tasks.
/// @Return true if a scan completed, which means that there is potential for
/// new Tasks to be started.
bool ChunkDisk::removeInflight(wbase::Task::Ptr const& e) {
    std::lock_guard<std::mutex> lock(_inflightMutex);
    int chunkId = e->msg->chunkid();
    LOGS(_log, LOG_LVL_DEBUG, "ChunkDisk remove for "
         << chunkId << ": " << e->msg->fragment(0).query(0));
    _inflight.erase(e.get());
    {
        std::lock_guard<std::mutex> lock(_queueMutex);
        return _chunkState.markComplete(chunkId);
    }
}

}}} // namespace lsst::qserv::wsched
