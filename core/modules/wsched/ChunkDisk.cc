// -*- LSST-C++ -*-
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
#include "wsched/ChunkDisk.h"

#include <ctime>
#include <sstream>
#include "wlog/WLogger.h"
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

namespace lsst {
namespace qserv {
namespace worker {
////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

/// @return chunkId of task
inline int taskChunkId(Task const& e) {
    assert(e.msg);
    assert(e.msg->has_chunkid());
    return e.msg->chunkid();
}
namespace {
/// @return true if the task is missing a TaskMsg or a chunkId
inline bool taskBad(ChunkDisk::TaskPtr p) {
    if(p->msg && p->msg->has_chunkid()) { return false; }
    return true;
}

/// @return true if NO elements in the queue test true for taskBad
///
template <class Q> // Use template to avoid naming Queue, which is private
inline bool checkQueueOk(Q& q) {
    typename Q::Container& container = q.impl();
    return container.end() == std::find_if(container.begin(), container.end(),
                                           std::ptr_fun(taskBad));
}

} // anonymous

////////////////////////////////////////////////////////////////////////
// ChunkDisk implementation
////////////////////////////////////////////////////////////////////////
ChunkDisk::IterablePq::value_type& ChunkDisk::IterablePq::top() {
    return _c.front();
}
ChunkDisk::IterablePq::value_type const& ChunkDisk::IterablePq::top() const {
    return _c.front();
}
void ChunkDisk::IterablePq::push(ChunkDisk::IterablePq::value_type& v) {
    _c.push_back(v);
    std::push_heap(_c.begin(), _c.end());
}
void ChunkDisk::IterablePq::pop() {
    std::pop_heap(_c.begin(), _c.end());
    _c.pop_back();
}
void ChunkDisk::IterablePq::_maintainHeap() {
    std::make_heap(_c.begin(), _c.end());
}


////////////////////////////////////////////////////////////////////////
// ChunkDisk implementation
////////////////////////////////////////////////////////////////////////
ChunkDisk::TaskSet ChunkDisk::getInflight() const {
    boost::lock_guard<boost::mutex> lock(_inflightMutex);
    return TaskSet(_inflight);
}

void ChunkDisk::enqueue(TaskPtr a) {
    boost::lock_guard<boost::mutex> lock(_queueMutex);
    int chunkId = taskChunkId(*a);
    time(&a->entryTime);
    /// Compute entry time to reduce spurious valgrind errors
    ::ctime_r(&a->entryTime, a->timestr);

    std::ostringstream os;
    os << "ChunkDisk enqueue " << chunkId;

    if(_chunkState.empty()) {
        _activeTasks.push(a);
    } else {
        if(chunkId < _chunkState.lastScan()) {
            _pendingTasks.push(a);
            os << "  PENDING";
        } else if(_chunkState.isScan(chunkId)) {
            // FIXME: If outside quantum, put on pending.
            _activeTasks.push(a);
            os << "  ACTIVE";
        } else { // Ok to be part of scan. chunk not yet started
            _activeTasks.push(a);
            os << "  ACTIVE";
        }
    }
    _logger->debug(os.str());
    os.str("");
    os << "Top of ACTIVE is now: ";
    if(_activeTasks.empty()) { os << "(empty)" ; }
    else { taskChunkId(*_activeTasks.top()); }
    _logger->debug(os.str());
}

/// Get the next task, popping it off the queue. Client must do
/// something with this task.
ChunkDisk::TaskPtr ChunkDisk::getNext(bool allowAdvance) {
    boost::lock_guard<boost::mutex> lock(_queueMutex);

    // If the current queue is empty and the pending is not,
    // Switch to the pending queue.
    if(_activeTasks.empty() && !_pendingTasks.empty()) {
        // Swap
        std::swap(_activeTasks, _pendingTasks);
        _logger->debug("ChunkDisk active-pending swap");
    }
    // If the pending was empty too, nothing to do.
    if(_activeTasks.empty()) { return TaskPtr(); }

    // Check the chunkId.
    TaskPtr e = _activeTasks.top();
    std::ostringstream os;
    int chunkId = taskChunkId(*e);
    os << "ChunkDisk getNext: current="
       << _chunkState << " candidate=" << chunkId;
    _logger->debug(os.str());
    os.str("");

    bool idle = !_chunkState.hasScan();
    bool inScan = _chunkState.isScan(chunkId);
    if(allowAdvance || idle || inScan) {
        os << "ChunkDisk allowing task for " << chunkId
           << " (advance=" << (allowAdvance ? "yes" : "no")
           << " idle=" << (idle ? "yes" : "no")
           << " inScan=" << (inScan ? "yes" : "no")
           << ")";
        _logger->debug(os.str());
        _activeTasks.pop();
        _chunkState.addScan(chunkId);
        return e;
    } else {
        _logger->debug("ChunkDisk denying task");
    }
    return TaskPtr();
    // If next chunk is of a different chunk, only continue if current
    // chunk has completed a scan already.

    // FIXME: If time for chunk has expired, advance to next chunk
    // Get the next chunk from the queue.
}

bool ChunkDisk::busy() const {
    // Simplistic view, only one chunk in flight.
    // We are busy if the inflight list is non-empty
    boost::lock_guard<boost::mutex> lock(_queueMutex);
    bool busy = _chunkState.hasScan();
    std::ostringstream os;
    os << "ChunkDisk busyness: " << (busy ? "yes" : "no");
    _logger->debug(os.str());
    return busy;

    // More advanced:
    // If we have finished one task on the current chunk, we are
    // non-busy. We infer that the resource is non-busy, assuming that
    // the chunk is now cached.

    // Should track which tables are loaded.
}

bool ChunkDisk::empty() const {
    boost::lock_guard<boost::mutex> lock(_queueMutex);
    return _activeTasks.empty() && _pendingTasks.empty();
}
struct matchHash {
    matchHash(std::string const& hash_) : hash(hash_) {}
    bool operator()(ChunkDisk::TaskPtr const& t) {
        return t && t->hash == hash;
    }
    std::string hash;
};
int ChunkDisk::removeByHash(std::string const& hash) {
    boost::lock_guard<boost::mutex> lock(_queueMutex);
    int numErased;
    matchHash mh(hash);
    numErased = _activeTasks.removeIf(mh);
    numErased += _pendingTasks.removeIf(mh);
    return numErased;
}

void
ChunkDisk::registerInflight(TaskPtr const& e) {
    boost::lock_guard<boost::mutex> lock(_inflightMutex);
    std::ostringstream os;
    os << "ChunkDisk registering for " << e->msg->chunkid()
       << " : " << e->msg->fragment(0).query(0) << " p="
       << (void*) e.get();
    _logger->debug(os.str());
    _inflight.insert(e.get());

}

void ChunkDisk::removeInflight(TaskPtr const& e) {
    boost::lock_guard<boost::mutex> lock(_inflightMutex);
    int chunkId = e->msg->chunkid();
    std::ostringstream os;
    os << "ChunkDisk remove for " << chunkId
       << " : " << e->msg->fragment(0).query(0);
    _logger->debug(os.str());
    _inflight.erase(e.get());
    {
        boost::lock_guard<boost::mutex> lock(_queueMutex);
        _chunkState.markComplete(chunkId);
    }
}


// @return true if things are okay
bool ChunkDisk::checkIntegrity() {
    boost::lock_guard<boost::mutex> lock(_queueMutex);
    // Check for chunk ids in all tasks.
    return checkQueueOk(_activeTasks) && checkQueueOk(_pendingTasks);
}

}}} // lsst::qserv::worker
