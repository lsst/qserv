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
#ifndef LSST_QSERV_WSCHED_CHUNKDISK_H
#define LSST_QSERV_WSCHED_CHUNKDISK_H
 /**
  * @file
  *
  * @brief ChunkDisk is a resource that queues tasks for chunks on a disk.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <algorithm>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

// Qserv headers
#include "proto/worker.pb.h"
#include "wbase/Task.h"
#include "wsched/ChunkState.h"


namespace lsst {
namespace qserv {
namespace wsched {

/// ChunkDisk is meant to keep disc i/o down by reading a single chunk at
/// a time. It is meant to be connected to a single disk. It provides
/// Tasks for a single chunk and once it is told that reading has completed,
/// it advances to the next chunk.
///
/// It may be possible to improve performance by queuing Tasks on the
/// active heap if the chunk was recently read in. There are pitfalls
/// to doing this as continual requests could prevent ChunkDisk from
/// advancing to the next chunk and no requests would finish.
class ChunkDisk {
public:
    using TaskSet = std::set<wbase::Task const*>;

    TaskSet getInflight() const;

    // Queue management
    void enqueue(wbase::Task::Ptr const& a);
    wbase::Task::Ptr getTask();
    bool busy() const; /// Busy scanning a chunk?
    bool empty() const;
    bool ready();
    std::size_t getSize() const;

    // Inflight management
    void registerInflight(wbase::Task::Ptr const& e);
    bool removeInflight(wbase::Task::Ptr const& e);

    /// Class that keeps the minimum chunk id at the front of the heap.
    class MinHeap {
    public:
        // Using a greater than comparison function results in a minimum value heap.
        std::function<bool(wbase::Task::Ptr const&, wbase::Task::Ptr const&)> compareFunc =
            [](wbase::Task::Ptr const& x, wbase::Task::Ptr const& y) -> bool {
                if(!x || !y) { return false; }
                if((!x->msg) || (!y->msg)) { return false; }
                return x->msg->chunkid() > y->msg->chunkid();
        };
        void push(wbase::Task::Ptr const& task);
        wbase::Task::Ptr pop();
        wbase::Task::Ptr top() { return _tasks.front(); }
        bool empty() const { return _tasks.empty(); }
        void heapify() {
            std::make_heap(_tasks.begin(), _tasks.end(), compareFunc);
        }

        std::vector<wbase::Task::Ptr> _tasks;
    };

private:
    bool _busy() const;
    bool _empty() const;
    bool _ready();

    mutable std::mutex _queueMutex;
    MinHeap _activeTasks;
    MinHeap _pendingTasks;
    ChunkState _chunkState;
    mutable std::mutex _inflightMutex;
    TaskSet _inflight;
};

}}} // namespace

#endif // LSST_QSERV_WSCHED_CHUNKDISK_H
