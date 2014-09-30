// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
#include <set>
#include <vector>

// Third-party headers
#include "boost/shared_ptr.hpp"
#include "boost/thread.hpp"

// Local headers
#include "lsst/log/Log.h"
#include "proto/worker.pb.h"
#include "wsched/ChunkState.h"
#include "wbase/Task.h"


namespace lsst {
namespace qserv {
namespace wsched {

class ChunkDisk {
public:
    typedef boost::shared_ptr<wbase::Task> TaskPtr;
    typedef std::set<wbase::Task const*> TaskSet;

    ChunkDisk(LOG_LOGGER const& logger)
        : _chunkState(2), _logger(logger) {}
    TaskSet getInflight() const;

    // Queue management
    void enqueue(TaskPtr a);
    TaskPtr getNext(bool allowAdvance);
    bool busy() const; /// Busy scanning a chunk?
    bool empty() const;
    int removeByHash(std::string const& hash); ///< Remove queued elt by hash

    // Inflight management
    void registerInflight(TaskPtr const& e);
    void removeInflight(TaskPtr const& e);

    bool checkIntegrity();

private:
    class TaskPtrCompare {
    public:
        bool operator()(TaskPtr const& x, TaskPtr const& y) {
            if(!x || !y) { return false; }
            if((!x->msg) || (!y->msg)) { return false; }
            return x->msg->chunkid()  > y->msg->chunkid();
        }
    };
    class IterablePq {
    public:
        typedef TaskPtr value_type;
        // pqueue takes "less" and provides a maxheap.
        // We want minheap, so provide "more"
        typedef wbase::Task::ChunkIdGreater compare;

        typedef std::vector<value_type> Container;
        Container& impl() { return _c; }

        bool empty() const { return _c.empty(); }
        Container::size_type size() const { return _c.size(); }
        value_type& top();
        value_type const& top() const;
        void push(value_type& v);
        void pop();

        template <class F>
        int removeIf(F f) {
            // Slightly expensive O(N logN) removal
            int numErased = 0;
            typename Container::iterator i = _c.begin();
            typename Container::iterator e = _c.end();
            while(i != e) {
                if(f(*i)) {
                    ++numErased;
                i = _c.erase(i);
                } else { // no match, continue
                    ++i;
                }
            }
            _maintainHeap();
            return numErased;
        }
    private:
        void _maintainHeap();
        Container _c;
    };
    typedef IterablePq Queue;


    mutable boost::mutex _queueMutex;
    Queue _activeTasks;
    Queue _pendingTasks;
    ChunkState _chunkState;
    mutable boost::mutex _inflightMutex;
    TaskSet _inflight;
    bool _completed;
    LOG_LOGGER _logger;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_CHUNKDISK_H
