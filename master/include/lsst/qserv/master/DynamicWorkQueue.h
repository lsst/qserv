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
/// @file
#ifndef LSST_QSERV_MASTER_DYNAMICWORKQUEUE_H
#define LSST_QSERV_MASTER_DYNAMICWORKQUEUE_H

#include <set>

#include "boost/thread.hpp"
#include "boost/unordered_map.hpp"


namespace lsst { namespace qserv { namespace master {

class AsyncQueryManager;

/// A dynamic work queue is a pool of threads created with some initial
/// number of threads (by default 0). As work is added, threads are created,
/// up to some maximum count. If work is scarce, threads are scavenged, down
/// to some minimum count.
///
/// Units of work must be encapsulated in `Callable` sub-classes. They are
/// added to a queue along with an associated query. The assignment of
/// work to threads of execution seeks to give each query an even
/// share of the available threads.
class DynamicWorkQueue {
    struct Queue;
    struct QueuePtrCmp { bool operator()(Queue const *, Queue const *) const; };
    struct Runner;
public:
    /// Functor encapsulating a unit of work.
    class Callable {
    public:
        Callable() : _next(0) { }

        /// Must halt current operation.
        virtual ~Callable() { _next = 0; }

        /// Execute work.
        virtual void operator()() = 0;

        /// Halt while running or otherwise.
        virtual void abort() { }

        /// Cleanup.
        virtual void cancel() { }

    private:
        Callable * _next;
        friend class DynamicWorkQueue;
        friend struct DynamicWorkQueue::Queue;
    };

    DynamicWorkQueue(size_t minThreads,       ///< Minimum # of threads.
                     size_t maxThreads,       ///< Maximum # of threads.
                     size_t initThreads = 0); ///< # of threads to create now.

    ~DynamicWorkQueue();

    /// Add `callable` to the queue, associating it with `query`.
    /// This queue assumes ownership of `callable`.
    void add(AsyncQueryManager const * query, Callable * callable);

    /// Remove and `cancel()` any `Callable` objects associated with `query`
    /// from this queue.
    void cancelQueued(AsyncQueryManager const * query);

private:
    // Disable copy-construction and assignment.
    DynamicWorkQueue(DynamicWorkQueue const &);
    DynamicWorkQueue & operator=(DynamicWorkQueue const &);

    typedef boost::unordered_map<AsyncQueryManager const *, Queue *> QueryQueueMap;
    typedef std::set<Queue *, QueuePtrCmp> QueueSet;

    size_t const  _minThreads;
    size_t const  _maxThreads;

    boost::mutex  _mutex;
    size_t        _numCallables;
    size_t        _numThreads;
    bool          _exitNow;
    QueryQueueMap _queries;
    QueueSet      _queues;
    boost::condition_variable _nonEmpty;
    boost::condition_variable _threadsExited;

    friend struct Runner;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_DYNAMICWORKQUEUE_H

