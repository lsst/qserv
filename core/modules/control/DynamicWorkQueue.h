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
#ifndef LSST_QSERV_CONTROL_DYNAMICWORKQUEUE_H
#define LSST_QSERV_CONTROL_DYNAMICWORKQUEUE_H

// System headers
#include <map>
#include <set>

// Third-party headers
#include "boost/thread.hpp"


namespace lsst {
namespace qserv {
namespace control {

/// A dynamic work queue is a pool of threads created with some initial
/// number of threads (by default 0). As work is added, threads are created,
/// up to some maximum count. If work is scarce, threads are scavenged, down
/// to some minimum count.
///
/// Units of work must be encapsulated in `Callable` sub-classes. They are
/// added to a queue along with an associated session. The assignment of
/// work to threads of execution seeks to give each session an even
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

        /// Cleanup. Must not throw.
        virtual void cancel() { }

    private:
        Callable * _next; // Embedded singly linked-list pointer; not owned.
        friend class DynamicWorkQueue;
        friend struct DynamicWorkQueue::Queue;
    };

    DynamicWorkQueue(
        size_t minThreads,           ///< Minimum # of threads overall.
        size_t minThreadsPerSession, ///< Minimum # of threads per session.
        size_t maxThreads,           ///< Maximum # of threads overall.
        size_t initThreads = 0);     ///< # of threads to create up front.

    ~DynamicWorkQueue();

    /// Add `callable` to the queue, associating it with `session`.
    /// Ownership of `callable` is transfered from the caller to the queue.
    void add(void const * session, Callable * callable);

    /// Remove and `cancel()` any `Callable` objects associated with `session`
    /// from this queue.
    void cancelQueued(void const * session);

private:
    // Disable copy-construction and assignment.
    DynamicWorkQueue(DynamicWorkQueue const &);
    DynamicWorkQueue & operator=(DynamicWorkQueue const &);

    typedef std::map<void const *, Queue *> SessionQueueMap;
    typedef std::set<Queue *, QueuePtrCmp> QueueSet;

    // Call only while holding a lock on _mutex.
    bool _shouldIncreaseThreadCount() const;
    bool _shouldDecreaseThreadCount() const;

    size_t const _minThreads;
    size_t const _minThreadsPerSession;
    size_t const _maxThreads;

    boost::mutex              _mutex;
    size_t                    _numCallables;
    size_t                    _numThreads;
    bool                      _exitNow;
    SessionQueueMap           _sessions;
    QueueSet                  _nonEmptyQueues;
    boost::condition_variable _workAvailable;
    boost::condition_variable _threadsExited;

    friend struct Runner;
};

}}} // namespace lsst::qserv::control

#endif // LSST_QSERV_CONTROL_DYNAMICWORKQUEUE_H

