// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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

// Class header
#include "util/DynamicWorkQueue.h"

// System headers
#include <cassert>
#include <stdexcept>
#include <sys/time.h>

// Third-party headers
#include <boost/scoped_ptr.hpp>

namespace lsst {
namespace qserv {
namespace util {

// A linked list of Callable objects associated with a specific session.
struct DynamicWorkQueue::Queue {
    // # of threads running work from this queue.
    size_t numThreads;
    // Queue creation time in seconds since the Epoch.
    double createTime;
    // Opaque handle used to look up the Queue for a session by DynamicWorkQueue.
    void const * session;
    // Singly linked list of callables.
    DynamicWorkQueue::Callable * head;
    DynamicWorkQueue::Callable * tail;

    Queue(void const * handle) :
        numThreads(0), session(handle), head(0), tail(0)
    {
        struct ::timeval t;
        ::gettimeofday(&t, NULL);
        createTime = t.tv_sec + 0.000001 * t.tv_usec;
    }

    ~Queue() {
        Callable * c = head;
        head = tail = 0;
        while (c) {
            Callable * next = c->_next;
            delete c;
            c = next;
        }
    }

    bool empty() const { return head == 0; }

    // Take ownership of a Callable and add it to the end of the queue.
    void put(Callable * c) {
        if (c) {
            if (tail) {
                tail->_next = c;
                tail = c;
            } else {
                head = tail = c;
            }
        }
    }

    // Remove a Callable from the beginning of the queue and relinquish
    // ownership of it. If the queue is empty, NULL is returned.
    Callable * take() {
        Callable * c = head;
        if (c) {
            Callable * next = c->_next;
            head = next;
            if (next == 0) {
                tail = 0;
            }
        }
        return c;
    }

    // Remove and relinquish ownership for all Callable objects in the queue.
    Callable * takeAll() {
        Callable * c = head;
        head = tail = 0;
        return c;
    }
};


// Order queue pointers lexicographically by
// (active thread count, queue creation time, queue memory address).
bool DynamicWorkQueue::QueuePtrCmp::operator()(
    DynamicWorkQueue::Queue const *x,
    DynamicWorkQueue::Queue const *y) const
{
    if (x->numThreads < y->numThreads) {
        return true;
    } else if (x->numThreads == y->numThreads) {
        if (x->createTime < y->createTime) {
            return true;
        } else if (x->createTime == y->createTime) {
            return x < y;
        }
    }
    return false;
}


// Wraps a DynamicWorkQueue reference and implements the work scheduling loop.
struct DynamicWorkQueue::Runner {
    Runner(DynamicWorkQueue & queue) : wq(queue) { }
    void operator()();
    DynamicWorkQueue & wq;
};

void DynamicWorkQueue::Runner::operator()() {
    std::unique_lock<std::mutex> lock(wq._mutex);
    do {
        // Wait for work or an exit signal.
        while (wq._nonEmptyQueues.empty() && !wq._exitNow) {
            wq._workAvailable.wait(lock);
        }
        if (wq._exitNow) { break; }
        // The first set element is the oldest of the queues with the smallest
        // active thread count.
        Queue *q = *wq._nonEmptyQueues.begin();
        // Remove q from _nonEmptyQueues prior to updating it - this is
        // necessary because the queues may be reordered by the update.
        //
        // Unfortunately, this means that q may have to be reinserted
        // later. While this breaks the exception safety of the code, it only
        // does so when the insert fails to allocate memory, in which case it
        // is probably OK to terminate() the czar.
        wq._nonEmptyQueues.erase(q);
        assert(q && !q->empty());

        q->numThreads += 1; // Increment the active thread count for q.
        // Remove a callable from q and take responsibility for deleting it.
        boost::scoped_ptr<Callable> c(q->take());
        if (!q->empty()) {
            // Work remains in q, so make it available to other threads.
            wq._nonEmptyQueues.insert(q);
        }
        lock.unlock();

        // Execute work
        (*c)();
        c.reset();

        lock.lock();
        wq._numCallables -= 1;

        // Remove q from _nonEmptyQueues prior to updating. Note that another
        // thread may have inserted q into _nonEmptyQueues (when add()ing a
        // Callable for the same session), even if q wasn't reinserted above.
        wq._nonEmptyQueues.erase(q);
        q->numThreads -= 1; // Decrement active thread count for q.
        if (!q->empty()) {
            // Work remains in q, so make it available to other threads.
            wq._nonEmptyQueues.insert(q);
        } else if (q->numThreads == 0) {
            // q is empty and no threads are in-flight for the associated session.
            wq._sessions.erase(q->session);
            delete q;
        }
    } while (!wq._shouldDecreaseThreadCount());
    wq._numThreads -= 1;
    // ~DynamicWorkQueue waits for all Runner threads to complete before
    // proceeding. If this is the last Runner, signal the destructor
    // that it's OK to proceed.
    if (wq._numThreads == 0) {
        wq._threadsExited.notify_one();
    }
}

void
DynamicWorkQueue::_startRunner(DynamicWorkQueue& dwq) {
    Runner r(dwq);
    r();
}

DynamicWorkQueue::DynamicWorkQueue(size_t minThreads,
                                   size_t minThreadsPerSession,
                                   size_t maxThreads,
                                   size_t initThreads) :
    _minThreads(minThreads),
    _minThreadsPerSession(minThreadsPerSession),
    _maxThreads(maxThreads),
    _numCallables(0),
    _numThreads(std::min(initThreads, maxThreads)),
    _exitNow(false)
{
    if (minThreads > maxThreads || maxThreads == 0) {
        throw std::runtime_error("Invalid DynamicWorkQueue min/max thread counts.");
    }
    for (size_t n = _numThreads; n != 0; --n) {
        std::thread(Runner(*this));
    }
}

DynamicWorkQueue::~DynamicWorkQueue()
{
    std::unique_lock<std::mutex> lock(_mutex);
    // Signal all threads to exit, and wait until they do. This
    // is necessary because each Runner created by this DynamicWorkQueue
    // has a reference to *this which must not be invalidated from underfoot.
    _exitNow = true;
    _workAvailable.notify_all();
    while (_numThreads != 0) {
        _threadsExited.wait(lock);
    }
    _nonEmptyQueues.clear();
    // Destroy remaining queues.
    for (SessionQueueMap::iterator i = _sessions.begin(), e = _sessions.end();
         i != e; ++i) {
        delete i->second;
    }
    _sessions.clear();
}

void DynamicWorkQueue::add(void const * session,
                           DynamicWorkQueue::Callable * callable)
{
    std::lock_guard<std::mutex> lock(_mutex);
    if (_shouldIncreaseThreadCount()) {
        std::thread t(_startRunner, std::ref(*this));
        t.detach();
        // Increment the thread count. Note, if this were done by Runner in
        // operator()(), the following sequence of events would become
        // possible:
        //
        //  - [thread 1] A DynamicWorkQueue D is created with 0 initial threads.
        //  - [thread 1] D.add() is called, creating thread 2, but leaving
        //               _numThreads unchanged at 0.
        //  - [thread 1] The destructor for D is called. Upon seeing that
        //               D._numThreads is 0, D is destroyed immediately.
        //  - [thread 2] The Runner created by D.add() fires up. It now
        //               contains a reference to D, even though D has been
        //               destroyed, and may be located in memory that has
        //               been freed.
        //  - Havoc ensues.
        //
        // Therefore, add() increments the thread count when a new
        // thread is created, and Runner decrements it before exiting.
        _numThreads += 1;
    }
    Queue * q = 0;
    SessionQueueMap::iterator i = _sessions.find(session);
    if (i != _sessions.end()) {
        // There is an existing queue for session.
        q = i->second;
        _nonEmptyQueues.insert(q);
    } else {
        // Create a Queue for session and put callable on it.
        std::auto_ptr<Queue> qp(new Queue(session));
        q = qp.get();
        _sessions.insert(std::make_pair(session, q));
        try {
            _nonEmptyQueues.insert(q);
        } catch (...) {
            _sessions.erase(session);
            throw;
        }
        qp.release();
    }
    q->put(callable);
    // Wake up a thread waiting on work.
    _numCallables += 1;
    _workAvailable.notify_one();
}

void DynamicWorkQueue::cancelQueued(void const * session)
{
    Callable * c = 0;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        SessionQueueMap::iterator i = _sessions.find(session);
        if (i != _sessions.end()) {
            Queue * q = i->second;
            c = q->takeAll();
            _nonEmptyQueues.erase(q);
            if (q->numThreads == 0) {
                // q is empty and no threads are in-flight for session
                _sessions.erase(i);
                delete q;
            }
        }
    }
    while (c) {
        Callable * next = c->_next;
        c->cancel(); // TODO: what if cancel() throws?
        delete c;
        c = next;
    }
}

bool DynamicWorkQueue::_shouldIncreaseThreadCount() const {
    if (_numThreads < _minThreads) {
        return _numThreads < _numCallables + 1;
    }
    size_t maxOverflow = (_sessions.size() + 1)*_minThreadsPerSession;
    return _numThreads < _maxThreads &&
           _numThreads - _minThreads < maxOverflow;
}

bool DynamicWorkQueue::_shouldDecreaseThreadCount() const {
    if (_numThreads <= _minThreads) {
        return false;
    }
    return _numThreads > _numCallables ||
           _numThreads - _minThreads > _sessions.size()*_minThreadsPerSession;
}

}}} // namespace lsst::qserv::util
