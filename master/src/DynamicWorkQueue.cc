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
#include "lsst/qserv/master/DynamicWorkQueue.h"

#include <stdexcept>

#include "lsst/qserv/master/AsyncQueryManager.h"
#include "lsst/qserv/master/xrdfile.h"


namespace lsst { namespace qserv { namespace master {

// A linked list of Callable objects associated with a specific query.
struct DynamicWorkQueue::Queue {
    size_t numThreads; // # of threads running work from this queue.
    double startTime;  // Query start time in seconds since the Epoch.
    AsyncQueryManager const * query;    // Query associated with this queue.
    DynamicWorkQueue::Callable * head;  // First work-item in queue.
    DynamicWorkQueue::Callable * tail;  // Last work-item in queue.

    Queue() : numThreads(0), startTime(0), query(0), head(0), tail(0) { }

    ~Queue() {
        Callable * c = head;
        head = tail = 0;
        query = 0;
        while (c) {
            Callable * next = c->_next;
            delete c;
            c = next;
        }
    }

    bool empty() const { return head == 0; }

    void push(Callable * c) {
        if (c) {
            if (tail) {
                tail->_next = c;
                tail = c;
            } else {
                head = tail = c;
            }
        }
    }

    Callable * pop() {
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

    Callable * popAll() {
        Callable * c = head;
        head = tail = 0;
        return c;
    }
};

// Order queue pointers lexicographically by
// (active thread count, query start time, memory address).
bool DynamicWorkQueue::QueuePtrCmp::operator()(
    DynamicWorkQueue::Queue const *x,
    DynamicWorkQueue::Queue const *y) const
{
    if (x->numThreads < y->numThreads) {
        return true;
    } else if (x->numThreads == y->numThreads) {
        if (x->startTime < y->startTime) {
            return true;
        } else if (x->startTime == y->startTime) {
            return x < y;
        }
    }
    return false;
}


struct DynamicWorkQueue::Runner {
    Runner(DynamicWorkQueue & queue) : wq(queue) { }
    void operator()();
    DynamicWorkQueue & wq;
};

void DynamicWorkQueue::Runner::operator()() {
    boost::unique_lock<boost::mutex> lock(wq._mutex);
    do {
        // Wait for an available queue or for an exit signal.
        while (wq._queues.empty() && !wq._exitNow) {
            wq._nonEmpty.wait(lock);
        }
        if (wq._exitNow) { break; }

        // The first set element is the oldest of the queues with the smallest
        // active thread count.
        Queue *q = *wq._queues.begin();
        // Remove q from _queues prior to updating it - this is necessary
        // because the updates may change how it compares to other queues.
        wq._queues.erase(q);
        assert(q && !q->empty());

        q->numThreads += 1; // Increment the active thread count for q.
        boost::scoped_ptr<Callable> c(q->pop());
        if (!q->empty()) {
            // If work remains in q, make it available to other threads.
            wq._queues.insert(q);
            wq._nonEmpty.notify_one();
        }
        lock.unlock();

        // Execute work
        (*c)();
        c.reset();

        lock.lock();
        wq._numCallables -= 1;

        wq._queues.erase(q); // Remove q from _queues prior to updating.
        q->numThreads -= 1; // Decrement active thread count for q.
        if (!q->empty()) {
            // Work remains in q, so make it available to other threads.
            wq._queues.insert(q);
        } else if (q->numThreads == 0) {
            // q is empty and no threads are in-flight for the associated query.
            wq._queries.erase(q->query);
            delete q;
        }
        // If there are more than the minimum number of threads,
        // and less work-items than threads, exit.
    } while (wq._numThreads <= wq._minThreads ||
             wq._numThreads <= wq._numCallables);
    wq._numThreads -= 1;
    if (wq._numThreads == 0) {
        wq._threadsExited.notify_one();
    }
}


DynamicWorkQueue::DynamicWorkQueue(size_t minThreads,
                                   size_t maxThreads,
                                   size_t initThreads) :
    _minThreads(minThreads),
    _maxThreads(maxThreads),
    _numCallables(0),
    _numThreads(std::min(initThreads, maxThreads)),
    _exitNow(false)
{
    if (minThreads > maxThreads || maxThreads == 0) {
        throw std::runtime_error("Invalid DynamicWorkQueue min/max thread counts.");
    }
    for (size_t n = _numThreads; n != 0; --n) {
        boost::thread(Runner(*this));
    }
}

DynamicWorkQueue::~DynamicWorkQueue()
{
    boost::unique_lock<boost::mutex> lock(_mutex);
    // Signal all threads to exit, and wait until they do.
    _exitNow = true;
    while (_numThreads != 0) {
        _threadsExited.wait(lock);
    }
    // Clean up allocated memory.
    _queues.clear();
    for (QueryQueueMap::iterator i = _queries.begin(), e = _queries.end();
         i != e; ++i) {
        delete i->second;
    }
    _queries.clear();
}

void DynamicWorkQueue::add(AsyncQueryManager const * query,
                           DynamicWorkQueue::Callable * callable)
{
    boost::lock_guard<boost::mutex> lock(_mutex);
    Queue *& q = _queries[query];
    if (q == 0) {
        // fresh insertion
        q = new Queue();
        q->query = query;
        q->startTime = query ? query->getStartTime() : 0.0;
    }
    q->push(callable);
    _queues.insert(q);
    _numCallables += 1;
    _nonEmpty.notify_one();
    if (_numThreads < _maxThreads && _numCallables > _numThreads) {
        _numThreads += 1;
        boost::thread(Runner(*this));
    }
}

void DynamicWorkQueue::cancelQueued(AsyncQueryManager const * query)
{
    Callable * c = 0;
    {
        boost::lock_guard<boost::mutex> lock(_mutex);
        QueryQueueMap::iterator i = _queries.find(query);
        if (i != _queries.end()) {
            Queue * q = i->second;
            c = q->popAll();
            if (q->numThreads == 0) {
                // q is empty and no threads are in-flight for query
                _queries.erase(i);
                _queues.erase(q);
                delete q;
            }
        }
    }
    while (c) {
        Callable * next = c->_next;
        c->cancel();
        delete c;
        c = next;
    }
}

}}} // namespace lsst::qserv::master
