// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2016 AURA/LSST.
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
//  WorkQueue.cc - implementation of the WorkQueue class
//  Threads block on a condition variable in the queue, and are signalled
//  when there is work to do.
// When the WorkQueue is destructed, it poisons the queue and waits until
// all threads have died before returning.

// Class header
#include "util/WorkQueue.h"

// System headers
#include <iostream>
#include <sstream>
#include <thread>

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.WorkQueue");
}

namespace lsst {
namespace qserv {
namespace util {


////////////////////////////////////////////////////////////////////////
// class WorkQueue::Runner
////////////////////////////////////////////////////////////////////////
class WorkQueue::Runner {
public:
    Runner(WorkQueue& w) : _c(NULL), _w(w) {}
    void operator()() {
        _w.registerRunner(this);
        std::shared_ptr<Callable> c = _w.getNextCallable();
        _c = c.get();
        //std::cerr << "got first job\n";
        while(!_w.isPoison(c.get())) {
            (*c)();
            _c = 0;
            c = _w.getNextCallable();
            LOGS(_log, LOG_LVL_DEBUG, "Runner running job");
        } // Keep running until we get poisoned.
        _w.signalDeath(this);
    }

    void terminate() {
        if(_c) {
            _c->abort();
        }
    }
    Callable* _c;
    WorkQueue& _w;
};

////////////////////////////////////////////////////////////////////////
// class WorkQueue
////////////////////////////////////////////////////////////////////////
WorkQueue::WorkQueue(int numRunners) : _isDead(false) {
    for(int i = 0; i < numRunners; ++i) {
        _addRunner();
    }
}

WorkQueue::~WorkQueue() {
    _dropQueue(true);
    int poisonToMake = 2*_runners.size(); // double dose of poison
    for(int i = 0; i < poisonToMake; ++i) {
        add(std::shared_ptr<Callable>()); // add poison
    }
    std::unique_lock<std::mutex> lock(_runnersMutex);

    while(_runners.size() > 0) {
        _runnersEmpty.wait(lock);
        // std::cerr << "signalled... " << _runners.size()
        //           << " remain\n";
    }
}

void
WorkQueue::add(std::shared_ptr<WorkQueue::Callable> c) {
    std::lock_guard<std::mutex> lock(_mutex);
    if(_isDead && !isPoison(c.get())) {
        //std::cerr << "Queue refusing work: dead\n";
    } else {
        _queue.push_back(c);
        _queueNonEmpty.notify_all();
    }
}

void
WorkQueue::cancelQueued() {
    std::unique_lock<std::mutex> lock(_mutex);
    std::shared_ptr<Callable> c;
    while(!_queue.empty()) {
        c = _queue.front();
        _queue.pop_front();
        lock.unlock();
        c->cancel();
        lock.lock();
    }
}

std::shared_ptr<WorkQueue::Callable>
WorkQueue::getNextCallable() {
    std::unique_lock<std::mutex> lock(_mutex);
    while(_queue.empty()) {
        _queueNonEmpty.wait(lock);
    }
    std::shared_ptr<Callable> c = _queue.front();
    _queue.pop_front();
    return c;
}

void
WorkQueue::registerRunner(Runner* r) {
    std::lock_guard<std::mutex> lock(_runnersMutex);
    _runners.push_back(r);
    _runnerRegistered.notify_all();
}

void
WorkQueue::signalDeath(Runner* r) {
    std::lock_guard<std::mutex> lock(_runnersMutex);
    RunnerDeque::iterator end = _runners.end();
    for(RunnerDeque::iterator i = _runners.begin(); i != end; ++i) {
        if(*i == r) {
            _runners.erase(i);
            _runnersEmpty.notify_all();
            return;
        }
    }
}

void
WorkQueue::_startRunner(WorkQueue& wq) {
    Runner r(wq);
    r();
}

void
WorkQueue::_addRunner() {
    std::unique_lock<std::mutex> lock(_runnersMutex);
    std::thread t(_startRunner, std::ref(*this));
    t.detach();
    _runnerRegistered.wait(lock);
}

void
WorkQueue::_dropQueue(bool final) {
    std::lock_guard<std::mutex> lock(_mutex);
    _queue.clear();
    if(final) _isDead = true;
}

}}} // namespace lsst::qserv:util

//////////////////////////////////////////////////////////////////////
// Test code
//////////////////////////////////////////////////////////////////////

namespace {

/// MyCallable is only used by test function below, for debugging purpose
class MyCallable : public lsst::qserv::util::WorkQueue::Callable {
public:
    typedef std::shared_ptr<MyCallable> Ptr;

    MyCallable(int id, float time)  : _myId(id), _spinTime(time) {}
    virtual void operator()() {
        std::stringstream ss;
        struct timespec ts;
        struct timespec rem;

        ss << "MyCallable " << _myId << " (" << _spinTime
           << ") STARTED spinning\n";
        std::cerr << ss.str();
        ss.str() = "";
        ts.tv_sec = (long)_spinTime;
        ts.tv_nsec = (long)((1e9)*(_spinTime - ts.tv_sec));
        if(-1 == nanosleep(&ts, &rem)) {
            ss << "Interrupted ";
        }

        ss << "MyCallable " << _myId << " (" << _spinTime
           << ") STOPPED spinning\n";
        std::cerr << ss.str();
    }
    int _myId;
    float _spinTime;
};

#if UNUSED
void test() {
    using namespace std;
    lsst::qserv::util::WorkQueue wq(10);
    for(int i=0; i < 50; ++i) {
        wq.add(std::make_shared<MyCallable>(i, 0.2));
    }
}
#endif // UNUSED

} // anonymous namespace
