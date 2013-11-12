/*
 * LSST Data Management System
 * Copyright 2009-2013 LSST Corporation.
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
//
#include "util/WorkQueue.h"
#include <iostream>

////////////////////////////////////////////////////////////////////////
// class WorkQueue::Runner
////////////////////////////////////////////////////////////////////////
class lsst::qserv::WorkQueue::Runner {
public:
    Runner(WorkQueue& w) : _w(w) {
    }
    void operator()() {
        _w.registerRunner(this);
        //std::cerr << "Started!" << std::endl;
        boost::shared_ptr<Callable> c = _w.getNextCallable();
        _c = c.get();
        //std::cerr << "got first job" << std::endl;
        while(!_w.isPoison(c.get())) {
            (*c)();
            _c = 0;
            c = _w.getNextCallable();
            std::cerr << "Runner running job" << std::endl;
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
lsst::qserv::WorkQueue::WorkQueue(int numRunners) : _isDead(false) {
    for(int i = 0; i < numRunners; ++i) {
        _addRunner();
    }
}

lsst::qserv::WorkQueue::~WorkQueue() {
    _dropQueue(true);
    int poisonToMake = 2*_runners.size(); // double dose of poison
    for(int i = 0; i < poisonToMake; ++i) {
        add(boost::shared_ptr<Callable>()); // add poison
    }
    boost::unique_lock<boost::mutex> lock(_runnersMutex);

    while(_runners.size() > 0) {
        _runnersEmpty.wait(lock);
        // std::cerr << "signalled... " << _runners.size()
        //           << " remain" << std::endl;
    }
}

void
lsst::qserv::WorkQueue::add(boost::shared_ptr<lsst::qserv::WorkQueue::Callable> c) {
    boost::lock_guard<boost::mutex> lock(_mutex);
    if(_isDead && !isPoison(c.get())) {
        //std::cerr << "Queue refusing work: dead" << std::endl;
    } else {
        _queue.push_back(c);
        _queueNonEmpty.notify_all();
    }
}

void lsst::qserv::WorkQueue::cancelQueued() {
    boost::unique_lock<boost::mutex> lock(_mutex);
    boost::shared_ptr<Callable> c;
    while(!_queue.empty()) {
        c = _queue.front();
        _queue.pop_front();
        lock.unlock();
        c->cancel();
        lock.lock();
    }
}

boost::shared_ptr<lsst::qserv::WorkQueue::Callable>
lsst::qserv::WorkQueue::getNextCallable() {
    boost::unique_lock<boost::mutex> lock(_mutex);
    while(_queue.empty()) {
        _queueNonEmpty.wait(lock);
    }
    boost::shared_ptr<Callable> c = _queue.front();
    _queue.pop_front();
    return c;
}

void lsst::qserv::WorkQueue::registerRunner(Runner* r) {
    boost::lock_guard<boost::mutex> lock(_runnersMutex);
    _runners.push_back(r);
    _runnerRegistered.notify_all();
}

void lsst::qserv::WorkQueue::signalDeath(Runner* r) {
    boost::lock_guard<boost::mutex> lock(_runnersMutex);
    RunnerDeque::iterator end = _runners.end();
    //LOGGER_INF << (void*) r << " dying" << std::endl;
    for(RunnerDeque::iterator i = _runners.begin(); i != end; ++i) {
        if(*i == r) {
            _runners.erase(i);
            _runnersEmpty.notify_all();
            //LOGGER_INF << _runners.size() << " runners left" << std::endl;
            return;
        }
    }
    //std::cerr << "couldn't find self to remove" << std::endl;
}
void lsst::qserv::WorkQueue::_addRunner() {
    boost::unique_lock<boost::mutex> lock(_runnersMutex);
    boost::thread(Runner(*this));
    _runnerRegistered.wait(lock);
    //_runners.back()->run();
}

void lsst::qserv::WorkQueue::_dropQueue(bool final) {
    boost::lock_guard<boost::mutex> lock(_mutex);
    _queue.clear();
    if(final) _isDead = true;
}

//////////////////////////////////////////////////////////////////////
// Test code
//////////////////////////////////////////////////////////////////////

namespace {
class MyCallable : public lsst::qserv::WorkQueue::Callable {
public:
    typedef boost::shared_ptr<MyCallable> Ptr;

    MyCallable(int id, float time)  : _myId(id), _spinTime(time) {}
    virtual void operator()() {
        std::stringstream ss;
        struct timespec ts;
        struct timespec rem;

        ss << "MyCallable " << _myId << " (" << _spinTime
           << ") STARTED spinning" << std::endl;
        std::cerr << ss.str();
        ss.str() = "";
        ts.tv_sec = (long)_spinTime;
        ts.tv_nsec = (long)((1e9)*(_spinTime - ts.tv_sec));
        if(-1 == nanosleep(&ts, &rem)) {
            ss << "Interrupted " ;
        }

        ss << "MyCallable " << _myId << " (" << _spinTime
           << ") STOPPED spinning" << std::endl;
        std::cerr << ss.str();
    }
    int _myId;
    float _spinTime;
};

void test() {
    using namespace std;
    //struct timespec ts;
    //struct timespec rem;
    //ts.tv_sec = 10;
    //ts.tv_nsec=0;
    cout << "main started" << endl;
    lsst::qserv::WorkQueue wq(10);
    cout << "wq started " << endl;
    for(int i=0; i < 50; ++i) {
        wq.add(MyCallable::Ptr(new MyCallable(i, 0.2)));
    }
    cout << "added items" << endl;
    //nanosleep(&ts,&rem);
}

} // anonymous namespace
