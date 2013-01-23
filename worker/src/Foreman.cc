// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2008-2013 LSST Corporation.
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
/// class Foreman implementation
#include "lsst/qserv/worker/Foreman.h"

// Std C++
#include <deque>
#include <iostream>

// Boost
#include <boost/thread.hpp>
#include <boost/make_shared.hpp>

// Pkg: lsst::qserv::worker
#include "lsst/qserv/worker/TodoList.h"
#include "lsst/qserv/worker/FifoScheduler.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/Logger.h"

namespace qWorker = lsst::qserv::worker;
////////////////////////////////////////////////////////////////////////
// anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace { 
    template <typename Q> 
    bool popFrom(Q& q, typename Q::value_type const& v) {
        typename Q::iterator i = std::find(q.begin(), q.end(), v);
        if(i == q.end()) return false;
        q.erase(i);
        return true;
    }
}

////////////////////////////////////////////////////////////////////////
// ForemanImpl declaration
////////////////////////////////////////////////////////////////////////
class ForemanImpl : public lsst::qserv::worker::Foreman {
public:
    ForemanImpl(Scheduler::Ptr s, qWorker::TodoList::Ptr t, 
                qWorker::Logger::Ptr log);
    virtual ~ForemanImpl();
    
    //boost::shared_ptr<Callable> getNextCallable();
    // For use by runners.
    class Runner;
    class RunnerMgr;
    class RunnerMgr {
    public:
        RunnerMgr(ForemanImpl& f) : _f(f) {}
        void registerRunner(Runner* r, qWorker::Task::Ptr t);
        void reportComplete(qWorker::Task::Ptr t);
        void reportStart(qWorker::Task::Ptr t);
        void signalDeath(Runner* r);
        qWorker::Task::Ptr getNextTask(Runner* r, qWorker::Task::Ptr previous);
        qWorker::Logger::Ptr getLog();
        
    private:
        class StartTaskF;
        ForemanImpl& _f;
    };

    friend class RunnerMgr;
    class Watcher; 
    friend class Watcher;

private:
    typedef std::deque<Runner*> RunnerDeque;

    void _startRunner(qWorker::Task::Ptr t);
    
    boost::mutex _mutex;
    boost::mutex _runnersMutex;
#if 0
    boost::condition_variable _queueNonEmpty;
    boost::condition_variable _runnersEmpty;
    boost::condition_variable _runnerRegistered;
#endif
    Scheduler::Ptr _scheduler;
    qWorker::TodoList::Ptr _todo;
    RunnerDeque _runners;
    RunnerMgr _rManager;
    qWorker::Logger::Ptr _log;
                         
    TaskQueuePtr  _running;
    boost::shared_ptr<Watcher> _watcher;

};
////////////////////////////////////////////////////////////////////////
// Foreman factory function
////////////////////////////////////////////////////////////////////////
qWorker::Foreman::Ptr 
qWorker::newForeman(qWorker::TodoList::Ptr tl, qWorker::Logger::Ptr log) {
    qWorker::FifoScheduler::Ptr fsch(new qWorker::FifoScheduler());
    ForemanImpl::Ptr fmi(new ForemanImpl(fsch, tl, log));
    return fmi;;
}

////////////////////////////////////////////////////////////////////////
// ForemanImpl::Watcher
////////////////////////////////////////////////////////////////////////
class ForemanImpl::Watcher : public qWorker::TodoList::Watcher {
public:
    typedef boost::shared_ptr<Watcher> Ptr;
    Watcher(ForemanImpl& f);
    virtual ~Watcher() {}
    virtual void handleAccept(qWorker::Task::Ptr t); // Must not block.
private:
    ForemanImpl& _f;
};

ForemanImpl::Watcher::Watcher(ForemanImpl& f) : _f(f) {
}

void ForemanImpl::Watcher::handleAccept(qWorker::Task::Ptr t) {
    assert(_f._scheduler.get());
    TaskQueuePtr newReady = _f._scheduler->newTaskAct(t, 
                                                      _f._todo, 
                                                      _f._running);
    // Perform only what the scheduler requests.
    if(newReady.get() && (newReady->size() > 0)) {
        qWorker::TodoList::TaskQueue::iterator i = newReady->begin();
        for(; i != newReady->end(); ++i) {
            _f._startRunner(*i);
        }
    }
}
////////////////////////////////////////////////////////////////////////
// class ForemanImpl::RunnerMgr
////////////////////////////////////////////////////////////////////////
class ForemanImpl::RunnerMgr::StartTaskF {
public:
    StartTaskF(ForemanImpl& f) : _f(f) {}
    void operator()(qWorker::Task::Ptr t) {
        _f._startRunner(t);
    }
private:
    ForemanImpl& _f;
};

void ForemanImpl::RunnerMgr::registerRunner(Runner* r, qWorker::Task::Ptr t) {
   boost::lock_guard<boost::mutex> lock(_f._runnersMutex); 
    _f._runners.push_back(r);
    _f._running->push_back(t);
}

void ForemanImpl::RunnerMgr::reportComplete(qWorker::Task::Ptr t) {
    boost::lock_guard<boost::mutex> lock(_f._runnersMutex); 
    bool popped = popFrom(*_f._running, t);
    assert(popped);
}

void ForemanImpl::RunnerMgr::reportStart(qWorker::Task::Ptr t) {
   boost::lock_guard<boost::mutex> lock(_f._runnersMutex); 
    _f._running->push_back(t);
}

void ForemanImpl::RunnerMgr::signalDeath(Runner* r) {
    boost::lock_guard<boost::mutex> lock(_f._runnersMutex); 
    RunnerDeque::iterator end = _f._runners.end();
    // std::cout << (void*) r << " dying" << std::endl;
    for(RunnerDeque::iterator i = _f._runners.begin(); i != end; ++i) {
        if(*i == r) {
            _f._runners.erase(i);
            //_f._runnersEmpty.notify_all(); // Still needed?
            return;
        }
    }
}

qWorker::Task::Ptr 
ForemanImpl::RunnerMgr::getNextTask(Runner* r, qWorker::Task::Ptr previous) {
    TaskQueuePtr tq;
    tq = _f._scheduler->taskFinishAct(previous, _f._todo, _f._running);
    if(!tq.get()) {
        return qWorker::Task::Ptr();
    }
    if(tq->size() > 1) {
        std::for_each(tq->begin()+1, tq->end(), StartTaskF(_f));
    }
    return tq->front();    
}

qWorker::Logger::Ptr ForemanImpl::RunnerMgr::getLog() {
    return _f._log;
}


////////////////////////////////////////////////////////////////////////
// class ForemanImpl::Runner
////////////////////////////////////////////////////////////////////////
class ForemanImpl::Runner  {
public:
    Runner(RunnerMgr& rm, qWorker::Task::Ptr firstTask)
        : _rm(rm), 
          _task(firstTask),
          _isPoisoned(false),
          _log(rm.getLog()) { 
        // nothing to do.
    }
    void poison() { _isPoisoned = true; }
    void operator()() {
        _rm.registerRunner(this, _task);
        while(!_isPoisoned) { 
            // Run my task.
            qWorker::QueryRunnerArg a(_log, _task);
            qWorker::QueryRunner qr(a);
            std::stringstream ss;
            ss << "Runner running " << *_task;
            _log->info(ss.str());
            qr.actOnce();
            if(_isPoisoned) break;
            // Request new work from the manager
            // (mgr is a role of the foreman, who will check with the
            // scheduler for the next assignment) 
            _rm.reportComplete(_task);
            _task = _rm.getNextTask(this, _task);
            if(!_task.get()) break; // No more work?
            _rm.reportStart(_task);
        } // Keep running until we get poisoned.
        _rm.signalDeath(this);
    }
private:
    RunnerMgr& _rm;
    qWorker::Task::Ptr _task;
    qWorker::Logger::Ptr _log;
    bool _isPoisoned;
};


////////////////////////////////////////////////////////////////////////
// ForemanImpl
////////////////////////////////////////////////////////////////////////
ForemanImpl::ForemanImpl(Scheduler::Ptr s,
                         qWorker::TodoList::Ptr t,
                         qWorker::Logger::Ptr log)
    : _scheduler(s), _todo(t), _rManager(*this), _log(log),
      _running(new qWorker::TodoList::TaskQueue()) {
    if(!_log.get()) {
        // Make basic logger.
        _log.reset(new qWorker::Logger());
    }
    _watcher.reset(new Watcher(*this));
    _todo->addWatcher(_watcher); // Callbacks are now possible.
    // ...
    
}
ForemanImpl::~ForemanImpl() {
    _todo->removeWatcher(_watcher);
    _watcher.reset();
    // FIXME: Poison and drain runners.
}

void ForemanImpl::_startRunner(qWorker::Task::Ptr t) {
    // FIXME: Is this all that is needed?
    boost::thread(Runner(_rManager, t));
}

