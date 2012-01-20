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

namespace qWorker = lsst::qserv::worker;

// ForemanImpl
class ForemanImpl : public lsst::qserv::worker::Foreman {
public:
    ForemanImpl(Scheduler::Ptr s, qWorker::TodoList::Ptr t);
    virtual ~ForemanImpl() {}
    
    //boost::shared_ptr<Callable> getNextCallable();
    // For use by runners.
    class Runner;
    class RunnerMgr;
    class RunnerMgr {
    public:
        RunnerMgr(ForemanImpl& f) : _f(f) {}
        void registerRunner(Runner* r);
        void signalDeath(Runner* r);
    private:
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
    boost::condition_variable _queueNonEmpty;
    boost::condition_variable _runnersEmpty;
    boost::condition_variable _runnerRegistered;
    //    WorkDeque _queue;
    RunnerDeque _runners;
    RunnerMgr _rManager;
    Scheduler::Ptr _scheduler;
    qWorker::TodoList::Ptr _todo;
    TaskQueuePtr  _running;
};
////////////////////////////////////////////////////////////////////////
// Foreman factory function
////////////////////////////////////////////////////////////////////////
qWorker::Foreman::Ptr newForeman(qWorker::TodoList::Ptr tl) {
    qWorker::FifoScheduler::Ptr fsch(new qWorker::FifoScheduler());
    ForemanImpl::Ptr fmi(new ForemanImpl(fsch, tl));
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
    // Done. 
}
////////////////////////////////////////////////////////////////////////
// class ForemanImpl::RunnerMgr
////////////////////////////////////////////////////////////////////////
void ForemanImpl::RunnerMgr::registerRunner(Runner* r) {
   boost::lock_guard<boost::mutex> lock(_f._runnersMutex); 
    _f._runners.push_back(r);
}
void ForemanImpl::RunnerMgr::signalDeath(Runner* r) {
   boost::lock_guard<boost::mutex> lock(_f._runnersMutex); 
   RunnerDeque::iterator end = _f._runners.end();
    std::cout << (void*) r << " dying" << std::endl;
    for(RunnerDeque::iterator i = _f._runners.begin(); i != end; ++i) {
        if(*i == r) {
            _f._runners.erase(i);
            _f._runnersEmpty.notify_all();
            return;
        }
    }
}
////////////////////////////////////////////////////////////////////////
// class ForemanImpl::Runner
////////////////////////////////////////////////////////////////////////
class ForemanImpl::Runner  {
public:
    Runner(RunnerMgr& rm, qWorker::Task::Ptr firstTask)
        : _rm(rm), _task(firstTask), _isPoisoned(false) { 
    }
    void poison() { _isPoisoned = true; }
    void operator()() {
        _rm.registerRunner(this);
        while(!_isPoisoned) { 
            // TODO: run first task.
            // TODO: request new work.
            //(*c)();
            //c = _w.getNextCallable();
            std::cerr << "Runner running job" << std::endl;
        } // Keep running until we get poisoned.
        _rm.signalDeath(this);
    }
private:
    RunnerMgr& _rm;
    qWorker::Task::Ptr _task;    
    bool _isPoisoned;
};


////////////////////////////////////////////////////////////////////////
// ForemanImpl
////////////////////////////////////////////////////////////////////////
ForemanImpl::ForemanImpl(Scheduler::Ptr s, qWorker::TodoList::Ptr t)
    : _scheduler(s), _todo(t), _rManager(*this) {
    Watcher::Ptr w(new Watcher(*this));
    _todo->addWatcher(w);
    // ...
    
}

void ForemanImpl::_startRunner(qWorker::Task::Ptr t) {
    // FIXME: start the runner.
    boost::thread(Runner(_rManager, t));
}

#if 0
Runner::run() {
    while(1) {
        performTask();
        if(shouldDie) break;
        t = _getNewTask();
        if(t) reset(t);
        else break; // stop running
    }
}
Runner::poison() {
    shouldDie = true;
}
#endif
