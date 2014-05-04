// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2014 LSST Corporation.
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
#include "wcontrol/Foreman.h"

// System headers
#include <deque>
#include <iostream>

// Third-party headers
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>

// Local headers
#include "wbase/Base.h"
#include "wdb/QueryRunner.h"
#include "wlog/WLogger.h"
#include "wsched/FifoScheduler.h"

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
} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wcontrol {

////////////////////////////////////////////////////////////////////////
// ForemanImpl declaration
////////////////////////////////////////////////////////////////////////
class ForemanImpl : public Foreman {
public:
    ForemanImpl(Scheduler::Ptr s, wlog::WLogger::Ptr log);
    virtual ~ForemanImpl();

    bool squashByHash(std::string const& hash);
    bool accept(boost::shared_ptr<proto::TaskMsg> msg);
    class RunnerMgr;
    class Runner  {
    public:
        Runner(RunnerMgr& rm, wcontrol::Task::Ptr firstTask);
        void poison();
        void operator()();
        std::string const& getHash() const { return _task->hash; }
    private:
        RunnerMgr& _rm;
        wcontrol::Task::Ptr _task;
        bool _isPoisoned;
        wlog::WLogger::Ptr _log;
    };
    // For use by runners.
    class RunnerMgr {
    public:
        RunnerMgr(ForemanImpl& f);
        void registerRunner(Runner* r, wcontrol::Task::Ptr t);
        boost::shared_ptr<wdb::QueryRunner> newQueryRunner(wcontrol::Task::Ptr t);
        void reportComplete(wcontrol::Task::Ptr t);
        void reportStart(wcontrol::Task::Ptr t);
        void signalDeath(Runner* r);
        wcontrol::Task::Ptr getNextTask(Runner* r, wcontrol::Task::Ptr previous);
        wlog::WLogger::Ptr getLog();
        bool squashByHash(std::string const& hash);

    private:
        void _reportStartHelper(wcontrol::Task::Ptr t);
        class StartTaskF;
        ForemanImpl& _f;
        Foreman::TaskWatcher& _taskWatcher;
    };

    friend class RunnerMgr;

private:
    typedef std::deque<Runner*> RunnerDeque;

    void _startRunner(wcontrol::Task::Ptr t);

    boost::mutex _mutex;
    boost::mutex _runnersMutex;
    Scheduler::Ptr _scheduler;
    RunnerDeque _runners;
    boost::scoped_ptr<RunnerMgr> _rManager;
    wlog::WLogger::Ptr _log;

    TaskQueuePtr _running;
};
////////////////////////////////////////////////////////////////////////
// Foreman factory function
////////////////////////////////////////////////////////////////////////
Foreman::Ptr
newForeman(Foreman::Scheduler::Ptr sched, wlog::WLogger::Ptr log) {
    if(!sched) {
        sched.reset(new wsched::FifoScheduler());
    }
    ForemanImpl::Ptr fmi(new ForemanImpl(sched, log));
    return fmi;;
}

////////////////////////////////////////////////////////////////////////
// class ForemanImpl::RunnerMgr
////////////////////////////////////////////////////////////////////////
class ForemanImpl::RunnerMgr::StartTaskF {
public:
    StartTaskF(ForemanImpl& f) : _f(f) {}
    void operator()(wcontrol::Task::Ptr t) {
        _f._startRunner(t);
    }
private:
    ForemanImpl& _f;
};

ForemanImpl::RunnerMgr::RunnerMgr(ForemanImpl& f)
    : _f(f), _taskWatcher(*f._scheduler) {
}

void
ForemanImpl::RunnerMgr::registerRunner(Runner* r, wcontrol::Task::Ptr t) {
    {
        boost::lock_guard<boost::mutex> lock(_f._runnersMutex);
        _f._runners.push_back(r);
    }

    std::ostringstream os;
    os << "Registered runner " << (void*)r;
    _f._log->debug(os.str());
    _reportStartHelper(t);
}

boost::shared_ptr<wdb::QueryRunner>
ForemanImpl::RunnerMgr::newQueryRunner(wcontrol::Task::Ptr t) {
    wdb::QueryRunnerArg a(_f._log, t);
    boost::shared_ptr<wdb::QueryRunner> qr(new wdb::QueryRunner(a));
    return qr;
}

void
ForemanImpl::RunnerMgr::reportComplete(wcontrol::Task::Ptr t) {
    {
        boost::lock_guard<boost::mutex> lock(_f._runnersMutex);
        bool popped = popFrom(*_f._running, t);
        assert(popped);
    }
    std::ostringstream os;
    os << "Finished task " << *t;
    _f._log->debug(os.str());
    _taskWatcher.markFinished(t);
}

void
ForemanImpl::RunnerMgr::reportStart(wcontrol::Task::Ptr t) {
   _reportStartHelper(t);
}

void
ForemanImpl::RunnerMgr::_reportStartHelper(wcontrol::Task::Ptr t) {
    {
        boost::lock_guard<boost::mutex> lock(_f._runnersMutex);
        _f._running->push_back(t);
    }
    std::ostringstream os;
    os << "Started task " << *t;
    _f._log->debug(os.str());
    _taskWatcher.markStarted(t);
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

wcontrol::Task::Ptr
ForemanImpl::RunnerMgr::getNextTask(Runner* r, wcontrol::Task::Ptr previous) {
    TaskQueuePtr tq;
    tq = _f._scheduler->taskFinishAct(previous, _f._running);
    if(!tq.get()) {
        return wcontrol::Task::Ptr();
    }
    if(tq->size() > 1) {
        std::for_each(tq->begin()+1, tq->end(), StartTaskF(_f));
    }
    return tq->front();
}

wlog::WLogger::Ptr 
ForemanImpl::RunnerMgr::getLog() {
    return _f._log;
}
/// matchHash: helper functor that matches queries by hash.
class matchHash {
public:
    matchHash(std::string const& hash_) : hash(hash_) {}
    inline bool operator()(wdb::QueryRunnerArg const& a) {
        return a.task->hash == hash;
    }
    inline bool operator()(ForemanImpl::Runner const* r) {
        return r->getHash() == hash;
    }
    std::string hash;
};

bool ForemanImpl::RunnerMgr::squashByHash(std::string const& hash) {
    boost::lock_guard<boost::mutex> lock(_f._runnersMutex);

    RunnerDeque::iterator b = _f._runners.begin();
    RunnerDeque::iterator e = _f._runners.end();
    RunnerDeque::iterator q = find_if(b, e, matchHash(hash));
    if(q != e) {
        (*q)->poison();
        return true;
    }
    return false;
}


////////////////////////////////////////////////////////////////////////
// class ForemanImpl::Runner
////////////////////////////////////////////////////////////////////////
ForemanImpl::Runner::Runner(RunnerMgr& rm, wcontrol::Task::Ptr firstTask)
    : _rm(rm),
      _task(firstTask),
      _isPoisoned(false),
      _log(rm.getLog()) {
    // nothing to do.
}

void ForemanImpl::Runner::poison() {
    _isPoisoned = true;
}

void ForemanImpl::Runner::operator()() {
    boost::shared_ptr<wdb::QueryRunner> qr;
    _rm.registerRunner(this, _task);
    while(!_isPoisoned) {
        // Run my task.
        qr = _rm.newQueryRunner(_task);
        std::stringstream ss;
        ss << "Runner running " << *_task;
        _log->info(ss.str());
        qr->actOnce();
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
////////////////////////////////////////////////////////////////////////
// ForemanImpl
////////////////////////////////////////////////////////////////////////
ForemanImpl::ForemanImpl(Scheduler::Ptr s,
                         wlog::WLogger::Ptr log)
    : _scheduler(s), _running(new TaskQueue()) {
    if(!log) {
        // Make basic logger.
        _log.reset(new wlog::WLogger());
    } else {
        _log.reset(new wlog::WLogger(log));
        _log->setPrefix("Foreman:");
    }
    _rManager.reset(new RunnerMgr(*this));
    assert(s); // Cannot operate without scheduler.

}
ForemanImpl::~ForemanImpl() {
    // FIXME: Poison and drain runners.
}

void
ForemanImpl::_startRunner(wcontrol::Task::Ptr t) {
    // FIXME: Is this all that is needed?
    boost::thread(Runner(*_rManager, t));
}

bool ForemanImpl::squashByHash(std::string const& hash) {
    boost::lock_guard<boost::mutex> m(_mutex);
    bool success = _scheduler->removeByHash(hash);
    success = success || _rManager->squashByHash(hash);
    if(success) {
        // Notify the tracker in case someone is waiting.
        ResultError r(-2, "Squashed by request");
        wdb::QueryRunner::getTracker().notify(hash, r);
        // Remove squash notification to prevent future poisioning.
        wdb::QueryRunner::getTracker().clearNews(hash);
    }
    return success;
}

bool
ForemanImpl::accept(boost::shared_ptr<proto::TaskMsg> msg) {
    // Pass to scheduler.
    assert(_scheduler);
    wcontrol::Task::Ptr t(new wcontrol::Task(msg));
    TaskQueuePtr newReady = _scheduler->newTaskAct(t, _running);
    // Perform only what the scheduler requests.
    if(newReady.get() && (newReady->size() > 0)) {
        TaskQueue::iterator i = newReady->begin();
        for(; i != newReady->end(); ++i) {
            _startRunner(*i);
        }
    }
    return false; // FIXME
}

}}} // namespace lsst::qserv::wcontrol
