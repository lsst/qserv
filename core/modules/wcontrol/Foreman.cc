// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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

// Class header
#include "wcontrol/Foreman.h"

// System headers
#include <cassert>
#include <deque>
#include <iostream>

// Third-party headers
#include "boost/make_shared.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/thread.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "proto/worker.pb.h"
#include "wbase/Base.h"
#include "wbase/MsgProcessor.h"
#include "wconfig/Config.h"
#include "wdb/ChunkResource.h"
#include "wdb/QueryAction.h"
#include "wdb/QueryRunner.h"
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

/// Implementation of class Foreman
/// Foreman uses internal classes to do most of its work.
/// Processor: a task acceptor (by processing messages)
/// Runner: a task actor
/// RunnerMgr: management of task Runners
/// A scheduler is consulted when state changes (a new task arrives, a Runner
/// completes its task, etc), in order to decide if new tasks should be
/// started. Schedulers never decide to cancel tasks already in flight.
class ForemanImpl : public Foreman {
public:
    ForemanImpl(Scheduler::Ptr s);
    virtual ~ForemanImpl();

    bool squashByHash(std::string const& hash);
    bool accept(boost::shared_ptr<proto::TaskMsg> msg);
    void newTaskAction(wbase::Task::Ptr task);

    virtual boost::shared_ptr<wbase::MsgProcessor> getProcessor();

    class Processor;
    class RunnerMgr;
    class Runner  {
    public:
        Runner(RunnerMgr& rm, wbase::Task::Ptr firstTask);
        void poison();
        void operator()();
        std::string const& getHash() const { return _task->hash; }

    private:
        void _runWithDump();
        void _runProtocol2();
        RunnerMgr& _rm;
        wbase::Task::Ptr _task;
        bool _isPoisoned;
        LOG_LOGGER _log;
        boost::shared_ptr<wdb::QueryAction> _action;
    };
    // For use by runners.
    class RunnerMgr {
    public:
        RunnerMgr(ForemanImpl& f);
        void registerRunner(Runner* r, wbase::Task::Ptr t);
        boost::shared_ptr<wdb::QueryRunner> newQueryRunner(wbase::Task::Ptr t);
        boost::shared_ptr<wdb::QueryAction> newQueryAction(wbase::Task::Ptr t);
        void reportComplete(wbase::Task::Ptr t);
        void reportStart(wbase::Task::Ptr t);
        void signalDeath(Runner* r);
        wbase::Task::Ptr getNextTask(Runner* r, wbase::Task::Ptr previous);
        LOG_LOGGER getLog();
        bool squashByHash(std::string const& hash);

    private:
        void _reportStartHelper(wbase::Task::Ptr t);
        class StartTaskF;
        ForemanImpl& _f;
        Foreman::TaskWatcher& _taskWatcher;
    };
    friend class RunnerMgr;

private:
    typedef std::deque<Runner*> RunnerDeque;

    void _startRunner(wbase::Task::Ptr t);

    boost::shared_ptr<wdb::ChunkResourceMgr> _chunkResourceMgr;
    boost::mutex _mutex;
    boost::mutex _runnersMutex;
    Scheduler::Ptr _scheduler;
    RunnerDeque _runners;
    boost::scoped_ptr<RunnerMgr> _rManager;
    LOG_LOGGER _log;

    wbase::TaskQueuePtr _running;
};
////////////////////////////////////////////////////////////////////////
// Foreman factory function
////////////////////////////////////////////////////////////////////////
Foreman::Ptr
newForeman(Foreman::Scheduler::Ptr sched) {
    if(!sched) {
        sched = boost::make_shared<wsched::FifoScheduler>();
    }
    ForemanImpl::Ptr fmi = boost::make_shared<ForemanImpl>(sched);
    return fmi;;
}

////////////////////////////////////////////////////////////////////////
// class ForemanImpl::RunnerMgr
////////////////////////////////////////////////////////////////////////
class ForemanImpl::RunnerMgr::StartTaskF {
public:
    StartTaskF(ForemanImpl& f) : _f(f) {}
    void operator()(wbase::Task::Ptr t) {
        _f._startRunner(t);
    }
private:
    ForemanImpl& _f;
};

ForemanImpl::RunnerMgr::RunnerMgr(ForemanImpl& f)
    : _f(f), _taskWatcher(*f._scheduler) {
}

void
ForemanImpl::RunnerMgr::registerRunner(Runner* r, wbase::Task::Ptr t) {
    {
        boost::lock_guard<boost::mutex> lock(_f._runnersMutex);
        _f._runners.push_back(r);
    }

    LOGF(_f._log, LOG_LVL_DEBUG, "Registered runner %1%" % (void*)r);
    _reportStartHelper(t);
}

boost::shared_ptr<wdb::QueryAction>
ForemanImpl::RunnerMgr::newQueryAction(wbase::Task::Ptr t) {
    wdb::QueryActionArg a(_f._log, t, _f._chunkResourceMgr);
    boost::shared_ptr<wdb::QueryAction> qa = boost::make_shared<wdb::QueryAction>(a);
    return qa;
}

boost::shared_ptr<wdb::QueryRunner>
ForemanImpl::RunnerMgr::newQueryRunner(wbase::Task::Ptr t) {
    wdb::QueryRunnerArg a(_f._log, t);
    boost::shared_ptr<wdb::QueryRunner> qr = boost::make_shared<wdb::QueryRunner>(a);
    return qr;
}

void
ForemanImpl::RunnerMgr::reportComplete(wbase::Task::Ptr t) {
    {
        boost::lock_guard<boost::mutex> lock(_f._runnersMutex);
        bool popped = popFrom(*_f._running, t);
        assert(popped);
    }

    LOGF(_f._log, LOG_LVL_DEBUG, "Finished task %1%" % *t);
    _taskWatcher.markFinished(t);
}

void
ForemanImpl::RunnerMgr::reportStart(wbase::Task::Ptr t) {
   _reportStartHelper(t);
}

void
ForemanImpl::RunnerMgr::_reportStartHelper(wbase::Task::Ptr t) {
    {
        boost::lock_guard<boost::mutex> lock(_f._runnersMutex);
        _f._running->push_back(t);
    }
    LOGF(_f._log, LOG_LVL_DEBUG, "Started task %1%" % *t);
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

wbase::Task::Ptr
ForemanImpl::RunnerMgr::getNextTask(Runner* r, wbase::Task::Ptr previous) {
    wbase::TaskQueuePtr tq;
    tq = _f._scheduler->taskFinishAct(previous, _f._running);
    if(!tq.get()) {
        return wbase::Task::Ptr();
    }
    if(tq->size() > 1) {
        std::for_each(tq->begin()+1, tq->end(), StartTaskF(_f));
    }
    return tq->front();
}

LOG_LOGGER
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
ForemanImpl::Runner::Runner(RunnerMgr& rm, wbase::Task::Ptr firstTask)
    : _rm(rm),
      _task(firstTask),
      _isPoisoned(false),
      _log(rm.getLog()) {
    // nothing to do.
}

void ForemanImpl::Runner::poison() {
    // No safe way(within reason) to access _action in order to send it a
    // poison() message.
    // --can't use a mutex in a Runner, because Runner instances must be
    // copyable to use boost::thread interface.
    // -- can store mutex outside (e.g., in RunnerMgr or ForemanImpl), but
    // managing its storage is a hassle.
    // Probably not worth it because Runners probably won't be poisoned through
    // this interface anyway--poison reqs will come through xrootd.
    _isPoisoned = true;
}

void ForemanImpl::Runner::_runWithDump() {
    boost::shared_ptr<wdb::QueryRunner> qr;
    qr = _rm.newQueryRunner(_task);
    qr->actOnce();
}

void ForemanImpl::Runner::_runProtocol2() {
    _action = _rm.newQueryAction(_task);
    (*_action)();
}

void ForemanImpl::Runner::operator()() {
    _rm.registerRunner(this, _task);
    while(!_isPoisoned) {
        LOGF(_log, LOG_LVL_INFO, "Runner running %1%" % *_task);
        proto::TaskMsg const& msg = *_task->msg;
        if(msg.has_protocol() && msg.protocol() == 2) {
            _runProtocol2();
        } else {
            _runWithDump();
        }
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
// class ForemanImpl::Processor
////////////////////////////////////////////////////////////////////////
class ForemanImpl::Processor : public wbase::MsgProcessor {
public:
    class Cancel : public util::VoidCallable<void> {
    public:
        Cancel(wbase::Task::Ptr t) : _t(t) {}
        virtual void operator()() {
            _t->poison();
        }
        wbase::Task::Ptr _t;
    };
    Processor(ForemanImpl& f) : _foremanImpl(f) {}

    virtual boost::shared_ptr<util::VoidCallable<void> >
    operator()(boost::shared_ptr<proto::TaskMsg> taskMsg,
               boost::shared_ptr<wbase::SendChannel> replyChannel) {

        wbase::Task::Ptr t = boost::make_shared<wbase::Task>(taskMsg, replyChannel);
        _foremanImpl.newTaskAction(t);
        boost::shared_ptr<Cancel> c = boost::make_shared<Cancel>(t);
        return c;
    }
    ForemanImpl& _foremanImpl;
};
////////////////////////////////////////////////////////////////////////
// ForemanImpl
////////////////////////////////////////////////////////////////////////
ForemanImpl::ForemanImpl(Scheduler::Ptr s)
    : _scheduler(s), _log(LOG_GET("Foreman")),
      _running(boost::make_shared<wbase::TaskQueue>()) {
    // Make the chunk resource mgr
    mysql::MySqlConfig c(wconfig::getConfig().getSqlConfig());
    _chunkResourceMgr = wdb::ChunkResourceMgr::newMgr(c);

    _rManager.reset(new RunnerMgr(*this));
    assert(s); // Cannot operate without scheduler.

}
ForemanImpl::~ForemanImpl() {
    // FIXME: Poison and drain runners.
}

void
ForemanImpl::_startRunner(wbase::Task::Ptr t) {
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
    wbase::Task::Ptr t = boost::make_shared<wbase::Task>(msg);
    newTaskAction(t);
    return true;
}

void ForemanImpl::newTaskAction(wbase::Task::Ptr task) {
    // Pass to scheduler.
    assert(_scheduler);
    wbase::TaskQueuePtr newReady = _scheduler->newTaskAct(task, _running);
    // Perform only what the scheduler requests.
    if(newReady.get() && (newReady->size() > 0)) {
        wbase::TaskQueue::iterator i = newReady->begin();
        for(; i != newReady->end(); ++i) {
            _startRunner(*i);
        }
    }
}



boost::shared_ptr<wbase::MsgProcessor> ForemanImpl::getProcessor() {
    return boost::shared_ptr<Processor>(new Processor(*this));
}

}}} // namespace lsst::qserv::wcontrol
