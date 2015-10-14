// Class header
#include "wcontrol/RunnerMgr.h"

// System headers
#include <cassert>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/worker.pb.h"
#include "wbase/SendChannel.h"
#include "wdb/QueryRunner.h"

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

class RunnerMgr::StartTaskF {
public:
    StartTaskF(Foreman& f) : _f(f) {}
    void operator()(wbase::Task::Ptr t) {
        _f._startRunner(t);
    }
private:
    Foreman& _f;
};

RunnerMgr::RunnerMgr(Foreman& f)
    : _f(f), _taskWatcher(*f._scheduler) {
}

void RunnerMgr::registerRunner(Runner::Ptr const& r, wbase::Task::Ptr const& t) {
    {
        std::lock_guard<std::mutex> lock(_runnersMutex);
        _runners.push_back(r);
    }

    LOGF(_f._log, LOG_LVL_DEBUG, "Registered runner %1%" % (void*)r.get());
    _reportStartHelper(t);
}

std::shared_ptr<wdb::QueryRunner> RunnerMgr::newQueryAction(wbase::Task::Ptr const& t) {
    wdb::QueryRunnerArg a(_f._log, t, _f._chunkResourceMgr);
    std::shared_ptr<wdb::QueryRunner> qa = std::make_shared<wdb::QueryRunner>(a);
    return qa;
}

void RunnerMgr::reportComplete(wbase::Task::Ptr const& t) {
    {
        std::lock_guard<std::mutex> lock(_runnersMutex);
        bool popped = popFrom(*_running, t);
        assert(popped);
    }

    LOGF(_f._log, LOG_LVL_DEBUG, "Finished task %1%" % *t);
    _taskWatcher.markFinished(t);
}

void RunnerMgr::reportStart(wbase::Task::Ptr const& t) {
   _reportStartHelper(t);
}

void RunnerMgr::_reportStartHelper(wbase::Task::Ptr const& t) {
    {
        std::lock_guard<std::mutex> lock(_runnersMutex);
        _running->push_back(t);
    }
    LOGF(_f._log, LOG_LVL_DEBUG, "Started task %1%" % *t);
    _taskWatcher.markStarted(t);
}

void RunnerMgr::signalDeath(Runner::Ptr const& r) {
    std::lock_guard<std::mutex> lock(_runnersMutex);
    for(auto iter = _runners.begin(), e = _runners.end(); iter != e; ++iter) {
        auto const& runner = *iter;
        if(runner.get() == r.get()) {
            _runners.erase(iter);
            //_f._runnersEmpty.notify_all(); // Still needed?
            return;
        }
    }
}

wbase::Task::Ptr RunnerMgr::getNextTask(Runner::Ptr const& r, wbase::Task::Ptr previous) {
    wbase::TaskQueuePtr tq;
    {
        std::lock_guard<std::mutex> lock(_runnersMutex);
        tq = _f._scheduler->taskFinishAct(previous, _running);
    }
    if(!tq.get()) {
        return wbase::Task::Ptr();
    }
    if(tq->size() > 1) {
        std::for_each(tq->begin()+1, tq->end(), StartTaskF(_f));
    }
    return tq->front();
}

LOG_LOGGER RunnerMgr::getLog() {
    return _f._log;
}

/// This will attach the 'task' to 'scheduler', which will result in the task being run at some point.
wbase::TaskQueuePtr RunnerMgr::queueTask(wbase::Task::Ptr const& task, Scheduler::Ptr const& scheduler) {
    std::lock_guard<std::mutex> lock(_runnersMutex);
    wbase::TaskScheduler::Ptr p = scheduler;
    task->setTaskScheduler(p);
    return scheduler->newTaskAct(task, _running);
}

Runner::Runner(RunnerMgr *rm, wbase::Task::Ptr const& firstTask) : _rm{rm}, _task{firstTask} {
    LOGF(_rm->getLog(), LOG_LVL_DEBUG, "Runner::Runner()");
}

Runner::~Runner() {
    LOGF(_rm->getLog(), LOG_LVL_DEBUG, "Runner::~Runner()");
}

/// Run when Foreman creates the thread. It runs the task passed to the constructor and then
/// goes back to the scheduler for more tasks.
/// Note: This function never exits as _poisoned is never set to true.
/// Expect significant changes in DM-3945.
void Runner::operator()() {
    // Real purpose of thisPtr is to keep Runner from being deleted before this function exits.
    Runner::Ptr thisPtr(shared_from_this());
    _rm->registerRunner(thisPtr, _task);
    while(!_poisoned) {
        LOGF(_rm->getLog(), LOG_LVL_DEBUG, "Runner running %1%" % *_task);
        proto::TaskMsg const& msg = *_task->msg;
        if(!msg.has_protocol() || msg.protocol() < 2) {
            _task->sendChannel->sendError("Unsupported wire protocol", 1);
        } else {
            auto qr = _rm->newQueryAction(_task);
            qr->runQuery();
        }
        if(_poisoned) break;
        // Request new work from the manager
        // (mgr is a role of the foreman, who will check with the
        // scheduler for the next assignment)
        _rm->reportComplete(_task);
        _task = _rm->getNextTask(thisPtr, _task);
        if(!_task.get()) break; // No more work?
        _rm->reportStart(_task);
    } // Keep running until we get poisoned.
    _rm->signalDeath(thisPtr);
}

}}} // namespace
