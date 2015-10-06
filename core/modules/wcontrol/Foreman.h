// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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
//  class Foreman -- A class that implements a pooling thread manager.
//  The Foreman is parameterized with a scheduling algorithm that
//  maintains its own container of ready-to-run and running tasks.
//  Thus the thread count is not fixed, but delegated to the
//  scheduler's responsiblity. The scheduler is solely responsible for
//  any I/O optimizations performed.
//
//  Approach: Using WorkQueue's API as a starting point, expand to
//  include a pluggable scheduler API and fill in the plumbing to make
//  sure the scheduler can see queued and running tasks.
//
//  The scheduler must be defined to handle multiple scheduling events:
//  nop: nothing has happened (no-operation) in terms of new tasks or
//  task completions, but the scheduler may decide that additional
//  execution is appropriate according to its own metrics.
//  newTask: a new task has arrived
//  taskFinish: a task has completed. The first Task returned by the
//  scheduler (if any) should be executed by the finishing thread.
//
//  The Foreman was originally intended to be as generic as WorkQueue,
//  but its dependencies mean that much of its work would become
//  templated or polymorphic, with little obvious value at this point
//  in development, aside from increased testability.  The affected
//  classes include at least: Task, QueryRunner. Those that
//  depend on them would need tweaks, but should be able to escape
//  templating or polymorphism.
#ifndef LSST_QSERV_WCONTROL_FOREMAN_H
#define LSST_QSERV_WCONTROL_FOREMAN_H

// System headers
#include <atomic>
#include <memory>

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "wbase/Base.h"
#include "wbase/Task.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
    class MsgProcessor;
}
namespace wdb {
	class ChunkResourceMgr;
}
}}

namespace lsst {
namespace qserv {
namespace wcontrol {

class RunnerMgr;

/// An abstract interface. Runners receive a reference to an
/// object implementing this and make calls to report start and
/// finish events for tasks they run. Schedulers must return
/// TaskWatcher objects so that runners can make reports.
class TaskWatcher {
public:
    virtual ~TaskWatcher() {}
    virtual void markStarted(wbase::Task::Ptr t) {}
    virtual void markFinished(wbase::Task::Ptr t) {}
};

/// An abstract scheduler interface. Foreman objects use Scheduler instances
/// to determine what tasks to launch upon triggering events.
class Scheduler : public TaskWatcher, public wbase::TaskScheduler {
public:
	using Ptr = std::shared_ptr<Scheduler>;
    virtual ~Scheduler() {}

    virtual bool removeByHash(std::string const& hash) { return false; }
    /** Take appropriate action when a task in the Schedule is cancelled. Doing
     * nothing should be harmless, but some Schedulers may work better if cancelled
     * tasks are removed.
     */
    virtual void taskCancelled(wbase::Task *task) { return; }
    virtual void queueTaskAct(wbase::Task::Ptr incoming) = 0;
    virtual wbase::TaskQueuePtr nopAct(wbase::TaskQueuePtr running) = 0;
    virtual wbase::TaskQueuePtr newTaskAct(wbase::Task::Ptr incoming,
                                           wbase::TaskQueuePtr running) = 0;
    virtual wbase::TaskQueuePtr taskFinishAct(wbase::Task::Ptr finished,
                                              wbase::TaskQueuePtr running) = 0;
};

/** Foreman is a pooling thread manager that is pluggable with different scheduling objects.
 *
 */
class Foreman {
public:
	using Ptr = std::shared_ptr<Foreman>;
	static Foreman::Ptr newForeman(Scheduler::Ptr const& s);
    Foreman(Scheduler::Ptr s);
    virtual ~Foreman();
    // This class should not be copied.
    Foreman(Foreman&) = delete;
    Foreman& operator=(Foreman&) = delete;

    void newTaskAction(wbase::Task::Ptr const& task);
    virtual std::shared_ptr<wbase::MsgProcessor> getProcessor();

    class Processor;
    friend class RunnerMgr;

protected:
    void _startRunner(wbase::Task::Ptr const& t);

    std::shared_ptr<wdb::ChunkResourceMgr> _chunkResourceMgr;
    std::mutex _mutex;
    Scheduler::Ptr _scheduler;
    std::unique_ptr<RunnerMgr> _rManager;
    LOG_LOGGER _log {LOG_GET("Foreman")};
};

}}}  // namespace lsst::qserv::wcontrol

#endif // LSST_QSERV_WCONTROL_FOREMAN_H
