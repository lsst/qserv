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
//  class Foreman -- A class that implements a pooling thread manager.
//  The Foreman is parameterized with a scheduling algorithm that
//  decides what tasks to run from a TodoList and how many threads can
//  be allocated.  Thus the thread count is not fixed.  Foreman
//  provides the scheduler with a TodoList and a list of current work,
//  so that the scheduler can use resources efficiently and leverage
//  "synergies" (e.g., I/O reuse) when possible.
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
//  classes include at least: Task, TodoList, QueryRunner. Those that
//  depend on them would need tweaks, but should be able to escape
//  templating or polymorphism.
#ifndef LSST_QSERV_WORKER_FOREMAN_H
#define LSST_QSERV_WORKER_FOREMAN_H
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/worker/TodoList.h"
#include "lsst/qserv/worker/Task.h"

namespace lsst {
namespace qserv {
namespace worker {
class Logger; // Forward

class Foreman {
public:
    typedef boost::shared_ptr<Foreman> Ptr;

    typedef boost::shared_ptr<TodoList::TaskQueue> TaskQueuePtr;
    class Scheduler {
    public:
        typedef boost::shared_ptr<Scheduler> Ptr;
        virtual ~Scheduler() {}
    
        virtual TaskQueuePtr nopAct(TodoList::Ptr todo, 
                                    TaskQueuePtr running) = 0;
        virtual TaskQueuePtr newTaskAct(Task::Ptr incoming,
                                        TodoList::Ptr todo, 
                                        TaskQueuePtr running) = 0;
        virtual TaskQueuePtr taskFinishAct(Task::Ptr finished,
                                           TodoList::Ptr todo, 
                                           TaskQueuePtr running) = 0;
    };

    virtual ~Foreman() {}

protected:
    explicit Foreman() {}
};

Foreman::Ptr newForeman(TodoList::Ptr tl, boost::shared_ptr<Logger> log);

}}}  // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_FOREMAN_H
