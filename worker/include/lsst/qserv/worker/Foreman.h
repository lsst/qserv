/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
//  The scheduler must be defined to handle multiple scheduling events
#ifndef LSST_QSERV_WORKER_FOREMAN_H
#define LSST_QSERV_WORKER_FOREMAN_H
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/worker/TodoList.h"
#include "lsst/qserv/worker/Task.h"

namespace lsst {
namespace qserv {
namespace worker {

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

    void add(boost::shared_ptr<Task> c) = 0;
protected:
    explicit Foreman() {}
};

Foreman::Ptr newForeman();

}}}  // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_FOREMAN_H
