/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
 /**
  * @file FifoScheduler.cc
  *
  * @brief A simple scheduler implementation for ordering query tasks
  * to send to the mysqld.
  *
  * @author Daniel L. Wang, SLAC
  */ 
#include "lsst/qserv/worker/FifoScheduler.h"
#include <boost/thread.hpp>
// #include <iostream> // Enable for debugging.

namespace lsst {
namespace qserv {
namespace worker {

typedef Foreman::TaskQueuePtr TaskQueuePtr;

FifoScheduler::FifoScheduler()
    : _maxRunning(4) 
      // FIXME: _maxRunning needs some design. The optimal value can
      // be quite complex, and is probably dynamic. This is noted as a
      // long-term design issue on
      // https://dev.lsstcorp.org/trac/wiki/db/Qserv/WorkerParallelism
{}    

TaskQueuePtr FifoScheduler::nopAct(TodoList::Ptr todo,
                                   TaskQueuePtr running) {
    // For now, do nothing when there is no event.  

    // Perhaps better: Check to see how many are running, and schedule
    // a task if the number of running jobs is below a threshold.
    return TaskQueuePtr();
}

TaskQueuePtr FifoScheduler::newTaskAct(Task::Ptr incoming,
                                       TodoList::Ptr todo,
                                       TaskQueuePtr running) {

    boost::lock_guard<boost::mutex> guard(_mutex);
    TaskQueuePtr tq;
    assert(running.get());
    assert(todo.get());
    assert(incoming.get());
    if(running->size() < _maxRunning) {
        // If we have space, start running.
        Task::Ptr t = todo->popTask();
        if (t) {
            tq.reset(new TodoList::TaskQueue());
            tq->push_back(t);
        }
    }
    return tq;
}

TaskQueuePtr FifoScheduler::taskFinishAct(Task::Ptr finished,
                                          TodoList::Ptr todo,
                                          TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    TaskQueuePtr tq;
    assert(running.get());
    assert(todo.get());
    assert(finished.get());

    // FIFO always replaces a finishing task with a new task, always
    // maintaining a constant number of running threads (as long as
    // there is work to do)
    Task::Ptr t = todo->popTask();
    if (t) {
        tq.reset(new TodoList::TaskQueue());
        tq->push_back(t);
    }
    return tq;
}

}}} // namespace lsst::qserv::worker
