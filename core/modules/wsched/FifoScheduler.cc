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
#include "wsched/FifoScheduler.h"
#include <boost/thread.hpp>
// #include <iostream> // Enable for debugging.

namespace lsst {
namespace qserv {
namespace wsched {

FifoScheduler::FifoScheduler(int maxRunning)
    : _maxRunning(maxRunning) {
    if(maxRunning < 0) {
        _maxRunning = boost::thread::hardware_concurrency();
    }
    // FIXME: _maxRunning needs some design. The optimal value can
    // be quite complex, and is probably dynamic. This is noted as a
    // long-term design issue on
    // https://dev.lsstcorp.org/trac/wiki/db/Qserv/WorkerParallelism
}

void
FifoScheduler::queueTaskAct(wcontrol::Task::Ptr incoming) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    _queue.push_back(incoming);
}

wcontrol::TaskQueuePtr
FifoScheduler::nopAct(wcontrol::TaskQueuePtr running) {
    // For now, do nothing when there is no event.

    // Perhaps better: Check to see how many are running, and schedule
    // a task if the number of running jobs is below a threshold.
    return wcontrol::TaskQueuePtr();
}

wcontrol::TaskQueuePtr
FifoScheduler::newTaskAct(wcontrol::Task::Ptr incoming,
                          wcontrol::TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(running.get());
    assert(incoming.get());

    _queue.push_back(incoming);
    if(running->size() < _maxRunning) {
        return _fetchTask();
    }
    return wcontrol::TaskQueuePtr();
}

wcontrol::TaskQueuePtr
FifoScheduler::taskFinishAct(wcontrol::Task::Ptr finished,
                             wcontrol::TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    wcontrol::TaskQueuePtr tq;
    assert(running.get());
    assert(finished.get());

    // FIFO always replaces a finishing task with a new task, always
    // maintaining a constant number of running threads (as long as
    // there is work to do)
    return _fetchTask();
}

/// Fetch a task from the queue
/// precondition: Caller must have _mutex locked.
wcontrol::TaskQueuePtr
FifoScheduler::_fetchTask() {
    wcontrol::TaskQueuePtr tq;
    if(!_queue.empty()) {
        wcontrol::Task::Ptr t = _queue.front();
        _queue.pop_front();
        assert(t); // Memory corruption if t is null.
        tq.reset(new wcontrol::TaskQueue());
        tq->push_back(t);
    }
    return tq;
}

}}} // namespace lsst::qserv::wsched
