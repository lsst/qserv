/*
 * LSST Data Management System
 * Copyright 2010-2013 LSST Corporation.
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
#ifndef LSST_QSERV_WORKER_FIFOSCHEDULER_H
#define LSST_QSERV_WORKER_FIFOSCHEDULER_H

#include <boost/thread/mutex.hpp>
#include "lsst/qserv/worker/Foreman.h"

namespace lsst {
namespace qserv {
namespace worker {

class FifoScheduler : public Foreman::Scheduler {
public:
    typedef boost::shared_ptr<FifoScheduler> Ptr;

    explicit FifoScheduler(int maxRunning=-1);
    virtual ~FifoScheduler() {}

    virtual void queueTaskAct(Task::Ptr incoming);
    virtual TaskQueuePtr nopAct(TaskQueuePtr running);
    virtual TaskQueuePtr newTaskAct(Task::Ptr incoming,
                                    TaskQueuePtr running);
    virtual TaskQueuePtr taskFinishAct(Task::Ptr finished,
                                       TaskQueuePtr running);
    static std::string getName() { return std::string("FifoSched"); }
private:
    TaskQueuePtr _fetchTask();

    boost::mutex _mutex;
    TaskQueue _queue;
    int _maxRunning;
};
}}} // lsst::qserv::worker
#endif // LSST_QSERV_WORKER_FIFOSCHEDULER_H
