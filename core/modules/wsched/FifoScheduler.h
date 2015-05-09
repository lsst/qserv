// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2010-2015 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_FIFOSCHEDULER_H
#define LSST_QSERV_WSCHED_FIFOSCHEDULER_H

// System headers
#include <mutex>

// Qserv headers
#include "wcontrol/Foreman.h"

namespace lsst {
namespace qserv {

namespace wcontrol {
    // Forward
    class Task;
}

namespace wsched {

class FifoScheduler : public wcontrol::Foreman::Scheduler {
public:
    typedef std::shared_ptr<FifoScheduler> Ptr;

    explicit FifoScheduler(int maxRunning=-1);
    virtual ~FifoScheduler() {}

    virtual void queueTaskAct(wbase::Task::Ptr incoming);
    virtual wbase::TaskQueuePtr nopAct(wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr newTaskAct(wbase::Task::Ptr incoming,
                                           wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr taskFinishAct(wbase::Task::Ptr finished,
                                              wbase::TaskQueuePtr running);
    static std::string getName() { return std::string("FifoSched"); }
private:
    wbase::TaskQueuePtr _fetchTask();

    std::mutex _mutex;
    wbase::TaskQueue _queue;
    int _maxRunning;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_FIFOSCHEDULER_H
