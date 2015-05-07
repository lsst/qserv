// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_GROUPSCHEDULER_H
#define LSST_QSERV_WSCHED_GROUPSCHEDULER_H

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wcontrol/Foreman.h"
#include "wsched/GroupedQueue.h"

namespace lsst {
namespace qserv {
namespace wsched {

/// GroupScheduler -- A scheduler that is a cross between FIFO and shared scan.
/// Tasks are ordered as they come in, except that queries for the
/// same chunks are grouped together.
class GroupScheduler : public wcontrol::Foreman::Scheduler {
public:
    typedef std::shared_ptr<GroupScheduler> Ptr;

    GroupScheduler();
    virtual ~GroupScheduler() {}

    virtual bool removeByHash(std::string const& hash);
    virtual void queueTaskAct(wbase::Task::Ptr incoming);
    virtual wbase::TaskQueuePtr nopAct(wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr newTaskAct(wbase::Task::Ptr incoming,
                                              wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr taskFinishAct(wbase::Task::Ptr finished,
                                                 wbase::TaskQueuePtr running);
    static std::string getName()  { return std::string("GroupSched"); }
    bool checkIntegrity();

    typedef GroupedQueue<wbase::Task::Ptr, wbase::Task::ChunkEqual> Queue;

private:
    void _enqueueTask(wbase::Task::Ptr incoming);
    bool _integrityHelper();
    wbase::TaskQueuePtr _getNextIfAvail(int runCount);
    wbase::TaskQueuePtr _getNextTasks(int max);

    std::mutex _mutex;

    Queue _queue;
    int _maxRunning;
    LOG_LOGGER _logger;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_GROUPSCHEDULER_H
