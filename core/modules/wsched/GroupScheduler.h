// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
    typedef boost::shared_ptr<GroupScheduler> Ptr;

    GroupScheduler(boost::shared_ptr<wlog::WLogger> logger);
    virtual ~GroupScheduler() {}

    virtual bool removeByHash(std::string const& hash);
    virtual void queueTaskAct(wcontrol::Task::Ptr incoming);
    virtual wcontrol::TaskQueuePtr nopAct(wcontrol::TaskQueuePtr running);
    virtual wcontrol::TaskQueuePtr newTaskAct(wcontrol::Task::Ptr incoming,
                                              wcontrol::TaskQueuePtr running);
    virtual wcontrol::TaskQueuePtr taskFinishAct(wcontrol::Task::Ptr finished,
                                                 wcontrol::TaskQueuePtr running);
    static std::string getName()  { return std::string("GroupSched"); }
    bool checkIntegrity();

    typedef GroupedQueue<wcontrol::Task::Ptr, wcontrol::Task::ChunkEqual> Queue;

private:
    void _enqueueTask(wcontrol::Task::Ptr incoming);
    bool _integrityHelper();
    wcontrol::TaskQueuePtr _getNextIfAvail(int runCount);
    wcontrol::TaskQueuePtr _getNextTasks(int max);

    boost::mutex _mutex;

    Queue _queue;
    int _maxRunning;
    boost::shared_ptr<wlog::WLogger> _logger;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_GROUPSCHEDULER_H
