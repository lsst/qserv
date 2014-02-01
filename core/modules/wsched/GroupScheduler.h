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
#ifndef LSST_QSERV_WORKER_GROUPSCHEDULER_H
#define LSST_QSERV_WORKER_GROUPSCHEDULER_H

#include "wcontrol/Foreman.h"
#include "wsched/GroupedQueue.h"

namespace lsst {
namespace qserv {
namespace worker {
/// GroupScheduler -- A scheduler that is a cross between FIFO and shared scan.
/// Tasks are ordered as they come in, except that queries for the
/// same chunks are grouped together.
class GroupScheduler : public Foreman::Scheduler {
public:
    typedef boost::shared_ptr<GroupScheduler> Ptr;

    GroupScheduler(boost::shared_ptr<WLogger> logger);
    virtual ~GroupScheduler() {}

    virtual bool removeByHash(std::string const& hash);
    virtual void queueTaskAct(Task::Ptr incoming);
    virtual TaskQueuePtr nopAct(TaskQueuePtr running);
    virtual TaskQueuePtr newTaskAct(Task::Ptr incoming,
                                    TaskQueuePtr running);
    virtual TaskQueuePtr taskFinishAct(Task::Ptr finished,
                                       TaskQueuePtr running);
    static std::string getName()  { return std::string("GroupSched"); }
    bool checkIntegrity();

    struct ChunkEqual {
        bool operator()(Task::Ptr const& x, Task::Ptr const& y) {
            if(!x || !y) { return false; }
            if((!x->msg) || (!y->msg)) { return false; }
            return x->msg->has_chunkid() && y->msg->has_chunkid()
                && x->msg->chunkid()  == y->msg->chunkid();
        }
    };
    typedef GroupedQueue<Task::Ptr, ChunkEqual> Queue;

private:
    void _enqueueTask(Task::Ptr incoming);
    bool _integrityHelper();
    TaskQueuePtr _getNextIfAvail(int runCount);
    TaskQueuePtr _getNextTasks(int max);

    boost::mutex _mutex;

    Queue _queue;
    int _maxRunning;
    boost::shared_ptr<WLogger> _logger;
};
}}} // lsst::qserv::worker
#endif // LSST_QSERV_WORKER_GROUPSCHEDULER_H
