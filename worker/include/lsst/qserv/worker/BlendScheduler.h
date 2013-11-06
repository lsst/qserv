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
#ifndef LSST_QSERV_WORKER_BLENDSCHEDULER_H
#define LSST_QSERV_WORKER_BLENDSCHEDULER_H

#include "lsst/qserv/worker/Foreman.h"

namespace lsst {
namespace qserv {
namespace worker {
class GroupScheduler;
class ScanScheduler;

/// BlendScheduler -- A scheduler that switches between two underlying
/// schedulers based on the incoming task properties. If the incoming
/// task has a scanTables spec in its message, it is scheduled with a
/// ScanScheduler; otherwise it uses the GroupScheduler.
/// The GroupScheduler has concessions for chunk grouping as well, but
/// it should be set for reduced concurrency limited I/O sharing.
class BlendScheduler : public Foreman::Scheduler {
public:
    typedef boost::shared_ptr<BlendScheduler> Ptr;

    BlendScheduler(boost::shared_ptr<Logger> logger,
                   boost::shared_ptr<GroupScheduler> group,
                   boost::shared_ptr<ScanScheduler> scan);
    virtual ~BlendScheduler() {}

    virtual void queueTaskAct(Task::Ptr incoming);
    virtual TaskQueuePtr nopAct(TaskQueuePtr running);
    virtual TaskQueuePtr newTaskAct(Task::Ptr incoming,
                                    TaskQueuePtr running);
    virtual TaskQueuePtr taskFinishAct(Task::Ptr finished,
                                       TaskQueuePtr running);

    // TaskWatcher interface
    virtual void markStarted(Task::Ptr t);
    virtual void markFinished(Task::Ptr t);

    static std::string getName()  { return std::string("BlendSched"); }
    bool checkIntegrity();

    Foreman::Scheduler* lookup(Task::Ptr p);
private:
    TaskQueuePtr _getNextIfAvail(TaskQueuePtr running);
    bool _integrityHelper() const;
    Foreman::Scheduler* _lookup(Task::Ptr p);

    boost::shared_ptr<GroupScheduler> _group;
    boost::shared_ptr<ScanScheduler> _scan;
    boost::shared_ptr<Logger> _logger;
    typedef std::map<Task*, Foreman::Scheduler*> Map;
    Map _map;
    boost::mutex _mapMutex;
};
}}} // lsst::qserv::worker
extern lsst::qserv::worker::BlendScheduler* dbgBlendScheduler; //< A symbol for gdb
#endif // LSST_QSERV_WORKER_BLENDSCHEDULER_H
