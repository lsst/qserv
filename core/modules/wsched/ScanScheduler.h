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
#ifndef LSST_QSERV_WORKER_SCANSCHEDULER_H
#define LSST_QSERV_WORKER_SCANSCHEDULER_H

#include <boost/thread/mutex.hpp>
#include "wcontrol/Foreman.h"

namespace lsst {
namespace qserv {
namespace worker {
class ChunkDisk; // Forward
class WLogger;

class ScanScheduler : public Foreman::Scheduler {
public:
    typedef boost::shared_ptr<ScanScheduler> Ptr;
    typedef std::vector<boost::shared_ptr<ChunkDisk> > ChunkDiskList;

    ScanScheduler(boost::shared_ptr<WLogger> logger);
    virtual ~ScanScheduler() {}

    virtual bool removeByHash(std::string const& hash);
    virtual void queueTaskAct(Task::Ptr incoming);
    virtual TaskQueuePtr nopAct(TaskQueuePtr running);
    virtual TaskQueuePtr newTaskAct(Task::Ptr incoming,
                                    TaskQueuePtr running);
    virtual TaskQueuePtr taskFinishAct(Task::Ptr finished,
                                       TaskQueuePtr running);
    // TaskWatcher interface
    virtual void markStarted(Task::Ptr t);
    virtual void markFinished(Task::Ptr t);

    static std::string getName()  { return std::string("ScanSched"); }
    bool checkIntegrity();
private:
    TaskQueuePtr _getNextTasks(int max);
    void _enqueueTask(Task::Ptr incoming);
    bool _integrityHelper();

    ChunkDiskList _disks;
    boost::shared_ptr<WLogger> _logger;
    boost::mutex _mutex;
    int _maxRunning;
};
}}} // lsst::qserv::worker
extern lsst::qserv::worker::ScanScheduler* dbgScanScheduler; //< A symbol for gdb
extern lsst::qserv::worker::ChunkDisk* dbgChunkDisk1; //< A symbol for gdb
#endif // LSST_QSERV_WORKER_SCANSCHEDULER_H

