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
#ifndef LSST_QSERV_WSCHED_SCANSCHEDULER_H
#define LSST_QSERV_WSCHED_SCANSCHEDULER_H

// System headers
#include <mutex>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wcontrol/Foreman.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wsched {
    class ChunkDisk;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wsched {

class ScanScheduler : public wcontrol::Foreman::Scheduler {
public:
    typedef std::shared_ptr<ScanScheduler> Ptr;
    typedef std::vector<std::shared_ptr<ChunkDisk> > ChunkDiskList;

    ScanScheduler();
    virtual ~ScanScheduler() {}

    virtual bool removeByHash(std::string const& hash);
    virtual void queueTaskAct(wbase::Task::Ptr incoming);
    virtual wbase::TaskQueuePtr nopAct(wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr newTaskAct(wbase::Task::Ptr incoming,
                                           wbase::TaskQueuePtr running);
    virtual wbase::TaskQueuePtr taskFinishAct(wbase::Task::Ptr finished,
                                              wbase::TaskQueuePtr running);
    // TaskWatcher interface
    virtual void markStarted(wbase::Task::Ptr t);
    virtual void markFinished(wbase::Task::Ptr t);

    static std::string getName()  { return std::string("ScanSched"); }
    bool checkIntegrity();
private:
    wbase::TaskQueuePtr _getNextTasks(int max);
    void _enqueueTask(wbase::Task::Ptr incoming);
    bool _integrityHelper();

    int _maxRunning;
    ChunkDiskList _disks;
    LOG_LOGGER _logger;
    std::mutex _mutex;
};

}}} // namespace lsst::qserv::wsched

extern lsst::qserv::wsched::ScanScheduler* dbgScanScheduler; ///< A symbol for gdb
extern lsst::qserv::wsched::ChunkDisk* dbgChunkDisk1; ///< A symbol for gdb

#endif // LSST_QSERV_WSCHED_SCANSCHEDULER_H

