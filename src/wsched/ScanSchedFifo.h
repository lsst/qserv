// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_SCANSCHEDFIFO_H
#define LSST_QSERV_WSCHED_SCANSCHEDFIFO_H

// System headers
#include <atomic>
#include <list>
#include <memory>
#include <mutex>

// Qserv headers
#include "wsched/ScanScheduler.h"

// Forward declarations
namespace lsst::qserv::wbase {
class Task;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::wsched {
class BlendScheduler;
}  // namespace lsst::qserv::wsched

namespace lsst::qserv::wsched {

class ScanSchedFifo : public ScanScheduler {  //&&&
public:
    typedef std::shared_ptr<ScanScheduler> Ptr;

    ScanSchedFifo(std::string const& name, int maxThreads, int maxReserve, int priority, int maxActiveChunks,
                  int minRating, int maxRating, double maxTimeMinutes);
    virtual ~ScanSchedFifo() {}

    void queCmd(std::vector<util::Command::Ptr> const& cmds) override;  //&&& check
    void queCmd(util::Command::Ptr const& cmd) override;                //&&& check

    util::Command::Ptr getCmd(bool wait) override;  //&&& check
    // void commandStart(util::Command::Ptr const& cmd) override; //&&& ok
    // void commandFinish(util::Command::Ptr const& cmd) override; //&&& ok

    // SchedulerBase overrides
    bool ready() override;                 //&&& check
    std::size_t getSize() const override;  //&&& check

    bool removeTask(wbase::Task::Ptr const& task, bool removeRunning) override;  //&&& check

private:
    wbase::Task::Ptr _removeTask(wbase::Task::Ptr const& task);

    /// Return true if a Task is ready to be run.
    /// util::CommandQueue::_mx must be locked before running.
    bool _ready() const;

    /// Protects _taskFifo as it has no internal mutex protection.
    mutable std::mutex _taskFifoMtx;
    std::shared_ptr<std::list<wbase::Task::Ptr>> _taskFifo{new std::list<wbase::Task::Ptr>()};
};

}  // namespace lsst::qserv::wsched

#endif  // LSST_QSERV_WSCHED_SCANSCHEDFIFO_H
