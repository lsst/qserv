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
#ifndef LSST_QSERV_WSCHED_SCANSCHEDULER_H
#define LSST_QSERV_WSCHED_SCANSCHEDULER_H

// System headers
#include <mutex>

// Qserv headers
#include "memman/MemMan.h"
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

/// The purpose of the scan scheduler is to try to limit disk i/o.
/// Tasks given to ScanScheduler are parts of user queries that are
/// expected to touch most or all of the chunks on the worker.
///
/// It groups Tasks by chunk id and loops through chunks in
/// ascending order running all Tasks for each chunk as it goes and
//  wrapping back to the lowest chunk at the end.
///
/// It only advances to the next chunk after the current chunk
/// has been read from disk. It waits for at least one query on the
/// current chunk to finish as an indicator that the entire chunk
/// was read from disk.
///
/// This is intended to be done for each disk in the system,
/// but currently only supports a single disk.
class ScanScheduler : public wcontrol::Scheduler {
public:
    typedef std::shared_ptr<ScanScheduler> Ptr;
    //typedef std::vector<std::shared_ptr<ChunkDisk> > ChunkDiskList; &&& delete

    ScanScheduler(std::string name, int maxThreads, memman::MemMan::Ptr const& memman);
    virtual ~ScanScheduler() {}

    // util::CommandQueue overrides
    void queCmd(util::Command::Ptr const& cmd) override;
    util::Command::Ptr getCmd(bool wait) override;
    void commandStart (util::Command::Ptr const& cmd) override;
    void commandFinish (util::Command::Ptr const& cmd) override;

    // wcontrol::Scheduler overrides
    int getInFlight() const override { return _inFlight; }
    std::string getName() const override { return _name; }
    bool ready() override;
    std::size_t getSize() const override ;
    void maxThreadAdjust(int tempMax) override;

private:
    bool _ready();
    int _maxInFlight() { return std::min(_maxThreads, _maxThreadsAdj); }

    std::string _name{""};
    int _maxThreads{1};
    int _maxThreadsAdj{1}; //< This must be used carefully as not protected.
    std::shared_ptr<ChunkDisk> _disk; //< Constrains access to files.
    memman::MemMan::Ptr _memMan;
    std::atomic<int> _inFlight{0};
};

}}} // namespace lsst::qserv::wsched

extern lsst::qserv::wsched::ScanScheduler* dbgScanScheduler; ///< A symbol for gdb
extern lsst::qserv::wsched::ChunkDisk* dbgChunkDisk1; ///< A symbol for gdb

#endif // LSST_QSERV_WSCHED_SCANSCHEDULER_H

