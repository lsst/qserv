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

class ScanScheduler : public wcontrol::Scheduler {
public:
    typedef std::shared_ptr<ScanScheduler> Ptr;
    typedef std::vector<std::shared_ptr<ChunkDisk> > ChunkDiskList;

    ScanScheduler(int maxThreads);
    virtual ~ScanScheduler() {}

    void queCmd(util::Command::Ptr const& cmd) override;
    util::Command::Ptr getCmd(bool wait) override;
    void commandStart(util::Command::Ptr const& cmd) override;
    void commandFinish(util::Command::Ptr const& cmd) override;
    int getInFlight() { return _inFlight; }

    static std::string getName()  { return std::string("ScanSched"); }
    bool ready();
    std::size_t getSize();
private:
    bool _ready();

    int _maxThreads;
    ChunkDiskList _disks;
    LOG_LOGGER _logger;
    std::atomic<int> _inFlight{0};
};

}}} // namespace lsst::qserv::wsched

extern lsst::qserv::wsched::ScanScheduler* dbgScanScheduler; ///< A symbol for gdb
extern lsst::qserv::wsched::ChunkDisk* dbgChunkDisk1; ///< A symbol for gdb

#endif // LSST_QSERV_WSCHED_SCANSCHEDULER_H

