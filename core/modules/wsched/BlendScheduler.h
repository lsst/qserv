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
#ifndef LSST_QSERV_WSCHED_BLENDSCHEDULER_H
#define LSST_QSERV_WSCHED_BLENDSCHEDULER_H

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wcontrol/Foreman.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wsched {
    class GroupScheduler;
    class ScanScheduler;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wsched {

/// BlendScheduler -- A scheduler that switches between two underlying
/// schedulers based on the incoming task properties. If the incoming
/// task has a scanTables spec in its message, it is scheduled with a
/// ScanScheduler; otherwise it uses the GroupScheduler.
/// The GroupScheduler has concessions for chunk grouping as well, but
/// it should be set for reduced concurrency limited I/O sharing.
class BlendScheduler : public wcontrol::Scheduler {
public:
    using Ptr = std::shared_ptr<BlendScheduler>;

    BlendScheduler(std::shared_ptr<GroupScheduler> group,
                   std::shared_ptr<ScanScheduler> scan);
    virtual ~BlendScheduler() {}

    void queCmd(util::Command::Ptr const& cmd) override;
    util::Command::Ptr getCmd(bool wait) override;

    void commandStart(util::Command::Ptr const& cmd) override;
    void commandFinish(util::Command::Ptr const& cmd) override;

    static std::string getName()  { return std::string("BlendSched"); }
    wcontrol::Scheduler* lookup(wbase::Task::Ptr p);
private:
    bool _ready();

    std::shared_ptr<GroupScheduler> _group;
    std::shared_ptr<ScanScheduler> _scan;
    bool _lastCmdFromScan{false};
    LOG_LOGGER _logger;
    std::map<wbase::Task*, wcontrol::Scheduler*> _map;
    std::mutex _mapMutex;
};

}}} // namespace lsst::qserv::wsched

extern lsst::qserv::wsched::BlendScheduler* dbgBlendScheduler; ///< A symbol for gdb

#endif // LSST_QSERV_WSCHED_BLENDSCHEDULER_H
