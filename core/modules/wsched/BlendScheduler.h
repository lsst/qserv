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
#ifndef LSST_QSERV_WSCHED_BLENDSCHEDULER_H
#define LSST_QSERV_WSCHED_BLENDSCHEDULER_H

// System headers
#include <map>

// Qserv headers
#include "wsched/SchedulerBase.h"

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

/// BlendScheduler is a scheduler that places queries in one of
/// 4 sub-schedulers. Interactive queries are placed on the GroupScheduler
/// _group, which has the highest priority. Other queries, which are
/// expected to require all, or most, of the chunks on this node, go
/// to one of the ScanSchedulers: _scanFast, _scanMedium, _scanSlow.
/// The priority is _group, _scanFast, _scanMedium, _scaneSlow. This
/// should match the list in _schedulers.
///
/// There are several constraints on BlendSheduler places on the sub-schedulers.
/// The schedulers run Tasks in a limited pool of threads. At any time,
/// all sub-schedulers should be able to run at least one thread. This is to
/// keep sub-schedulers from getting jammed by heavy loads, or prevent
/// high priority/fast sub-schedulers being stuck waiting for low priority/slow
/// sub-schedulers to finish a Task.
///
/// Limiting threads for sub-schedulers is handler mostly with
/// For every Task inFlight beyond 1, the maximum threads available to other schedulers is reduced by one.
/// If configured properly, each scheduler has a maxThreads limit that will leave one thread available for
/// each of the other schedulers.
/// An example:
/// Assuming 12 threads, 4 schedulers, which gives a base _subSchedulerMaxThreads = 9.
/// The group scheduler has 1 Task inFlight, it has no effect on the other schedulers' maxThread limit.
/// The scanFast scheduler has 5 Tasks inFlight, so
///     the scanMedium and scanSlow schedulers' maxThread limit will drop by 4.
/// scanMedium has 0 Tasks inFlight, it has no effect on other schedulers' maxThreads
/// The scanSlow puts 5 Tasks inFlight, using all that is allowed by its adjusted maxThreads value (9 - 4).
/// 11 of the 12 threads are in use. If something is put on scanMedium, it can run immediately.
/// If several tasks are put on the group scheduler, it will grab threads as other Tasks finish, as threads
///      ask _group first for new Tasks.
///
/// Secondly, the ScanScheduler schedulers are only allowed to advance to a new chunk
/// if resources are available to read the chunk into memory, or if the sub-scheduler
/// has no Tasks inFlight (same thing as having zero threads).
class BlendScheduler : public wsched::SchedulerBase {
public:
    using Ptr = std::shared_ptr<BlendScheduler>;

    BlendScheduler(std::string const& name,
                   int subSchedMaxThreads,
                   std::shared_ptr<GroupScheduler> const& group,
                   std::shared_ptr<ScanScheduler> const& scanFast,
                   std::shared_ptr<ScanScheduler> const& scanMedium,
                   std::shared_ptr<ScanScheduler> const& scanSlow);
    virtual ~BlendScheduler() {}

    void queCmd(util::Command::Ptr const& cmd) override;
    util::Command::Ptr getCmd(bool wait) override;

    void commandStart(util::Command::Ptr const& cmd) override;
    void commandFinish(util::Command::Ptr const& cmd) override;

    // SchedulerBase overrides methods.
    std::size_t getSize() const override;
    int getInFlight() const override;
    bool ready() override;
    int applyAvailableThreads(int tempMax) override { return tempMax;} //< does nothing

    wcontrol::Scheduler* lookup(wbase::Task::Ptr p);
    int calcAvailableTheads();

private:
    int _getAdjustedMaxThreads(int oldAdjMax, int inFlight);
    bool _ready();

    int _schedMaxThreads; //< maximum number of threads that can run.

    // Sub-schedulers.
    std::shared_ptr<GroupScheduler> _group;
    std::shared_ptr<ScanScheduler> _scanFast;
    std::shared_ptr<ScanScheduler> _scanMedium;
    std::shared_ptr<ScanScheduler> _scanSlow;
    // List of schedulers in order of priority.
    std::vector<SchedulerBase*> _schedulers;
    bool _lastCmdFromScan{false};
    std::map<wbase::Task*, SchedulerBase*> _map;
    std::mutex _mapMutex;
};

}}} // namespace lsst::qserv::wsched

extern lsst::qserv::wsched::BlendScheduler* dbgBlendScheduler; ///< A symbol for gdb

#endif // LSST_QSERV_WSCHED_BLENDSCHEDULER_H
