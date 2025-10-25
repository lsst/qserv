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
#include <atomic>
#include <mutex>

// Qserv headers
#include "wsched/ChunkTaskCollection.h"
#include "wsched/SchedulerBase.h"

// Forward declarations
namespace lsst::qserv::wsched {
class BlendScheduler;
}  // namespace lsst::qserv::wsched

namespace lsst::qserv::wsched {

/// The purpose of the scan scheduler is to try to limit disk i/o.
/// Tasks given to ScanScheduler are parts of user queries that are
/// expected to touch most or all of the chunks on the worker.
///
/// It groups Tasks by chunk id and loops through chunks in
/// ascending order running all Tasks for each chunk as it goes and
//  wrapping back to the lowest chunk at the end.
///
/// It only advances to the next chunk if system resources are available.
class ScanScheduler : public SchedulerBase {
public:
    typedef std::shared_ptr<ScanScheduler> Ptr;

    ScanScheduler(std::string const& name, int maxThreads, int maxReserve, int priority, int maxActiveChunks,
                  int minRating, int maxRating, double maxTimeMinutes);
    virtual ~ScanScheduler() {}

    // util::CommandQueue overrides
    /// Queuing atomically is very important for ScanSchedulers. All fragments
    /// for the query should be run on the same scheduler pass of the chunk
    /// to free up resources as soon as possible.
    void queCmd(std::vector<util::Command::Ptr> const& cmds) override;

    /// To avoid duplicate code paths, 'cmd' is wrapped in a vector and passed to
    /// void queCmd(std::vector<Command::Ptr> const& cmds).
    void queCmd(util::Command::Ptr const& cmd) override;

    util::Command::Ptr getCmd(bool wait) override;
    void commandStart(util::Command::Ptr const& cmd) override;
    void commandFinish(util::Command::Ptr const& cmd) override;
    bool isRatingInRange(int rating) const { return _minRating <= rating && rating <= _maxRating; }
    std::string getRatingStr() const;

    // SchedulerBase overrides
    bool ready() override;
    std::size_t getSize() const override;

    double getMaxTimeMinutes() const { return _maxTimeMinutes; }
    bool removeTask(wbase::Task::Ptr const& task, bool removeRunning) override;

private:
    bool _ready();
    std::shared_ptr<ChunkTaskCollection> _taskQueue;  ///< Constrains access to files.

    /// Scans placed on this scheduler should have a rating between(inclusive) _minRating and _maxRating.
    const int _minRating;
    const int _maxRating;

    /// Maximum amount of time a UserQuery (all of its Tasks for this worker) should
    /// take to complete on this scheduler.
    double _maxTimeMinutes;

    std::atomic<bool> _infoChanged{true};  ///< "Used to limit the amount of debug logging.
};

}  // namespace lsst::qserv::wsched

#endif  // LSST_QSERV_WSCHED_SCANSCHEDULER_H
