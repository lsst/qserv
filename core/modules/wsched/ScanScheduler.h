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
#include<atomic>
#include <mutex>

// Qserv headers
#include "memman/MemMan.h"
#include "wsched/ChunkTaskCollection.h"
#include "wsched/SchedulerBase.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wsched {
    class BlendScheduler;
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
/// It only advances to the next chunk if system resources are available.
class ScanScheduler : public SchedulerBase {
public:
    typedef std::shared_ptr<ScanScheduler> Ptr;

    ScanScheduler(std::string const& name, int maxThreads, int maxReserve, int priority,
                  int maxActiveChunks, memman::MemMan::Ptr const& memman,
                  int minRating, int maxRating);
    virtual ~ScanScheduler() {}

    void setBlendScheduler(BlendScheduler *blend) {
        _blendScheduler = blend;
    }

    // util::CommandQueue overrides
    void queCmd(util::Command::Ptr const& cmd) override;
    util::Command::Ptr getCmd(bool wait) override;
    void commandStart(util::Command::Ptr const& cmd) override;
    void commandFinish(util::Command::Ptr const& cmd) override;
    bool isRatingInRange(int rating) { return _minRating <= rating && rating <= _maxRating; }

    // SchedulerBase overrides
    bool ready() override;
    std::size_t getSize() const override ;

    void logMemManStats();

    wbase::Task::Ptr removeTask(wbase::Task::Ptr const& task) override;

private:
    bool _ready();
    std::shared_ptr<ChunkTaskCollection> _taskQueue; ///< Constrains access to files.

    memman::MemMan::Ptr _memMan; ///< Limits queries when resources not available.
    memman::MemMan::Handle _memManHandleToUnlock{memman::MemMan::HandleType::INVALID};

    /// Scans placed on this scheduler should have a rating between(inclusive) _minRating and _maxRating.
    const int _minRating;
    const int _maxRating;

    std::atomic<bool> _infoChanged{true}; ///< "Used to limit the amount of debug logging.
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_SCANSCHEDULER_H

