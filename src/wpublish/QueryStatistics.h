/*
 * LSST Data Management System
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

#ifndef LSST_QSERV_WPUBLISH_QUERYSTATISTICS_H
#define LSST_QSERV_WPUBLISH_QUERYSTATISTICS_H

// System headers
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "wbase/Task.h"
#include "wsched/SchedulerBase.h"

/* &&&
// Forward declarations
namespace lsst::qserv::wbase {
struct TaskSelector;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::wsched {
class SchedulerBase;
class BlendScheduler;
class ScanScheduler;
}  // namespace lsst::qserv::wsched

namespace lsst::qserv::wpublish {
class QueriesAndChunks;
}  // namespace lsst::qserv::wpublish
*/


// This header declarations
namespace lsst::qserv::wpublish {

/// Statistics for a single user query.
/// This class stores some statistics for each Task in the user query on this worker.
class QueryStatistics {
public:
    using Ptr = std::shared_ptr<QueryStatistics>;

    void addTask(wbase::Task::Ptr const& task);
    bool isDead(std::chrono::seconds deadTime, TIMEPOINT now);
    int getTasksBooted();
    bool getQueryBooted() { return _queryBooted; }

    /// Add statistics related to the running of the query in the task.
    /// If there are subchunks in the user query, several Tasks may be needed for one chunk.
    /// @param runTimeSeconds - How long it took to run the query.
    /// @param subchunkRunTimeSeconds - How long the query spent waiting for the
    ///                         subchunk temporary tables to be made. It's important to
    ///                         remember that it's very common for several tasks to be waiting
    ///                         on the same subchunk tables at the same time.
    void addTaskRunQuery(double runTimeSeconds, double subchunkRunTimeSeconds);

    /// Add statistics related to transmitting results back to the czar.
    /// If there are subchunks in the user query, several Tasks may be needed for one chunk.
    /// @param timeSeconds - time to transmit data back to the czar for one Task
    /// @param bytesTransmitted - number of bytes transmitted to the czar for one Task.
    /// @param rowsTransmitted - number of rows transmitted to the czar for one Task.
    /// @param bufferFillSecs - time spent filling the buffer from the sql result.
    void addTaskTransmit(double timeSeconds, int64_t bytesTransmitted, int64_t rowsTransmitted,
                         double bufferFillSecs);

    /// This class tracks how many `Task`s a particular query has placed on
    /// particular schedulers and the time window during which they were added.
    /// This is a sum total of all `Task`s added. `Task`s are not removed
    /// when they finish.
    /// Copying is fine, but setting one equal to another would cause issues.
    class SchedulerTasksInfo {
    public:
        SchedulerTasksInfo() = delete;
        SchedulerTasksInfo(std::shared_ptr<wsched::SchedulerBase> const& sched_, int taskCount_)
            : scheduler(sched_), schedulerName(sched_->getName()), firstTaskAdded(CLOCK::now()),
              mostRecentTaskAdded(firstTaskAdded), taskCount(taskCount_) {}
        SchedulerTasksInfo(SchedulerTasksInfo const&) = default;
        SchedulerTasksInfo& operator=(SchedulerTasksInfo const&) = delete;

        std::weak_ptr<wsched::SchedulerBase> const scheduler;
        std::string const schedulerName;
        TIMEPOINT const firstTaskAdded; ///< The time the first `task` was added to this scheduler.
        TIMEPOINT mostRecentTaskAdded; ///< The time the last `task` was added to this scheduler.
        int taskCount = 0; ///< how many tasks were added to this scheduler.
    };

    /// Type definition for consistency.
    using SchedTasksInfoMap = std::map<std::string, SchedulerTasksInfo>;

    /// Track that `numberOfTasksAdded` has been added to `sched` where all of the `Task`s are related
    /// to this UserQuery.
    void tasksAddedToScheduler(std::shared_ptr<wsched::SchedulerBase> const& sched, int numberOfTasksAdded);

    /// Return a copy of `_taskSchedMap`.
    SchedTasksInfoMap getSchedulerTasksInfoMap();

    TIMEPOINT const creationTime;
    QueryId const queryId;

    /// Return a json object containing high level data, such as histograms.
    nlohmann::json getJsonHist() const;

    /// Retrieve a status of the tasks as defined by the query selector.
    /// @note If no restrictors are specified in the task selector then the method
    /// can return a very large object. So it should be used sparingly.
    /// @param taskSelector Task selection criterias.
    /// @return a json object containing information about tasks in the requested scope.
    nlohmann::json getJsonTasks(wbase::TaskSelector const& taskSelector) const;

    friend class QueriesAndChunks;
    friend std::ostream& operator<<(std::ostream& os, QueryStatistics const& q);

private:
    explicit QueryStatistics(QueryId const& queryId);
    bool _isMostlyDead() const;

    mutable std::mutex _qStatsMtx;

    std::chrono::system_clock::time_point _touched = std::chrono::system_clock::now();

    int _size = 0;
    int _tasksCompleted = 0;
    int _tasksRunning = 0;
    int _tasksBooted = 0;                   ///< Number of Tasks booted for being too slow.
    std::atomic<bool> _queryBooted{false};  ///< True when the entire query booted.

    double _totalTimeMinutes = 0.0;

    std::vector<wbase::Task::Ptr> _tasks;  ///< A collection of all tasks of the query

    util::Histogram::Ptr _histTimeRunningPerTask;  ///< Histogram of SQL query run times.
    util::Histogram::Ptr
            _histTimeSubchunkPerTask;  ///< Histogram of time waiting for temporary table generation.
    util::Histogram::Ptr _histTimeTransmittingPerTask;  ///< Histogram of time spent transmitting.
    util::Histogram::Ptr _histTimeBufferFillPerTask;    ///< Histogram of time filling buffers.
    util::Histogram::Ptr _histSizePerTask;              ///< Histogram of bytes per Task.
    util::Histogram::Ptr _histRowsPerTask;              ///< Histogram of rows per Task.

    SchedTasksInfoMap _taskSchedMap; ///< &&& rename
};

}  // namespace lsst::qserv::wpublish

#endif  // LSST_QSERV_WPUBLISH_QUERYSTATISTICS_H
