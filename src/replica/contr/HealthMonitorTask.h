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
#ifndef LSST_QSERV_HEALTHMONITORTASK_H
#define LSST_QSERV_HEALTHMONITORTASK_H

// System headers
#include <atomic>
#include <cstddef>
#include <functional>
#include <set>

// Qserv headers
#include "replica/contr/Task.h"
#include "replica/util/Mutex.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class ClusterHealthJob;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HealthMonitorTask represents a task which monitors a status of
 * the Replication and Qserv worker services and report worker(s) eligible
 * for eviction if they're not responding within the specified timeout.
 */
class HealthMonitorTask : public Task {
public:
    /// Delays (seconds) in getting responses from the worker services (both Qserv and
    /// the Replication system)
    typedef std::map<std::string,           // worker
                     std::map<std::string,  // service ('qserv', 'replication')
                              unsigned int>>
            WorkerResponseDelay;

    /// The function type for notifications on the worker eviction events
    typedef std::function<void(std::string const&)> WorkerEvictCallbackType;

    HealthMonitorTask() = delete;
    HealthMonitorTask(HealthMonitorTask const&) = delete;
    HealthMonitorTask& operator=(HealthMonitorTask const&) = delete;

    virtual ~HealthMonitorTask() final = default;

    /**
     * Create a new task with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param controller
     *   a reference to the Controller for launching requests, jobs, etc.
     *
     * @param onTerminated
     *   callback function to be called upon abnormal termination
     *   of the task. Set it to 'nullptr' if no call back should be made.
     *
     * @param onWorkerEvictTimeout
     *   callback function to be called when one or more workers
     *   are continuously not responding during the specified period of
     *   time (configuration parameter ("controller", "worker-evict-timeout").
     *   An affected worker becomes eligible for eviction from the cluster if both
     *   the Replication and the Qserv worker services are both not responding.
     *
     * @return
     *   the smart pointer to a new object
     */
    static std::shared_ptr<HealthMonitorTask> create(
            std::shared_ptr<Controller> const& controller,
            Task::AbnormalTerminationCallbackType const& onTerminated,
            WorkerEvictCallbackType const& onWorkerEvictTimeout);

    /// @return delays (seconds) in getting responses from the worker services
    WorkerResponseDelay workerResponseDelay() const;

protected:
    virtual void onStart() final;
    virtual bool onRun() final;

private:
    HealthMonitorTask(std::shared_ptr<Controller> const& controller,
                      Task::AbnormalTerminationCallbackType const& onTerminated,
                      WorkerEvictCallbackType const& onWorkerEvictTimeout);

    /**
     * Log a persistent event on the started job
     * @param job A pointer to the job
     */
    void _logStartedEvent(std::shared_ptr<ClusterHealthJob> const& job) const;

    /**
     * Log a persistent event on the finished job
     * @param job A pointer to the job
     */
    void _logFinishedEvent(std::shared_ptr<ClusterHealthJob> const& job) const;

    // Input parameters

    WorkerEvictCallbackType const _onWorkerEvictTimeout;

    /// The thread-safe counter of the finished jobs
    std::atomic<std::size_t> _numFinishedJobs;

    /// Mutex guarding internal state. This object is made protected
    /// to allow subclasses use it.
    mutable replica::Mutex _mtx;

    /// Accumulate here non-response intervals for each workers until either will
    /// reach the "eviction" threshold. Then trigger worker eviction sequence.
    WorkerResponseDelay _workerServiceNoResponseSec;

    /// Last time the workers response delays were updated
    std::uint64_t _prevUpdateTimeMs;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HEALTHMONITORTASK_H
