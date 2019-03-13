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
#ifndef LSST_QSERV_DELETEWORKERTASK_H
#define LSST_QSERV_DELETEWORKERTASK_H

// System headers
#include <functional>
#include <set>

// Qserv headers
#include "replica/DeleteWorkerJob.h"
#include "replica/Task.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class DeleteWorkerTask represents a task which evicts a single worker
 * from a cluster. Note, that depending on the amount of data in catalogs served
 * by the cluster, a desired replication level, and existing replica disposition,
 * removal of a worker could be a lengthy process.
 */
class DeleteWorkerTask : public Task {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteWorkerTask> Ptr;

    // Default construction and copy semantics are prohibited

    DeleteWorkerTask() = delete;
    DeleteWorkerTask(DeleteWorkerTask const&) = delete;
    DeleteWorkerTask& operator=(DeleteWorkerTask const&) = delete;

    ~DeleteWorkerTask() override = default;

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
     * @param worker
     *   the name of a worker to be evicted.
     *
     * @param permanentDelete
     *   the flag of set to 'true' will result in complete removal of the evicted
     *   worker from the Replication system's Configuration, thus preventing it from
     *   being re-enabled w/o reconfiguring the cluster.
     *
     * @return
     *   the smart pointer to a new object
     */
    static Ptr create(Controller::Ptr const& controller,
                      Task::AbnormalTerminationCallbackType const& onTerminated,
                      std::string const& worker,
                      bool permanentDelete);

protected:

    /**
     * @see Task::onStart()
     */
    void onStart() override;

private:

    /**
     * The constructor is available to the class's factory method
     *
     * @see DeleteWorkerTask::create()
     */
    DeleteWorkerTask(Controller::Ptr const& controller,
                     Task::AbnormalTerminationCallbackType const& onTerminated,
                     std::string const& worker,
                     bool permanentDelete);

    /**
     * Log a persistent event on the started job
     *
     * @param job
     *   pointer to the worker eviction job
     */
    void _logStartedEvent(DeleteWorkerJob::Ptr const& job) const;

    /**
     * Log a persistent event on the finished job
     *
     * @param job
     *   pointer to the worker eviction job
     */
    void _logFinishedEvent(DeleteWorkerJob::Ptr const& job) const;

private:

    /// The name of a worker to be evicted
    std::string const _worker;

    /// The flag of set to 'true' will result in complete removal of
    /// the evicted worker from the Replication system's Configuration.
    bool const _permanentDelete;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_DELETEWORKERTASK_H
