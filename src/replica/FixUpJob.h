/*
 * LSST Data Management System
= *
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
#ifndef LSST_QSERV_REPLICA_FIXUPJOB_H
#define LSST_QSERV_REPLICA_FIXUPJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <queue>
#include <string>

// Qserv headers
#include "replica/Job.h"
#include "replica/FindAllJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ReplicationRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure FixUpJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct FixUpJobResult {
    /// Results reported by workers upon the successful completion
    /// of the corresponding requests
    std::list<ReplicaInfo> replicas;

    /// Results grouped by: chunk number, database, worker
    ChunkDatabaseWorkerReplicaInfo chunks;

    /// Per-worker counters indicating the number of failed requests
    std::map<std::string, size_t> workers;
};

/**
 * Class FixUpJob represents a tool which will fix chunk collocation within
 * a specified database family. Note that the current implementation of
 * the class won't take into consideration the minimum replication level
 * (if any) configured for the family. Also note that as a results of
 * the ('fixup') operation chunks may get a higher number of replicas
 * then others (not affected by the operation).
 */
class FixUpJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<FixUpJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily The name of a database family
     * @param controller This is needed for launching requests
     * @param parentJobId An identifier of the parent job
     * @param onFinish The callback function to be called upon a completion of the job
     * @param priority Priority level of the job
     * @return pointer to the created object
     */
    static Ptr create(std::string const& databaseFamily, Controller::Ptr const& controller,
                      std::string const& parentJobId, CallbackType const& onFinish, int priority);

    FixUpJob() = delete;
    FixUpJob(FixUpJob const&) = delete;
    FixUpJob& operator=(FixUpJob const&) = delete;

    ~FixUpJob() final = default;

    /// @return the name of a database family defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

    /**
     * Return the result of the operation.
     *
     * @note The method should be invoked only after the job has finished
     *   (primary status is set to Job::Status::FINISHED). Otherwise exception
     *   std::logic_error will be thrown
     * @note The result will be extracted from requests which have successfully
     *   finished. Please, verify the primary and extended status of the object
     *   to ensure that all requests have finished.
     * @return the data structure to be filled upon the completion of the job.
     * @throws std::logic_error if the job didn't finished at a time when the method
     *   was called
     */
    FixUpJobResult const& getReplicaData() const;

    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    void startImpl(replica::Lock const& lock) final;
    void cancelImpl(replica::Lock const& lock) final;
    void notify(replica::Lock const& lock) final;

private:
    FixUpJob(std::string const& databaseFamily, Controller::Ptr const& controller,
             std::string const& parentJobId, CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of the precursor job
     * which harvests chunk disposition across relevant worker nodes.
     */
    void _onPrecursorJobFinish();

    /**
     * The callback function to be invoked on a completion of each replica
     * creation request launched by the job.
     */
    void _onRequestFinish(ReplicationRequest::Ptr const& request);

    // Input parameters

    std::string const _databaseFamily;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// The chained job to be completed first in order to figure out
    /// replica disposition.
    FindAllJob::Ptr _findAllJob;

    /**
     * Analyze the work queue for the specified worker and launch up to
     * the specified number of the replication requests for the worker. The method
     * will eliminate input tasks from the work queue as it goes.
     *
     * @param lock The lock to be be held for the thread safety of the operation
     * @param destinationWorker The name of a replica receiving worker
     * @param maxRequests The maximum number of requests to be launched
     * @return The number of requests launched or 0 if no tasks existed for the worker.
     */
    size_t _launchNext(replica::Lock const& lock, std::string const& destinationWorker, size_t maxRequests);

    /// Structure ReplicationTask encapsulates a task to be schedule for executing
    /// as a replication request.
    struct ReplicationTask {
        std::string destinationWorker;
        std::string sourceWorker;
        std::string database;
        unsigned int chunk;
    };

    /// A collection of tasks to be executed for each destination worker.
    /// The collection is populated once, based on results reported by
    /// the "precursor" job. The tasks are pulled from the collection and turned
    /// into requests by method FixUpJob::_launchNext().
    std::map<std::string, std::queue<ReplicationTask>> _destinationWorker2tasks;

    /// A collection of launched requests implementing the operation
    std::list<ReplicationRequest::Ptr> _requests;

    size_t _numFinished = 0;  ///< the total number of finished requests
    size_t _numSuccess = 0;   ///< the number of successfully completed requests

    /// The result of the operation (gets updated as requests are finishing)
    FixUpJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_FIXUPJOB_H
