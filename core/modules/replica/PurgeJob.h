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
#ifndef LSST_QSERV_REPLICA_PURGEJOB_H
#define LSST_QSERV_REPLICA_PURGEJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <queue>
#include <string>

// Qserv headers
#include "replica/DeleteReplicaJob.h"
#include "replica/FindAllJob.h"
#include "replica/ReplicaInfo.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * The structure PurgeJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct PurgeJobResult {

    /// Results reported by workers upon the successful completion
    /// of the corresponding jobs
    std::list<ReplicaInfo> replicas;

    /// Results grouped by: chunk number, database, worker
    std::map<unsigned int,                  // chunk
             std::map<std::string,          // database
                      std::map<std::string, // worker
                               ReplicaInfo>>> chunks;

    /// Per-worker counters indicating the number of failed jobs
    std::map<std::string, size_t> workers;
};

/**
  * Class PurgeJob represents a tool which will increase the minimum
  * number of each chunk's replica up to the requested level.
  */
class PurgeJob : public Job  {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<PurgeJob> Ptr;

    /// The function type for notifications on the completion of the job
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a job
    static Job::Options const& defaultOptions();

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily the name of a database family
     * @param numReplicas
     *   the optional (if not 0) override for the maximum number of replicas
     *   for each chunk. If the parameter is set to 0 then the corresponding
     *   configuration option for the database family will be assumed.
     * @param controller for launching jobs
     * @param parentJobId optional identifier of a parent job
     * @param onFinish optional callback function to be called upon a completion
     *   of the job
     * @param options (optional) job options
     * @return pointer to the created object
     */
    static Ptr create(std::string const& databaseFamily,
                      unsigned int numReplicas,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId=std::string(),
                      CallbackType const& onFinish=nullptr,
                      Job::Options const& options=defaultOptions());

    PurgeJob() = delete;
    PurgeJob(PurgeJob const&) = delete;
    PurgeJob& operator=(PurgeJob const&) = delete;

    ~PurgeJob() final = default;

    /**
     * @return maximum number of each chunk's good replicas to be reached when
     *   the job successfully finishes.
     */
    unsigned int numReplicas() const { return _numReplicas; }

    /// @return name of a database defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

    /**
     * Return the result of the operation.
     *
     * @note
     *   The method should be invoked only after the job has finished (primary
     *   status is set to Job::Status::FINISHED). Otherwise exception
     *   std::logic_error will be thrown
     * @note
     *   The result will be extracted from jobs which have successfully
     *   finished. Please, verify the primary and extended status of the object
     *   to ensure that all jobs have finished.
     * @return the data structure to be filled upon the completion of the job.
     * @throws std::logic_error if the job didn't finished at a time when
     *   the method was called
     */
    PurgeJobResult const& getReplicaData() const;

    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:
    void startImpl(util::Lock const& lock) final;
    void cancelImpl(util::Lock const& lock) final;
    void notify(util::Lock const& lock) final;

private:
    PurgeJob(std::string const& databaseFamily,
             unsigned int numReplicas,
             Controller::Ptr const& controller,
             std::string const& parentJobId,
             CallbackType const& onFinish,
             Job::Options const& options);

    /**
     * The callback function to be invoked on a completion of the precursor job
     * which harvests chunk disposition across relevant worker nodes.
     */
    void _onPrecursorJobFinish();

    /**
     * The callback function to be invoked on a completion of each job
     * @param job pointer to a job
     */
    void _onDeleteJobFinish(DeleteReplicaJob::Ptr const& job);

    // Input parameters

    std::string  const _databaseFamily;
    unsigned int const _numReplicas;
    CallbackType       _onFinish;       /// @note is reset when the job finishes

    /// The chained job to be completed first in order to figure out
    /// replica disposition.
    FindAllJob::Ptr _findAllJob;

    /**
     * Analyze the work queue for the specified worker and launch up to
     * the specified number of the replica removal jobs for the worker. The method
     * will eliminate input tasks from the work queue as it goes.
     * 
     * @param lock The lock to be be held for the thread safety of the operation
     * @param targetWorker The name of a worker where the affected replicas are residing
     * @param maxJobs The maximum number of jobs to be launched
     * @return The number of jobs launched or 0 if no tasks existed for the worker.
     */
    size_t _launchNext(util::Lock const& lock,
                       std::string const& targetWorker,
                       size_t maxJobs);

    /// Structure ReplicaPurgeTask encapsulates a task to be schedule for executing
    /// as a replica removal job.
    struct ReplicaPurgeTask {
        unsigned int chunk;
        std::string targetWorker;
    };
    
    /// A collection of tasks to be executed for each target worker.
    /// The collection is populated once, based on results reported by
    /// the "precursor" job. The tasks are pulled from the collection and turned
    /// into jobs by method PurgeJob::_launchNext().
    std::map<std::string, std::queue<ReplicaPurgeTask>> _targetWorker2tasks;

    /// A collection of jobs implementing the operation
    std::list<DeleteReplicaJob::Ptr> _jobs;

    size_t _numFinished = 0;    ///< the total number of finished jobs
    size_t _numSuccess  = 0;    ///< the number of successfully completed jobs

    /// The result of the operation (gets updated as jobs are finishing)
    PurgeJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_PURGEJOB_H
