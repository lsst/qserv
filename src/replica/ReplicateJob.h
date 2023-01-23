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
#ifndef LSST_QSERV_REPLICA_REPLICATEJOB_H
#define LSST_QSERV_REPLICA_REPLICATEJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/CreateReplicaJob.h"
#include "replica/Job.h"
#include "replica/FindAllJob.h"
#include "replica/ReplicaInfo.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure ReplicateJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct ReplicateJobResult {
    /// Results reported by workers upon the successful completion
    /// of the corresponding replica creation jobs
    std::list<ReplicaInfo> replicas;

    /// New replicas created by the operation
    ChunkDatabaseWorkerReplicaInfo chunks;

    /// Per-worker flags indicating if the corresponding replica creation
    /// job succeeded.
    std::map<std::string, bool> workers;
};

/**
 * Class ReplicateJob represents a tool which will increase the minimum
 * number of each chunk's replica up to the requested level.
 */
class ReplicateJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<ReplicateJob> Ptr;

    /// The function type for notifications on the completion of the job
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily
     *   name of a database family
     *
     * @param numReplicas
     *   optional (if not 0) override for the minimum number of replicas
     *   for each chunk. If the parameter is set to 0 then the corresponding
     *   configuration option for the database family will be assumed.
     *
     * @param controller
     *   for launching jobs
     *
     * @param parentJobId
     *   an identifier of a parent job
     *`
     * @param onFinish
     *   a callback function to be called upon job completion
     *
     * @param priority
     *   the priority level of the job
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(std::string const& databaseFamily, unsigned int numReplicas,
                      Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    // Default construction and copy semantics are prohibited

    ReplicateJob() = delete;
    ReplicateJob(ReplicateJob const&) = delete;
    ReplicateJob& operator=(ReplicateJob const&) = delete;

    ~ReplicateJob() final = default;

    /**
     * @return
     *   the minimum number of each chunk's replicas to be reached when
     *   the job successfully finishes.
     */
    unsigned int numReplicas() const { return _numReplicas; }

    /// @return the name of a database family defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

    /**
     * Return the result of the operation.
     *
     * @note
     *   The method should be invoked only after the job has finished (primary
     *   status is set to Job::Status::FINISHED). Otherwise exception
     *   std::logic_error will be thrown
     *
     * @note
     *   The result will be extracted from the replica creation which have successfully
     *   finished. Please, verify the primary and extended status of the object
     *   to ensure that all jobs have finished.
     *
     * @return
     *   the data structure to be filled upon the completion of the job.
     *
     * @throw std::logic_error
     *   if the job didn't finished at a time when the method was called
     */
    ReplicateJobResult const& getReplicaData() const;

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    /// @see Job::startImpl()
    void startImpl(replica::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(replica::Lock const& lock) final;

    /// @see Job::notify()
    void notify(replica::Lock const& lock) final;

private:
    /// @see ReplicateJob::create()
    ReplicateJob(std::string const& databaseFamily, unsigned int numReplicas,
                 Controller::Ptr const& controller, std::string const& parentJobId,
                 CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of the precursor job
     * which harvests chunk disposition across relevant worker nodes.
     */
    void _onPrecursorJobFinish();

    /**
     * The callback function to be invoked on a completion of each replication job
     *
     * @param job
     *   pointer to a job
     */
    void _onCreateJobFinish(CreateReplicaJob::Ptr const& job);

    /**
     * Submit a batch of the replica creation job
     *
     * This method implements a load balancing algorithm which tries to
     * prevent excessive use of resources by controllers and to avoid
     * "hot spots" or under-utilization at workers.
     *
     * @param lock
     *   a lock on Job::_mtx must be acquired before calling this method
     *
     * @param numJobs
     *   desired number of jobs to submit
     *
     * @retun
     *   actual number of submitted jobs
     */
    size_t _launchNextJobs(replica::Lock const& lock, size_t numJobs);

protected:
    // Input parameters

    std::string const _databaseFamily;
    unsigned int const _numReplicas;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// The chained job to be completed first in order to figure out
    /// replica disposition.
    FindAllJob::Ptr _findAllJob;

    /// Replica creation jobs which are ready to be launched
    std::list<CreateReplicaJob::Ptr> _jobs;

    /// Jobs which are already active
    std::list<CreateReplicaJob::Ptr> _activeJobs;

    size_t _numLaunched = 0;  ///< the total number of replica creation jobs launched
    size_t _numFinished = 0;  ///< the total number of finished jobs
    size_t _numSuccess = 0;   ///< the number of successfully completed jobs

    /// The result of the operation (gets updated as jobs are finishing)
    ReplicateJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REPLICATEJOB_H
