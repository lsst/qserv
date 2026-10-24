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
#include <cstddef>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class CreateReplicaJob;
class FindAllJob;
} // namespace lsst::qserv::replica

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
    typedef std::function<void(std::shared_ptr<ReplicateJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan and memory management
     * of instances created otherwise (as values or via low-level pointers).
     *
     * @param databaseFamily The name of a database family.
     * @param numReplicas The optional (if not 0) override for the minimum number of replicas
     *  for each chunk. If the parameter is set to 0 then the corresponding
     *  configuration option for the database family will be assumed.
     * @param controller for launching jobs
     * @param parentJobId An identifier of a parent job
     * @param onFinish A callback function to be called upon job completion
     * @param priority The priority level of the job
     * @return A pointer to the created object
     */
    static std::shared_ptr<ReplicateJob> create(std::string const& databaseFamily, unsigned int numReplicas,
                      std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    ReplicateJob() = delete;
    ReplicateJob(ReplicateJob const&) = delete;
    ReplicateJob& operator=(ReplicateJob const&) = delete;

    virtual ~ReplicateJob() final = default;

    unsigned int numReplicas() const { return _numReplicas; }
    std::string const& databaseFamily() const { return _databaseFamily; }

    /**
     * Return the result of the operation.
     *
     * @note The method should be invoked only after the job has finished (primary status is set
     *  to Job::Status::FINISHED). Otherwise exception of type std::logic_error will be thrown.
     *  Verify the primary and extended status of the object to ensure that all jobs have finished.
     * @return The data structure to be filled upon the completion of the job.
     * @throw std::logic_error if the job didn't finished at a time when the method was called
     */
    ReplicateJobResult const& getReplicaData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    ReplicateJob(std::string const& databaseFamily, unsigned int numReplicas,
                 std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
                 CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of the precursor job
     * which harvests chunk disposition across relevant worker nodes.
     */
    void _onPrecursorJobFinish();

    /**
     * The callback function to be invoked on a completion of each replication job
     * @param job A pointer to a job
     */
    void _onCreateJobFinish(std::shared_ptr<CreateReplicaJob> const& job);

    /**
     * Submit a batch of the replica creation job
     *
     * This method implements a load balancing algorithm which tries to
     * prevent excessive use of resources by controllers and to avoid
     * "hot spots" or under-utilization at workers.
     *
     * @param lock A lock on Job::_mtx must be acquired before calling this method
     * @param numJobs The desired number of jobs to submit
     * @return The actual number of submitted jobs
     */
    std::size_t _launchNextJobs(replica::Lock const& lock, std::size_t numJobs);

protected:
    // Input parameters

    std::string const _databaseFamily;
    unsigned int const _numReplicas;
    CallbackType _onFinish;  ///< @note is reset when the job finishes

    /// The chained job to be completed first in order to figure out replica disposition.
    std::shared_ptr<FindAllJob> _findAllJob;

    /// Replica creation jobs which are ready to be launched
    std::list<std::shared_ptr<CreateReplicaJob>> _jobs;

    /// Jobs which are already active
    std::list<std::shared_ptr<CreateReplicaJob>> _activeJobs;

    std::size_t _numLaunched = 0;  ///< the total number of replica creation jobs launched
    std::size_t _numFinished = 0;  ///< the total number of finished jobs
    std::size_t _numSuccess = 0;   ///< the number of successfully completed jobs

    /// The result of the operation (gets updated as jobs are finishing)
    ReplicateJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REPLICATEJOB_H
