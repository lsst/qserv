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
#ifndef LSST_QSERV_REPLICA_REBALANCEJOB_H
#define LSST_QSERV_REPLICA_REBALANCEJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/Job.h"
#include "replica/FindAllJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/MoveReplicaJob.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure RebalanceJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct RebalanceJobResult {
    /// Results reported by workers upon the successful completion
    /// of the new replica creation requests
    std::list<ReplicaInfo> createdReplicas;

    /// New replica creation results grouped by:
    ///
    ///   <chunk>, <database>, <destination worker>
    ///
    ChunkDatabaseWorkerReplicaInfo createdChunks;

    /// Results reported by workers upon the successful completion
    /// of the replica deletion requests
    std::list<ReplicaInfo> deletedReplicas;

    /// Replica deletion results grouped by:
    ///
    ///   <chunk>, <database>, <source worker>
    ///
    ChunkDatabaseWorkerReplicaInfo deletedChunks;

    /// Per-worker flags indicating if the corresponding replica retrieval
    /// request succeeded.
    std::map<std::string, bool> workers;

    /// Replication plan
    ///
    /// ATTENTION: if the job is run in the 'estimateOnly' mode the plan and
    /// relevant variables defined after the plan are captured at the first (and only)
    /// iteration of the job. For the real re-balance regime these contain parameters
    /// of the last planning only.
    std::map<unsigned int,          // chunk
             std::map<std::string,  // source worker
                      std::string>>
            plan;  // destination worker

    // Parameters of the planner

    size_t totalWorkers = 0;     /// not counting workers which failed to report chunks
    size_t totalGoodChunks = 0;  /// good chunks reported by the precursor job
    size_t avgChunks = 0;        /// per worker average
};

/**
 * Class RebalanceJob represents a tool which will re-balance replica disposition
 * across worker nodes in order to achieve close-to-equal distribution of chunks
 * across workers.
 *
 * These are basic requirements to the algorithm:
 *
 * - key metrics for the algorithm are:
 *     + a database family to be rebalanced
 *     + total number of replicas within a database family
 *     + the total number and names of workers which are available (up and running)
 *     + the average number of replicas per worker node
 *
 * - re-balance each database family independently of each other because
 *   this should still yeld an equal distribution of chunks across any database
 *
 * - a subject of each move is (chunk,all databases of the family) residing
 *   on a node
 *
 * - the operation deals with 'good' (meaning 'colocated' and 'complete')
 *   chunk replicas only
 *
 * - the operation won't affect the number of replicas, it will only
 *   move replicas between workers
 *
 * - when re-balancing is over then investigate two options: finish it and launch
 *   it again externally using some sort of a scheduler, or have an internal ASYNC
 *   timer (based on Boost ASIO).
 *
 * - in the pilot implementation replica disposition should be requested directly
 *   from the worker nodes using precursor FindAllJob. More advanced implementation
 *   may switch to pulling this information from a database. That would work better
 *   at a presence of other activities keeping the database content updated.
 *
 * - [TO BE CONFIRMED] at a each iteration a limited number (from the Configuration?)
 *   of replicas will be processed. Then chunk disposition will be recomputed to adjust
 *   for other parallel activities (replication, purge, etc.).
 */
class RebalanceJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<RebalanceJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily
     *   the name of a database family
     *
     * @param estimateOnly
     *   do not perform any changes to chunk disposition. Just produce
     *   an estimate report.
     *
     * @param controller
     *   for launching requests
     *
     * @param parentJobId
     *   an identifier of a parent job
     *
     * @param onFinish
     *   a callback function to be called upon job completion
     *
     * @param priority
     *   the priority level of the job
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(std::string const& databaseFamily, bool estimateOnly, Controller::Ptr const& controller,
                      std::string const& parentJobId, CallbackType const& onFinish, int priority);

    // Default construction and copy semantics are prohibited

    RebalanceJob() = delete;
    RebalanceJob(RebalanceJob const&) = delete;
    RebalanceJob& operator=(RebalanceJob const&) = delete;

    ~RebalanceJob() final = default;

    /// @return the name of a database defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// @return the estimate mode option
    bool estimateOnly() const { return _estimateOnly; }

    /**
     * Return the result of the operation.
     *
     * @note
     *   The method should be invoked only after the job has finished (primary
     *   status is set to Job::Status::FINISHED). Otherwise exception
     *   std::logic_error will be thrown
     *
     * @note
     *   The result will be extracted from requests which have successfully
     *   finished. Please, verify the primary and extended status of the object
     *   to ensure that all requests have finished.
     *
     * @return
     *   the data structure to be filled upon the completion of the job.
     *
     * @throw std::logic_error
     *   if the job didn't finished at a time when the method was called
     */
    RebalanceJobResult const& getReplicaData() const;

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
    /// @see RebalanceJob::create()
    RebalanceJob(std::string const& databaseFamily, bool estimateOnly, Controller::Ptr const& controller,
                 std::string const& parentJobId, CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of the precursor job
     * which harvests chunk disposition across relevant worker nodes.
     */
    void _onPrecursorJobFinish();

    /**
     * The callback function to be invoked on a completion of each replica
     * creation request.
     *
     * @param request
     *   a pointer to a request
     */
    void _onJobFinish(MoveReplicaJob::Ptr const& job);

    /**
     * Submit a batch of the replica movement job
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
     *   the actual number of submitted jobs
     */
    size_t _launchNextJobs(replica::Lock const& lock, size_t numJobs);

    // Input parameters

    std::string const _databaseFamily;
    bool const _estimateOnly;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// The chained job to be completed first in order to figure out
    /// replica disposition.
    FindAllJob::Ptr _findAllJob;

    /// Replica creation jobs which are ready to be launched
    std::list<MoveReplicaJob::Ptr> _jobs;

    /// Jobs which are already active
    std::list<MoveReplicaJob::Ptr> _activeJobs;

    size_t _numLaunched = 0;  ///< the total number of replica creation jobs launched
    size_t _numFinished = 0;  ///< the total number of finished jobs
    size_t _numSuccess = 0;   ///< the number of successfully completed jobs

    /// The result of the operation (gets updated as requests are finishing)
    RebalanceJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REBALANCEJOB_H
