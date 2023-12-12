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
#ifndef LSST_QSERV_REPLICA_DELETEREPLICAJOB_H
#define LSST_QSERV_REPLICA_DELETEREPLICAJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>
#include <vector>

// Qserv headers
#include "replica/RemoveReplicaQservMgtRequest.h"
#include "replica/Job.h"
#include "replica/ReplicaInfo.h"
#include "replica/DeleteRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure DeleteReplicaJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct DeleteReplicaJobResult {
    /**
     * Results reported by workers upon the successful completion
     * of the replica deletion requests
     */
    std::list<ReplicaInfo> replicas;

    /// Replica deletion results grouped by: chunk number, database name, worker name
    std::map<unsigned int, std::map<std::string, std::map<std::string, ReplicaInfo>>> chunks;
};

/**
 * Class DeleteReplicaJob represents a tool which will delete a chunk replica
 * from a worker.
 */
class DeleteReplicaJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteReplicaJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param databaseFamily The name of a database family involved into the operation.
     * @param chunk The chunk whose replica will be deleted from the target worker.
     * @param workerName The name of a worker where the affected replica is residing.
     * @param controller A service for launching requests.
     * @param parentJobId An optional identifier of a parent job.
     * @param onFiniss A callback function to be called upon a completion of the job.
     * @param priority The priority level of the job.
     * @return A pointer to the created object.
     */
    static Ptr create(std::string const& databaseFamily, unsigned int chunk, std::string const& workerName,
                      Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    // Default construction and copy semantics are prohibited

    DeleteReplicaJob() = delete;
    DeleteReplicaJob(DeleteReplicaJob const&) = delete;
    DeleteReplicaJob& operator=(DeleteReplicaJob const&) = delete;

    ~DeleteReplicaJob() final = default;

    /// @return the name of a database family
    std::string const& databaseFamily() const { return _databaseFamily; }

    ///@return the chunk number
    unsigned int chunk() const { return _chunk; }

    /// @return the name of a source worker where the affected replica is residing
    std::string const& workerName() const { return _workerName; }

    /**
     * Return the result of the operation.
     * @note The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown
     * @note The result will be extracted from requests which have successfully
     *  finished. Please, verify the primary and extended status of the object
     *  to ensure that all requests have finished.
     * @return the data structure to be filled upon the completion of the job.
     * @throws std::logic_error if the job didn't finished at a time when the method was called
     */
    DeleteReplicaJobResult const& getReplicaData() const;

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
    /// @see DeleteReplicaJob::create()
    DeleteReplicaJob(std::string const& databaseFamily, unsigned int chunk, std::string const& workerName,
                     Controller::Ptr const& controller, std::string const& parentJobId,
                     CallbackType const& onFinish, int priority);

    /**
     * Initiate a process of removing the replica from the source worker
     * @param lock a lock on Job::_mtx must be acquired before calling this method
     */
    void _beginDeleteReplica(replica::Lock const& lock);

    /**
     * The callback function to be invoked on a completion of each replica
     * deletion request.
     * @param request a pointer to a request
     */
    void _onRequestFinish(DeleteRequest::Ptr const& request);

    /**
     * Notify Qserv about a new chunk added to its database.
     * @param lock  A lock on Job::_mtx must be acquired by a caller of the method.
     * @param chunk  A chunk whose replicas are removed from the worker.
     * @param databases  The names of databases involved into the operation.
     * @param workerName  The name of a worker to be notified.
     * @param force  The flag indicating of the removal should be done regardless
     *   of the usage status of the replica.
     * @param onFinish  An (optional) callback function to be called upon completion
     *   of the operation.
     */
    void _qservRemoveReplica(replica::Lock const& lock, unsigned int chunk,
                             std::vector<std::string> const& databases, std::string const& workerName,
                             bool force,
                             RemoveReplicaQservMgtRequest::CallbackType const& onFinish = nullptr);

    // Input parameters

    std::string const _databaseFamily;
    unsigned int const _chunk;
    std::string const _workerName;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// Cached replicas for determining which databases have contributions in the chunk
    std::vector<ReplicaInfo> _replicas;

    /// A collection of the replica deletion requests implementing the operation
    std::vector<DeleteRequest::Ptr> _requests;

    size_t _numRequestsFinished = 0;  // gets incremented for each completed request
    size_t _numRequestsSuccess = 0;   // gets incremented for each successfully completed request

    DeleteReplicaJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_DELETEREPLICAJOB_H
