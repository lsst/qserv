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
#ifndef LSST_QSERV_REPLICA_CREATEREPLICAJOB_H
#define LSST_QSERV_REPLICA_CREATEREPLICAJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>
#include <vector>

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/qserv/AddReplicaQservMgtRequest.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
class HttpCreateReplicaRequest;
#else
class ReplicationRequest;
#endif
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure CreateReplicaJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct CreateReplicaJobResult {
    /// Results reported by workers upon the successful completion
    /// of the new replica creation requests
    std::list<ReplicaInfo> replicas;

    /// New replica creation results grouped by: chunk number, database, worker
    std::map<unsigned int,                   // chunk
             std::map<std::string,           // database
                      std::map<std::string,  // destination worker
                               ReplicaInfo>>>
            chunks;
};

/**
 * Class CreateReplicaJob represents a tool which will copy a chunk replica
 * from a source worker to some other (destination) worker.
 */
class CreateReplicaJob : public Job {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<CreateReplicaJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Create the job.
     *
     * @param databaseFamily The name of a database family involved into the operation
     * @param chunk The chunk whose replica will be created at the destination worker
     * @param sourceWorker The name of a source worker where the input replica is residing
     * @param destinationWorker The name of a destination worker where the output replica will be placed
     * @param controller For launching requests
     * @param parentJobId Optional identifier of a parent job
     * @param onFinish A callback function to be called upon a completion of the job
     * @param priority The priority level of the job
     */
    static std::shared_ptr<CreateReplicaJob> create(std::string const& databaseFamily, unsigned int chunk,
                                                    std::string const& sourceWorker,
                                                    std::string const& destinationWorker,
                                                    std::shared_ptr<Controller> const& controller,
                                                    std::string const& parentJobId,
                                                    CallbackType const& onFinish, int priority);

    CreateReplicaJob() = delete;
    CreateReplicaJob(CreateReplicaJob const&) = delete;
    CreateReplicaJob& operator=(CreateReplicaJob const&) = delete;

    virtual ~CreateReplicaJob() final = default;

    std::string const& databaseFamily() const { return _databaseFamily; }
    unsigned int chunk() const { return _chunk; }
    std::string const& sourceWorker() const { return _sourceWorker; }
    std::string const& destinationWorker() const { return _destinationWorker; }

    /**
     * Return the result of the operation.
     *
     * @note The method should be invoked only after the job has finished (primary status is set
     *  to Job::Status::FINISHED). Otherwise exception std::logic_error will be thrown.
     * @note The result will be extracted from requests which have successfully finished. Please, verify
     *  the primary and extended status of the object to ensure that all requests have finished.
     * @return The data structure to be filled upon the completion of the job.
     * @throws std::logic_error if the job didn't finished at a time when the method was called
     */
    CreateReplicaJobResult const& getReplicaData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    CreateReplicaJob(std::string const& databaseFamily, unsigned int chunk, std::string const& sourceWorker,
                     std::string const& destinationWorker, std::shared_ptr<Controller> const& controller,
                     std::string const& parentJobId, CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of each replica creation request.
     * @param request a pointer to a request
     */
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
    void _onRequestFinish(std::shared_ptr<HttpCreateReplicaRequest> const& request);
#else
    void _onRequestFinish(std::shared_ptr<ReplicationRequest> const& request);
#endif

    /**
     * Notify Qserv about a new chunk added to its database.
     * @param lock  A lock on Job::_mtx must be acquired by a caller of the method.
     * @param chunk  The chunk whose replicas are added.
     * @param databases  The names of databases involved into the operation.
     * @param worker  The name of a worker to be notified.
     * @param onFinish  An (optional) callback function to be called upon completion
     *  of the operation.
     */
    void _qservAddReplica(replica::Lock const& lock, unsigned int chunk,
                          std::vector<std::string> const& databases, std::string const& worker,
                          AddReplicaQservMgtRequest::CallbackType const& onFinish = nullptr);

    // Input parameters

    std::string const _databaseFamily;
    unsigned int const _chunk;
    std::string const _sourceWorker;
    std::string const _destinationWorker;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// A collection of the replication requests implementing the operation
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
    std::vector<std::shared_ptr<HttpCreateReplicaRequest>> _requests;
#else
    std::vector<std::shared_ptr<ReplicationRequest>> _requests;
#endif

    std::size_t _numRequestsFinished = 0;  ///< gets incremented for each completed request
    std::size_t _numRequestsSuccess = 0;   ///< gets incremented for each successfully completed request

    /// The result of the operation (gets updated as requests are finishing)
    CreateReplicaJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CREATEREPLICAJOB_H
