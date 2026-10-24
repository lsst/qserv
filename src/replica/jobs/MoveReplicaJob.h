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
#ifndef LSST_QSERV_REPLICA_MOVEREPLICAJOB_H
#define LSST_QSERV_REPLICA_MOVEREPLICAJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class CreateReplicaJob;
class DeleteReplicaJob;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Structure MoveReplicaJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct MoveReplicaJobResult {
    /// Results reported by workers upon the successful completion
    /// of the new replica creation requests
    std::list<ReplicaInfo> createdReplicas;

    /// New replica creation results grouped by: chunk number, database, destination worker
    ChunkDatabaseWorkerReplicaInfo createdChunks;

    /// Results reported by workers upon the successful completion
    /// of the replica deletion requests
    std::list<ReplicaInfo> deletedReplicas;

    /// Replica deletion results grouped by: chunk number, database, source worker
    ChunkDatabaseWorkerReplicaInfo deletedChunks;
};

/**
 * Class MoveReplicaJob represents a tool which will move a chunk replica
 * from a source worker to some other (destination) worker. The input replica
 * may be deleted if requested.
 */
class MoveReplicaJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<MoveReplicaJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<MoveReplicaJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan and memory management
     * of instances created otherwise (as values or via low-level pointers).
     *
     * @param databaseFamily The name of a database family involved into the operation
     * @param chunk The chunk number of replicas to be moved between workers
     * @param sourceWorker The name of a source worker where the input replica is residing
     * @param destinationWorker The name of a destination worker where the output replica will be placed
     * @param purge The flag indicating if the input replica should be purged
     * @param controller For launching requests
     * @param parentJobId An identifier of a parent job
     * @param onFinish A callback function to be called upon a completion of the job
     * @param priority The priority level of the job
     * @return A pointer to the created object
     */
    static std::shared_ptr<MoveReplicaJob> create(std::string const& databaseFamily, unsigned int chunk, std::string const& sourceWorker,
                      std::string const& destinationWorker, bool purge, std::shared_ptr<Controller> const& controller,
                      std::string const& parentJobId, CallbackType const& onFinish, int priority);

    // Default construction and copy semantics are prohibited

    MoveReplicaJob() = delete;
    MoveReplicaJob(MoveReplicaJob const&) = delete;
    MoveReplicaJob& operator=(MoveReplicaJob const&) = delete;

    virtual ~MoveReplicaJob() final = default;

    std::string const& databaseFamily() const { return _databaseFamily; }
    unsigned int chunk() const { return _chunk; }
    std::string const& sourceWorker() const { return _sourceWorker; }
    std::string const& destinationWorker() const { return _destinationWorker; }
    bool purge() const { return _purge; }

    /**
     * Get a result of the job
     *
     * @note The method should be invoked only after the job has finished (primary status is set to
     *  Job::Status::FINISHED). Otherwise exception std::logic_error will be thrown. Verify the primary
     *  and extended status of the object to ensure that all requests have finished.
     * @throws std::logic_error if the job didn't finished at a time when the method was called
     * @return The result of the operation
     */
    MoveReplicaJobResult const& getReplicaData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    MoveReplicaJob(std::string const& databaseFamily, unsigned int chunk, std::string const& sourceWorker,
                   std::string const& destinationWorker, bool purge, std::shared_ptr<Controller> const& controller,
                   std::string const& parentJobId, CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of the replica creation job.
     */
    void _onCreateJobFinish();

    /**
     * The callback function to be invoked on a completion of the replica deletion job.
     */
    void _onDeleteJobFinish();

    // Input parameters

    std::string const _databaseFamily;
    unsigned int const _chunk;
    std::string const _sourceWorker;
    std::string const _destinationWorker;
    bool const _purge;
    CallbackType _onFinish;  ///< @note is reset when the job finishes

    /// The chained job implementing the first stage of the move
    std::shared_ptr<CreateReplicaJob> _createReplicaJob;

    /// The chained job implementing the second stage of the move
    std::shared_ptr<DeleteReplicaJob> _deleteReplicaJob;

    /// The result of the operation (gets updated as requests are finishing)
    MoveReplicaJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_MOVEREPLICAJOB_H
