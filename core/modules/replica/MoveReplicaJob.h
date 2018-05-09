/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_MOVE_REPLICA_JOB_H
#define LSST_QSERV_REPLICA_MOVE_REPLICA_JOB_H

/// MoveReplicaJob.h declares:
///
/// struct MoveReplicaJobResult
/// class  MoveReplicaJob
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Job.h"
#include "replica/ReplicaInfo.h"
#include "replica/CreateReplicaJob.h"
#include "replica/DeleteReplicaJob.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The structure MoveReplicaJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct MoveReplicaJobResult {

    /// Results reported by workers upon the successfull completion
    /// of the new replica creation requests
    std::list<ReplicaInfo> createdReplicas;

    /// New replica creation results groupped by: chunk number, database, worker
    std::map<unsigned int,                  // chunk
             std::map<std::string,          // database
                      std::map<std::string, // destination worker
                               ReplicaInfo>>> createdChunks;

    /// Results reported by workers upon the successfull completion
    /// of the replica deletion requests
    std::list<ReplicaInfo> deletedReplicas;

    /// Replica deletion results groupped by: chunk number, database, worker
    std::map<unsigned int,                  // chunk
             std::map<std::string,          // database
                      std::map<std::string, // source worker
                               ReplicaInfo>>> deletedChunks;
};

/**
  * Class MoveReplicaJob represents a tool which will move a chunk replica
  * from a source worker to some other (destination) worker. The input replica
  * may be deleted if requested.
  */
class MoveReplicaJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MoveReplicaJob> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily    - the name of a database family involved into the operation
     * @param chunk             - the chunk number
     * @param sourceWorker      - the name of a source worker where the input replica is residing
     * @param destinationWorker - the name of a destination worker where the output replica will be placed
     * @param purge             - the flag indicating if the input replica should be purged
     * @param controller        - for launching requests
     * @param parentJobId       - optional identifier of a parent job
     * @param onFinish          - callback function to be called upon a completion of the job
     * @param options           - job options
     */
    static Ptr create(std::string const& databaseFamily,
                      unsigned int chunk,
                      std::string const& sourceWorker,
                      std::string const& destinationWorker,
                      bool purge,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType onFinish,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    MoveReplicaJob() = delete;
    MoveReplicaJob(MoveReplicaJob const&) = delete;
    MoveReplicaJob& operator=(MoveReplicaJob const&) = delete;

    /// Destructor
    ~MoveReplicaJob() override = default;

    /// The name of a database family
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// The chunk number
    unsigned int chunk() const { return _chunk; }

    /// Return the name of a source worker where the input replica is residing
    std::string const& sourceWorker() const { return _sourceWorker; }

    /// Return the name of a destination worker where the output replica will be placed
    std::string const& destinationWorker() const { return _destinationWorker; }

    /// Return the flag indicating if the input replica should be purged
    bool purge() const { return _purge; }

    /**
     * Return the result of the operation.
     *
     * IMPORTANT NOTES:
     * - the method should be invoked only after the job has finished (primary
     *   status is set to Job::Status::FINISHED). Otherwise exception
     *   std::logic_error will be thrown
     *
     * - the result will be extracted from requests which have successfully
     *   finished. Please, verify the primary and extended status of the object
     *   to ensure that all requests have finished.
     *
     * @return the data structure to be filled upon the completin of the job.
     *
     * @throws std::logic_error - if the job dodn't finished at a time
     *                            when the method was called
     */
    MoveReplicaJobResult const& getReplicaData() const;

    /**
     * Implement the corresponding method of the base class.
     *
     * @see Job::extendedPersistentState()
     */
    std::string extendedPersistentState(SqlGeneratorPtr const& gen) const override;

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @see MoveReplicaJob::create()
     */
    MoveReplicaJob(std::string const& databaseFamily,
                   unsigned int chunk,
                   std::string const& sourceWorker,
                   std::string const& destinationWorker,
                   bool purge,
                   Controller::Ptr const& controller,
                   std::string const& parentJobId,
                   CallbackType onFinish,
                   Job::Options const& options);

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::startImpl()
      */
    void startImpl() override;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::startImpl()
      */
    void cancelImpl() override;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::notify()
      */
    void notify() override;

    /**
     * The calback function to be invoked on a completion of the replica
     * creation job.
     */
    void onCreateJobFinish();

    /**
     * The calback function to be invoked on a completion of the replica
     * deletion job.
     */
     void onDeleteJobFinish();

protected:

    /// The name of a database family
    std::string _databaseFamily;

    /// The chunk number
    unsigned int _chunk;

    /// The name of a source worker where the input replica is residing
    std::string _sourceWorker;

    /// The name of a destination worker where the output replica will be placed
    std::string _destinationWorker;

    /// The flag indicating if the input replica should be purged
    bool _purge;

    /// Client-defined function to be called upon the completion of the job
    CallbackType _onFinish;

    /// The chained job implementing the first stage of the move
    CreateReplicaJob::Ptr _createReplicaJob;

    /// The chained job implementing the second stage of the move
    DeleteReplicaJob::Ptr _deleteReplicaJob;

    /// The result of the operation (gets updated as requests are finishing)
    MoveReplicaJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_MOVE_REPLICA_JOB_H
