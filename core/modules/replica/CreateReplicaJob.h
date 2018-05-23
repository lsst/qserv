/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

/// CreateReplicaJob.h declares:
///
/// struct CreateReplicaJobResult
/// class  CreateReplicaJob
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
#include "replica/ReplicationRequest.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The struct CreateReplicaJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct CreateReplicaJobResult {

    /// Results reported by workers upon the successfull completion
    /// of the new replica creation requests
    std::list<ReplicaInfo> replicas;

    /// New replica creation results groupped by: chunk number, database, worker
    std::map<unsigned int,                  // chunk
             std::map<std::string,          // database
                      std::map<std::string, // destination worker
                               ReplicaInfo>>> chunks;
};

/**
  * Class CreateReplicaJob represents a tool which will copy a chunk replica
  * from a source worker to some other (destination) worker.
  */
class CreateReplicaJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<CreateReplicaJob> Ptr;

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
     * @param controller        - for launching requests
     * @param parentJobId       - optional identifier of a parent job
     * @param onFinish          - a callback function to be called upon a completion of the job
     * @param options           - (optional) job options
     */
    static Ptr create(std::string const& databaseFamily,
                      unsigned int chunk,
                      std::string const& sourceWorker,
                      std::string const& destinationWorker,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType onFinish,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    CreateReplicaJob() = delete;
    CreateReplicaJob(CreateReplicaJob const&) = delete;
    CreateReplicaJob& operator=(CreateReplicaJob const&) = delete;

    ~CreateReplicaJob() final = default;

    /// @return the name of a database family
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// @return the chunk number
    unsigned int chunk() const { return _chunk; }

    /// @return the name of a source worker where the input replica is residing
    std::string const& sourceWorker() const { return _sourceWorker; }

    /// @return the name of a destination worker where the output replica will be placed
    std::string const& destinationWorker() const { return _destinationWorker; }

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
    CreateReplicaJobResult const& getReplicaData() const;

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
     * @see CreateReplicaJob::create()
     */
    CreateReplicaJob(std::string const& databaseFamily,
                     unsigned int chunk,
                     std::string const& sourceWorker,
                     std::string const& destinationWorker,
                     Controller::Ptr const& controller,
                     std::string const& parentJobId,
                     CallbackType onFinish,
                     Job::Options const& options);

    /**
      * @see Job::startImpl()
      */
    void startImpl(util::Lock const& lock) final;

    /**
      * @see Job::startImpl()
      */
    void cancelImpl(util::Lock const& lock) final;

    /**
      * @see Job::notifyImpl()
      */
    void notifyImpl() final;

    /**
     * The calback function to be invoked on a completion of each replica
     * creation request.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish(ReplicationRequest::Ptr const& request);

protected:

    /// The name of a database family
    std::string _databaseFamily;

    /// The chunk number
    unsigned int _chunk;

    /// The name of a source worker where the input replica is residing
    std::string _sourceWorker;

    /// The name of a destination worker where the output replica will be placed
    std::string _destinationWorker;

    /// Client-defined function to be called upon the completion of the job
    CallbackType _onFinish;

    /// A collection of the replication requests implementing the operation
    std::vector<ReplicationRequest::Ptr> _requests;

    /// The result of the operation (gets updated as requests are finishing)
    CreateReplicaJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CREATEREPLICAJOB_H
