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
#ifndef LSST_QSERV_REPLICA_DELETE_REPLICA_JOB_H
#define LSST_QSERV_REPLICA_DELETE_REPLICA_JOB_H

/// DeleteReplicaJob.h declares:
///
/// struct DeleteReplicaJobResult
/// class  DeleteReplicaJob
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
#include "replica/DeleteRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The structure DeleteReplicaJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct DeleteReplicaJobResult {

    /// Results reported by workers upon the successfull completion
    /// of the replica deletion requests
    std::list<ReplicaInfo> replicas;

    /// Replica deletion results groupped by: chunk number, database, worker
    std::map<unsigned int,                  // chunk
             std::map<std::string,          // database
                      std::map<std::string, // source worker
                               ReplicaInfo>>> chunks;
};

/**
  * Class DeleteReplicaJob represents a tool which will delete a chunk replica
  * from a worker.
  */
class DeleteReplicaJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteReplicaJob> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily    - the name of a database family involved into the operation
     * @param chunk             - the chunk number
     * @param worker            - the name of a worker where the affected replica is residing
     * @param controller        - for launching requests
     * @param parentJobId       - optional identifier of a parent job
     * @param onFinish          - a callback function to be called upon a completion of the job
     * @param priority          - set the desired job priority (larger values
     *                            mean higher priorities). A job with the highest
     *                            priority will be select from an input queue by
     *                            the JobScheduler.
     * @param exclusive         - set to 'true' to indicate that the job can't be
     *                            running simultaneously alongside other jobs.
     * @param preemptable       - set to 'true' to indicate that this job can be
     *                            interrupted to give a way to some other job of
     *                            high importancy.
     */
    static pointer create(std::string const& databaseFamily,
                          unsigned int chunk,
                          std::string const& worker,
                          Controller::pointer const& controller,
                          std::string const& parentJobId,
                          callback_type onFinish,
                          int  priority    = -2,
                          bool exclusive   = false,
                          bool preemptable = true);

    // Default construction and copy semantics are prohibited

    DeleteReplicaJob() = delete;
    DeleteReplicaJob(DeleteReplicaJob const&) = delete;
    DeleteReplicaJob& operator=(DeleteReplicaJob const&) = delete;

    /// Destructor
    ~DeleteReplicaJob() override = default;

    /// @return the name of a database family
    std::string const& databaseFamily() const { return _databaseFamily; }

    ///@return the chunk number
    unsigned int chunk() const { return _chunk; }

    /// Return the name of a source worker where the affected replica is residing
    std::string const& worker() const { return _worker; }

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
    DeleteReplicaJobResult const& getReplicaData() const;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::track()
      */
    void track(bool progressReport,
               bool errorReport,
               bool chunkLocksReport,
               std::ostream& os) const override;

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @see DeleteReplicaJob::create()
     */
    DeleteReplicaJob(std::string const& databaseFamily,
                     unsigned int chunk,
                     std::string const& worker,
                     Controller::pointer const& controller,
                     std::string const& parentJobId,
                     callback_type onFinish,
                     int  priority,
                     bool exclusive,
                     bool preemptable);

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
     * Initiate a process of removing the replica from the source worker
     */
    void beginDeleteReplica();

    /**
     * The calback function to be invoked on a completion of each replica
     * deletion request.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish(DeleteRequest::pointer const& request);

protected:

    /// The name of a database family
    std::string _databaseFamily;

    /// The chunk number
    unsigned int _chunk;

    /// The name of a worker where the affected replica is residing
    std::string _worker;

    /// Client-defined function to be called upon the completion of the job
    callback_type _onFinish;

    /// Cached replicas is needed to figure out wich specific databases
    /// have contrinutions into the chunk.
    std::vector<ReplicaInfo> _replicas;

    /// A collection of the replica deletion requests implementing the operation
    std::vector<DeleteRequest::pointer> _requests;

    /// The result of the operation (gets updated as requests are finishing)
    DeleteReplicaJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DELETE_REPLICA_JOB_H
