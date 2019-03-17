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
#include "replica/Job.h"
#include "replica/ReplicaInfo.h"
#include "replica/DeleteRequest.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

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

    /// Replica deletion results grouped by: chunk number, database, worker
    std::map<unsigned int,                  // chunk
             std::map<std::string,          // database
                      std::map<std::string, // source worker
                               ReplicaInfo>>> chunks;
};

/**
  * Class DeleteReplicaJob represents a tool which will delete a chunk replica
  * from a worker.
  */
class DeleteReplicaJob : public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteReplicaJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily
     *   the name of a database family involved into the operation
     *
     * @param chunk
     *   the chunk whose replica will be deleted from the target worker
     *
     * @param worker
     *   the name of a worker where the affected replica is residing
     *
     * @param controller
     *   for launching requests
     *
     * @param parentJobId
     *   optional identifier of a parent job
     *
     * @param onFinish
     *   a callback function to be called upon a completion of the job
     *
     * @param options
     *   job options
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(std::string const& databaseFamily,
                      unsigned int chunk,
                      std::string const& worker,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType const& onFinish,
                      Job::Options const& options = defaultOptions());

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
    std::string const& worker() const { return _worker; }

    /**
     * Return the result of the operation.
     *
     * @note:
     *  The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown
     *
     * @note
     *  The result will be extracted from requests which have successfully
     *  finished. Please, verify the primary and extended status of the object
     *  to ensure that all requests have finished.
     *
     * @return
     *   the data structure to be filled upon the completion of the job.
     *
     * @throws std::logic_error
     *   if the job didn't finished at a time when the method was called
     */
    DeleteReplicaJobResult const& getReplicaData() const;

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string,std::string>> persistentLogData() const final;

protected:

    /// @see Job::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(util::Lock const& lock) final;

    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see DeleteReplicaJob::create()
    DeleteReplicaJob(std::string const& databaseFamily,
                     unsigned int chunk,
                     std::string const& worker,
                     Controller::Ptr const& controller,
                     std::string const& parentJobId,
                     CallbackType const& onFinish,
                     Job::Options const& options);

    /**
     * Initiate a process of removing the replica from the source worker
     *
     * @param lock
     *   a lock on Job::_mtx must be acquired before calling this method
     */
    void _beginDeleteReplica(util::Lock const& lock);

    /**
     * The callback function to be invoked on a completion of each replica
     * deletion request.
     *
     * @param request
     *   a pointer to a request
     */
    void _onRequestFinish(DeleteRequest::Ptr const& request);


    // Input parameters

    std::string  const _databaseFamily;
    unsigned int const _chunk;
    std::string  const _worker;
    CallbackType       _onFinish;       /// @note is reset when the job finishes

    /// Cached replicas for determining which databases have contributions in the chunk
    std::vector<ReplicaInfo> _replicas;

    /// A collection of the replica deletion requests implementing the operation
    std::vector<DeleteRequest::Ptr> _requests;

    /// The result of the operation (gets updated as requests are finishing)
    DeleteReplicaJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DELETEREPLICAJOB_H
