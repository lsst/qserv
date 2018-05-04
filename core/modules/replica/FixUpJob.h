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
#ifndef LSST_QSERV_REPLICA_FIXUP_JOB_H
#define LSST_QSERV_REPLICA_FIXUP_JOB_H

/// FixUpJob.h declares:
///
/// struct FixUpJobResult
/// class  FixUpJob
///
/// (see individual class documentation for more information)

// System headers
#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/Job.h"
#include "replica/FindAllJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ReplicationRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The structure FixUpJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct FixUpJobResult {

    /// Results reported by workers upon the successfull completion
    /// of the corresponidng requests
    std::list<ReplicaInfo> replicas;

    /// Results groupped by: chunk number, database, worker
    std::map<unsigned int,                  // chunk
             std::map<std::string,          // database
                      std::map<std::string, // worker
                               ReplicaInfo>>> chunks;

    /// Per-worker flags indicating if the corresponidng replica retreival
    /// request succeeded.
    std::map<std::string, bool> workers;
};

/**
  * Class FixUpJob represents a tool which will fix chunk collocation within
  * a specified database family. Note that the current implementation of
  * the class won't take into consideration the minimum replication level
  * (if any) configured for the family. Also note that as a results of
  * the ('fix-up') operation chunks may get a higher number of replicas
  * then others (not affected by the operation).
  */
class FixUpJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FixUpJob> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily - the name of a database family
     * @param controller     - for launching requests
     * @param parentJobId    - optional identifier of a parent job
     * @param onFinish       - callback function to be called upon a completion of the job
     * @param options        - job options
     */
    static Ptr create(std::string const& databaseFamily,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType onFinish,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    FixUpJob() = delete;
    FixUpJob(FixUpJob const&) = delete;
    FixUpJob& operator=(FixUpJob const&) = delete;

    /// Destructor (non-trivial)
    ~FixUpJob() override;

    /// Return the name of a database family defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

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
    FixUpJobResult const& getReplicaData() const;

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @see FixUpJob::create()
     */
    FixUpJob(std::string const& databaseFamily,
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
     * The calback function to be invoked on a completion of the precursor job
     * which harvests chunk disposition accross relevant worker nodes.
     */
    void onPrecursorJobFinish();

    /**
     * The calback function to be invoked on a completion of each request.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish(ReplicationRequest::Ptr const& request);

    /**
     * Restart the job from scratch. This method will reset object context
     * to a state it was before method Job::startImpl() called and then call
     * Job::startImpl() again.
     */
    void restart();

    /**
     * Unconditionally release the specified chunk
     *
     * @param chunk - the chunk number
     */
    void release(unsigned int chunk);

protected:

    /// The name of the database family
    std::string _databaseFamily;

    /// Client-defined function to be called upon the completion of the job
    CallbackType _onFinish;

    /// The chained job to be completed first in order to figure out
    /// replica disposition.
    FindAllJob::Ptr _findAllJob;

    /// The total number of iterations the job has gone so far
    size_t _numIterations;

    /// The number of chunks which require the fix-up but couldn't be locked
    /// in the exclusive mode. The counter will be analyzed upon a completion
    /// of the last request, and if it were found not empty another iteraton
    /// of the job will be undertaken
    size_t _numFailedLocks;

    /// A collection of requests groupped by the corresponidng chunk
    /// number. The main idea is simplify tracking the completion status
    /// of the operation on each chunk. Requests will be added to the
    /// corresponding group as they're launched, and removed when they
    /// finished. This allows releasing (unlocking) chunks before
    /// the whole job finishes.
    ///
    /// [chunk][worker][database]
    //
    std::map<unsigned int,
             std::map<std::string,
                      std::map<std::string,
                               ReplicationRequest::Ptr>>> _chunk2requests;

    /// A collection of requests implementing the operation
    std::list<ReplicationRequest::Ptr> _requests;

    // The counter of requests which will be updated. They need to be atomic
    // to avoid race condition between the onFinish() callbacks executed within
    // the Controller's thread and this thread.

    std::atomic<size_t> _numLaunched;   ///< the total number of requests launched
    std::atomic<size_t> _numFinished;   ///< the total number of finished requests
    std::atomic<size_t> _numSuccess;    ///< the number of successfully completed requests

    /// The result of the operation (gets updated as requests are finishing)
    FixUpJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FIXUP_JOB_H
