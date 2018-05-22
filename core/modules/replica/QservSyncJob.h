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
#ifndef LSST_QSERV_REPLICA_QSERV_SYNC_JOB_H
#define LSST_QSERV_REPLICA_QSERV_SYNC_JOB_H

/// QservSyncJob.h declares:
///
/// struct QservSyncJobResult
/// class  QservSyncJob
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
#include "replica/SetReplicasQservMgtRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The structure QservSyncJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct QservSyncJobResult {

    /// Per-worker flags indicating if the the synchronization request sent
    /// to the corresponding worker has succeeded.
    ///
    std::map<std::string, bool> workers;

    /// Previous replica disposition as reported by workers upon the successfull
    /// completion of the corresponidng requests
    ///
    std::map<std::string, QservReplicaCollection> prevReplicas;

    /// New replica disposition pushed to workers upon the successfull completion
    /// of the corresponidng requests
    ///
    std::map<std::string, QservReplicaCollection> newReplicas;
};

/**
  * Class QservSyncJob represents a tool which will configure Qserv workers
  * to be in sync with the "good" replicas which are known to the Replication
  * system. The job will contact all workers. And the scope of the job is
  * is limited to a database family.
  *
  * ATTENTION: The current implementation of the job's algorithm assumes
  * that the latest state of replicas is already recorded in the Replication
  * System's database.
  */
class QservSyncJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservSyncJob> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return default options object for this type of a request
    static Job::Options const& defaultOptions();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily - name of a database family
     * @param controller     - for launching requests
     * @param parentJobId    - optional identifier of a parent job
     * @param force          - proceed with the operation even if some replicas affceted by
     *                         the operation are in use.
     * @param onFinish       - callback function to be called upon a completion of the job
     * @param options        - job options
     */
    static Ptr create(std::string const& databaseFamily,
                      Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      bool force = false,
                      CallbackType onFinish = nullptr,
                      Job::Options const& options = defaultOptions());

    // Default construction and copy semantics are prohibited

    QservSyncJob() = delete;
    QservSyncJob(QservSyncJob const&) = delete;
    QservSyncJob& operator=(QservSyncJob const&) = delete;

    ~QservSyncJob() final = default;

    /// @return name of a database family defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// @return flag indicating (if set) the 'force' mode of the operation
    bool force() const { return _force; }

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
    QservSyncJobResult const& getReplicaData() const;

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
     * @see QservSyncJob::create()
     */
    QservSyncJob(std::string const& databaseFamily,
                 Controller::Ptr const& controller,
                 std::string const& parentJobId,
                 bool force,
                 CallbackType onFinish,
                 Job::Options const& options);

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::startImpl()
      */
    void startImpl(util::Lock const& lock) final;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::startImpl()
      */
    void cancelImpl(util::Lock const& lock) final;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::notifyImpl()
      */
    void notifyImpl() final;

    /**
     * The calback function to be invoked on a completion of each request.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish(SetReplicasQservMgtRequest::Ptr const& request);

protected:

    /// The name of the database family
    std::string _databaseFamily;

    /// Flag indicating to report (if set) the 'force' mode of the operation
    bool _force;

    /// Client-defined function to be called upon the completion of the job
    CallbackType _onFinish;

    /// A collection of requests implementing the operation
    std::list<SetReplicasQservMgtRequest::Ptr> _requests;

    // The counter of requests which will be updated. They need to be atomic
    // to avoid race condition between the onFinish() callbacks executed within
    // the Controller's thread and this thread.

    std::atomic<size_t> _numLaunched;   ///< the total number of requests launched
    std::atomic<size_t> _numFinished;   ///< the total number of finished requests
    std::atomic<size_t> _numSuccess;    ///< the number of successfully completed requests

    /// The result of the operation (gets updated as requests are finishing)
    QservSyncJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_QSERV_SYNC_JOB_H
