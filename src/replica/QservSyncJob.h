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
#ifndef LSST_QSERV_REPLICA_QSERVSYNCJOB_H
#define LSST_QSERV_REPLICA_QSERVSYNCJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/Job.h"
#include "replica/SetReplicasQservMgtRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure QservSyncJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct QservSyncJobResult {
    /// Per-worker flags indicating if the the synchronization request sent
    /// to the corresponding worker has succeeded.
    std::map<std::string, bool> workers;

    /// Previous replica disposition as reported by workers upon the successful
    /// completion of the corresponding requests
    std::map<std::string, QservReplicaCollection> prevReplicas;

    /// New replica disposition pushed to workers upon the successful completion
    /// of the corresponding requests
    std::map<std::string, QservReplicaCollection> newReplicas;
};

/**
 * Class QservSyncJob represents a tool which will configure Qserv workers
 * to be in sync with the "good" replicas which are known to the Replication
 * system. The job will contact all workers. And the scope of the job is
 * is limited to a database family.
 *
 * @note
 *    The current implementation of the job's algorithm assumes
 *   that the latest state of replicas is already recorded in the Replication
 *   System's database.
 */
class QservSyncJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservSyncJob> Ptr;

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
     *   name of a database family
     *
     * @param requestExpirationIvalSec
     *   override the default value of the corresponding parameter from
     *   the Configuration.
     *
     * @param force
     *   proceed with the operation even if some replicas affected by
     *   the operation are in use.
     *
     * @param controller
     *   for launching requests
     *
     * @param parentJobId
     *   (optional) identifier of a parent job
     *
     * @param onFinish
     *   (optional) callback function to be called upon a completion of the job
     *
     * @param priority
     *   (optional) priority level of the jobs. Note that the priority system is
     *   not presently used in communications with Qserv workers over the XROOTD/SSI
     *   protocol. This parameter is present here only for the sake of compatibility
     *   with other job types.
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(std::string const& databaseFamily, unsigned int requestExpirationIvalSec, bool force,
                      Controller::Ptr const& controller, std::string const& parentJobId = std::string(),
                      CallbackType const& onFinish = nullptr, int priority = PRIORITY_NORMAL);

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
    QservSyncJobResult const& getReplicaData() const;

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

    /// @see Job::persistentLogData()
    std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    /// @see Job::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Job::cancelImpl()
    void cancelImpl(util::Lock const& lock) final;

    /// @see Job::notify()
    void notify(util::Lock const& lock) final;

private:
    /// @see QservSyncJob::create()
    QservSyncJob(std::string const& databaseFamily, unsigned int requestExpirationIvalSec, bool force,
                 Controller::Ptr const& controller, std::string const& parentJobId,
                 CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of each request.
     *
     * @param request
     *   a pointer to a request
     */
    void _onRequestFinish(SetReplicasQservMgtRequest::Ptr const& request);

    // Input parameters

    std::string const _databaseFamily;
    unsigned int const _requestExpirationIvalSec;
    bool const _force;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// A collection of requests implementing the operation
    std::list<SetReplicasQservMgtRequest::Ptr> _requests;

    size_t _numLaunched = 0;  ///< the total number of requests launched
    size_t _numFinished = 0;  ///< the total number of finished requests
    size_t _numSuccess = 0;   ///< the number of successfully completed requests

    /// The result of the operation (gets updated as requests are finishing)
    QservSyncJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERVSYNCJOB_H
