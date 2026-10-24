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
#ifndef LSST_QSERV_REPLICA_QSERV_GETREPLICASJOB_H
#define LSST_QSERV_REPLICA_QSERV_GETREPLICASJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/util/Common.h"
#include "replica/util/SemanticMaps.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class GetReplicasQservMgtRequest;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure QservGetReplicasJobResult represents a combined result received
 * from the Qserv worker management services upon a completion of the job.
 */
struct QservGetReplicasJobResult {
    /// Per-worker flags indicating if the corresponding replica retrieval
    /// request succeeded.
    std::map<std::string, bool> workers;

    /// Results grouped by:
    ///
    ///   [worker]
    std::map<std::string, QservReplicaCollection> replicas;

    /// Results grouped by:
    ///
    ///   [chunk][database][worker]
    ///
    /// This structure also reports the use counter for each chunks
    ChunkDatabaseWorkerMap<size_t> useCount;
};

/**
 * Class QservGetReplicasJob represents a tool which will find all replicas
 * of all chunks on all worker nodes.
 */
class QservGetReplicasJob : public Job {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservGetReplicasJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<QservGetReplicasJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan and memory management
     * of instances created otherwise (as values or via low-level pointers).
     *
     * @param databaseFamily The name of a database family
     * @param inUseOnly The flag indicating to return replicas which are presently in use.
     * @param allWorkers The flag indicating whether to engage all known workers regardless of their status.
     * @param controller The controller for launching requests
     * @param parentJobId The optional identifier of a parent job
     * @param onFinish The optional callback function to be called upon a job completion
     * @param priority The optional priority level of the job
     * @return A pointer to the created object
     */
    static std::shared_ptr<QservGetReplicasJob> create(std::string const& databaseFamily, bool inUseOnly,
                                                       bool allWorkers,
                                                       std::shared_ptr<Controller> const& controller,
                                                       std::string const& parentJobId = std::string(),
                                                       CallbackType const& onFinish = nullptr,
                                                       int priority = PRIORITY_NORMAL);

    // Default construction and copy semantics are prohibited

    QservGetReplicasJob() = delete;
    QservGetReplicasJob(QservGetReplicasJob const&) = delete;
    QservGetReplicasJob& operator=(QservGetReplicasJob const&) = delete;

    virtual ~QservGetReplicasJob() final = default;

    /// @return the name of a database family defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// @return flag indicating (if set) to report a subset of chunks which are in use
    bool inUseOnly() const { return _inUseOnly; }

    /// @return 'true' if all known workers were engaged
    bool allWorkers() const { return _allWorkers; }

    /**
     * @return the result of the operation (when the job finishes)
     *
     * @note The method should be invoked only after the job has finished (primary status is set
     *  to Job::Status::FINISHED). Otherwise exception std::logic_error will be thrown. Verify
     *  the primary and extended status of the object to ensure that all requests have finished.
     *
     * @throw std::logic_error if the job didn't finished at a time when the method was called
     */
    QservGetReplicasJobResult const& getReplicaData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    QservGetReplicasJob(std::string const& databaseFamily, bool inUseOnly, bool allWorkers,
                        std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
                        CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of each request.
     * @param request a pointer to a request
     */
    void _onRequestFinish(std::shared_ptr<GetReplicasQservMgtRequest> const& request);

    // Input parameters

    std::string const _databaseFamily;
    bool const _inUseOnly;
    bool const _allWorkers;
    CallbackType _onFinish;  ///< @note is reset when the job finishes

    /// A collection of requests implementing the operation
    std::list<std::shared_ptr<GetReplicasQservMgtRequest>> _requests;

    std::size_t _numLaunched = 0;  ///< the total number of requests launched
    std::size_t _numFinished = 0;  ///< the total number of finished requests
    std::size_t _numSuccess = 0;   ///< the number of successfully completed requests

    /// The result of the operation (gets updated as requests are finishing)
    QservGetReplicasJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERV_GETREPLICASJOB_H
