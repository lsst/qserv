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
#ifndef LSST_QSERV_REPLICA_FINDALLJOB_H
#define LSST_QSERV_REPLICA_FINDALLJOB_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/util/ReplicaInfo.h"
#include "replica/util/SemanticMaps.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
class HttpFindAllRequest;
#else
class FindAllRequest;
#endif
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * The structure FindAllJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct FindAllJobResult {
    /// Per-worker flags indicating if the corresponding replica retrieval
    /// request succeeded for all databases in the family.
    ///
    std::map<std::string, bool> workers;

    /// Results reported by workers upon the successful completion
    /// of the corresponding requests
    ///
    std::list<ReplicaInfoCollection> replicas;

    /// [ALL CHUNKS]  Results grouped by:
    ///
    ///      [chunk][database][worker]
    ///
    ChunkDatabaseWorkerMap<ReplicaInfo> chunks;

    /// [ALL CHUNKS]  The participating databases for a chunk.
    ///
    /// NOTE: chunks don't have be present in all databases because databases
    ///       may have different spatial coverage.
    ///
    ///      [chunk]
    ///
    std::map<unsigned int, std::list<std::string>> databases;

    /// [SUBSET OF CHUNKS]  Workers hosting complete chunks
    ///
    ///      [chunk][database]->(worker,worker,,,)
    ///
    std::map<unsigned int, std::map<std::string, std::list<std::string>>> complete;

    /// [ALL CHUNKS]  The 'colocated' replicas are the ones in which all
    ///               participating databases are represented on the replica's
    ///               worker.
    ///
    /// NOTE: this doesn't guarantee that there may be problems with
    ///       database-specific chunks. Please, consider using 'isGood'
    ///       if that's a requirement.
    ///
    ///      [chunk][worker]
    ///
    std::map<unsigned int, std::map<std::string, bool>> isColocated;

    /// [ALL CHUNKS]  The 'good' replicas are the 'colocated' one in which
    ///               all database-specific chunks are also complete (healthy).
    ///
    ///      [chunk][worker]
    ///
    std::map<unsigned int, std::map<std::string, bool>> isGood;
};

/**
 * Class FindAllJob represents a tool which will find all replicas
 * of all chunks on all worker nodes.
 */
class FindAllJob : public Job {
public:
    typedef std::shared_ptr<FindAllJob> Ptr;
    typedef std::function<void(std::shared_ptr<FindAllJob>)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param databaseFamily The name of a database family.
     * @param saveReplicaInfo Save replica info in a database.
     * @param allWorkers Engage all known workers regardless of their status.
     * @param controller An instance of the controller for launching requests.
     * @param parentJobId An identifier of a parent job.
     * @param onFinish Callback function to be called upon a completion of the job.
     * @param priority Priority level of the job.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<FindAllJob> create(std::string const& databaseFamily, bool saveReplicaInfo,
                                              bool allWorkers, std::shared_ptr<Controller> const& controller,
                                              std::string const& parentJobId, CallbackType const& onFinish,
                                              int priority);

    FindAllJob() = delete;
    FindAllJob(FindAllJob const&) = delete;
    FindAllJob& operator=(FindAllJob const&) = delete;

    virtual ~FindAllJob() final = default;

    /// @return the name of a database family defining a scope of the operation
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// @return 'true' if replica info has to be saved in a database
    bool saveReplicaInfo() const { return _saveReplicaInfo; }

    /// @return 'true' if all known workers were engaged
    bool allWorkers() const { return _allWorkers; }

    /**
     * Return the result of the operation.
     *
     * @note The method should be invoked only after the job has finished (primary
     *  status is set to Job::Status::FINISHED). Otherwise exception
     *  std::logic_error will be thrown
     * @note The result will be extracted from requests which have successfully
     *  finished. Please, verify the primary and extended status of the object
     *  to ensure that all requests have finished.
     * @return The data structure to be filled upon the completion of the job.
     * @throws std::logic_error If the job isn't finished at the time when the method was called.
     */
    FindAllJobResult const& getReplicaData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;
    virtual std::list<std::pair<std::string, std::string>> persistentLogData() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    FindAllJob(std::string const& databaseFamily, bool saveReplicaInfo, bool allWorkers,
               std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
               CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of each request.
     * @param request A pointer to a request.
     */
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
    void _onRequestFinish(std::shared_ptr<HttpFindAllRequest> const& request);
#else
    void _onRequestFinish(std::shared_ptr<FindAllRequest> const& request);
#endif

    // Input parameters

    std::string const _databaseFamily;
    bool const _saveReplicaInfo;
    bool const _allWorkers;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// A collection of requests implementing the operation
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
    std::list<std::shared_ptr<HttpFindAllRequest>> _requests;
#else
    std::list<std::shared_ptr<FindAllRequest>> _requests;
#endif

    /// Per-worker and per database flags indicating if the corresponding replica
    /// retrieval request succeeded for all databases in the family.
    std::map<std::string, std::map<std::string, bool>> _workerDatabaseSuccess;

    std::size_t _numLaunched = 0;  ///< the total number of requests launched
    std::size_t _numFinished = 0;  ///< the total number of finished requests
    std::size_t _numSuccess = 0;   ///< the number of successfully completed requests

    /// The result of the operation (gets updated as requests are finishing)
    FindAllJobResult _replicaData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_FINDALLJOB_H
