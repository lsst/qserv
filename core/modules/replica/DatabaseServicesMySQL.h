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
#ifndef LSST_QSERV_REPLICA_DATABASESERVICESMYSQL_H
#define LSST_QSERV_REPLICA_DATABASESERVICESMYSQL_H

/**
 * This header represents a MyQL-backed implementation of the
 * persistent database services of the Replication Framework.
 *
 * @see class DatabaseServices
 */

// System headers
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "util/Mutex.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class DatabaseServicesMySQL is a MySQL_specific implementation of the database
  * services for replication entities: Controller, Job and Request.
  *
  * @see class DatabaseServices
  */
class DatabaseServicesMySQL : public DatabaseServices {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseServicesMySQL> Ptr;

    // Default construction and copy semantics are prohibited

    DatabaseServicesMySQL() = delete;
    DatabaseServicesMySQL(DatabaseServicesMySQL const&) = delete;
    DatabaseServicesMySQL& operator=(DatabaseServicesMySQL const&) = delete;

    /**
     * @param configuration
     *    the configuration service
     */
    explicit DatabaseServicesMySQL(Configuration::Ptr const& configuration);

    ~DatabaseServicesMySQL() override = default;

    /// @see DatabaseServices::saveState()
    void saveState(ControllerIdentity const& identity,
                   uint64_t startTime) final;

    /// @see DatabaseServices::saveState()
    void saveState(Job const& job,
                   Job::Options const& options) final;

    /// @see DatabaseServices::updateHeartbeatTime()
     void updateHeartbeatTime(Job const& job) final;

    /// @see DatabaseServices::saveState()
    void saveState(QservMgtRequest const& request,
                   Performance const& performance,
                   std::string const& serverError) final;

    /// @see DatabaseServices::saveState()
    void saveState(Request const& request,
                   Performance const& performance) final;

    /// @see DatabaseServices::updateRequestState()
    void updateRequestState(Request const& request,
                            std::string const& targetRequestId,
                            Performance const& targetRequestPerformance) final;

    /// @see DatabaseServices::saveReplicaInfo()
    void saveReplicaInfo(ReplicaInfo const& info) final;

    /// @see DatabaseServices::saveReplicaInfoCollection()
    void saveReplicaInfoCollection(std::string const& worker,
                                   std::string const& database,
                                   ReplicaInfoCollection const& newReplicaInfoCollection) final;

    /// @see DatabaseServices::findOldestReplica()
    void findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                            size_t maxReplicas,
                            bool enabledWorkersOnly,
                            bool allDatabases,
                            bool isPublished) final;

    /// @see DatabaseServices::findReplicas()
    void findReplicas(std::vector<ReplicaInfo>& replicas,
                      unsigned int chunk,
                      std::string const& database,
                      bool enabledWorkersOnly) final;

    /// @see DatabaseServices::findWorkerReplicas()
    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            std::string const& worker,
                            std::string const& database,
                            bool allDatabases,
                            bool isPublished,
                            bool includeFileInfo) final;

    /// @see DatabaseServices::numWorkerReplicas()
    uint64_t numWorkerReplicas(std::string const& worker,
                               std::string const& database,
                               bool allDatabases,
                               bool isPublished) final;

    /// @see DatabaseServices::findWorkerReplicas()
    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            unsigned int chunk,
                            std::string const& worker,
                            std::string const& databaseFamily,
                            bool allDatabases,
                            bool isPublished) final;

    /// @see DatabaseServices::findDatabaseReplicas()
    void findDatabaseReplicas(std::vector<ReplicaInfo>& replicas,
                              std::string const& database,
                              bool enabledWorkersOnly) final;

    /// @see DatabaseServices::findDatabaseChunks()
    void findDatabaseChunks(std::vector<unsigned int>& chunks,
                            std::string const& database,
                            bool enabledWorkersOnly) final;

    /// @see DatabaseServices::actualReplicationLevel()
    std::map<unsigned int, size_t> actualReplicationLevel(
                                        std::string const& database,
                                        std::vector<std::string> const& workersToExclude) final;

    /// @see DatabaseServices::numOrphanChunks()
    size_t numOrphanChunks(std::string const& database,
                           std::vector<std::string> const& uniqueOnWorkers) final;

    /// @see DatabaseServices::logControllerEvent()
    void logControllerEvent(ControllerEvent const& event) final;

    /// @see DatabaseServices::readControllerEvents()
    std::list<ControllerEvent> readControllerEvents(std::string const& controllerId,
                                                    uint64_t fromTimeStamp,
                                                    uint64_t toTimeStamp,
                                                    size_t maxEntries) final;

    /// @see DatabaseServices::controller()
    ControllerInfo controller(std::string const& id) final;

    /// @see DatabaseServices::controllers()
    std::list<ControllerInfo> controllers(uint64_t fromTimeStamp,
                                          uint64_t toTimeStamp,
                                          size_t maxEntries) final;

    /// @see DatabaseServices::controller()
    RequestInfo request(std::string const& id) final;

    /// @see DatabaseServices::requests()
    std::list<RequestInfo> requests(std::string const& jobId,
                                    uint64_t fromTimeStamp,
                                    uint64_t toTimeStamp,
                                    size_t maxEntries) final;

    /// @see DatabaseServices::job()
    JobInfo job(std::string const& id) final;

    /// @see DatabaseServices::jobs()
    std::list<JobInfo> jobs(std::string const& controllerId,
                            std::string const& parentJobId,
                            uint64_t fromTimeStamp,
                            uint64_t toTimeStamp,
                            size_t maxEntries) final;


    // Operations with super-transactions

    TransactionInfo transaction(uint32_t id) final;

    std::vector<TransactionInfo> transactions(std::string const& databaseName=std::string()) final;

    TransactionInfo beginTransaction(std::string const& databaseName) final;

    TransactionInfo endTransaction(uint32_t id,
                                   bool abort=false) final;

private:

    std::string _context(std::string const& func=std::string()) const;

    /**
     * Thread unsafe implementation of the corresponding public method.
     * This operation is supposed to be invoked in a context where proper
     * thread safety synchronization has been taken care of.
     *
     * @param lock
     *   a lock on DatabaseServicesMySQL::_mtx must be acquired before calling
     *   this method
     *
     * @param replicas
     *   collection of replicas found upon a successful completion
     *
     * @param worker
     *   worker name (as per the request)
     *
     * @param database
     *   (optional) database name (as per the request)
     *
     * @param allDatabases
     *   (optional) flag which if set to 'true' will include into the search all
     *   known database entries regardless of their PUBLISHED status. Otherwise
     *   a subset of databases as determined by the second flag 'isPublished'
     *   will get assumed. Note, this flag is used only if a value of
     *   parameter 'database' is empty.
     * 
     * @param isPublished
     *   (optional) flag which is used if flag 'all' is set to 'false'
     *   to narrow a collection of databases included into the search.
     * 
     * @param includeFileInfo
     *   if set to 'true' then file info will also be added to each replica
     */
    void _findWorkerReplicasImpl(util::Lock const& lock,
                                 std::vector<ReplicaInfo>& replicas,
                                 std::string const& worker,
                                 std::string const& database=std::string(),
                                 bool allDatabases=false,
                                 bool isPublished=true,
                                 bool includeFileInfo=true);

    /**
     * Actual implementation of the replica update algorithm.
     *
     * @param lock
     *   a lock on DatabaseServicesMySQL::_mtx must be acquired before calling
     *   this method
     *
     * @param info
     *   replica to be added/updated or deleted
     */
    void _saveReplicaInfoImpl(util::Lock const& lock,
                              ReplicaInfo const& info);

    /**
     * Actual implementation of the multiple replicas update algorithm.
     *
     * @param lock
     *   a lock on DatabaseServicesMySQL::_mtx must be acquired before calling
     *   this method
     *
     * @param worker
     *   worker name (as per the request)
     *
     * @param database
     *   database name (as per the request)
     *
     * @param newReplicaInfoCollection
     *   collection of new replicas
     */
    void _saveReplicaInfoCollectionImpl(util::Lock const& lock,
                                        std::string const& worker,
                                        std::string const& database,
                                        ReplicaInfoCollection const& newReplicaInfoCollection);

    /**
     * Delete a replica from the database.
     *
     * @param lock
     *   a lock on DatabaseServicesMySQL::_mtx must be acquired before calling
     *   this method
     *
     * @param worker
     *   worker name
     *
     * @param database
     *   database name
     *
     * @param chunk
     *   the chunk whose replicas will be removed
     */
    void _deleteReplicaInfoImpl(util::Lock const& lock,
                                std::string const& worker,
                                std::string const& database,
                                unsigned int chunk);
    /**
     * Fetch replicas satisfying the specified query
     *
     * @param lock
     *   a lock on DatabaseServicesMySQL::_mtx must be acquired before calling
     *   this method
     *
     * @param replicas
     *   collection of replicas to be returned
     *
     * @param query
     *   SQL query against the corresponding table
     * 
     * @param includeFileInfo
     *   if set to 'true' then file info will also be added to each replica
     */
    void _findReplicasImpl(util::Lock const& lock,
                           std::vector<ReplicaInfo>& replicas,
                           std::string const& query,
                           bool includeFileInfo=true);

    /**
     * Fetch files for the replicas
     *
     * @param lock
     *   a lock on DatabaseServicesMySQL::_mtx must be acquired before calling
     *   this method
     *
     * @param id2replica
     *   input/output collection of incomplete replicas to be extended with
     *   files (if any found)
     */
    void _findReplicaFilesImpl(util::Lock const& lock,
                               std::map<uint64_t, ReplicaInfo>& id2replica);

    /**
     * Fetch replicas satisfying the specified query
     *
     * @param lock
     *   a lock on DatabaseServicesMySQL::_mtx must be acquired before calling
     *   this method
     *
     * @param chunks
     *   collection of chunks to be returned
     *
     * @param query
     *   SQL query against the corresponding table
     */
    void _findChunksImpl(util::Lock const& lock,
                         std::vector<unsigned int>& chunks,
                         std::string const& query);

    /// @see DatabaseServicesMySQL::logControllerEvent()
    void _logControllerEvent(util::Lock const& lock,
                             ControllerEvent const& event);

    /// @see DatabaseServices::readControllerEvents()
    std::list<ControllerEvent> _readControllerEvents(util::Lock const& lock,
                                                     std::string const& controllerId,
                                                     uint64_t fromTimeStamp,
                                                     uint64_t toTimeStamp,
                                                     size_t maxEntries);

    /// @see DatabaseServicesMySQL::controller()
    ControllerInfo _controller(util::Lock const& lock,
                               std::string const& id);

    /// @see DatabaseServicesMySQL::controllers()
    std::list<ControllerInfo> _controllers(util::Lock const& lock,
                                           uint64_t fromTimeStamp,
                                           uint64_t toTimeStamp,
                                           size_t maxEntries);

    /// @see DatabaseServicesMySQL::request()
    RequestInfo _request(util::Lock const& lock,
                         std::string const& id);

    /// @see DatabaseServicesMySQL::requests()
    std::list<RequestInfo> _requests(util::Lock const& lock,
                                     std::string const& jobId,
                                     uint64_t fromTimeStamp,
                                     uint64_t toTimeStamp,
                                     size_t maxEntries);

    /// @see DatabaseServicesMySQL::job()
    JobInfo _job(util::Lock const& lock,
                 std::string const& id);

    /// @see DatabaseServicesMySQL::jobs()
    std::list<JobInfo> _jobs(util::Lock const& lock,
                             std::string const& controllerId,
                             std::string const& parentJobId,
                             uint64_t fromTimeStamp,
                             uint64_t toTimeStamp,
                             size_t maxEntries);

    /// @see DatabaseServicesMySQL::findTransaction()
    TransactionInfo _findTransactionImpl(util::Lock const& lock,
                                         std::string const& predicate);

    std::vector<TransactionInfo> _findTransactionsImpl(util::Lock const& lock,
                                                       std::string const& predicate);

    // Input parameters

    Configuration::Ptr const _configuration;

    /// Database connection
    database::mysql::Connection::Ptr const _conn;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable util::Mutex _mtx;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DATABASESERVICESMYSQL_H
