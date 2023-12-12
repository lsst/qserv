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
#include <cstdint>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

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
     * @param configuration the configuration service
     */
    explicit DatabaseServicesMySQL(Configuration::Ptr const& configuration);

    ~DatabaseServicesMySQL() override = default;

    void saveState(ControllerIdentity const& identity, uint64_t startTime) final;

    void saveState(Job const& job) final;

    void updateHeartbeatTime(Job const& job) final;

    void saveState(QservMgtRequest const& request, Performance const& performance,
                   std::string const& serverError) final;

    void saveState(Request const& request, Performance const& performance) final;

    void updateRequestState(Request const& request, std::string const& targetRequestId,
                            Performance const& targetRequestPerformance) final;

    void saveReplicaInfo(ReplicaInfo const& info) final;

    void saveReplicaInfoCollection(std::string const& workerName, std::string const& database,
                                   ReplicaInfoCollection const& newReplicaInfoCollection) final;

    void findOldestReplicas(std::vector<ReplicaInfo>& replicas, size_t maxReplicas, bool enabledWorkersOnly,
                            bool allDatabases, bool isPublished) final;

    void findReplicas(std::vector<ReplicaInfo>& replicas, unsigned int chunk, std::string const& database,
                      bool enabledWorkersOnly, bool includeFileInfo) final;

    void findReplicas(std::vector<ReplicaInfo>& replicas, std::vector<unsigned int> const& chunks,
                      std::string const& database, bool enabledWorkersOnly, bool includeFileInfo) final;

    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas, std::string const& workerName,
                            std::string const& database, bool allDatabases, bool isPublished,
                            bool includeFileInfo) final;

    uint64_t numWorkerReplicas(std::string const& workerName, std::string const& database, bool allDatabases,
                               bool isPublished) final;

    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas, unsigned int chunk,
                            std::string const& workerName, std::string const& databaseFamily,
                            bool allDatabases, bool isPublished) final;

    void findDatabaseReplicas(std::vector<ReplicaInfo>& replicas, std::string const& database,
                              bool enabledWorkersOnly) final;

    void findDatabaseChunks(std::vector<unsigned int>& chunks, std::string const& database,
                            bool enabledWorkersOnly) final;

    std::map<unsigned int, size_t> actualReplicationLevel(
            std::string const& database, std::vector<std::string> const& workersToExclude) final;

    size_t numOrphanChunks(std::string const& database,
                           std::vector<std::string> const& uniqueOnWorkers) final;

    void logControllerEvent(ControllerEvent const& event) final;

    std::list<ControllerEvent> readControllerEvents(std::string const& controllerId, uint64_t fromTimeStamp,
                                                    uint64_t toTimeStamp, size_t maxEntries,
                                                    std::string const& task, std::string const& operation,
                                                    std::string const& operationStatus) final;

    nlohmann::json readControllerEventDict(std::string const& controllerId) final;

    ControllerInfo controller(std::string const& id) final;

    std::list<ControllerInfo> controllers(uint64_t fromTimeStamp, uint64_t toTimeStamp,
                                          size_t maxEntries) final;

    RequestInfo request(std::string const& id) final;

    std::list<RequestInfo> requests(std::string const& jobId, uint64_t fromTimeStamp, uint64_t toTimeStamp,
                                    size_t maxEntries) final;

    JobInfo job(std::string const& id) final;

    std::list<JobInfo> jobs(std::string const& controllerId, std::string const& parentJobId,
                            uint64_t fromTimeStamp, uint64_t toTimeStamp, size_t maxEntries) final;

    // Operations with super-transactions

    TransactionInfo transaction(TransactionId id, bool includeContext = false, bool includeLog = false) final;

    std::vector<TransactionInfo> transactions(std::string const& databaseName = std::string(),
                                              bool includeContext = false, bool includeLog = false) final;

    std::vector<TransactionInfo> transactions(TransactionInfo::State state, bool includeContext = false,
                                              bool includeLog = false) final;

    TransactionInfo createTransaction(
            std::string const& databaseName, NamedMutexRegistry& namedMutexRegistry,
            std::unique_ptr<replica::Lock>& namedMutexLock,
            nlohmann::json const& transactionContext = nlohmann::json::object()) final;

    TransactionInfo updateTransaction(TransactionId id, TransactionInfo::State newState) final;

    TransactionInfo updateTransaction(
            TransactionId id, nlohmann::json const& transactionContext = nlohmann::json::object()) final;

    TransactionInfo updateTransaction(TransactionId id,
                                      std::unordered_map<std::string, nlohmann::json> const& events) final;

    TransactionContribInfo transactionContrib(unsigned int id, bool includeWarnings = false,
                                              bool includeRetries = false) final;

    std::vector<TransactionContribInfo> transactionContribs(
            TransactionId transactionId, std::string const& table = std::string(),
            std::string const& workerName = std::string(),
            TransactionContribInfo::TypeSelector typeSelector =
                    TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC,
            bool includeWarnings = false, bool includeRetries = false) final;

    std::vector<TransactionContribInfo> transactionContribs(
            TransactionId transactionId, TransactionContribInfo::Status status,
            std::string const& table = std::string(), std::string const& workerName = std::string(),
            TransactionContribInfo::TypeSelector typeSelector =
                    TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC,
            bool includeWarnings = false, bool includeRetries = false) final;

    std::vector<TransactionContribInfo> transactionContribs(
            std::string const& database, std::string const& table = std::string(),
            std::string const& workerName = std::string(),
            TransactionContribInfo::TypeSelector typeSelector =
                    TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC,
            bool includeWarnings = false, bool includeRetries = false) final;

    TransactionContribInfo createdTransactionContrib(
            TransactionContribInfo const& info, bool failed = false,
            TransactionContribInfo::Status statusOnFailed =
                    TransactionContribInfo::Status::CREATE_FAILED) final;

    TransactionContribInfo updateTransactionContrib(TransactionContribInfo const& info) final;

    TransactionContribInfo saveLastTransactionContribRetry(TransactionContribInfo const& info) final;

    DatabaseIngestParam ingestParam(std::string const& database, std::string const& category,
                                    std::string const& param) final;

    std::vector<DatabaseIngestParam> ingestParams(std::string const& database,
                                                  std::string const& category = std::string()) final;

    void saveIngestParam(std::string const& database, std::string const& category, std::string const& param,
                         std::string const& value) final;

    TableRowStats tableRowStats(std::string const& database, std::string const& table,
                                TransactionId transactionId = 0) final;

    void saveTableRowStats(TableRowStats const& stats) final;

    void deleteTableRowStats(
            std::string const& databaseName, std::string const& tableName,
            ChunkOverlapSelector overlapSelector = ChunkOverlapSelector::CHUNK_AND_OVERLAP) final;

private:
    static std::string _context(std::string const& func = std::string());

    /**
     * Thread unsafe implementation of the corresponding public method.
     * This operation is supposed to be invoked in a context where proper
     * thread safety synchronization has been taken care of.
     *
     * @param lock a lock on DatabaseServicesMySQL::_mtx must be acquired before
     *   calling this method
     * @param replicas collection of replicas found upon a successful completion
     * @param workerName worker name (as per the request)
     * @param database (optional) database name (as per the request)
     * @param allDatabases (optional) flag which if set to 'true' will include
     *   into the search all known database entries regardless of their PUBLISHED
     *   status. Otherwise a subset of databases as determined by the second flag
     *   'isPublished' will get assumed. Note, this flag is used only if a value
     *   of parameter 'database' is empty.
     * @param isPublished (optional) flag which is used if flag 'all' is set
     *   to 'false' to narrow a collection of databases included into the search.
     * @param includeFileInfo if set to 'true' then file info will also be added
     *   to each replica
     */
    void _findWorkerReplicasImpl(replica::Lock const& lock, std::vector<ReplicaInfo>& replicas,
                                 std::string const& workerName, std::string const& database = std::string(),
                                 bool allDatabases = false, bool isPublished = true,
                                 bool includeFileInfo = true);

    /**
     * Actual implementation of the replica update algorithm.
     *
     * @param lock a lock on DatabaseServicesMySQL::_mtx must be acquired
     *   before calling this method
     * @param info replica to be added/updated or deleted
     */
    void _saveReplicaInfoImpl(replica::Lock const& lock, ReplicaInfo const& info);

    /**
     * Actual implementation of the multiple replicas update algorithm.
     *
     * @param lock a lock on DatabaseServicesMySQL::_mtx must be acquired
     *   before calling this method
     * @param workerName worker name (as per the request)
     * @param database database name (as per the request)
     * @param newReplicaInfoCollection collection of new replicas
     */
    void _saveReplicaInfoCollectionImpl(replica::Lock const& lock, std::string const& workerName,
                                        std::string const& database,
                                        ReplicaInfoCollection const& newReplicaInfoCollection);

    /**
     * Delete a replica from the database.
     *
     * @param lock a lock on DatabaseServicesMySQL::_mtx must be acquired before
     *   calling this method
     * @param workerName worker name
     * @param database database name
     * @param chunk the chunk whose replicas will be removed
     */
    void _deleteReplicaInfoImpl(replica::Lock const& lock, std::string const& workerName,
                                std::string const& database, unsigned int chunk);
    /**
     * Fetch replicas satisfying the specified query
     *
     * @param lock a lock on DatabaseServicesMySQL::_mtx must be acquired
     *   before calling this method
     * @param replicas collection of replicas to be returned
     * @param query SQL query against the corresponding table
     * @param includeFileInfo if set to 'true' then file info will also be added
     *   to each replica
     */
    void _findReplicasImpl(replica::Lock const& lock, std::vector<ReplicaInfo>& replicas,
                           std::string const& query, bool includeFileInfo = true);

    /**
     * Fetch files for the replicas
     *
     * @param lock a lock on DatabaseServicesMySQL::_mtx must be acquired before
     *   calling this method
     * @param id2replica input/output collection of incomplete replicas to be
     *   extended with files (if any found)
     */
    void _findReplicaFilesImpl(replica::Lock const& lock, std::map<uint64_t, ReplicaInfo>& id2replica);

    /**
     * Fetch replicas satisfying the specified query
     *
     * @param lock a lock on DatabaseServicesMySQL::_mtx must be acquired before
     *   calling this method
     * @param chunks collection of chunks to be returned
     * @param query SQL query against the corresponding table
     */
    void _findChunksImpl(replica::Lock const& lock, std::vector<unsigned int>& chunks,
                         std::string const& query);

    void _logControllerEvent(replica::Lock const& lock, ControllerEvent const& event);

    std::list<ControllerEvent> _readControllerEvents(replica::Lock const& lock,
                                                     std::string const& controllerId, uint64_t fromTimeStamp,
                                                     uint64_t toTimeStamp, size_t maxEntries,
                                                     std::string const& task, std::string const& operation,
                                                     std::string const& operationStatus);

    nlohmann::json _readControllerEventDict(replica::Lock const& lock, std::string const& controllerId);

    ControllerInfo _controller(replica::Lock const& lock, std::string const& id);

    std::list<ControllerInfo> _controllers(replica::Lock const& lock, uint64_t fromTimeStamp,
                                           uint64_t toTimeStamp, size_t maxEntries);

    RequestInfo _request(replica::Lock const& lock, std::string const& id);

    std::list<RequestInfo> _requests(replica::Lock const& lock, std::string const& jobId,
                                     uint64_t fromTimeStamp, uint64_t toTimeStamp, size_t maxEntries);

    JobInfo _job(replica::Lock const& lock, std::string const& id);

    std::list<JobInfo> _jobs(replica::Lock const& lock, std::string const& controllerId,
                             std::string const& parentJobId, uint64_t fromTimeStamp, uint64_t toTimeStamp,
                             size_t maxEntries);

    std::string _typeSelectorPredicate(TransactionContribInfo::TypeSelector typeSelector) const;

    std::vector<TransactionInfo> _transactions(replica::Lock const& lock, std::string const& predicate,
                                               bool includeContext, bool includeLog);

    TransactionInfo _findTransactionImpl(replica::Lock const& lock, std::string const& predicate,
                                         bool includeContext, bool includeLog);

    std::vector<TransactionInfo> _findTransactionsImpl(replica::Lock const& lock,
                                                       std::string const& predicate, bool includeContext,
                                                       bool includeLog);

    std::vector<TransactionContribInfo> _transactionContribs(replica::Lock const& lock,
                                                             std::string const& predicate,
                                                             bool includeWarnings = false,
                                                             bool includeRetries = false);

    TransactionContribInfo _transactionContribImpl(replica::Lock const& lock, std::string const& predicate,
                                                   bool includeWarnings = false, bool includeRetries = false);

    std::vector<TransactionContribInfo> _transactionContribsImpl(replica::Lock const& lock,
                                                                 std::string const& predicate,
                                                                 bool includeWarnings = false,
                                                                 bool includeRetries = false);

    DatabaseIngestParam _ingestParamImpl(replica::Lock const& lock, std::string const& predicate);

    std::vector<DatabaseIngestParam> _ingestParamsImpl(replica::Lock const& lock,
                                                       std::string const& predicate);

    TableRowStats _tableRowStats(replica::Lock const& lock, std::string const& database,
                                 std::string const& table, TransactionId transactionId);

    /*
     * The assert methods below will ensure that the specified entries exist
     * in the Configuration. The methods will throw std::invalid_argument if
     * the name of the entry is empty, or if it refers to non-existing entry.
     */
    void _assertDatabaseFamilyIsValid(std::string const& context, std::string const& familyName) const;
    void _assertDatabaseIsValid(std::string const& context, std::string const& databaseName) const;
    void _assertWorkerIsValid(std::string const& context, std::string const& workerName) const;

    // Input parameters

    Configuration::Ptr const _configuration;

    /// Database connection
    database::mysql::Connection::Ptr const _conn;

    /// The query generator tied to the connector
    database::mysql::QueryGenerator const _g;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable replica::Mutex _mtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_DATABASESERVICESMYSQL_H
