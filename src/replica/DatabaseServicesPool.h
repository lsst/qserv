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
#ifndef LSST_QSERV_REPLICA_DATABASESERVICESPOOL_H
#define LSST_QSERV_REPLICA_DATABASESERVICESPOOL_H

/**
 * This header is for a class which implements the DatabaseServices
 * service of the controller-side Replication Framework. The header
 * is not supposed to be include directly into the user's code.
 */

// System headers
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <list>

// Qserv headers
#include "replica/Common.h"
#include "replica/DatabaseServices.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class DatabaseServicesPool is a pool of service objects.
  * @see class DatabaseServices
  */
class DatabaseServicesPool: public DatabaseServices {
public:
    /// This class which implements the RAII paradigm is used by
    /// the implementation of the pool.
    friend class ServiceAllocator;

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseServicesPool> Ptr;

    /**
     * The factory method for instantiating a proper service object based
     * on an application configuration.
     *
     * @param configuration the configuration service
     * @return pointer to the created object
     */
    static Ptr create(ConfigurationPtr const& configuration);

    DatabaseServicesPool() = delete;
    DatabaseServicesPool(DatabaseServicesPool const&) = delete;
    DatabaseServicesPool& operator=(DatabaseServicesPool const&) = delete;

    ~DatabaseServicesPool() override = default;

    void saveState(ControllerIdentity const& identity,
                   uint64_t startTime) final;

    void saveState(Job const& job,
                   Job::Options const& options) final;

    void updateHeartbeatTime(Job const& job) final;

    void saveState(QservMgtRequest const& request,
                   Performance const& performance,
                   std::string const& serverError) final;

    void saveState(Request const& request,
                   Performance const& performance) final;

    void updateRequestState(Request const& request,
                            std::string const& targetRequestId,
                            Performance const& targetRequestPerformance) final;

    void saveReplicaInfo(ReplicaInfo const& info) final;

    void saveReplicaInfoCollection(std::string const& worker,
                                   std::string const& database,
                                   ReplicaInfoCollection const& newReplicaInfoCollection) final;

    void findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                            size_t maxReplicas,
                            bool enabledWorkersOnly,
                            bool allDatabases,
                            bool isPublished) final;

    void findReplicas(std::vector<ReplicaInfo>& replicas,
                      unsigned int chunk,
                      std::string const& database,
                      bool enabledWorkersOnly,
                      bool includeFileInfo) final;

    void findReplicas(std::vector<ReplicaInfo>& replicas,
                      std::vector<unsigned int> const& chunks,
                      std::string const& database,
                      bool enabledWorkersOnly,
                      bool includeFileInfo) final;

    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            std::string const& worker,
                            std::string const& database,
                            bool allDatabases,
                            bool isPublished,
                            bool includeFileInfo) final;

    uint64_t numWorkerReplicas(std::string const& worker,
                               std::string const& database,
                               bool allDatabases,
                               bool isPublished) final;

    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            unsigned int chunk,
                            std::string const& worker,
                            std::string const& databaseFamily,
                            bool allDatabases,
                            bool isPublished) final;

    void findDatabaseReplicas(std::vector<ReplicaInfo>& replicas,
                              std::string const& database,
                              bool enabledWorkersOnly) final;

    void findDatabaseChunks(std::vector<unsigned int>& chunks,
                            std::string const& database,
                            bool enabledWorkersOnly) final;

    std::map<unsigned int, size_t> actualReplicationLevel(
                                        std::string const& database,
                                        std::vector<std::string> const& workersToExclude) final;

    size_t numOrphanChunks(std::string const& database,
                           std::vector<std::string> const& uniqueOnWorkers) final;

    void logControllerEvent(ControllerEvent const& event) final;

    std::list<ControllerEvent> readControllerEvents(std::string const& controllerId,
                                                    uint64_t fromTimeStamp,
                                                    uint64_t toTimeStamp,
                                                    size_t maxEntries) final;

    ControllerInfo controller(std::string const& id) final;

    std::list<ControllerInfo> controllers(uint64_t fromTimeStamp,
                                          uint64_t toTimeStamp,
                                          size_t maxEntries) final;

    RequestInfo request(std::string const& id) final;

    std::list<RequestInfo> requests(std::string const& jobId,
                                    uint64_t fromTimeStamp,
                                    uint64_t toTimeStamp,
                                    size_t maxEntries) final;

    JobInfo job(std::string const& id) final;

    std::list<JobInfo> jobs(std::string const& controllerId,
                            std::string const& parentJobId,
                            uint64_t fromTimeStamp,
                            uint64_t toTimeStamp,
                            size_t maxEntries) final;

    TransactionInfo transaction(TransactionId id) final;

    std::vector<TransactionInfo> transactions(std::string const& databaseName=std::string()) final;

    TransactionInfo beginTransaction(std::string const& databaseName) final;

    TransactionInfo endTransaction(TransactionId id,
                                   bool abort=false) final;

    std::vector<TransactionContribInfo> transactionContribs(TransactionId transactionId,
                                                            std::string const& table=std::string(),
                                                            std::string const& worker=std::string()) final;

    std::vector<TransactionContribInfo> transactionContribs(std::string const& database,
                                                            std::string const& table=std::string(),
                                                            std::string const& worker=std::string()) final;

    TransactionContribInfo beginTransactionContrib(TransactionId transactionId,
                                                   std::string const& table,
                                                   unsigned int chunk,
                                                   bool isOverlap,
                                                   std::string const& worker,
                                                   std::string const& url) final;

    TransactionContribInfo endTransactionContrib(TransactionContribInfo const& info) final;

    DatabaseIngestParam ingestParam(std::string const& database,
                                    std::string const& category,
                                    std::string const& param) final;

    std::vector<DatabaseIngestParam> ingestParams(std::string const& database,
                                                  std::string const& category=std::string()) final;

    void saveIngestParam(std::string const& database,
                         std::string const& category,
                         std::string const& param,
                         std::string const& value) final;

private:
    /**
     * @param configuration the configuration service
     */
    explicit DatabaseServicesPool(ConfigurationPtr const& configuration);

    /**
     * Allocate the next available service object.
     *
     * @note the requester must return the service back after it's no longer needed.
     * @return pointer to a service
     * @see DatabaseServicesPool::_releaseService()
     */
    DatabaseServices::Ptr _allocateService();

    /**
     * Return a service object back into the pool of the available ones.
     *
     * @param service service object to be returned back
     * @throws std::logic_error if the service object was not previously allocated
     * @see DatabaseServicesPool::_allocateService()
     */
    void _releaseService(DatabaseServices::Ptr const& service);


    /// Service objects which are available
    std::list<DatabaseServices::Ptr> _availableServices;

    /// Service objects which are in use
    std::list<DatabaseServices::Ptr> _usedServices;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations. The mutex is locked by methods _allocateService
    /// and _releaseService when moving requests between the lists (defined above).
    mutable std::mutex _mtx;

    /// The condition variable for notifying clients waiting for the next
    /// available service.
    std::condition_variable _available;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DATABASESERVICESPOOL_H
