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
#include <mutex>
#include <list>

// Qserv headers
#include "replica/DatabaseServices.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class DatabaseServicesPool is a pool of service objects.
  *
  * @see class DatabaseServices
  */
class DatabaseServicesPool : public DatabaseServices {

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
     * @param configuration
     *   the configuration service
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ConfigurationPtr const& configuration);

    // Default construction and copy semantics are prohibited

    DatabaseServicesPool() = delete;
    DatabaseServicesPool(DatabaseServicesPool const&) = delete;
    DatabaseServicesPool& operator=(DatabaseServicesPool const&) = delete;

    ~DatabaseServicesPool() override = default;

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
                            bool enabledWorkersOnly) final;

    /// @see DatabaseServices::findReplicas()
    void findReplicas(std::vector<ReplicaInfo>& replicas,
                      unsigned int chunk,
                      std::string const& database,
                      bool enabledWorkersOnly) final;

    /// @see DatabaseServices::findWorkerReplicas()
    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            std::string const& worker,
                            std::string const& database) final;

    /// @see DatabaseServices::numWorkerReplicas()
    uint64_t numWorkerReplicas(std::string const& worker,
                               std::string const& database=std::string()) final;

    /// @see DatabaseServices::findWorkerReplicas()
    void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                            unsigned int chunk,
                            std::string const& worker,
                            std::string const& databaseFamily) final;

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

private:
    /**
     * Construct the object.
     *
     * @param configuration
     *   the configuration service
     */
    explicit DatabaseServicesPool(ConfigurationPtr const& configuration);

    /**
     * Allocate the next available service object.
     *
     * @note
     *   the requester must return the service back after it's no longer needed.
     *
     * @return
     *   pointer to a service
     *
     * @see DatabaseServicesPool::_releaseService()
     */
    DatabaseServices::Ptr _allocateService();

    /**
     * Return a service object back into the pool of the available ones.
     *
     * @param service
     *   service object to be returned back
     *
     * @throws std::logic_error
     *   if the service object was not previously allocated
     *
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
