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

// Class header
#include "replica/DatabaseServicesPool.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServicesPool");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

// ======================
// == ServiceAllocator ==
// ======================

/**
 * Class ServiceAllocator implements the RAII paradigm by allocating
 * a service (and storing its reference in the corresponding data member)
 * from the pool in the class's constructor and releasing it back into
 * the pool in the destructor.
 */
class ServiceAllocator {
public:
    
    ServiceAllocator(DatabaseServicesPool::Ptr const& pool)
        :   _pool(pool),
            _service(pool->allocateService()) {
    }

    // Default construction and copy semantics are prohibited

    ServiceAllocator() = delete;
    ServiceAllocator(ServiceAllocator const&) = delete;
    ServiceAllocator& operator=(ServiceAllocator const&) = delete;

    ~ServiceAllocator() {
        _pool->releaseService(_service);
    }

    /// @return a reference to the allocated service
    DatabaseServices::Ptr const& operator()() { return _service; }

private:
    DatabaseServicesPool::Ptr const _pool;
    DatabaseServices::Ptr const _service;
};

// ==========================
// == DatabaseServicesPool ==
// ==========================

DatabaseServicesPool::Ptr DatabaseServicesPool::create(Configuration::Ptr const& configuration) {
    return DatabaseServicesPool::Ptr(new DatabaseServicesPool(configuration));
}

DatabaseServicesPool::DatabaseServicesPool(Configuration::Ptr const& configuration)
    :   DatabaseServices() {

    for (size_t i = 0; i < configuration->databaseServicesPoolSize(); ++i) {
        _availableServices.push_back(DatabaseServices::create(configuration));
    }
}

void DatabaseServicesPool::saveState(ControllerIdentity const& identity,
                                      uint64_t startTime) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(identity, startTime);
}

void DatabaseServicesPool::saveState(Job const& job,
                                      Job::Options const& options) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(job, options);
}

void DatabaseServicesPool::updateHeartbeatTime(Job const& job) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->updateHeartbeatTime(job);
}

void DatabaseServicesPool::saveState(QservMgtRequest const& request,
                                      Performance const& performance,
                                      std::string const& serverError) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(request, performance, serverError);
}

void DatabaseServicesPool::saveState(Request const& request,
                                     Performance const& performance) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(request, performance);
}

void DatabaseServicesPool::updateRequestState(Request const& request,
                                              std::string const& targetRequestId,
                                              Performance const& targetRequestPerformance) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->updateRequestState(
        request,
        targetRequestId,
        targetRequestPerformance
    );
}

void DatabaseServicesPool::saveReplicaInfo(ReplicaInfo const& info) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveReplicaInfo(info);
}

void DatabaseServicesPool::saveReplicaInfoCollection(std::string const& worker,
                                                     std::string const& database,
                                                     ReplicaInfoCollection const& newReplicaInfoCollection) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveReplicaInfoCollection(worker,
                                         database,
                                         newReplicaInfoCollection);
}

void DatabaseServicesPool::findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                                              size_t maxReplicas,
                                              bool enabledWorkersOnly) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findOldestReplicas(replicas,
                                  maxReplicas,
                                  enabledWorkersOnly);
}

void DatabaseServicesPool::findReplicas(std::vector<ReplicaInfo>& replicas,
                                        unsigned int chunk,
                                        std::string const& database,
                                        bool enabledWorkersOnly) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findReplicas(replicas,
                            chunk,
                            database,
                            enabledWorkersOnly);
}

void DatabaseServicesPool::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                              std::string const& worker,
                                              std::string const& database) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findWorkerReplicas(replicas,
                                  worker,
                                  database);
}

uint64_t DatabaseServicesPool::numWorkerReplicas(std::string const& worker,
                                                 std::string const& database) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->numWorkerReplicas(worker,
                                        database);
}

void DatabaseServicesPool::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                              unsigned int chunk,
                                              std::string const& worker,
                                              std::string const& databaseFamily) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findWorkerReplicas(replicas,
                                  chunk,
                                  worker,
                                  databaseFamily);
}

std::map<unsigned int, size_t> DatabaseServicesPool::actualReplicationLevel(
                                    std::string const& database,
                                    std::vector<std::string> const& workersToExclude) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->actualReplicationLevel(database,
                                             workersToExclude);
}


size_t DatabaseServicesPool::numOrphanChunks(std::string const& database,
                                             std::vector<std::string> const& uniqueOnWorkers) {

    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->numOrphanChunks(database,
                                      uniqueOnWorkers);
}


DatabaseServices::Ptr DatabaseServicesPool::allocateService() {

    std::string const context = "DatabaseServicesPool::allocateService  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    std::unique_lock<std::mutex> lock(_mtx);

    auto self = shared_from_base<DatabaseServicesPool>();

    _available.wait(lock, [self]() {
        return not self->_availableServices.empty();
    });
    
    // Get the next request and move it between queues.

    DatabaseServices::Ptr service = _availableServices.front();
    _availableServices.pop_front();
    _usedServices.push_back(service);

    return service;
}

void DatabaseServicesPool::releaseService(DatabaseServices::Ptr const& service) {

    std::string const context = "DatabaseServicesPool::releaseService  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    std::unique_lock<std::mutex> lock(_mtx);

    // Move it between queues.

    size_t numRemoved = 0;
    _usedServices.remove_if(
        [&numRemoved, &service] (DatabaseServices::Ptr const& ptr) {
            if (ptr == service) {
                numRemoved++;
                return true;
            }
            return false;
        }
    );
    if (1 != numRemoved) {
        throw std::logic_error(
                "DatabaseServicesPool::releaseService  inappropriate use of the method");
    }
    _availableServices.push_back(service);

    // Notify one client (if any) waiting for a service

    lock.unlock();
    _available.notify_one();
}

}}} // namespace lsst::qserv::replica
