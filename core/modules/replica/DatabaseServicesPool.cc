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

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->saveState(identity, startTime);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::saveState(Job const& job,
                                      Job::Options const& options) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->saveState(job, options);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::updateHeartbeatTime(Job const& job) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->updateHeartbeatTime(job);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::saveState(QservMgtRequest const& request,
                                      Performance const& performance,
                                      std::string const& serverError) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->saveState(request, performance, serverError);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::saveState(Request const& request,
                                     Performance const& performance) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->saveState(request, performance);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::updateRequestState(Request const& request,
                                              std::string const& targetRequestId,
                                              Performance const& targetRequestPerformance) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->updateRequestState(request,
                                    targetRequestId,
                                    targetRequestPerformance);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::saveReplicaInfo(ReplicaInfo const& info) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->saveReplicaInfo(info);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::saveReplicaInfoCollection(std::string const& worker,
                                                     std::string const& database,
                                                     ReplicaInfoCollection const& newReplicaInfoCollection) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->saveReplicaInfoCollection(worker,
                                           database,
                                           newReplicaInfoCollection);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                                              size_t maxReplicas,
                                              bool enabledWorkersOnly) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->findOldestReplicas(replicas,
                                    maxReplicas,
                                    enabledWorkersOnly);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::findReplicas(std::vector<ReplicaInfo>& replicas,
                                        unsigned int chunk,
                                        std::string const& database,
                                        bool enabledWorkersOnly) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->findReplicas(replicas,
                              chunk,
                              database,
                              enabledWorkersOnly);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                              std::string const& worker,
                                              std::string const& database) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->findWorkerReplicas(replicas,
                                    worker,
                                    database);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
}

void DatabaseServicesPool::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                              unsigned int chunk,
                                              std::string const& worker,
                                              std::string const& databaseFamily) {

    // IMPLEMENTATION NOTE: exceptions are temporary intercepted (and re-thrown)
    // in order to release allocated services.

    auto service = allocateService();
    try {
        service->findWorkerReplicas(replicas,
                                    chunk,
                                    worker,
                                    databaseFamily);
        releaseService(service);

    } catch (std::exception const& ex) {
        releaseService(service);
        throw;
    }
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
                "DatabaseServicesPool::releaseService  inappropried use of the method");
    }
    _availableServices.push_back(service);

    // Notify one cient (if any) waiting for a service

    lock.unlock();
    _available.notify_one();
}

}}} // namespace lsst::qserv::replica
