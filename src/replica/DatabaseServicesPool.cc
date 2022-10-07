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

// Class header
#include "replica/DatabaseServicesPool.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServicesPool");

}  // namespace

namespace lsst::qserv::replica {

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
            : _pool(pool), _service(pool->_allocateService()) {}

    // Default construction and copy semantics are prohibited

    ServiceAllocator() = delete;
    ServiceAllocator(ServiceAllocator const&) = delete;
    ServiceAllocator& operator=(ServiceAllocator const&) = delete;

    ~ServiceAllocator() { _pool->_releaseService(_service); }

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

DatabaseServicesPool::DatabaseServicesPool(Configuration::Ptr const& configuration) : DatabaseServices() {
    for (size_t i = 0; i < configuration->get<size_t>("database", "services-pool-size"); ++i) {
        _availableServices.push_back(DatabaseServices::create(configuration));
    }
}

void DatabaseServicesPool::saveState(ControllerIdentity const& identity, uint64_t startTime) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(identity, startTime);
}

void DatabaseServicesPool::saveState(Job const& job) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(job);
}

void DatabaseServicesPool::updateHeartbeatTime(Job const& job) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->updateHeartbeatTime(job);
}

void DatabaseServicesPool::saveState(QservMgtRequest const& request, Performance const& performance,
                                     string const& serverError) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(request, performance, serverError);
}

void DatabaseServicesPool::saveState(Request const& request, Performance const& performance) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveState(request, performance);
}

void DatabaseServicesPool::updateRequestState(Request const& request, string const& targetRequestId,
                                              Performance const& targetRequestPerformance) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->updateRequestState(request, targetRequestId, targetRequestPerformance);
}

void DatabaseServicesPool::saveReplicaInfo(ReplicaInfo const& info) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveReplicaInfo(info);
}

void DatabaseServicesPool::saveReplicaInfoCollection(string const& worker, string const& database,
                                                     ReplicaInfoCollection const& newReplicaInfoCollection) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveReplicaInfoCollection(worker, database, newReplicaInfoCollection);
}

void DatabaseServicesPool::findOldestReplicas(vector<ReplicaInfo>& replicas, size_t maxReplicas,
                                              bool enabledWorkersOnly, bool allDatabases, bool isPublished) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findOldestReplicas(replicas, maxReplicas, enabledWorkersOnly, allDatabases, isPublished);
}

void DatabaseServicesPool::findReplicas(vector<ReplicaInfo>& replicas, unsigned int chunk,
                                        string const& database, bool enabledWorkersOnly,
                                        bool includeFileInfo) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findReplicas(replicas, chunk, database, enabledWorkersOnly, includeFileInfo);
}

void DatabaseServicesPool::findReplicas(vector<ReplicaInfo>& replicas, vector<unsigned int> const& chunks,
                                        string const& database, bool enabledWorkersOnly,
                                        bool includeFileInfo) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findReplicas(replicas, chunks, database, enabledWorkersOnly, includeFileInfo);
}

void DatabaseServicesPool::findWorkerReplicas(vector<ReplicaInfo>& replicas, string const& worker,
                                              string const& database, bool allDatabases, bool isPublished,
                                              bool includeFileInfo) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findWorkerReplicas(replicas, worker, database, allDatabases, isPublished, includeFileInfo);
}

uint64_t DatabaseServicesPool::numWorkerReplicas(string const& worker, string const& database,
                                                 bool allDatabases, bool isPublished) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->numWorkerReplicas(worker, database, allDatabases, isPublished);
}

void DatabaseServicesPool::findWorkerReplicas(vector<ReplicaInfo>& replicas, unsigned int chunk,
                                              string const& worker, string const& databaseFamily,
                                              bool allDatabases, bool isPublished) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findWorkerReplicas(replicas, chunk, worker, databaseFamily, allDatabases, isPublished);
}

void DatabaseServicesPool::findDatabaseReplicas(vector<ReplicaInfo>& replicas, string const& database,
                                                bool enabledWorkersOnly) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findDatabaseReplicas(replicas, database, enabledWorkersOnly);
}

void DatabaseServicesPool::findDatabaseChunks(vector<unsigned int>& chunks, string const& database,
                                              bool enabledWorkersOnly) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->findDatabaseChunks(chunks, database, enabledWorkersOnly);
}

map<unsigned int, size_t> DatabaseServicesPool::actualReplicationLevel(
        string const& database, vector<string> const& workersToExclude) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->actualReplicationLevel(database, workersToExclude);
}

size_t DatabaseServicesPool::numOrphanChunks(string const& database, vector<string> const& uniqueOnWorkers) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->numOrphanChunks(database, uniqueOnWorkers);
}

void DatabaseServicesPool::logControllerEvent(ControllerEvent const& event) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->logControllerEvent(event);
}

list<ControllerEvent> DatabaseServicesPool::readControllerEvents(string const& controllerId,
                                                                 uint64_t fromTimeStamp, uint64_t toTimeStamp,
                                                                 size_t maxEntries, string const& task,
                                                                 string const& operation,
                                                                 string const& operationStatus) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->readControllerEvents(controllerId, fromTimeStamp, toTimeStamp, maxEntries, task,
                                           operation, operationStatus);
}

json DatabaseServicesPool::readControllerEventDict(string const& controllerId) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->readControllerEventDict(controllerId);
}

ControllerInfo DatabaseServicesPool::controller(string const& id) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->controller(id);
}

list<ControllerInfo> DatabaseServicesPool::controllers(uint64_t fromTimeStamp, uint64_t toTimeStamp,
                                                       size_t maxEntries) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->controllers(fromTimeStamp, toTimeStamp, maxEntries);
}

RequestInfo DatabaseServicesPool::request(string const& id) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->request(id);
}

list<RequestInfo> DatabaseServicesPool::requests(string const& jobId, uint64_t fromTimeStamp,
                                                 uint64_t toTimeStamp, size_t maxEntries) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->requests(jobId, fromTimeStamp, toTimeStamp, maxEntries);
}

JobInfo DatabaseServicesPool::job(string const& id) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->job(id);
}

list<JobInfo> DatabaseServicesPool::jobs(string const& controllerId, string const& parentJobId,
                                         uint64_t fromTimeStamp, uint64_t toTimeStamp, size_t maxEntries) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->jobs(controllerId, parentJobId, fromTimeStamp, toTimeStamp, maxEntries);
}

TransactionInfo DatabaseServicesPool::transaction(TransactionId id, bool includeContext, bool includeLog) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->transaction(id, includeContext, includeLog);
}

vector<TransactionInfo> DatabaseServicesPool::transactions(string const& databaseName, bool includeContext,
                                                           bool includeLog) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->transactions(databaseName, includeContext, includeLog);
}

vector<TransactionInfo> DatabaseServicesPool::transactions(TransactionInfo::State state, bool includeContext,
                                                           bool includeLog) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->transactions(state, includeContext, includeLog);
}

TransactionInfo DatabaseServicesPool::createTransaction(string const& databaseName,
                                                        NamedMutexRegistry& namedMutexRegistry,
                                                        unique_ptr<util::Lock>& namedMutexLock,
                                                        json const& transactionContext) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->createTransaction(databaseName, namedMutexRegistry, namedMutexLock, transactionContext);
}

TransactionInfo DatabaseServicesPool::updateTransaction(TransactionId id, TransactionInfo::State newState) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->updateTransaction(id, newState);
}

TransactionInfo DatabaseServicesPool::updateTransaction(TransactionId id, json const& transactionContext) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->updateTransaction(id, transactionContext);
}

TransactionInfo DatabaseServicesPool::updateTransaction(TransactionId id,
                                                        unordered_map<string, json> const& events) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->updateTransaction(id, events);
}

TransactionContribInfo DatabaseServicesPool::transactionContrib(unsigned int id, bool includeWarnings) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->transactionContrib(id, includeWarnings);
}

vector<TransactionContribInfo> DatabaseServicesPool::transactionContribs(
        TransactionId transactionId, string const& table, string const& worker,
        TransactionContribInfo::TypeSelector typeSelector, bool includeWarnings) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->transactionContribs(transactionId, table, worker, typeSelector, includeWarnings);
}

vector<TransactionContribInfo> DatabaseServicesPool::transactionContribs(
        TransactionId transactionId, TransactionContribInfo::Status status, string const& table,
        string const& worker, TransactionContribInfo::TypeSelector typeSelector, bool includeWarnings) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->transactionContribs(transactionId, status, table, worker, typeSelector,
                                          includeWarnings);
}

vector<TransactionContribInfo> DatabaseServicesPool::transactionContribs(
        string const& database, string const& table, string const& worker,
        TransactionContribInfo::TypeSelector typeSelector, bool includeWarnings) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->transactionContribs(database, table, worker, typeSelector, includeWarnings);
}

TransactionContribInfo DatabaseServicesPool::createdTransactionContrib(
        TransactionContribInfo const& info, bool failed, TransactionContribInfo::Status statusOnFailed) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->createdTransactionContrib(info, failed, statusOnFailed);
}

TransactionContribInfo DatabaseServicesPool::updateTransactionContrib(TransactionContribInfo const& info) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->updateTransactionContrib(info);
}

DatabaseIngestParam DatabaseServicesPool::ingestParam(string const& database, string const& category,
                                                      string const& param) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->ingestParam(database, category, param);
}

vector<DatabaseIngestParam> DatabaseServicesPool::ingestParams(string const& database,
                                                               string const& category) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->ingestParams(database, category);
}

void DatabaseServicesPool::saveIngestParam(string const& database, string const& category,
                                           string const& param, string const& value) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveIngestParam(database, category, param, value);
}

TableRowStats DatabaseServicesPool::tableRowStats(string const& database, string const& table,
                                                  TransactionId transactionId) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    return service()->tableRowStats(database, table, transactionId);
}

void DatabaseServicesPool::saveTableRowStats(TableRowStats const& stats) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->saveTableRowStats(stats);
}

void DatabaseServicesPool::deleteTableRowStats(string const& databaseName, string const& tableName,
                                               ChunkOverlapSelector overlapSelector) {
    ServiceAllocator service(shared_from_base<DatabaseServicesPool>());
    service()->deleteTableRowStats(databaseName, tableName, overlapSelector);
}

DatabaseServices::Ptr DatabaseServicesPool::_allocateService() {
    string const context = "DatabaseServicesPool::" + string(__func__) + "  ";
    LOGS(_log, LOG_LVL_TRACE, context);

    unique_lock<mutex> lock(_mtx);
    auto self = shared_from_base<DatabaseServicesPool>();
    _available.wait(lock, [self]() { return not self->_availableServices.empty(); });

    // Get the next request and move it between queues.
    DatabaseServices::Ptr service = _availableServices.front();
    _availableServices.pop_front();
    _usedServices.push_back(service);

    return service;
}

void DatabaseServicesPool::_releaseService(DatabaseServices::Ptr const& service) {
    string const context = "DatabaseServicesPool::" + string(__func__) + "  ";
    LOGS(_log, LOG_LVL_TRACE, context);
    unique_lock<mutex> lock(_mtx);

    // Move it between queues.
    size_t numRemoved = 0;
    _usedServices.remove_if([&numRemoved, &service](DatabaseServices::Ptr const& ptr) {
        if (ptr == service) {
            numRemoved++;
            return true;
        }
        return false;
    });
    if (1 != numRemoved) {
        throw logic_error(context + "inappropriate use of the method");
    }
    _availableServices.push_back(service);

    // Notify one client (if any) waiting for a service
    lock.unlock();
    _available.notify_one();
}

}  // namespace lsst::qserv::replica
