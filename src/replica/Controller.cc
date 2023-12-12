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
#include "replica/Controller.h"

// System headers
#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>

// Qserv headers
#include "replica/Common.h"
#include "replica/ConfigCzar.h"
#include "replica/Configuration.h"
#include "replica/ConfigWorker.h"
#include "replica/DatabaseServices.h"
#include "replica/DeleteRequest.h"
#include "replica/DisposeRequest.h"
#include "replica/EchoRequest.h"
#include "replica/FileUtils.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/DirectorIndexRequest.h"
#include "replica/Messenger.h"
#include "replica/Performance.h"
#include "replica/Registry.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlAlterTablesRequest.h"
#include "replica/SqlQueryRequest.h"
#include "replica/SqlCreateDbRequest.h"
#include "replica/SqlCreateIndexesRequest.h"
#include "replica/SqlCreateTableRequest.h"
#include "replica/SqlCreateTablesRequest.h"
#include "replica/SqlDeleteDbRequest.h"
#include "replica/SqlDeleteTablePartitionRequest.h"
#include "replica/SqlDeleteTableRequest.h"
#include "replica/SqlDisableDbRequest.h"
#include "replica/SqlDropIndexesRequest.h"
#include "replica/SqlGetIndexesRequest.h"
#include "replica/SqlEnableDbRequest.h"
#include "replica/SqlGrantAccessRequest.h"
#include "replica/SqlRemoveTablePartitionsRequest.h"
#include "replica/SqlRowStatsRequest.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::chrono_literals;
using namespace lsst::qserv::replica;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Controller");

void tracker(weak_ptr<Controller> const& controller, string const& context) {
    LOGS(_log, LOG_LVL_INFO, context << "started tracking workers.");
    while (true) {
        Controller::Ptr const ptr = controller.lock();
        if (ptr == nullptr) break;

        auto const config = ptr->serviceProvider()->config();
        bool const autoRegisterWorkers =
                config->get<unsigned int>("controller", "auto-register-workers") != 0;
        vector<ConfigWorker> workers;
        try {
            workers = ptr->serviceProvider()->registry()->workers();
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN,
                 context << "failed to pull worker info from the registry, ex: " << ex.what());
        }
        for (auto&& worker : workers) {
            try {
                if (config->isKnownWorker(worker.name)) {
                    auto const prevWorker = config->worker(worker.name);
                    if (prevWorker != worker) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "worker '" << worker.name << "' logged in from '" << worker.svcHost
                                     << "'. Updating worker's record in the configuration.");
                        config->updateWorker(worker);
                    }
                } else {
                    if (autoRegisterWorkers) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "new worker '" << worker.name << "' logged in from '"
                                     << worker.svcHost << "'. Registering new worker in the configuration.");
                        config->addWorker(worker);
                    }
                }
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_WARN,
                     context << "failed to process worker info, worker '" << worker.name
                             << "', ex: " << ex.what());
            }
        }
        bool const autoRegisterCzars = config->get<unsigned int>("controller", "auto-register-czars") != 0;
        vector<ConfigCzar> czars;
        try {
            czars = ptr->serviceProvider()->registry()->czars();
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN,
                 context << "failed to pull Czar info from the registry, ex: " << ex.what());
        }
        for (auto&& czar : czars) {
            try {
                if (config->isKnownCzar(czar.name)) {
                    auto const prevCzar = config->czar(czar.name);
                    if (prevCzar != czar) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "Czar '" << czar.name << "' logged in from '" << czar.host
                                     << "'. Updating Czar's record in the configuration.");
                        config->updateCzar(czar);
                    }
                } else {
                    if (autoRegisterCzars) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "new Czar '" << czar.name << "' logged in from '" << czar.host
                                     << "'. Registering new Czar in the configuration.");
                        config->addCzar(czar);
                    }
                }
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_WARN,
                     context << "failed to process Czar info, Czar '" << czar.name << "', ex: " << ex.what());
            }
        }
        this_thread::sleep_for(
                chrono::seconds(max(1U, config->get<unsigned int>("registry", "heartbeat-ival-sec"))));
    }
    LOGS(_log, LOG_LVL_INFO, context << "finished tracking workers.");
}

}  // namespace

namespace lsst::qserv::replica {

ostream& operator<<(ostream& os, ControllerIdentity const& identity) {
    os << "ControllerIdentity(id=" << identity.id << ",host=" << identity.host << ",pid=" << identity.pid
       << ")";
    return os;
}

Controller::Ptr Controller::create(ServiceProvider::Ptr const& serviceProvider) {
    auto const ptr = Controller::Ptr(new Controller(serviceProvider));

    // The code below is starting the worker status tracking algorithm that would
    // be running in the detached thread. The thread will cache the weak pointer to
    // the Controller and check its status to see if the Controller is still alive.
    // And if it's not then the thread would terminate. This technique is needed to
    // avoid having the live pointer to the Controller within the thread that would
    // prevent the normal completion of the process.
    //
    // IMPORTANT: updated states of the configuration parameters are obtained at each
    // iteration of the 'for' loop to allow external control over enabling/disabling
    // new workers to join the cluster. Also note that the automatic registration of
    // workers should be only allowed in the Master Replication Controller.

    string const context = ptr->_context(__func__) + "  ";
    weak_ptr<Controller> w = ptr;
    thread t([controller = move(w), context]() { ::tracker(controller, context); });
    t.detach();
    return ptr;
}

Controller::Controller(ServiceProvider::Ptr const& serviceProvider)
        : _identity({Generators::uniqueId(), boost::asio::ip::host_name(), getpid()}),
          _startTime(util::TimeUtils::now()),
          _serviceProvider(serviceProvider) {
    serviceProvider->databaseServices()->saveState(_identity, _startTime);
}

string Controller::_context(string const& func) const {
    return "R-CONTR " + _identity.id + "  " + _identity.host + "[" + to_string(_identity.pid) + "]  " + func;
}

void Controller::verifyFolders(bool createMissingFolders) const {
    vector<string> const folders = {
            serviceProvider()->config()->get<string>("database", "qserv-master-tmp-dir")};
    FileUtils::verifyFolders("CONTROLLER", folders, createMissingFolders);
}

ReplicationRequest::Ptr Controller::replicate(string const& workerName, string const& sourceWorkerName,
                                              string const& database, unsigned int chunk,
                                              ReplicationRequest::CallbackType const& onFinish, int priority,
                                              bool keepTracking, bool allowDuplicate, string const& jobId,
                                              unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<ReplicationRequest, decltype(sourceWorkerName), decltype(database), decltype(chunk),
                   decltype(allowDuplicate)>(workerName, sourceWorkerName, database, chunk, allowDuplicate,
                                             onFinish, priority, keepTracking, jobId,
                                             requestExpirationIvalSec);
}

DeleteRequest::Ptr Controller::deleteReplica(string const& workerName, string const& database,
                                             unsigned int chunk, DeleteRequest::CallbackType const& onFinish,
                                             int priority, bool keepTracking, bool allowDuplicate,
                                             string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<DeleteRequest, decltype(database), decltype(chunk), decltype(allowDuplicate)>(
            workerName, database, chunk, allowDuplicate, onFinish, priority, keepTracking, jobId,
            requestExpirationIvalSec);
}

FindRequest::Ptr Controller::findReplica(string const& workerName, string const& database, unsigned int chunk,
                                         FindRequest::CallbackType const& onFinish, int priority,
                                         bool computeCheckSum, bool keepTracking, string const& jobId,
                                         unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<FindRequest, decltype(database), decltype(chunk), decltype(computeCheckSum)>(
            workerName, database, chunk, computeCheckSum, onFinish, priority, keepTracking, jobId,
            requestExpirationIvalSec);
}

FindAllRequest::Ptr Controller::findAllReplicas(string const& workerName, string const& database,
                                                bool saveReplicaInfo,
                                                FindAllRequest::CallbackType const& onFinish, int priority,
                                                bool keepTracking, string const& jobId,
                                                unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<FindAllRequest, decltype(database), decltype(saveReplicaInfo)>(
            workerName, database, saveReplicaInfo, onFinish, priority, keepTracking, jobId,
            requestExpirationIvalSec);
}

EchoRequest::Ptr Controller::echo(string const& workerName, string const& data, uint64_t delay,
                                  EchoRequest::CallbackType const& onFinish, int priority, bool keepTracking,
                                  string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<EchoRequest, decltype(data), decltype(delay)>(
            workerName, data, delay, onFinish, priority, keepTracking, jobId, requestExpirationIvalSec);
}

DirectorIndexRequest::Ptr Controller::directorIndex(string const& workerName, string const& database,
                                                    string const& directorTable, unsigned int chunk,
                                                    bool hasTransactions, TransactionId transactionId,
                                                    DirectorIndexRequest::CallbackType const& onFinish,
                                                    int priority, bool keepTracking, string const& jobId,
                                                    unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<DirectorIndexRequest, decltype(database), decltype(directorTable), decltype(chunk),
                   decltype(hasTransactions), decltype(transactionId)>(
            workerName, database, directorTable, chunk, hasTransactions, transactionId, onFinish, priority,
            keepTracking, jobId, requestExpirationIvalSec);
}

SqlAlterTablesRequest::Ptr Controller::sqlAlterTables(
        string const& workerName, string const& database, vector<string> const& tables,
        string const& alterSpec, function<void(SqlAlterTablesRequest::Ptr)> const& onFinish, int priority,
        bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlAlterTablesRequest, decltype(database), decltype(tables), decltype(alterSpec)>(
            workerName, database, tables, alterSpec, onFinish, priority, keepTracking, jobId,
            requestExpirationIvalSec);
}

SqlQueryRequest::Ptr Controller::sqlQuery(string const& workerName, string const& query, string const& user,
                                          string const& password, uint64_t maxRows,
                                          SqlQueryRequest::CallbackType const& onFinish, int priority,
                                          bool keepTracking, string const& jobId,
                                          unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlQueryRequest, decltype(query), decltype(user), decltype(password), decltype(maxRows)>(
            workerName, query, user, password, maxRows, onFinish, priority, keepTracking, jobId,
            requestExpirationIvalSec);
}

SqlCreateDbRequest::Ptr Controller::sqlCreateDb(string const& workerName, string const& database,
                                                SqlCreateDbRequest::CallbackType const& onFinish,
                                                int priority, bool keepTracking, string const& jobId,
                                                unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlCreateDbRequest, decltype(database)>(workerName, database, onFinish, priority,
                                                           keepTracking, jobId, requestExpirationIvalSec);
}

SqlDeleteDbRequest::Ptr Controller::sqlDeleteDb(string const& workerName, string const& database,
                                                SqlDeleteDbRequest::CallbackType const& onFinish,
                                                int priority, bool keepTracking, string const& jobId,
                                                unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlDeleteDbRequest, decltype(database)>(workerName, database, onFinish, priority,
                                                           keepTracking, jobId, requestExpirationIvalSec);
}

SqlEnableDbRequest::Ptr Controller::sqlEnableDb(string const& workerName, string const& database,
                                                SqlEnableDbRequest::CallbackType const& onFinish,
                                                int priority, bool keepTracking, string const& jobId,
                                                unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlEnableDbRequest, decltype(database)>(workerName, database, onFinish, priority,
                                                           keepTracking, jobId, requestExpirationIvalSec);
}

SqlDisableDbRequest::Ptr Controller::sqlDisableDb(string const& workerName, string const& database,
                                                  SqlDisableDbRequest::CallbackType const& onFinish,
                                                  int priority, bool keepTracking, string const& jobId,
                                                  unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlDisableDbRequest, decltype(database)>(workerName, database, onFinish, priority,
                                                            keepTracking, jobId, requestExpirationIvalSec);
}

SqlGrantAccessRequest::Ptr Controller::sqlGrantAccess(string const& workerName, string const& database,
                                                      string const& user,
                                                      SqlGrantAccessRequest::CallbackType const& onFinish,
                                                      int priority, bool keepTracking, string const& jobId,
                                                      unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlGrantAccessRequest, decltype(database), decltype(user)>(
            workerName, database, user, onFinish, priority, keepTracking, jobId, requestExpirationIvalSec);
}

SqlCreateIndexesRequest::Ptr Controller::sqlCreateTableIndexes(
        string const& workerName, string const& database, vector<string> const& tables,
        SqlRequestParams::IndexSpec const& indexSpec, string const& indexName, string const& indexComment,
        vector<SqlIndexColumn> const& indexColumns,
        function<void(SqlCreateIndexesRequest::Ptr)> const& onFinish, int priority, bool keepTracking,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlCreateIndexesRequest, decltype(database), decltype(tables), decltype(indexSpec),
                   decltype(indexName), decltype(indexComment), decltype(indexColumns)>(
            workerName, database, tables, indexSpec, indexName, indexComment, indexColumns, onFinish,
            priority, keepTracking, jobId, requestExpirationIvalSec);
}

SqlCreateTableRequest::Ptr Controller::sqlCreateTable(string const& workerName, string const& database,
                                                      string const& table, string const& engine,
                                                      string const& partitionByColumn,
                                                      list<SqlColDef> const& columns,
                                                      SqlCreateTableRequest::CallbackType const& onFinish,
                                                      int priority, bool keepTracking, string const& jobId,
                                                      unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlCreateTableRequest, decltype(database), decltype(table), decltype(engine),
                   decltype(partitionByColumn), decltype(columns)>(
            workerName, database, table, engine, partitionByColumn, columns, onFinish, priority, keepTracking,
            jobId, requestExpirationIvalSec);
}

SqlCreateTablesRequest::Ptr Controller::sqlCreateTables(string const& workerName, string const& database,
                                                        vector<string> const& tables, string const& engine,
                                                        string const& partitionByColumn,
                                                        list<SqlColDef> const& columns,
                                                        SqlCreateTablesRequest::CallbackType const& onFinish,
                                                        int priority, bool keepTracking, string const& jobId,
                                                        unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlCreateTablesRequest, decltype(database), decltype(tables), decltype(engine),
                   decltype(partitionByColumn), decltype(columns)>(
            workerName, database, tables, engine, partitionByColumn, columns, onFinish, priority,
            keepTracking, jobId, requestExpirationIvalSec);
}

SqlDeleteTableRequest::Ptr Controller::sqlDeleteTable(string const& workerName, string const& database,
                                                      vector<string> const& tables,
                                                      SqlDeleteTableRequest::CallbackType const& onFinish,
                                                      int priority, bool keepTracking, string const& jobId,
                                                      unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlDeleteTableRequest, decltype(database), decltype(tables)>(
            workerName, database, tables, onFinish, priority, keepTracking, jobId, requestExpirationIvalSec);
}

SqlRemoveTablePartitionsRequest::Ptr Controller::sqlRemoveTablePartitions(
        string const& workerName, string const& database, vector<string> const& tables,
        SqlRemoveTablePartitionsRequest::CallbackType const& onFinish, int priority, bool keepTracking,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlRemoveTablePartitionsRequest, decltype(database), decltype(tables)>(
            workerName, database, tables, onFinish, priority, keepTracking, jobId, requestExpirationIvalSec);
}

SqlDeleteTablePartitionRequest::Ptr Controller::sqlDeleteTablePartition(
        string const& workerName, string const& database, vector<string> const& tables,
        TransactionId transactionId, SqlDeleteTablePartitionRequest::CallbackType const& onFinish,
        int priority, bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlDeleteTablePartitionRequest, decltype(database), decltype(tables),
                   decltype(transactionId)>(workerName, database, tables, transactionId, onFinish, priority,
                                            keepTracking, jobId, requestExpirationIvalSec);
}

SqlDropIndexesRequest::Ptr Controller::sqlDropTableIndexes(
        string const& workerName, string const& database, vector<string> const& tables,
        string const& indexName, function<void(SqlDropIndexesRequest::Ptr)> const& onFinish, int priority,
        bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlDropIndexesRequest, decltype(database), decltype(tables), decltype(indexName)>(
            workerName, database, tables, indexName, onFinish, priority, keepTracking, jobId,
            requestExpirationIvalSec);
}

SqlGetIndexesRequest::Ptr Controller::sqlGetTableIndexes(
        string const& workerName, string const& database, vector<string> const& tables,
        function<void(SqlGetIndexesRequest::Ptr)> const& onFinish, int priority, bool keepTracking,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlGetIndexesRequest, decltype(database), decltype(tables)>(
            workerName, database, tables, onFinish, priority, keepTracking, jobId, requestExpirationIvalSec);
}

SqlRowStatsRequest::Ptr Controller::sqlRowStats(
        std::string const& workerName, string const& database, std::vector<std::string> const& tables,
        std::function<void(std::shared_ptr<SqlRowStatsRequest>)> const& onFinish, int priority,
        bool keepTracking, std::string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<SqlRowStatsRequest, decltype(database), decltype(tables)>(
            workerName, database, tables, onFinish, priority, keepTracking, jobId, requestExpirationIvalSec);
}

DisposeRequest::Ptr Controller::dispose(string const& workerName, vector<string> const& targetIds,
                                        function<void(DisposeRequest::Ptr)> const& onFinish, int priority,
                                        bool keepTracking, string const& jobId,
                                        unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));
    return _submit<DisposeRequest, decltype(targetIds)>(workerName, targetIds, onFinish, priority,
                                                        keepTracking, jobId, requestExpirationIvalSec);
}

ServiceSuspendRequest::Ptr Controller::suspendWorkerService(
        string const& workerName, ServiceSuspendRequest::CallbackType const& onFinish, int priority,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);
    return _submit<ServiceSuspendRequest>(workerName, onFinish, priority, jobId, requestExpirationIvalSec);
}

ServiceResumeRequest::Ptr Controller::resumeWorkerService(string const& workerName,
                                                          ServiceResumeRequest::CallbackType const& onFinish,
                                                          int priority, string const& jobId,
                                                          unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);
    return _submit<ServiceResumeRequest>(workerName, onFinish, priority, jobId, requestExpirationIvalSec);
}

ServiceStatusRequest::Ptr Controller::statusOfWorkerService(
        string const& workerName, ServiceStatusRequest::CallbackType const& onFinish, int priority,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);
    return _submit<ServiceStatusRequest>(workerName, onFinish, priority, jobId, requestExpirationIvalSec);
}

ServiceRequestsRequest::Ptr Controller::requestsOfWorkerService(
        string const& workerName, ServiceRequestsRequest::CallbackType const& onFinish, int priority,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__) + "  workerName: " << workerName);
    return _submit<ServiceRequestsRequest>(workerName, onFinish, priority, jobId, requestExpirationIvalSec);
}

ServiceDrainRequest::Ptr Controller::drainWorkerService(string const& workerName,
                                                        ServiceDrainRequest::CallbackType const& onFinish,
                                                        int priority, string const& jobId,
                                                        unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);
    return _submit<ServiceDrainRequest>(workerName, onFinish, priority, jobId, requestExpirationIvalSec);
}

ServiceReconfigRequest::Ptr Controller::reconfigWorkerService(
        string const& workerName, ServiceReconfigRequest::CallbackType const& onFinish, int priority,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);
    return _submit<ServiceReconfigRequest>(workerName, onFinish, priority, jobId, requestExpirationIvalSec);
}

size_t Controller::numActiveRequests() const {
    replica::Lock lock(_mtx, _context(__func__));
    return _registry.size();
}

void Controller::_debug(string const& func, string const& msg) const {
    LOGS(_log, LOG_LVL_TRACE, _context(func) << "  " << msg);
}

void Controller::_finish(string const& id) {
    // IMPORTANT: Make sure the lock is released before sending notifications:
    // - to avoid a possibility of deadlocking in case if the callback function
    //   to be notified will be doing any API calls of the controller.
    // - to reduce the controller API dead-time due to a prolonged
    //   execution time of of the callback function.
    RequestWrapper::Ptr request;
    {
        replica::Lock lock(_mtx, _context(__func__));
        request = _registry[id];
        _registry.erase(id);
    }
    request->notify();
}

void Controller::_assertIsRunning() const {
    if (not serviceProvider()->isRunning()) {
        throw runtime_error("ServiceProvider::" + string(__func__) + "  not running");
    }
}

void Controller::_logManagementRequest(string const& requestName, string const& workerName) {
    LOGS(_log, LOG_LVL_TRACE,
         _context(__func__) << "  workerName: " << workerName << "  requestName: " << requestName);
}

}  // namespace lsst::qserv::replica
