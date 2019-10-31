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
#include <iostream>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/DeleteRequest.h"
#include "replica/EchoRequest.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/IndexRequest.h"
#include "replica/Messenger.h"
#include "replica/Performance.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlQueryRequest.h"
#include "replica/SqlCreateDbRequest.h"
#include "replica/SqlCreateTableRequest.h"
#include "replica/SqlDeleteDbRequest.h"
#include "replica/SqlDeleteTablePartitionRequest.h"
#include "replica/SqlDeleteTableRequest.h"
#include "replica/SqlDisableDbRequest.h"
#include "replica/SqlEnableDbRequest.h"
#include "replica/SqlGrantAccessRequest.h"
#include "replica/SqlRemoveTablePartitionsRequest.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Controller");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

ostream& operator <<(ostream& os, ControllerIdentity const& identity) {
    os  << "ControllerIdentity(id=" << identity.id << ",host=" << identity.host << ",pid=" << identity.pid << ")";
    return os;
}


Controller::Ptr Controller::create(ServiceProvider::Ptr const& serviceProvider) {
    return Controller::Ptr(new Controller(serviceProvider));
}


Controller::Controller(ServiceProvider::Ptr const& serviceProvider)
    :   _identity({
            Generators::uniqueId(),
            boost::asio::ip::host_name(),
            getpid()}),
        _startTime(PerformanceUtils::now()),
        _serviceProvider(serviceProvider) {

    serviceProvider->databaseServices()->saveState(_identity, _startTime);
}

            
string Controller::_context(string const& func) const {
    return "R-CONTR " + _identity.id + "  " + _identity.host +
           "[" + to_string(_identity.pid) + "]  " + func;
}


ReplicationRequest::Ptr Controller::replicate(
        string const& workerName,
        string const& sourceWorkerName,
        string const& database,
        unsigned int chunk,
        ReplicationRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        bool allowDuplicate,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<ReplicationRequest,
                   decltype(sourceWorkerName),
                   decltype(database),
                   decltype(chunk),
                   decltype(allowDuplicate)>(
        workerName,
        sourceWorkerName,
        database,
        chunk,
        allowDuplicate,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


DeleteRequest::Ptr Controller::deleteReplica(
        string const& workerName,
        string const& database,
        unsigned int chunk,
        DeleteRequest::CallbackType const& onFinish,
        int  priority,
        bool keepTracking,
        bool allowDuplicate,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<DeleteRequest,
                   decltype(database),
                   decltype(chunk),
                   decltype(allowDuplicate)>(
        workerName,
        database,
        chunk,
        allowDuplicate,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


FindRequest::Ptr Controller::findReplica(
        string const& workerName,
        string const& database,
        unsigned int chunk,
        FindRequest::CallbackType const& onFinish,
        int  priority,
        bool computeCheckSum,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<FindRequest,
                   decltype(database),
                   decltype(chunk),
                   decltype(computeCheckSum)>(
        workerName,
        database,
        chunk,
        computeCheckSum,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


FindAllRequest::Ptr Controller::findAllReplicas(
        string const& workerName,
        string const& database,
        bool saveReplicaInfo,
        FindAllRequest::CallbackType const& onFinish,
        int  priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<FindAllRequest,
                   decltype(database),
                   decltype(saveReplicaInfo)>(
        workerName,
        database,
        saveReplicaInfo,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


EchoRequest::Ptr Controller::echo(
        string const& workerName,
        string const& data,
        uint64_t delay,
        EchoRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<EchoRequest,
                   decltype(data),
                   decltype(delay)>(
        workerName,
        data,
        delay,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


IndexRequest::Ptr Controller::index(
        string const& workerName,
        string const& database,
        unsigned int chunk,
        bool hasTransactions,
        uint32_t transactionId,
        IndexRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        std::string const& jobId,
        unsigned int requestExpirationIvalSec) {
 
    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<IndexRequest,
                   decltype(database),
                   decltype(chunk),
                   decltype(hasTransactions),
                   decltype(transactionId)>(
        workerName,
        database,
        chunk,
        hasTransactions,
        transactionId,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlQueryRequest::Ptr Controller::sqlQuery(
        string const& workerName,
        string const& query,
        string const& user,
        string const& password,
        uint64_t maxRows,
        SqlQueryRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlQueryRequest,
                   decltype(query),
                   decltype(user),
                   decltype(password),
                   decltype(maxRows)>(
        workerName,
        query,
        user,
        password,
        maxRows,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlCreateDbRequest::Ptr Controller::sqlCreateDb(
        string const& workerName,
        string const& database,
        SqlCreateDbRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlCreateDbRequest,
                   decltype(database)>(
        workerName,
        database,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlDeleteDbRequest::Ptr Controller::sqlDeleteDb(
        string const& workerName,
        string const& database,
        SqlDeleteDbRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlDeleteDbRequest,
                   decltype(database)>(
        workerName,
        database,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlEnableDbRequest::Ptr Controller::sqlEnableDb(
        string const& workerName,
        string const& database,
        SqlEnableDbRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlEnableDbRequest,
                   decltype(database)>(
        workerName,
        database,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlDisableDbRequest::Ptr Controller::sqlDisableDb(
        string const& workerName,
        string const& database,
        SqlDisableDbRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlDisableDbRequest,
                   decltype(database)>(
        workerName,
        database,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlGrantAccessRequest::Ptr Controller::sqlGrantAccess(
        string const& workerName,
        string const& database,
        string const& user,
        SqlGrantAccessRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlGrantAccessRequest,
                   decltype(database),
                   decltype(user)>(
        workerName,
        database,
        user,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlCreateTableRequest::Ptr Controller::sqlCreateTable(
        string const& workerName,
        string const& database,
        string const& table,
        string const& engine,
        string const& partitionByColumn,
        list<SqlColDef> const& columns,
        SqlCreateTableRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlCreateTableRequest,
                   decltype(database),
                   decltype(table),
                   decltype(engine),
                   decltype(partitionByColumn),
                   decltype(columns)>(
        workerName,
        database,
        table,
        engine,
        partitionByColumn,
        columns,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlDeleteTableRequest::Ptr Controller::sqlDeleteTable(
        string const& workerName,
        string const& database,
        string const& table,
        SqlDeleteTableRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlDeleteTableRequest,
                   decltype(database),
                   decltype(table)>(
        workerName,
        database,
        table,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlRemoveTablePartitionsRequest::Ptr Controller::sqlRemoveTablePartitions(
        string const& workerName,
        string const& database,
        string const& table,
        SqlRemoveTablePartitionsRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlRemoveTablePartitionsRequest,
                   decltype(database),
                   decltype(table)>(
        workerName,
        database,
        table,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


SqlDeleteTablePartitionRequest::Ptr Controller::sqlDeleteTablePartition(
        string const& workerName,
        string const& database,
        string const& table,
        uint32_t transactionId,
        SqlDeleteTablePartitionRequest::CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__));

    return _submit<SqlDeleteTablePartitionRequest,
                   decltype(database),
                   decltype(table),
                   decltype(transactionId)>(
        workerName,
        database,
        table,
        transactionId,
        onFinish,
        priority,
        keepTracking,
        jobId,
        requestExpirationIvalSec);
}


ServiceSuspendRequest::Ptr Controller::suspendWorkerService(
        string const& workerName,
        ServiceSuspendRequest::CallbackType const& onFinish,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);

    return _submit<ServiceSuspendRequest>(
        workerName,
        onFinish,
        jobId,
        requestExpirationIvalSec);
}


ServiceResumeRequest::Ptr Controller::resumeWorkerService(
        string const& workerName,
        ServiceResumeRequest::CallbackType const& onFinish,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);

    return _submit<ServiceResumeRequest>(
        workerName,
        onFinish,
        jobId,
        requestExpirationIvalSec);
}


ServiceStatusRequest::Ptr Controller::statusOfWorkerService(
        string const& workerName,
        ServiceStatusRequest::CallbackType const& onFinish,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);

    return _submit<ServiceStatusRequest>(
        workerName,
        onFinish,
        jobId,
        requestExpirationIvalSec);
}


ServiceRequestsRequest::Ptr Controller::requestsOfWorkerService(
        string const& workerName,
        ServiceRequestsRequest::CallbackType const& onFinish,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__) + "  workerName: " << workerName);

    return _submit<ServiceRequestsRequest>(
        workerName,
        onFinish,
        jobId,
        requestExpirationIvalSec);
}


ServiceDrainRequest::Ptr Controller::drainWorkerService(
        string const& workerName,
        ServiceDrainRequest::CallbackType const& onFinish,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);

    return _submit<ServiceDrainRequest>(
        workerName,
        onFinish,
        jobId,
        requestExpirationIvalSec);
}


ServiceReconfigRequest::Ptr Controller::reconfigWorkerService(
        string const& workerName,
        ServiceReconfigRequest::CallbackType const& onFinish,
        string const& jobId,
        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName);

    return _submit<ServiceReconfigRequest>(
        workerName,
        onFinish,
        jobId,
        requestExpirationIvalSec);
}


size_t Controller::numActiveRequests() const {
    util::Lock lock(_mtx, _context(__func__));
    return _registry.size();
}


void Controller::_debug(string const& func, string const& msg) const {
    LOGS(_log, LOG_LVL_TRACE, _context(func) << "  " << msg);
}


void Controller::_finish(string const& id) {

    // IMPORTANT:
    //
    //   Make sure the lock is released before sending notifications:
    //
    //   - to avoid a possibility of deadlocking in case if
    //     the callback function to be notified will be doing
    //     any API calls of the controller.
    //
    //   - to reduce the controller API dead-time due to a prolonged
    //     execution time of of the callback function.

    RequestWrapper::Ptr request;
    {
        util::Lock lock(_mtx, _context(__func__));
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


void Controller::_logManagementRequest(string const& requestName,
                                       string const& workerName) {
    LOGS(_log, LOG_LVL_TRACE, _context(__func__) << "  workerName: " << workerName
         << "  requestName: " << requestName);
}

}}} // namespace lsst::qserv::replica
