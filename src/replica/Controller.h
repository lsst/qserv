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
#ifndef LSST_QSERV_REPLICA_CONTROLLER_H
#define LSST_QSERV_REPLICA_CONTROLLER_H

/**
 * This header defines the Replication Controller service for creating and
 * managing requests sent to the remote worker services.
 */

// System headers
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <thread>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/Request.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {

    class ReplicationRequest;
    class DeleteRequest;
    class FindRequest;
    class FindAllRequest;
    class EchoRequest;
    class IndexRequest;
    class SqlAlterTablesRequest;
    class SqlQueryRequest;
    class SqlCreateDbRequest;
    class SqlDeleteDbRequest;
    class SqlEnableDbRequest;
    class SqlDisableDbRequest;
    class SqlGrantAccessRequest;
    class SqlCreateIndexesRequest;
    class SqlCreateTableRequest;
    class SqlCreateTablesRequest;
    class SqlDeleteTableRequest;
    class SqlDropIndexesRequest;
    class SqlGetIndexesRequest;
    class SqlRemoveTablePartitionsRequest;
    class SqlDeleteTablePartitionRequest;
    class DisposeRequest;

    typedef std::shared_ptr<ReplicationRequest> ReplicationRequestPtr;
    typedef std::shared_ptr<DeleteRequest>      DeleteRequestPtr;
    typedef std::shared_ptr<FindRequest>    FindRequestPtr;
    typedef std::shared_ptr<FindAllRequest> FindAllRequestPtr;
    typedef std::shared_ptr<EchoRequest> EchoRequestPtr;
    typedef std::shared_ptr<IndexRequest> IndexRequestPtr;
    typedef std::shared_ptr<SqlAlterTablesRequest>           SqlAlterTablesRequestPtr;
    typedef std::shared_ptr<SqlQueryRequest>                 SqlQueryRequestPtr;
    typedef std::shared_ptr<SqlCreateDbRequest>              SqlCreateDbRequestPtr;
    typedef std::shared_ptr<SqlDeleteDbRequest>              SqlDeleteDbRequestPtr;
    typedef std::shared_ptr<SqlEnableDbRequest>              SqlEnableDbRequestPtr;
    typedef std::shared_ptr<SqlDisableDbRequest>             SqlDisableDbRequestPtr;
    typedef std::shared_ptr<SqlGrantAccessRequest>           SqlGrantAccessRequestPtr;
    typedef std::shared_ptr<SqlCreateIndexesRequest>         SqlCreateIndexesRequestPtr;
    typedef std::shared_ptr<SqlCreateTableRequest>           SqlCreateTableRequestPtr;
    typedef std::shared_ptr<SqlCreateTablesRequest>          SqlCreateTablesRequestPtr;
    typedef std::shared_ptr<SqlDeleteTableRequest>           SqlDeleteTableRequestPtr;
    typedef std::shared_ptr<SqlDropIndexesRequest>           SqlDropIndexesRequestPtr;
    typedef std::shared_ptr<SqlGetIndexesRequest>            SqlGetIndexesRequestPtr;
    typedef std::shared_ptr<SqlRemoveTablePartitionsRequest> SqlRemoveTablePartitionsRequestPtr;
    typedef std::shared_ptr<SqlDeleteTablePartitionRequest>  SqlDeleteTablePartitionRequestPtr;
    typedef std::shared_ptr<DisposeRequest> DisposeRequestPtr;

    class StopReplicationRequestPolicy;
    class StopDeleteRequestPolicy;
    class StopFindRequestPolicy;
    class StopFindAllRequestPolicy;
    class StopEchoRequestPolicy;
    class StopIndexRequestPolicy;
    class StopSqlRequestPolicy;

    template <typename POLICY> class StopRequest;

    using StopReplicationRequest = StopRequest<StopReplicationRequestPolicy>;
    using StopDeleteRequest      = StopRequest<StopDeleteRequestPolicy>;
    using StopFindRequest        = StopRequest<StopFindRequestPolicy>;
    using StopFindAllRequest     = StopRequest<StopFindAllRequestPolicy>;
    using StopEchoRequest        = StopRequest<StopEchoRequestPolicy>;
    using StopIndexRequest       = StopRequest<StopIndexRequestPolicy>;
    using StopSqlAlterTablesRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlQueryRequest       = StopRequest<StopSqlRequestPolicy>;
    using StopSqlCreateDbRequest    = StopRequest<StopSqlRequestPolicy>;
    using StopSqlDeleteDbRequest    = StopRequest<StopSqlRequestPolicy>;
    using StopSqlEnableDbRequest    = StopRequest<StopSqlRequestPolicy>;
    using StopSqlDisableDbRequest   = StopRequest<StopSqlRequestPolicy>;
    using StopSqlGrantAccessRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlCreateIndexesRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlCreateTableRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlCreateTablesRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlDeleteTableRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlDropIndexesRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlGetIndexesRequest  = StopRequest<StopSqlRequestPolicy>;
    using StopSqlRemoveTablePartitionsRequest = StopRequest<StopSqlRequestPolicy>;
    using StopSqlDeleteTablePartitionRequest  = StopRequest<StopSqlRequestPolicy>;

    typedef std::shared_ptr<StopReplicationRequest> StopReplicationRequestPtr;
    typedef std::shared_ptr<StopDeleteRequest>      StopDeleteRequestPtr;
    typedef std::shared_ptr<StopFindRequest>        StopFindRequestPtr;
    typedef std::shared_ptr<StopFindAllRequest>     StopFindAllRequestPtr;
    typedef std::shared_ptr<StopEchoRequest>        StopEchoRequestPtr;
    typedef std::shared_ptr<StopIndexRequest>       StopIndexRequestPtr;
    typedef std::shared_ptr<StopSqlAlterTablesRequest> StopSqlAlterTablesRequestPtr;
    typedef std::shared_ptr<StopSqlQueryRequest>       StopSqlQueryRequestPtr;
    typedef std::shared_ptr<StopSqlCreateDbRequest>    StopSqlCreateDbRequestPtr;
    typedef std::shared_ptr<StopSqlDeleteDbRequest>    StopSqlDeleteDbRequestPtr;
    typedef std::shared_ptr<StopSqlEnableDbRequest>    StopSqlEnableDbRequestPtr;
    typedef std::shared_ptr<StopSqlDisableDbRequest>   StopSqlDisableDbRequestPtr;
    typedef std::shared_ptr<StopSqlGrantAccessRequest> StopSqlGrantAccessRequestPtr;
    typedef std::shared_ptr<StopSqlCreateIndexesRequest> StopSqlCreateIndexesRequestPtr;
    typedef std::shared_ptr<StopSqlCreateTableRequest> StopSqlCreateTableRequestPtr;
    typedef std::shared_ptr<StopSqlCreateTablesRequest> StopSqlCreateTablesRequestPtr;
    typedef std::shared_ptr<StopSqlDeleteTableRequest> StopSqlDeleteTableRequestPtr;
    typedef std::shared_ptr<StopSqlDropIndexesRequest> StopSqlDropIndexesRequestPtr;
    typedef std::shared_ptr<StopSqlGetIndexesRequest>  StopSqlGetIndexesRequestPtr;
    typedef std::shared_ptr<StopSqlRemoveTablePartitionsRequest> StopSqlRemoveTablePartitionsRequestPtr;
    typedef std::shared_ptr<StopSqlDeleteTablePartitionRequest>  StopSqlDeleteTablePartitionRequestPtr;

    class StatusReplicationRequestPolicy;
    class StatusDeleteRequestPolicy;
    class StatusFindRequestPolicy;
    class StatusFindAllRequestPolicy;
    class StatusEchoRequestPolicy;
    class StatusIndexRequestPolicy;
    class StatusSqlRequestPolicy;

    template <typename POLICY> class StatusRequest;

    using StatusReplicationRequest = StatusRequest<StatusReplicationRequestPolicy>;
    using StatusDeleteRequest      = StatusRequest<StatusDeleteRequestPolicy>;
    using StatusFindRequest        = StatusRequest<StatusFindRequestPolicy>;
    using StatusFindAllRequest     = StatusRequest<StatusFindAllRequestPolicy>;
    using StatusEchoRequest        = StatusRequest<StatusEchoRequestPolicy>;
    using StatusIndexRequest       = StatusRequest<StatusIndexRequestPolicy>;
    using StatusSqlAlterTablesRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlQueryRequest       = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlCreateDbRequest    = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlDeleteDbRequest    = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlEnableDbRequest    = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlDisableDbRequest   = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlGrantAccessRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlCreateIndexesRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlCreateTableRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlCreateTablesRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlDeleteTableRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlDropIndexesRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlGetIndexesRequest  = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlRemoveTablePartitionsRequest = StatusRequest<StatusSqlRequestPolicy>;
    using StatusSqlDeleteTablePartitionRequest  = StatusRequest<StatusSqlRequestPolicy>;

    typedef std::shared_ptr<StatusReplicationRequest> StatusReplicationRequestPtr;
    typedef std::shared_ptr<StatusDeleteRequest>      StatusDeleteRequestPtr;
    typedef std::shared_ptr<StatusFindRequest>        StatusFindRequestPtr;
    typedef std::shared_ptr<StatusFindAllRequest>     StatusFindAllRequestPtr;
    typedef std::shared_ptr<StatusEchoRequest>        StatusEchoRequestPtr;
    typedef std::shared_ptr<StatusIndexRequest>       StatusIndexRequestPtr;
    typedef std::shared_ptr<StatusSqlAlterTablesRequest> StatusSqlAlterTablesRequestPtr;
    typedef std::shared_ptr<StatusSqlQueryRequest>       StatusSqlQueryRequestPtr;
    typedef std::shared_ptr<StatusSqlCreateDbRequest>    StatusSqlCreateDbRequestPtr;
    typedef std::shared_ptr<StatusSqlDeleteDbRequest>    StatusSqlDeleteDbRequestPtr;
    typedef std::shared_ptr<StatusSqlEnableDbRequest>    StatusSqlEnableDbRequestPtr;
    typedef std::shared_ptr<StatusSqlDisableDbRequest>   StatusSqlDisableDbRequestPtr;
    typedef std::shared_ptr<StatusSqlGrantAccessRequest> StatusSqlGrantAccessRequestPtr;
    typedef std::shared_ptr<StatusSqlCreateIndexesRequest> StatusSqlCreateIndexesRequestPtr;
    typedef std::shared_ptr<StatusSqlCreateTableRequest> StatusSqlCreateTableRequestPtr;
    typedef std::shared_ptr<StatusSqlCreateTablesRequest> StatusSqlCreateTablesRequestPtr;
    typedef std::shared_ptr<StatusSqlDeleteTableRequest> StatusSqlDeleteTableRequestPtr;
    typedef std::shared_ptr<StatusSqlDropIndexesRequest> StatusSqlDropIndexesRequestPtr;
    typedef std::shared_ptr<StatusSqlGetIndexesRequest>  StatusSqlGetIndexesRequestPtr;
    typedef std::shared_ptr<StatusSqlRemoveTablePartitionsRequest> StatusSqlRemoveTablePartitionsRequestPtr;
    typedef std::shared_ptr<StatusSqlDeleteTablePartitionRequest>  StatusSqlDeleteTablePartitionRequestPtr;

    class ServiceSuspendRequestPolicy;
    class ServiceResumeRequestPolicy;
    class ServiceStatusRequestPolicy;
    class ServiceRequestsRequestPolicy;
    class ServiceDrainRequestPolicy;
    class ServiceReconfigRequestPolicy;

    template <typename POLICY> class ServiceManagementRequest;

    using ServiceSuspendRequest  = ServiceManagementRequest<ServiceSuspendRequestPolicy>;
    using ServiceResumeRequest   = ServiceManagementRequest<ServiceResumeRequestPolicy>;
    using ServiceStatusRequest   = ServiceManagementRequest<ServiceStatusRequestPolicy>;
    using ServiceRequestsRequest = ServiceManagementRequest<ServiceRequestsRequestPolicy>;
    using ServiceDrainRequest    = ServiceManagementRequest<ServiceDrainRequestPolicy>;
    using ServiceReconfigRequest = ServiceManagementRequest<ServiceReconfigRequestPolicy>;

    typedef std::shared_ptr<ServiceSuspendRequest>  ServiceSuspendRequestPtr;
    typedef std::shared_ptr<ServiceResumeRequest>   ServiceResumeRequestPtr;
    typedef std::shared_ptr<ServiceStatusRequest>   ServiceStatusRequestPtr;
    typedef std::shared_ptr<ServiceRequestsRequest> ServiceRequestsRequestPtr;
    typedef std::shared_ptr<ServiceDrainRequest>    ServiceDrainRequestPtr;
    typedef std::shared_ptr<ServiceReconfigRequest> ServiceReconfigRequestPtr;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ControllerIdentity encapsulates various attributes which identify
 * each instance of the Controller class. This information is meant to
 * be used in the multi-Controller setups to coordinate operations
 * between multiple instances and to avoid/resolve conflicts.
 */
class ControllerIdentity {
public:
    std::string id;     // A unique identifier of the Controller
    std::string host;   // The name of a host where it runs
    pid_t pid;          // An identifier of a process
};

std::ostream& operator <<(std::ostream& os, ControllerIdentity const& identity);

/**
 * Class Controller is used for pushing replication (etc.) requests
 * to the worker replication services. Only one instance of this class is
 * allowed per a thread. Request-specific methods of the class will
 * instantiate and start the requests.
 * 
 * All methods launching, stopping or checking status of requests
 * require that the server to be running. Otherwise it will throw
 * std::runtime_error. The current implementation of the server
 * doesn't support (yet?) an operation queuing mechanism.
 *
 * Methods which take worker names as parameters will throw exception
 * std::invalid_argument if the specified worker names are not found
 * in the configuration.
 */
class Controller : public std::enable_shared_from_this<Controller> {

public:

    friend class ControllerImpl;

    typedef std::shared_ptr<Controller> Ptr;

    static Ptr create(ServiceProvider::Ptr const& serviceProvider);

    Controller() = delete;
    Controller(Controller const&) = delete;
    Controller& operator=(Controller const&) = delete;

    ~Controller() = default;

    ControllerIdentity const& identity() const { return _identity; }

    uint64_t startTime() const { return _startTime; }

    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }

    boost::asio::io_service& io_service() { return serviceProvider()->io_service(); }

    /**
     * Check if required folders exist and they're write-enabled for an effective user
     * of the current process. Create missing folders if needed.
     * @param createMissingFolders The optional flag telling the method to create missing folders.
     * @throw std::runtime_error If any folder can't be created, or if any folder is not
     *   write-enabled for the current user.
     */
    void verifyFolders(bool createMissingFolders=false) const;

    ReplicationRequestPtr replicate(
            std::string const& workerName,
            std::string const& sourceWorkerName,
            std::string const& database,
            unsigned int chunk,
            std::function<void(ReplicationRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            bool allowDuplicate=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    DeleteRequestPtr deleteReplica(
            std::string const& workerName,
            std::string const& database,
            unsigned int chunk,
            std::function<void(DeleteRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            bool allowDuplicate=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    FindRequestPtr findReplica(
            std::string const& workerName,
            std::string const& database,
            unsigned int chunk,
            std::function<void(FindRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool computeCheckSum=false,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    FindAllRequestPtr findAllReplicas(
            std::string const& workerName,
            std::string const& database,
            bool saveReplicaInfo=true,
            std::function<void(FindAllRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    EchoRequestPtr echo(
            std::string const& workerName,
            std::string const& data,
            uint64_t delay,
            std::function<void(EchoRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    IndexRequestPtr index(
            std::string const& workerName,
            std::string const& database,
            std::string const& directorTable,
            unsigned int chunk,
            bool hasTransactions,
            TransactionId transactionId,
            std::function<void(IndexRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlAlterTablesRequestPtr sqlAlterTables(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            std::string const& alterSpec,
            std::function<void(SqlAlterTablesRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlQueryRequestPtr sqlQuery(
            std::string const& workerName,
            std::string const& query,
            std::string const& user,
            std::string const& password,
            uint64_t maxRows,
            std::function<void(SqlQueryRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlCreateDbRequestPtr sqlCreateDb(
            std::string const& workerName,
            std::string const& database,
            std::function<void(SqlCreateDbRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlDeleteDbRequestPtr sqlDeleteDb(
            std::string const& workerName,
            std::string const& database,
            std::function<void(SqlDeleteDbRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlEnableDbRequestPtr sqlEnableDb(
            std::string const& workerName,
            std::string const& database,
            std::function<void(SqlEnableDbRequestPtr)>  const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlDisableDbRequestPtr sqlDisableDb(
            std::string const& workerName,
            std::string const& database,
            std::function<void(SqlDisableDbRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlGrantAccessRequestPtr sqlGrantAccess(
            std::string const& workerName,
            std::string const& database,
            std::string const& user,
            std::function<void(SqlGrantAccessRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlCreateIndexesRequestPtr sqlCreateTableIndexes(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            SqlRequestParams::IndexSpec const& indexSpec,
            std::string const& indexName,
            std::string const& indexComment,
            std::vector<SqlIndexColumn> const& indexColumns,
            std::function<void(SqlCreateIndexesRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlCreateTableRequestPtr sqlCreateTable(
            std::string const& workerName,
            std::string const& database,
            std::string const& table,
            std::string const& engine,
            std::string const& partitionByColumn,
            std::list<SqlColDef> const& columns,
            std::function<void(SqlCreateTableRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlCreateTablesRequestPtr sqlCreateTables(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            std::string const& engine,
            std::string const& partitionByColumn,
            std::list<SqlColDef> const& columns,
            std::function<void(SqlCreateTablesRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlDeleteTableRequestPtr sqlDeleteTable(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            std::function<void(SqlDeleteTableRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlRemoveTablePartitionsRequestPtr sqlRemoveTablePartitions(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            std::function<void(SqlRemoveTablePartitionsRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlDeleteTablePartitionRequestPtr sqlDeleteTablePartition(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            TransactionId transactionId,
            std::function<void(SqlDeleteTablePartitionRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlDropIndexesRequestPtr sqlDropTableIndexes(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            std::string const& indexName,
            std::function<void(SqlDropIndexesRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    SqlGetIndexesRequestPtr sqlGetTableIndexes(
            std::string const& workerName,
            std::string const& database,
            std::vector<std::string> const& tables,
            std::function<void(SqlGetIndexesRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    DisposeRequestPtr dispose(
            std::string const& workerName,
            std::vector<std::string> const& targetIds,
            std::function<void(DisposeRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    template <class REQUEST>
    typename REQUEST::Ptr stopById(
            std::string const& workerName,
            std::string const& targetRequestId,
            typename REQUEST::CallbackType const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0) {

        _debug(__func__, "targetRequestId: " + targetRequestId);

        return _submit<REQUEST,
                       decltype(targetRequestId)>(
            workerName,
            targetRequestId,
            onFinish,
            priority,
            keepTracking,
            jobId,
            requestExpirationIvalSec);        
    }

    template <class REQUEST>
    typename REQUEST::Ptr statusById(
            std::string const& workerName,
            std::string const& targetRequestId,
            typename REQUEST::CallbackType const& onFinish=nullptr,
            int priority=PRIORITY_NORMAL,
            bool keepTracking=true,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0) {

        _debug(__func__, "targetRequestId: " + targetRequestId);

        return _submit<REQUEST,
                       decltype(targetRequestId)>(
            workerName,
            targetRequestId,
            onFinish,
            priority,
            keepTracking,
            jobId,
            requestExpirationIvalSec);
    }

    ServiceSuspendRequestPtr suspendWorkerService(
            std::string const& workerName,
            std::function<void(ServiceSuspendRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_VERY_HIGH,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    ServiceResumeRequestPtr resumeWorkerService(
            std::string const& workerName,
            std::function<void(ServiceResumeRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_VERY_HIGH,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    ServiceStatusRequestPtr statusOfWorkerService(
            std::string const& workerName,
            std::function<void(ServiceStatusRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_VERY_HIGH,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    ServiceRequestsRequestPtr requestsOfWorkerService(
            std::string const& workerName,
            std::function<void(ServiceRequestsRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_VERY_HIGH,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    ServiceDrainRequestPtr drainWorkerService(
            std::string const& workerName,
            std::function<void(ServiceDrainRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_VERY_HIGH,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    ServiceReconfigRequestPtr reconfigWorkerService(
            std::string const& workerName,
            std::function<void(ServiceReconfigRequestPtr)> const& onFinish=nullptr,
            int priority=PRIORITY_VERY_HIGH,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0);

    /**
     * Specialized version of the requests launcher for the worker service
     * management requests. This is an alternative request launching method
     * for the above defined operations. It allows upstream template specialization
     * in those cases when a generic code is desired.
     * 
     * @see Controller::suspendWorkerService()
     * @see Controller::resumeWorkerService()
     * @see Controller::statusOfWorkerService()
     * @see Controller::requestsOfWorkerService()
     * @see Controller::drainWorkerService()
     * @see Controller::reconfigWorkerService()
     * @see Controller::_submit()
     */
    template <class REQUEST>
    typename REQUEST::Ptr workerServiceRequest(
            std::string const& workerName,
            typename REQUEST::CallbackType const& onFinish=nullptr,
            int priority=PRIORITY_VERY_HIGH,
            std::string const& jobId="",
            unsigned int requestExpirationIvalSec=0) {

        _logManagementRequest(REQUEST::Policy::requestName(), workerName);

        return _submit<REQUEST>(
                workerName,
                onFinish,
                priority,
                jobId,
                requestExpirationIvalSec);
    }

    template <class REQUEST>
    void requestsOfType(std::vector<typename REQUEST::Ptr>& requests) const {
        util::Lock lock(_mtx, _context(__func__));
        requests.clear();
        for (auto&& itr: _registry)
            if (typename REQUEST::Ptr ptr =
                std::dynamic_pointer_cast<REQUEST>(itr.second->request())) {
                requests.push_back(ptr);
            }
    }

    template <class REQUEST>
    size_t numRequestsOfType() const {
        util::Lock lock(_mtx, _context(__func__));
        size_t result(0);
        for (auto&& itr: _registry) {
            if (typename REQUEST::Ptr request =
                std::dynamic_pointer_cast<REQUEST>(itr.second->request())) { ++result; }
        }
        return result;
    }

    size_t numActiveRequests() const;

    /**
     * Class RequestWrapper is the base class for implementing requests
     * registry as a polymorphic collection to store active requests. Pure virtual
     * methods of the class will be overridden by request-type-specific implementations.
     * @see class RequestWrapperImpl
     */
    class RequestWrapper {
    public:
        typedef std::shared_ptr<RequestWrapper> Ptr;

        virtual ~RequestWrapper() = default;

        /// to be called on completion of a request
        virtual void notify() = 0;

        /// @return stored request object by a pointer to its base class
        virtual std::shared_ptr<Request> request() const = 0;
    };

    /**
     * Class RequestWrapperImpl extends its base class to implement request-specific
     * pointer extraction and call-back notification.
     * @see class RequestWrapper
     */
    template <class T>
    class RequestWrapperImpl : public RequestWrapper {
    public:
        void notify() override {
            if (nullptr != _onFinish) {

                // Clearing the stored callback after finishing the up-stream notification
                // has two purposes:
                // 1. it guaranties (exactly) one time notification
                // 2. it breaks the up-stream dependency on a caller object if a shared
                //    pointer to the object was mentioned as the lambda-function's closure
                auto onFinish = move(_onFinish);
                _onFinish = nullptr;
                onFinish(_request);
            }
        }

        RequestWrapperImpl(typename T::Ptr const& request,
                           typename T::CallbackType const& onFinish)
            :   RequestWrapper(),
                _request(request),
                _onFinish(onFinish) {
        }

        ~RequestWrapperImpl() override = default;

        std::shared_ptr<Request> request() const override { return _request; }

    private:
        typename T::Ptr          _request;
        typename T::CallbackType _onFinish;
    };


private:

    explicit Controller(ServiceProvider::Ptr const& serviceProvider);

    std::string _context(std::string const& func=std::string()) const;

    void _debug(std::string const& func, std::string const& msg=std::string()) const;

    void _finish(std::string const& id);

    void _assertIsRunning() const;

    /**
     * Logger for the management requests
     */
    void _logManagementRequest(std::string const& requestName,
                               std::string const& workerName);

    /**
     * A generic version of the request creation and submission
     * for a variable collection of the request-specific parameters.
     */
    template <class REQUEST, typename...Targs>
    typename REQUEST::Ptr _submit(
            std::string const& workerName,
            Targs... Fargs,
            typename REQUEST::CallbackType const& onFinish,
            int priority,
            bool keepTracking,
            std::string const& jobId,
            unsigned int requestExpirationIvalSec) {

        _assertIsRunning();

        util::Lock lock(_mtx, _context(__func__));

        auto const controller = shared_from_this();
        auto const request = REQUEST::create(
            serviceProvider(),
            serviceProvider()->io_service(),
            workerName,
            Fargs...,
            [controller] (typename REQUEST::Ptr const& request) {
                controller->_finish(request->id());
            },
            priority,
            keepTracking,
            serviceProvider()->messenger()
        );

        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.

        _registry[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST>>(request, onFinish);

        // Initiate the request

        request->start(controller, jobId, requestExpirationIvalSec);

        return request;
    }

    /**
     * Specialized version of the requests launcher for the worker service
     * management requests.
     */
    template <class REQUEST>
    typename REQUEST::Ptr _submit(
            std::string const& workerName,
            typename REQUEST::CallbackType const& onFinish,
            int priority,
            std::string const& jobId,
            unsigned int requestExpirationIvalSec) {

        _assertIsRunning();

        util::Lock lock(_mtx, _context(__func__));

        auto const controller = shared_from_this();
        auto const request = REQUEST::create(
            serviceProvider(),
            serviceProvider()->io_service(),
            workerName,
            [controller] (typename REQUEST::Ptr const& request) {
                controller->_finish(request->id());
            },
            priority,
            serviceProvider()->messenger()
        );

        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.

        _registry[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST>>(request, onFinish);

        // Initiate the request

        request->start(controller, jobId, requestExpirationIvalSec);

        return request;
    }

    ControllerIdentity const _identity;     /// The unique identity of the instance

    uint64_t const _startTime;  /// The number of milliseconds since UNIX Epoch when
                                /// an instance of the Controller was created.

    ServiceProvider::Ptr const _serviceProvider;

    mutable util::Mutex _mtx;   /// for thread safety of the class's public API
                                /// and internal operations.

    std::map<std::string, std::shared_ptr<RequestWrapper>> _registry;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONTROLLER_H
