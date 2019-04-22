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
#include "replica/Messenger.h"
#include "replica/Performance.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlRequest.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Controller");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

template <class  T>
struct RequestWrapperImpl : Controller::RequestWrapper {

    void notify() override {

        if (nullptr != _onFinish) {

            // Clearing the stored callback after finishing the up-stream notification
            // has two purposes:
            //
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
        :   Controller::RequestWrapper(),
            _request(request),
            _onFinish(onFinish) {
    }

    ~RequestWrapperImpl() override = default;

    shared_ptr<Request> request() const override {
        return _request;
    }

private:

    typename T::Ptr          _request;
    typename T::CallbackType _onFinish;
};


/**
 * The utility class implementing operations on behalf of certain
 * methods of class Controller.
 *
 * THREAD SAFETY NOTE: Methods implemented within the class are NOT thread-safe.
 *                     They must be called from the thread-safe code only.
 */
class ControllerImpl {

public:

    ControllerImpl() = default;

    ControllerImpl(ControllerImpl const&) = delete;
    ControllerImpl& operator=(ControllerImpl const&) = delete;

    ~ControllerImpl() = default;

    /**
     * Generic method for managing requests such as stopping an outstanding
     * request or obtaining an updated status of a request.
     */
    template <class REQUEST_TYPE>
    static typename REQUEST_TYPE::Ptr requestManagementOperation(
            Controller::Ptr const& controller,
            string const& jobId,
            string const& workerName,
            string const& targetRequestId,
            typename REQUEST_TYPE::CallbackType const& onFinish,
            bool  keepTracking,
            typename Messenger::Ptr const& messenger,
            unsigned int requestExpirationIvalSec) {

        controller->_assertIsRunning();

        typename REQUEST_TYPE::Ptr request =
            REQUEST_TYPE::create(
                controller->serviceProvider(),
                controller->serviceProvider()->io_service(),
                workerName,
                targetRequestId,
                [controller] (typename REQUEST_TYPE::Ptr request) {
                    controller->_finish(request->id());
                },
                keepTracking,
                messenger
            );

        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.

        (controller->_registry)[request->id()] =
            make_shared<RequestWrapperImpl<REQUEST_TYPE>>(request, onFinish);

        // Initiate the request

        request->start(controller, jobId, requestExpirationIvalSec);

        return request;
    }

   /**
     * Generic method for launching worker service management requests such as suspending,
     * resuming or inspecting a status of the worker-side replication service.
     */
    template <class REQUEST_TYPE>
    static typename REQUEST_TYPE::Ptr serviceManagementOperation(
            Controller::Ptr const& controller,
            string const& jobId,
            string const& workerName,
            typename REQUEST_TYPE::CallbackType const& onFinish,
            typename Messenger::Ptr const& messenger,
            unsigned int requestExpirationIvalSec) {

        controller->_assertIsRunning();

        typename REQUEST_TYPE::Ptr request =
            REQUEST_TYPE::create(
                controller->serviceProvider(),
                controller->serviceProvider()->io_service(),
                workerName,
                [controller] (typename REQUEST_TYPE::Ptr request) {
                    controller->_finish(request->id());
                },
                messenger
            );

        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.

        (controller->_registry)[request->id()] =
            make_shared<RequestWrapperImpl<REQUEST_TYPE>>(request, onFinish);

        // Initiate the request

        request->start(controller, jobId, requestExpirationIvalSec);

        return request;
    }
};


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
                                int  priority,
                                bool keepTracking,
                                bool allowDuplicate,
                                string const& jobId,
                                unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    util::Lock lock(_mtx, _context(__func__));

    _assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = ReplicationRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        sourceWorkerName,
        database,
        chunk,
        [controller] (ReplicationRequest::Ptr request) {
            controller->_finish(request->id());
        },
        priority,
        keepTracking,
        allowDuplicate,
        serviceProvider()->messenger()
    );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        make_shared<RequestWrapperImpl<ReplicationRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
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

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    util::Lock lock(_mtx, _context(__func__));

    _assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = DeleteRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        database,
        chunk,
        [controller] (DeleteRequest::Ptr request) {
            controller->_finish(request->id());
        },
        priority,
        keepTracking,
        allowDuplicate,
        serviceProvider()->messenger()
    );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        make_shared<RequestWrapperImpl<DeleteRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
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

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    util::Lock lock(_mtx, _context(__func__));

    _assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = FindRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        database,
        chunk,
        [controller] (FindRequest::Ptr request) {
            controller->_finish(request->id());
        },
        priority,
        computeCheckSum,
        keepTracking,
        serviceProvider()->messenger()
    );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        make_shared<RequestWrapperImpl<FindRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
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

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    util::Lock lock(_mtx, _context(__func__));

    _assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = FindAllRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        database,
        saveReplicaInfo,
        [controller] (FindAllRequest::Ptr request) {
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
        make_shared<RequestWrapperImpl<FindAllRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
}


EchoRequest::Ptr Controller::echo(string const& workerName,
                                  string const& data,
                                  uint64_t delay,
                                  EchoRequestCallbackType const& onFinish,
                                  int priority,
                                  bool keepTracking,
                                  string const& jobId,
                                  unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    util::Lock lock(_mtx, _context(__func__));

    _assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = EchoRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        data,
        delay,
        [controller] (EchoRequest::Ptr request) {
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
        make_shared<RequestWrapperImpl<EchoRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
}


SqlRequest::Ptr Controller::sql(std::string const& workerName,
                                std::string const& query,
                                std::string const& user,
                                std::string const& password,
                                uint64_t maxRows,
                                SqlRequestCallbackType const& onFinish,
                                int  priority,
                                bool keepTracking,
                                std::string const& jobId,
                                unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    util::Lock lock(_mtx, _context(__func__));

    _assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = SqlRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        query,
        user,
        password,
        maxRows,
        [controller] (SqlRequest::Ptr request) {
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
        make_shared<RequestWrapperImpl<SqlRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
}


StopReplicationRequest::Ptr Controller::stopReplication(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StopReplicationRequest::CallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StopReplicationRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StopDeleteRequest::Ptr Controller::stopReplicaDelete(
                                string const& workerName,
                                string const& targetRequestId,
                                StopDeleteRequest::CallbackType const& onFinish,
                                bool keepTracking,
                                string const& jobId,
                                unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StopDeleteRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StopFindRequest::Ptr Controller::stopReplicaFind(
                            string const& workerName,
                            string const& targetRequestId,
                            StopFindRequest::CallbackType const& onFinish,
                            bool keepTracking,
                            string const& jobId,
                            unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StopFindRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StopFindAllRequest::Ptr Controller::stopReplicaFindAll(
                                string const& workerName,
                                string const& targetRequestId,
                                StopFindAllRequest::CallbackType const& onFinish,
                                bool keepTracking,
                                string const& jobId,
                                unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StopFindAllRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StopEchoRequest::Ptr Controller::stopEcho(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StopEchoRequestCallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StopEchoRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StopSqlRequest::Ptr Controller::stopSql(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StopSqlRequestCallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StopSqlRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StatusReplicationRequest::Ptr Controller::statusOfReplication(
                                        string const& workerName,
                                        string const& targetRequestId,
                                        StatusReplicationRequest::CallbackType const& onFinish,
                                        bool keepTracking,
                                        string const& jobId,
                                        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StatusReplicationRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StatusDeleteRequest::Ptr Controller::statusOfDelete(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StatusDeleteRequest::CallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StatusDeleteRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StatusFindRequest::Ptr Controller::statusOfFind(
                                string const& workerName,
                                string const& targetRequestId,
                                StatusFindRequest::CallbackType const& onFinish,
                                bool keepTracking,
                                string const& jobId,
                                unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StatusFindRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StatusFindAllRequest::Ptr Controller::statusOfFindAll(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StatusFindAllRequest::CallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StatusFindAllRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StatusEchoRequest::Ptr Controller::statusOfEcho(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StatusEchoRequest::CallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StatusEchoRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


StatusSqlRequest::Ptr Controller::statusOfSql(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StatusSqlRequest::CallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::requestManagementOperation<StatusSqlRequest>(
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


ServiceSuspendRequest::Ptr Controller::suspendWorkerService(
                                    string const& workerName,
                                    ServiceSuspendRequest::CallbackType const& onFinish,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  workerName: " << workerName);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::serviceManagementOperation<ServiceSuspendRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


ServiceResumeRequest::Ptr Controller::resumeWorkerService(
                                    string const& workerName,
                                    ServiceResumeRequest::CallbackType const& onFinish,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  workerName: " << workerName);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::serviceManagementOperation<ServiceResumeRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


ServiceStatusRequest::Ptr Controller::statusOfWorkerService(
                                    string const& workerName,
                                    ServiceStatusRequest::CallbackType const& onFinish,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  workerName: " << workerName);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::serviceManagementOperation<ServiceStatusRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


ServiceRequestsRequest::Ptr Controller::requestsOfWorkerService(
                                    string const& workerName,
                                    ServiceRequestsRequest::CallbackType const& onFinish,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) + "  workerName: " << workerName);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::serviceManagementOperation<ServiceRequestsRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


ServiceDrainRequest::Ptr Controller::drainWorkerService(
                                    string const& workerName,
                                    ServiceDrainRequest::CallbackType const& onFinish,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "  workerName: " << workerName);

    util::Lock lock(_mtx, _context(__func__));

    return ControllerImpl::serviceManagementOperation<ServiceDrainRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


size_t Controller::numActiveRequests() const {
    util::Lock lock(_mtx, _context(__func__));
    return _registry.size();
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

}}} // namespace lsst::qserv::replica
