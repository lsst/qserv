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
#include <sys/types.h>  // pid_t
#include <unistd.h>     // getpid()

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
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Controller");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

//////////////////////////////////////////////////////////////////////////
//////////////////////////  RequestWrapperImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////////

/**
 * Request-type specific wrappers
 */
template <class  T>
struct RequestWrapperImpl
    :   ControllerRequestWrapper {

    /// The implementation of the virtual method defined in the base class
    virtual void notify() {

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
        :   ControllerRequestWrapper(),
            _request(request),
            _onFinish(onFinish) {
    }

    /// Destructor
    ~RequestWrapperImpl() override = default;

    /// @see ControllerRequestWrapper::request()
    shared_ptr<Request> request() const override {
        return _request;
    }

private:

    typename T::Ptr          _request;
    typename T::CallbackType _onFinish;
};


//////////////////////////////////////////////////////////////////////
//////////////////////////  ControllerImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////

/**
 * The utility class implementing operations on behalf of certain
 * methods of class Controller.
 *
 * THREAD SAFETY NOTE: Methods implemented within the class are NOT thread-safe.
 *                     They must be called from the thread-safe code only.
 */
class ControllerImpl {

public:

    /// Default constructor
    ControllerImpl() = default;

    // Default copy semantics is prohibited

    ControllerImpl(ControllerImpl const&) = delete;
    ControllerImpl& operator=(ControllerImpl const&) = delete;

    /// Destructor
    ~ControllerImpl() = default;

    /**
     * Generic method for managing requests such as stopping an outstanding
     * request or obtaining an updated status of a request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be affected
     * @param onFinish        - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::Ptr requestManagementOperation(
            Controller::Ptr const& controller,
            string const& jobId,
            string const& workerName,
            string const& targetRequestId,
            typename REQUEST_TYPE::CallbackType const& onFinish,
            bool  keepTracking,
            typename Messenger::Ptr const& messenger,
            unsigned int requestExpirationIvalSec) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::Ptr request =
            REQUEST_TYPE::create(
                controller->serviceProvider(),
                controller->serviceProvider()->io_service(),
                workerName,
                targetRequestId,
                [controller] (typename REQUEST_TYPE::Ptr request) {
                    controller->finish(request->id());
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
     *
     * @param workerName - the name of a worker node where the service is run
     * @param onFinish   - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::Ptr serviceManagementOperation(
            Controller::Ptr const& controller,
            string const& jobId,
            string const& workerName,
            typename REQUEST_TYPE::CallbackType const& onFinish,
            typename Messenger::Ptr const& messenger,
            unsigned int requestExpirationIvalSec) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::Ptr request =
            REQUEST_TYPE::create(
                controller->serviceProvider(),
                controller->serviceProvider()->io_service(),
                workerName,
                [controller] (typename REQUEST_TYPE::Ptr request) {
                    controller->finish(request->id());
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


//////////////////////////////////////////////////////////////////////////
//////////////////////////  ControllerIdentity  //////////////////////////
//////////////////////////////////////////////////////////////////////////

ostream& operator <<(ostream& os, ControllerIdentity const& identity) {
    os  << "ControllerIdentity(id=" << identity.id << ",host=" << identity.host << ",pid=" << identity.pid << ")";
    return os;
}


//////////////////////////////////////////////////////////////////
//////////////////////////  Controller  //////////////////////////
//////////////////////////////////////////////////////////////////

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

            
string Controller::context() const {
    return "R-CONTR " + _identity.id + "  " + _identity.host +
           "[" + to_string(_identity.pid) + "]  ";
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

    LOGS(_log, LOG_LVL_DEBUG, context() << "replicate");

    util::Lock lock(_mtx, context() + "replicate");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = ReplicationRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        sourceWorkerName,
        database,
        chunk,
        [controller] (ReplicationRequest::Ptr request) {
            controller->finish(request->id());
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

    LOGS(_log, LOG_LVL_DEBUG, context() << "deleteReplica");

    util::Lock lock(_mtx, context() + "deleteReplica");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = DeleteRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        database,
        chunk,
        [controller] (DeleteRequest::Ptr request) {
            controller->finish(request->id());
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

    LOGS(_log, LOG_LVL_DEBUG, context() << "findReplica");

    util::Lock lock(_mtx, context() + "findReplica");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = FindRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        database,
        chunk,
        [controller] (FindRequest::Ptr request) {
            controller->finish(request->id());
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

    LOGS(_log, LOG_LVL_DEBUG, context() << "findAllReplicas");

    util::Lock lock(_mtx, context() + "findAllReplicas");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = FindAllRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        database,
        saveReplicaInfo,
        [controller] (FindAllRequest::Ptr request) {
            controller->finish(request->id());
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

    LOGS(_log, LOG_LVL_DEBUG, context() << "echo");

    util::Lock lock(_mtx, context() + "echo");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    auto const request = EchoRequest::create(
        serviceProvider(),
        serviceProvider()->io_service(),
        workerName,
        data,
        delay,
        [controller] (EchoRequest::Ptr request) {
            controller->finish(request->id());
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


StopReplicationRequest::Ptr Controller::stopReplication(
                                    string const& workerName,
                                    string const& targetRequestId,
                                    StopReplicationRequest::CallbackType const& onFinish,
                                    bool keepTracking,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "stopReplication  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "stopReplication");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "stopReplicaDelete  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "stopReplicaDelete");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "stopReplicaFind  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "stopReplicaFind");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "stopReplicaFindAll  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "stopReplicaFindAll");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "stopEcho  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "stopEcho");

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


StatusReplicationRequest::Ptr Controller::statusOfReplication(
                                        string const& workerName,
                                        string const& targetRequestId,
                                        StatusReplicationRequest::CallbackType const& onFinish,
                                        bool keepTracking,
                                        string const& jobId,
                                        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusOfReplication  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "statusOfReplication");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusOfDelete  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "statusOfDelete");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusOfFind  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "statusOfFind");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusOfFindAll  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "statusOfFindAll");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusOfEcho  targetRequestId = " << targetRequestId);

    util::Lock lock(_mtx, context() + "statusOfEcho");

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


ServiceSuspendRequest::Ptr Controller::suspendWorkerService(
                                    string const& workerName,
                                    ServiceSuspendRequest::CallbackType const& onFinish,
                                    string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "suspendWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "suspendWorkerService");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "resumeWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "resumeWorkerService");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusOfWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "statusOfWorkerService");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestsOfWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "requestsOfWorkerService");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "drainWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "drainWorkerService");

    return ControllerImpl::serviceManagementOperation<ServiceDrainRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        serviceProvider()->messenger(),
        requestExpirationIvalSec);
}


size_t Controller::numActiveRequests() const {
    util::Lock lock(_mtx, context() + "numActiveRequests");
    return _registry.size();
}


void Controller::finish(string const& id) {

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

    ControllerRequestWrapper::Ptr request;
    {
        util::Lock lock(_mtx, context() + "finish");
        request = _registry[id];
        _registry.erase(id);
    }
    request->notify();
}


void Controller::assertIsRunning() const {
    if (not serviceProvider()->isRunning()) {
        throw runtime_error("ServiceProvider is not running");
    }
}

}}} // namespace lsst::qserv::replica
