/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/DeleteRequest.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/Messenger.h"
#include "replica/Performance.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

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

    /// The implementation of the vurtual method defined in the base class
    virtual void notify () {
        if (_onFinish == nullptr) return;
        _onFinish(_request);
    }

    RequestWrapperImpl(typename T::pointer const& request,
                       typename T::callback_type  onFinish)

        :   ControllerRequestWrapper(),

            _request  (request),
            _onFinish (onFinish) {
    }

    /// Destructor
    ~RequestWrapperImpl() override = default;

    /// Implement a virtual method of the base class
    std::shared_ptr<Request> request () const override {
        return _request;
    }

private:

    // The context of the operation
    
    typename T::pointer       _request;
    typename T::callback_type _onFinish;
};

//////////////////////////////////////////////////////////////////////
//////////////////////////  ControllerImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////

/**
 * The utiliy class implementing operations on behalf of certain
 * methods of class Controller.
 * 
 * THREAD SAFETY NOTE: Methods implemented witin the class are NOT thread-safe.
 *                     They must be called from the thread-safe code only.
 */
class ControllerImpl {

public:

    /// Default constructor
    ControllerImpl () = default;

    // Default copy semantics is prohibited

    ControllerImpl (ControllerImpl const&) = delete;
    ControllerImpl& operator= (ControllerImpl const&) = delete;

    /// Destructor
    ~ControllerImpl () = default;

    /**
     * Generic method for managing requests such as stopping an outstanding
     * request or inquering a status of a request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be affected
     * @param onFinish        - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::pointer requestManagementOperation (

            Controller::pointer const&           controller,
            std::string const&                   jobId,
            std::string const&                   workerName,
            std::string const&                   targetRequestId,
            typename REQUEST_TYPE::callback_type onFinish,
            bool                                 keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
            typename Messenger::pointer const&   messenger,
#endif
            unsigned int                         requestExpirationIvalSec) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::pointer request =
            REQUEST_TYPE::create (
                controller->_serviceProvider,
                controller->_io_service,
                workerName,
                targetRequestId,
                [controller] (typename REQUEST_TYPE::pointer request) {
                    controller->finish(request->id());
                },
                keepTracking
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
                ,messenger
#endif
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (controller->_registry)[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>> (request, onFinish);  
    
        // Initiate the request

        request->start (controller, jobId, requestExpirationIvalSec);

        return request;
    }

   /**
     * Generic method for launching worker service management requests such as suspending,
     * resyming or inspecting a status of the worker-side replication service.
     *
     * @param workerName - the name of a worker node where the service is run
     * @param onFinish   - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::pointer serviceManagementOperation (

            Controller::pointer const&           controller,
            std::string const&                   jobId,
            std::string const&                   workerName,
            typename REQUEST_TYPE::callback_type onFinish,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
            typename Messenger::pointer const&   messenger,
#endif
            unsigned int                         requestExpirationIvalSec) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::pointer request =
            REQUEST_TYPE::create (
                controller->_serviceProvider,
                controller->_io_service,
                workerName,
                [controller] (typename REQUEST_TYPE::pointer request) {
                    controller->finish(request->id());
                }
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
                ,messenger
#endif
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.

        (controller->_registry)[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>> (request, onFinish);

        // Initiate the request

        request->start (controller, jobId, requestExpirationIvalSec);

        return request;
    }
};


//////////////////////////////////////////////////////////////////////////
//////////////////////////  ControllerIdentity  //////////////////////////
//////////////////////////////////////////////////////////////////////////

std::ostream&
operator << (std::ostream& os, ControllerIdentity const& identity) {
    os << "ControllerIdentity (id:'" << identity.id << "',host:'" << identity.host << "', pid:" << identity.pid << ")";
    return os;
}

//////////////////////////////////////////////////////////////////
//////////////////////////  Controller  //////////////////////////
//////////////////////////////////////////////////////////////////

Controller::pointer
Controller::create (ServiceProvider& serviceProvider) {
    return Controller::pointer(new Controller(serviceProvider));
}

Controller::Controller (ServiceProvider& serviceProvider)
    :   _identity ({
            Generators::uniqueId(),
            boost::asio::ip::host_name(),
            getpid()}),

        _startTime       (PerformanceUtils::now()),
        _serviceProvider (serviceProvider),
        _io_service      (),
        _work            (nullptr),
        _thread          (nullptr),
        _registry        () {

#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
    _messenger = Messenger::create (_serviceProvider, _io_service);
#endif

    LOGS(_log, LOG_LVL_DEBUG, "Controller  identity=" << _identity);

    serviceProvider.databaseServices()->saveState (_identity, _startTime);
}

void
Controller::run () {

    LOCK_GUARD;

    if (!isRunning()) {

        Controller::pointer controller = shared_from_this();
     
        _work.reset (
            new boost::asio::io_service::work(_io_service)
        );
        _thread.reset (
            new std::thread (
                [controller] () {
        
                    // This will prevent the I/O service from existing the .run()
                    // method event when it will run out of any requess to process.
                    // Unless the service will be explicitly stopped.
    
                    controller->_io_service.run();
                    
                    // Always reset the object in a preparation for its further
                    // usage.
    
                    controller->_io_service.reset();
                }
            )
        );
    }
}

bool
Controller::isRunning () const {
    return _thread.get() != nullptr;
}

void
Controller::stop () {

    if (!isRunning()) return;

    // IMPORTANT:
    //
    //   Never attempt running these operations within LOCK_GUARD
    //   due to a possibile deadlock when asynchronous handlers will be
    //   calling the thread-safe methods. A problem is that until they finish
    //   in a clean way (as per the _work.reset()) the thread will never finish,
    //   and the application will hang on _thread->join().

    // LOCK_GUARD  (disabled)

#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
    _messenger->stop();
#endif

    // Destoring this object will let the I/O service to (eventually) finish
    // all on-going work and shut down the thread. In that case there is no need
    // to stop the service explicitly (which is not a good idea anyway because
    // there may be outstanding synchronous requests, in which case the service
    // would get into an unpredictanle state.)

    _work.reset();

    // Join with the thread before clearning up the pointer

    _thread->join();

    _thread.reset(nullptr);
    
    // Double check that the collection of requests is empty.
    
    if (!_registry.empty())
        throw std::logic_error ("Controller::stop() the collection of outstanding requests is not empty");
}

void
Controller::join () {
    if (_thread) _thread->join();
}

ReplicationRequest::pointer
Controller::replicate (std::string const&                workerName,
                       std::string const&                sourceWorkerName,
                       std::string const&                database,
                       unsigned int                      chunk,
                       ReplicationRequest::callback_type onFinish,
                       int                               priority,
                       bool                              keepTracking,
                       bool                              allowDuplicate,
                       std::string const&                jobId,
                       unsigned int                      requestExpirationIvalSec) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    ReplicationRequest::pointer request =
        ReplicationRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            sourceWorkerName,
            database,
            chunk,
            [controller] (ReplicationRequest::pointer request) {
                controller->finish(request->id());
            },
            priority,
            keepTracking,
            allowDuplicate
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
            ,_messenger
#endif
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<ReplicationRequest>> (request, onFinish);

    // Initiate the request

    request->start (controller, jobId, requestExpirationIvalSec);

    return request;
}

DeleteRequest::pointer
Controller::deleteReplica (std::string const&           workerName,
                           std::string const&           database,
                           unsigned int                 chunk,
                           DeleteRequest::callback_type onFinish,
                           int                          priority,
                           bool                         keepTracking,
                           bool                         allowDuplicate,
                           std::string const&           jobId,
                           unsigned int                 requestExpirationIvalSec) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    DeleteRequest::pointer request =
        DeleteRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            database,
            chunk,
            [controller] (DeleteRequest::pointer request) {
                controller->finish(request->id());
            },
            priority,
            keepTracking,
            allowDuplicate
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
            ,_messenger
#endif
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<DeleteRequest>> (request, onFinish);

    // Initiate the request

    request->start (controller, jobId, requestExpirationIvalSec);

    return request;
}

FindRequest::pointer
Controller::findReplica (std::string const&         workerName,
                         std::string const&         database,
                         unsigned int               chunk,
                         FindRequest::callback_type onFinish,
                         int                        priority,
                         bool                       computeCheckSum,
                         bool                       keepTracking,
                         std::string const&         jobId,
                         unsigned int               requestExpirationIvalSec) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    FindRequest::pointer request =
        FindRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            database,
            chunk,
            [controller] (FindRequest::pointer request) {
                controller->finish(request->id());
            },
            priority,
            computeCheckSum,
            keepTracking
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
            ,_messenger
#endif
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindRequest>> (request, onFinish);

    // Initiate the request

    request->start (controller, jobId, requestExpirationIvalSec);

    return request;
}

FindAllRequest::pointer
Controller::findAllReplicas (std::string const&            workerName,
                             std::string const&            database,
                             FindAllRequest::callback_type onFinish,
                             int                           priority,
                             bool                          keepTracking,
                             std::string const&            jobId,
                             unsigned int                  requestExpirationIvalSec) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    FindAllRequest::pointer request =
        FindAllRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            database,
            [controller] (FindAllRequest::pointer request) {
                controller->finish(request->id());
            },
            priority,
            keepTracking
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
            ,_messenger
#endif
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindAllRequest>> (request, onFinish);

    // Initiate the request

    request->start (controller, jobId, requestExpirationIvalSec);

    return request;
}

StopReplicationRequest::pointer
Controller::stopReplication (std::string const&                    workerName,
                             std::string const&                    targetRequestId,
                             StopReplicationRequest::callback_type onFinish,
                             bool                                  keepTracking,
                             std::string const&                    jobId,
                             unsigned int                          requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplication  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopReplicationRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

StopDeleteRequest::pointer
Controller::stopReplicaDelete (std::string const&               workerName,
                               std::string const&               targetRequestId,
                               StopDeleteRequest::callback_type onFinish,
                               bool                             keepTracking,
                               std::string const&               jobId,
                               unsigned int                     requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaDelete  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopDeleteRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

StopFindRequest::pointer
Controller::stopReplicaFind (std::string const&             workerName,
                             std::string const&             targetRequestId,
                             StopFindRequest::callback_type onFinish,
                             bool                           keepTracking,
                             std::string const&             jobId,
                             unsigned int                   requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaFind  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopFindRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

StopFindAllRequest::pointer
Controller::stopReplicaFindAll (std::string const&                workerName,
                                std::string const&                targetRequestId,
                                StopFindAllRequest::callback_type onFinish,
                                bool                              keepTracking,
                                std::string const&                jobId,
                                unsigned int                      requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaFindAll  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopFindAllRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

StatusReplicationRequest::pointer
Controller::statusOfReplication (std::string const&                      workerName,
                                 std::string const&                      targetRequestId,
                                 StatusReplicationRequest::callback_type onFinish,
                                 bool                                    keepTracking,
                                 std::string const&                      jobId,
                                 unsigned int                            requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfReplication  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusReplicationRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

StatusDeleteRequest::pointer
Controller::statusOfDelete (std::string const&                 workerName,
                            std::string const&                 targetRequestId,
                            StatusDeleteRequest::callback_type onFinish,
                            bool                               keepTracking,
                            std::string const&                 jobId,
                            unsigned int                       requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfDelete  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusDeleteRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

StatusFindRequest::pointer
Controller::statusOfFind (std::string const&               workerName,
                          std::string const&               targetRequestId,
                          StatusFindRequest::callback_type onFinish,
                          bool                             keepTracking,
                          std::string const&               jobId,
                          unsigned int                     requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfFind  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusFindRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

StatusFindAllRequest::pointer
Controller::statusOfFindAll (std::string const&                  workerName,
                             std::string const&                  targetRequestId,
                             StatusFindAllRequest::callback_type onFinish,
                             bool                                keepTracking,
                             std::string const&                  jobId,
                             unsigned int                        requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfFindAll  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusFindAllRequest> (
        shared_from_this(),
        jobId,
        workerName,
        targetRequestId,
        onFinish,
        keepTracking,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

ServiceSuspendRequest::pointer
Controller::suspendWorkerService (std::string const&                   workerName,
                                  ServiceSuspendRequest::callback_type onFinish,
                                  std::string const&                   jobId,
                                  unsigned int                         requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "suspendWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceSuspendRequest> (
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

ServiceResumeRequest::pointer
Controller::resumeWorkerService (std::string const&                  workerName,
                                 ServiceResumeRequest::callback_type onFinish,
                                 std::string const&                  jobId,
                                 unsigned int                        requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "resumeWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceResumeRequest> (
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

ServiceStatusRequest::pointer
Controller::statusOfWorkerService (std::string const&                  workerName,
                                   ServiceStatusRequest::callback_type onFinish,
                                   std::string const&                  jobId,
                                   unsigned int                        requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceStatusRequest> (
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

ServiceRequestsRequest::pointer
Controller::requestsOfWorkerService (std::string const&                    workerName,
                                     ServiceRequestsRequest::callback_type onFinish,
                                     std::string const&                    jobId,
                                     unsigned int                          requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "requestsOfWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceRequestsRequest> (
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

ServiceDrainRequest::pointer
Controller::drainWorkerService (std::string const&                 workerName,
                                ServiceDrainRequest::callback_type onFinish,
                                std::string const&                 jobId,
                                unsigned int                       requestExpirationIvalSec) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "drainWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceDrainRequest> (
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
#ifndef LSST_QSERV_REPLICA_REQUEST_BASE_C
        _messenger,
#endif
        requestExpirationIvalSec);
}

size_t
Controller::numActiveRequests () const {
    LOCK_GUARD;
    return _registry.size();
}

void
Controller::finish (std::string const& id) {

    // IMPORTANT:
    //
    //   Make sure the notification is complete before removing
    //   the request from the registry. This has two reasons:
    //
    //   - it will avoid a possibility of deadlocking in case if
    //     the callback function to be notified will be doing
    //     any API calls of the controller.
    //
    //   - it will reduce the controller API dead-time due to a prolonged
    //     execution time of of the callback function.

    ControllerRequestWrapper::pointer request;
    {
        LOCK_GUARD;
        request = _registry[id];
        _registry.erase(id);
    }
    request->notify();
}

void
Controller::assertIsRunning () const {
    if (!isRunning())
        throw std::runtime_error("the replication service is not running");
}

}}} // namespace lsst::qserv::replica