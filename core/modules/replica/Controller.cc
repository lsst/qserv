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
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
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
        if (_onFinish == nullptr) return;
        _onFinish(_request);
    }

    RequestWrapperImpl(typename T::Ptr const& request,
                       typename T::CallbackType onFinish)
        :   ControllerRequestWrapper(),
            _request(request),
            _onFinish(onFinish) {
    }

    /// Destructor
    ~RequestWrapperImpl() override = default;

    /// @see ControllerRequestWrapper::request()
    std::shared_ptr<Request> request() const override {
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
 * The utiliy class implementing operations on behalf of certain
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
     * request or inquering a status of a request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be affected
     * @param onFinish        - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::Ptr requestManagementOperation(
            Controller::Ptr const& controller,
            std::string const& jobId,
            std::string const& workerName,
            std::string const& targetRequestId,
            typename REQUEST_TYPE::CallbackType onFinish,
            bool  keepTracking,
            typename Messenger::Ptr const& messenger,
            unsigned int requestExpirationIvalSec) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::Ptr request =
            REQUEST_TYPE::create(
                controller->_serviceProvider,
                controller->_io_service,
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
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>>(request, onFinish);

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
            std::string const& jobId,
            std::string const& workerName,
            typename REQUEST_TYPE::CallbackType onFinish,
            typename Messenger::Ptr const& messenger,
            unsigned int requestExpirationIvalSec) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::Ptr request =
            REQUEST_TYPE::create(
                controller->_serviceProvider,
                controller->_io_service,
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
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>>(request, onFinish);

        // Initiate the request

        request->start(controller, jobId, requestExpirationIvalSec);

        return request;
    }
};

//////////////////////////////////////////////////////////////////////////
//////////////////////////  ControllerIdentity  //////////////////////////
//////////////////////////////////////////////////////////////////////////

std::ostream& operator <<(std::ostream& os, ControllerIdentity const& identity) {
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
        _serviceProvider(serviceProvider),
        _numThreads(serviceProvider->config()->controllerThreads()) {

    if (0 == _numThreads) {
        throw std::runtime_error(
            "Controller:  configuration problem, the number of threads is set to 0");
    }

    _messenger = Messenger::create(_serviceProvider, _io_service);
    serviceProvider->databaseServices()->saveState(_identity, _startTime);
}

std::string Controller::context() const {
    return "R-CONTR " + _identity.id + "  " + _identity.host +
           "[" + std::to_string(_identity.pid) + "]  ";
}

void Controller::run() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "run");

    util::Lock lock(_mtx, context() + "run");

    if (not isRunning()) {

        _work.reset(new boost::asio::io_service::work(_io_service));

        Controller::Ptr const self = shared_from_this();

        _threads.clear();
        for (size_t i = 0; i < _numThreads; ++i) {
            _threads.emplace_back(std::make_shared<std::thread>(
                [self] () {

                    // This will prevent the I/O service from exiting the .run()
                    // method event when it will run out of any requests to process.
                    // Unless the service will be explicitly stopped.
                    self->_io_service.run();
                }
            ));
        }
    }
}

bool Controller::isRunning() const {
    return _threads.size() != 0;
}

void Controller::stop() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "stop");

    if (not isRunning()) return;

    // IMPORTANT:
    //
    //   Never attempt running these operations within Lock(_mtx,...)
    //   due to a possibile deadlock when asynchronous handlers will be
    //   calling the thread-safe methods. A problem is that until they finish
    //   in a clean way (as per the _work.reset()) the thread will never finish,
    //   and the application will hang on _thread->join().

    //   (disabled)
    //   util::Lock lock(_mtx, context() + "run");

    // These steps will cancel all oustanding requests
    _messenger->stop();

    // Destoring this object will let the I/O service to (eventually) finish
    // all on-going work and shut down all Controller's threads. In that case there
    // is no need to stop the service explicitly (which is not a good idea anyway
    // because there may be outstanding synchronous requests, in which case the service
    // would get into an unpredictanle state.)

    _work.reset();

    // At this point all outstanding requests should finish and all threads
    // should stop as well.
    join();

    // Always do so in order to put service into a clean state. This will prepare
    // it for further usage.
    _io_service.reset();

    _threads.clear();

    // Double check that the collection of requests is empty.

    if (not _registry.empty()) {
        throw std::logic_error(
            "Controller::stop() the collection of outstanding requests is not empty");
    }
}

void Controller::join() {
    for (auto&& t: _threads) t->join();
}

ReplicationRequest::Ptr Controller::replicate(
                                std::string const& workerName,
                                std::string const& sourceWorkerName,
                                std::string const& database,
                                unsigned int chunk,
                                ReplicationRequest::CallbackType onFinish,
                                int  priority,
                                bool keepTracking,
                                bool allowDuplicate,
                                std::string const& jobId,
                                unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "replicate");

    util::Lock lock(_mtx, context() + "replicate");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    ReplicationRequest::Ptr request =
        ReplicationRequest::create(
            _serviceProvider,
            _io_service,
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
            _messenger
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<ReplicationRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
}

DeleteRequest::Ptr Controller::deleteReplica(
                            std::string const& workerName,
                            std::string const& database,
                            unsigned int chunk,
                            DeleteRequest::CallbackType onFinish,
                            int  priority,
                            bool keepTracking,
                            bool allowDuplicate,
                            std::string const& jobId,
                            unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "deleteReplica");

    util::Lock lock(_mtx, context() + "deleteReplica");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    DeleteRequest::Ptr request =
        DeleteRequest::create(
            _serviceProvider,
            _io_service,
            workerName,
            database,
            chunk,
            [controller] (DeleteRequest::Ptr request) {
                controller->finish(request->id());
            },
            priority,
            keepTracking,
            allowDuplicate,
            _messenger
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<DeleteRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
}

FindRequest::Ptr Controller::findReplica(
                        std::string const& workerName,
                        std::string const& database,
                        unsigned int chunk,
                        FindRequest::CallbackType onFinish,
                        int  priority,
                        bool computeCheckSum,
                        bool keepTracking,
                        std::string const& jobId,
                        unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "findReplica");

    util::Lock lock(_mtx, context() + "findReplica");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    FindRequest::Ptr request =
        FindRequest::create(
            _serviceProvider,
            _io_service,
            workerName,
            database,
            chunk,
            [controller] (FindRequest::Ptr request) {
                controller->finish(request->id());
            },
            priority,
            computeCheckSum,
            keepTracking,
            _messenger
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
}

FindAllRequest::Ptr Controller::findAllReplicas(
                            std::string const& workerName,
                            std::string const& database,
                            bool saveReplicaInfo,
                            FindAllRequest::CallbackType onFinish,
                            int  priority,
                            bool keepTracking,
                            std::string const& jobId,
                            unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "findAllReplicas");

    util::Lock lock(_mtx, context() + "findAllReplicas");

    assertIsRunning();

    Controller::Ptr controller = shared_from_this();

    FindAllRequest::Ptr request =
        FindAllRequest::create(
            _serviceProvider,
            _io_service,
            workerName,
            database,
            saveReplicaInfo,
            [controller] (FindAllRequest::Ptr request) {
                controller->finish(request->id());
            },
            priority,
            keepTracking,
            _messenger
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindAllRequest>>(request, onFinish);

    // Initiate the request

    request->start(controller, jobId, requestExpirationIvalSec);

    return request;
}

StopReplicationRequest::Ptr Controller::stopReplication(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StopReplicationRequest::CallbackType onFinish,
                                    bool keepTracking,
                                    std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

StopDeleteRequest::Ptr Controller::stopReplicaDelete(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StopDeleteRequest::CallbackType onFinish,
                                bool keepTracking,
                                std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

StopFindRequest::Ptr Controller::stopReplicaFind(
                            std::string const& workerName,
                            std::string const& targetRequestId,
                            StopFindRequest::CallbackType onFinish,
                            bool keepTracking,
                            std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

StopFindAllRequest::Ptr Controller::stopReplicaFindAll(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StopFindAllRequest::CallbackType onFinish,
                                bool keepTracking,
                                std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

StatusReplicationRequest::Ptr Controller::statusOfReplication(
                                        std::string const& workerName,
                                        std::string const& targetRequestId,
                                        StatusReplicationRequest::CallbackType onFinish,
                                        bool keepTracking,
                                        std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

StatusDeleteRequest::Ptr Controller::statusOfDelete(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusDeleteRequest::CallbackType onFinish,
                                    bool keepTracking,
                                    std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

StatusFindRequest::Ptr Controller::statusOfFind(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StatusFindRequest::CallbackType onFinish,
                                bool keepTracking,
                                std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

StatusFindAllRequest::Ptr Controller::statusOfFindAll(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusFindAllRequest::CallbackType onFinish,
                                    bool keepTracking,
                                    std::string const& jobId,
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
        _messenger,
        requestExpirationIvalSec);
}

ServiceSuspendRequest::Ptr Controller::suspendWorkerService(
                                    std::string const& workerName,
                                    ServiceSuspendRequest::CallbackType onFinish,
                                    std::string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "suspendWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "suspendWorkerService");

    return ControllerImpl::serviceManagementOperation<ServiceSuspendRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        _messenger,
        requestExpirationIvalSec);
}

ServiceResumeRequest::Ptr Controller::resumeWorkerService(
                                    std::string const& workerName,
                                    ServiceResumeRequest::CallbackType onFinish,
                                    std::string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "resumeWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "resumeWorkerService");

    return ControllerImpl::serviceManagementOperation<ServiceResumeRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        _messenger,
        requestExpirationIvalSec);
}

ServiceStatusRequest::Ptr Controller::statusOfWorkerService(
                                    std::string const& workerName,
                                    ServiceStatusRequest::CallbackType onFinish,
                                    std::string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusOfWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "statusOfWorkerService");

    return ControllerImpl::serviceManagementOperation<ServiceStatusRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        _messenger,
        requestExpirationIvalSec);
}

ServiceRequestsRequest::Ptr Controller::requestsOfWorkerService(
                                    std::string const& workerName,
                                    ServiceRequestsRequest::CallbackType onFinish,
                                    std::string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestsOfWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "requestsOfWorkerService");

    return ControllerImpl::serviceManagementOperation<ServiceRequestsRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        _messenger,
        requestExpirationIvalSec);
}

ServiceDrainRequest::Ptr Controller::drainWorkerService(
                                    std::string const& workerName,
                                    ServiceDrainRequest::CallbackType onFinish,
                                    std::string const& jobId,
                                    unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "drainWorkerService  workerName: " << workerName);

    util::Lock lock(_mtx, context() + "drainWorkerService");

    return ControllerImpl::serviceManagementOperation<ServiceDrainRequest>(
        shared_from_this(),
        jobId,
        workerName,
        onFinish,
        _messenger,
        requestExpirationIvalSec);
}

size_t Controller::numActiveRequests() const {
    util::Lock lock(_mtx, context() + "numActiveRequests");
    return _registry.size();
}

void Controller::finish(std::string const& id) {

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
    if (not isRunning()) {
        throw std::runtime_error("the replication service is not running");
    }
}

}}} // namespace lsst::qserv::replica
