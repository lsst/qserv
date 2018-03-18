// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_CONTROLLER_H
#define LSST_QSERV_REPLICA_CONTROLLER_H

/// Controller.h declares:
///
/// class ControllerIdentity
/// class Controller
///
/// (see individual class documentation for more information)

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Request.h"
#include "replica/RequestTypesFwd.h"
#include "replica/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations

class ControllerImpl;
class Messenger;

/**
 * The base class for implementing requests registry as a polymorphic
 * collection to store active requests. Pure virtual methods of
 * the class will be overriden by request-type-specific implementations
 * (see struct RequestWrappeImplr<REQUEST_TYPE> in the .cc file) capturing
 * type-dependant pointer and a callback function.
 */
struct ControllerRequestWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ControllerRequestWrapper> pointer;

    /// Destructor
    virtual ~ControllerRequestWrapper() = default;

    /// This method will be called upon a completion of a request
    /// to notify a subscriber on the event.
    virtual void notify ()=0;

    /// Return a pointer to the stored request object
    virtual std::shared_ptr<Request> request () const=0;
};

/**
 * The data structure encapsulating various attributes which identity
 * each instane of the Controller class. This information is meant to
 * be used in the multi-Controller setups to coordinate operations
 * betweem multiple instances and to avoid/resolve conflicts.
 */
struct ControllerIdentity {

    /// A unique identifier of the Controller
    std::string id;

    /// The name of a hoste where it runs
    std::string host;

    /// An identifier of a process
    pid_t pid;
};

/// The overloaded streaming operator for Controller's identity
std::ostream& operator <<(std::ostream& os, ControllerIdentity const& identity);

/**
  * Class Controller is used for pushing replication (etc.) requests
  * to the worker replication services. Only one instance of this class is
  * allowed per a thread.
  *
  * NOTES:
  *
  * - all methods launching, stopping or checking status of requests
  *   require that the server was runinng. Otherwise it will throw
  *   std::runtime_error. The current implementation of the server
  *   doesn't support (yet?) amn operation queuing mechanism.
  *
  * - methods which take worker names as parameters will throw exception
  *   std::invalid_argument if the specified worker names were not found
  *   in the configuration.
  */
class Controller
    :   public std::enable_shared_from_this<Controller> {

public:

    /// Friend class behind this implementation must have access to
    /// the private methods
    friend class ControllerImpl;

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Controller> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, other services
     */
    static pointer create(ServiceProvider::pointer const& serviceProvider);

    // Default construction and copy semantics are prohibited

    Controller() = delete;
    Controller(Controller const&) = delete;
    Controller& operator=(Controller const&) = delete;

    /// Destructor
    ~Controller() = default;

    /// Return the unique identity of the instance
    ControllerIdentity const& identity() const { return _identity; }

    /// The start time of the instance (milliseconds since UNIX Epoch)
    uint64_t startTime() const { return _startTime; }

    /// Return the Service Provider used by the server
    ServiceProvider::pointer const& serviceProvider() { return _serviceProvider; }

    /// @return reference to the I/O service for ASYNC requests
    boost::asio::io_service& io_service() { return _io_service; }

    /// @return reference to the I/O service for the timer queue
    boost::asio::io_service& io_service2() { return _io_service2; }

    /**
     * Run the Controller in a dedicated thread unless it's already running.
     * It's safe to call this method multiple times from any thread.
     */
    void run();

    /**
     * Check if the service is running.
     *
     * @return true if the service is running.
     */
    bool isRunning() const;

    /**
     * Stop the server. This method will guarantee that all outstanding
     * opeations will finish and not aborted.
     *
     * This operation will also result in stopping the internal thread
     * in which the server is being run.
     */
    void stop();

    /**
     * Join with a thread in which the service is being run (if any).
     * If the service was not started or if it's stopped the the method
     * will return immediattely.
     *
     * This method is meant to be used for integration of the service into
     * a larger multi-threaded application which may require a proper
     * synchronization between threads.
     */
    void join();

    /**
     * Initiate a new replication request.
     *
     * The method will throw exception std::invalid_argument if the worker
     * names are equal.
     *
     * @param workerName       - the name of a worker node from which to copy the chunk
     * @param sourceWorkerName - the name of a worker node where the replica will be created
     * @param database         - database name
     * @param chunk            - the chunk number
     * @param onFinish         - an optional callback function to be called upon the completion of the request
     * @param priority         - a priority level of the request
     * @param keepTracking     - keep tracking the request before it finishes or fails
     * @param allowDuplicate   - follow a previously made request if the current one duplicates it
     * @param jobId            - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                         - an optional parameter (if differs from 0)
     *                           allowing to override the default value of the corresponding
     *                           parameter from the Configuration.
     *
     * @return a pointer to the replication request
     */
    ReplicationRequest_pointer replicate(std::string const& workerName,
                                         std::string const& sourceWorkerName,
                                         std::string const& database,
                                         unsigned int chunk,
                                         ReplicationRequest_callback_type onFinish=nullptr,
                                         int  priority=0,
                                         bool keepTracking=true,
                                         bool allowDuplicate=true,
                                         std::string const& jobId="",
                                         unsigned int requestExpirationIvalSec=0);

    /**
     * Initiate a new replica deletion request.
     *
     * @param workerName     - the name of a worker node where the replica will be deleted
     * @param database       - database name
     * @param chunk          - the chunk number
     * @param onFinish       - an optional callback function to be called upon the completion of the request
     * @param priority       - a priority level of the request
     * @param keepTracking   - keep tracking the request before it finishes or fails
     * @param allowDuplicate - follow a previously made request if the current one duplicates it
     * @param jobId          - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                       - an optional parameter (if differs from 0)
     *                         allowing to override the default value of the corresponding
     *                         parameter from the Configuration.
     *
     * @return a pointer to the replication request
     */
    DeleteRequest_pointer deleteReplica(std::string const& workerName,
                                        std::string const& database,
                                        unsigned int chunk,
                                        DeleteRequest_callback_type onFinish=nullptr,
                                        int  priority=0,
                                        bool keepTracking=true,
                                        bool allowDuplicate=true,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Initiate a new replica lookup request.
     *
     * PERFORMANCE NOTE: enabling 'computeCheckSum' will require reading each file.
     *                   Hence this will slow down the operation, and it may also
     *                   affcet the overall perfromance of Qserv on the corresponding
     *                   worker node.
     *
     * @param workerName      - the name of a worker node where the replica is located
     * @param database        - database name
     * @param chunk           - the chunk number
     * @param onFinish        - an optional callback function to be called upon the completion of the request
     * @param priority        - a priority level of the request
     * @param computeCheckSum - tell a worker server to compute check/control sum on each file
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                         - an optional parameter (if differs from 0)
     *                           allowing to override the default value of the corresponding
     *                           parameter from the Configuration.
     *
     * @return a pointer to the replication request
     */
    FindRequest_pointer findReplica(std::string const& workerName,
                                    std::string const& database,
                                    unsigned int chunk,
                                    FindRequest_callback_type onFinish=nullptr,
                                    int  priority=0,
                                    bool computeCheckSum=false,
                                    bool keepTracking=true,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Initiate a new replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the replicas are located
     * @param database        - database name
     * @param onFinish        - an optional callback function to be called upon the completion of the request
     * @param priority        - a priority level of the request
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the replication request
     */
    FindAllRequest_pointer findAllReplicas(
                                std::string const& workerName,
                                std::string const& database,
                                FindAllRequest_callback_type onFinish=nullptr,
                                int  priority=0,
                                bool keepTracking=true,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the stop request
     */
    StopReplicationRequest_pointer stopReplication(
                                        std::string const& workerName,
                                        std::string const& targetRequestId,
                                        StopReplicationRequest_callback_type onFinish=nullptr,
                                        bool keepTracking=true,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the stop request
     */
    StopDeleteRequest_pointer stopReplicaDelete(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StopDeleteRequest_callback_type onFinish=nullptr,
                                    bool keepTracking=true,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the stop request
     */
    StopFindRequest_pointer stopReplicaFind(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StopFindRequest_callback_type onFinish=nullptr,
                                bool keepTracking=true,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the stop request
     */
    StopFindAllRequest_pointer stopReplicaFindAll(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StopFindAllRequest_callback_type onFinish=nullptr,
                                    bool keepTracking=true,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the status inquery request
     */
    StatusReplicationRequest_pointer statusOfReplication(
                                        std::string const& workerName,
                                        std::string const& targetRequestId,
                                        StatusReplicationRequest_callback_type onFinish=nullptr,
                                        bool keepTracking=false,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the status inquery request
     */
    StatusDeleteRequest_pointer statusOfDelete(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusDeleteRequest_callback_type onFinish=nullptr,
                                    bool keepTracking=false,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the status inquery request
     */
    StatusFindRequest_pointer statusOfFind(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusFindRequest_callback_type onFinish=nullptr,
                                    bool keepTracking=false,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding (multiple) replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param jobId           - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - an optional parameter (if differs from 0)
     *                          allowing to override the default value of the corresponding
     *                          parameter from the Configuration.
     *
     * @return a pointer to the status inquery request
     */
    StatusFindAllRequest_pointer statusOfFindAll(
                                        std::string const& workerName,
                                        std::string const& targetRequestId,
                                        StatusFindAllRequest_callback_type onFinish=nullptr,
                                        bool keepTracking=false,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Tell the worker-side service to temporarily suspend processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     * @param jobId      - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - an optional parameter (if differs from 0)
     *                     allowing to override the default value of the corresponding
     *                     parameter from the Configuration.
     *
     * @return a pointer to the request
     */
    ServiceSuspendRequest_pointer suspendWorkerService(
                                        std::string const& workerName,
                                        ServiceSuspendRequest_callback_type onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Tell the worker-side service to resume processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     * @param jobId      - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - an optional parameter (if differs from 0)
     *                     allowing to override the default value of the corresponding
     *                     parameter from the Configuration.
     *
     * @return a pointer to the request
     */
    ServiceResumeRequest_pointer resumeWorkerService(
                                        std::string const& workerName,
                                        ServiceResumeRequest_callback_type onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);
    /**
     * Request the current status of the worker-side service
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     * @param jobId      - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - an optional parameter (if differs from 0)
     *                     allowing to override the default value of the corresponding
     *                     parameter from the Configuration.
     *
     * @return a pointer to the request
     */
    ServiceStatusRequest_pointer statusOfWorkerService(
                                        std::string const& workerName,
                                        ServiceStatusRequest_callback_type onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Request detailed info on which replication-related requests are known
     * to the worker-side service
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     * @param jobId      - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - an optional parameter (if differs from 0)
     *                     allowing to override the default value of the corresponding
     *                     parameter from the Configuration.
     *
     * @return a pointer to the request
     */
    ServiceRequestsRequest_pointer requestsOfWorkerService(
                                        std::string const& workerName,
                                        ServiceRequestsRequest_callback_type onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Cancel all queue or being processed replica-related requsts known
     * to the worker-side service and return detailed info on all known requests.
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     * @param jobId      - an optional identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - an optional parameter (if differs from 0)
     *                     allowing to override the default value of the corresponding
     *                     parameter from the Configuration.
     *
     * @return a pointer to the request
     */
    ServiceDrainRequest_pointer drainWorkerService(
                                        std::string const& workerName,
                                        ServiceDrainRequest_callback_type onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Return requests of a specific type
     *
     * @param requests - a collection to be populated with requests
     */
    template <class REQUEST_TYPE>
    void requestsOfType(std::vector<typename REQUEST_TYPE::pointer>& requests) const {
        std::lock_guard<std::mutex> lock(_mtx);
        requests.clear();
        for (auto const itr: _registry)
            if (typename REQUEST_TYPE::pointer ptr =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request())) {
                requests.push_back(ptr);
            }
    }

    /**
     * Return the number of requests of a specific type
     */
    template <class REQUEST_TYPE>
    size_t numRequestsOfType() const {
        std::lock_guard<std::mutex> lock(_mtx);
        size_t result {0};
        for (auto const itr: _registry) {
            if (typename REQUEST_TYPE::pointer request =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request())) { ++result; }
        }
        return result;
    }

    /**
     * Return the total number of requests of all kinds
     */
    size_t numActiveRequests() const;

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, other services
     */
    explicit Controller(ServiceProvider::pointer const& serviceProvider);

    /**
     * Finalize the completion of the request. This method will notify
     * a requestor on the completion of the operation and it will also
     * remove the request from the server's registry.
     */
    void finish(std::string const& id);

    /**
     * Make sure the server is runnning. Otherwise throw std::runtime_error.
     */
    void assertIsRunning() const;

private:

    /// The unique identity of the instance
    ControllerIdentity const _identity;

    /// The time (milliseconds since UNIX Epoch) when an instance of
    /// the Controller was created.
    uint64_t const _startTime;

    /// The provider of variou services
    ServiceProvider::pointer _serviceProvider;

    // Two sets of BOOST ASIO communication services & threads which run
    // them. The first set is used by requests. The second is dedicated for timers.

    boost::asio::io_service _io_service;
    std::unique_ptr<boost::asio::io_service::work> _work;
    std::unique_ptr<std::thread> _thread;

    boost::asio::io_service _io_service2;
    std::unique_ptr<boost::asio::io_service::work> _work2;
    std::unique_ptr<std::thread> _thread2;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable std::mutex _mtx;

    /// The registry of the on-going requests.
    std::map<std::string,std::shared_ptr<ControllerRequestWrapper>> _registry;

    /// Worker messenger service
    std::shared_ptr<Messenger> _messenger;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONTROLLER_H
