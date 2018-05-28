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
#include <thread>
#include <vector>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Request.h"
#include "replica/RequestTypesFwd.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations

class ControllerImpl;
class Messenger;

/**
 * Class ControllerRequestWrapper is the base class for implementing requests
 * registry as a polymorphic collection to store active requests. Pure virtual
 * methods of the class will be overriden by request-type-specific implementations
 * (see struct RequestWrappeImplr<REQUEST_TYPE> in the .cc file) capturing
 * type-dependant pointer and a callback function.
 */
struct ControllerRequestWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ControllerRequestWrapper> Ptr;

    virtual ~ControllerRequestWrapper() = default;

    /// This subscriber notification method will be called upon a completion of a requess
    virtual void notify()=0;

    /// Return a pointer to the stored request object
    virtual std::shared_ptr<Request> request() const=0;
};

/**
 * Struvcy ControllerIdentity encapsulates various attributes which identify
 * each instance of the Controller class. This information is meant to
 * be used in the multi-Controller setups to coordinate operations
 * between multiple instances and to avoid/resolve conflicts.
 */
struct ControllerIdentity {

    /// A unique identifier of the Controller
    std::string id;

    /// The name of a host where it runs
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
  *   require that the server to be runinng. Otherwise it will throw
  *   std::runtime_error. The current implementation of the server
  *   doesn't support (yet?) an operation queuing mechanism.
  *
  * - methods which take worker names as parameters will throw exception
  *   std::invalid_argument if the specified worker names are not found
  *   in the configuration.
  */
class Controller
    :   public std::enable_shared_from_this<Controller> {

public:

    /// Friend class behind this implementation must have access to the private methods
    friend class ControllerImpl;

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Controller> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, other services
     * @return ponter to an instance of the Class
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider);

    // Default construction and copy semantics are prohibited

    Controller() = delete;
    Controller(Controller const&) = delete;
    Controller& operator=(Controller const&) = delete;

    ~Controller() = default;

    /// @return the unique identity of the instance
    ControllerIdentity const& identity() const { return _identity; }

    /// @return the start time of the instance (milliseconds since UNIX Epoch)
    uint64_t startTime() const { return _startTime; }

    /// @return the Service Provider used by the server
    ServiceProvider::Ptr const& serviceProvider() { return _serviceProvider; }

    /// @return reference to the I/O service for ASYNC requests
    boost::asio::io_service& io_service() { return _io_service; }

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
     * Create and start a new request for creating a replica.
     *
     * The method will throw exception std::invalid_argument if the worker
     * names are equal.
     *
     * @param workerName       - the name of a worker node from which to copy the chunk
     * @param sourceWorkerName - the name of a worker node where the replica will be created
     * @param database         - database name
     * @param chunk            - the chunk number
     * @param onFinish         - (optional) callback function to be called upon the completion of the request
     * @param priority         - (optional) priority level of the request
     * @param keepTracking     - (optional) keep tracking the request before it finishes or fails
     * @param allowDuplicate   - (optional) follow a previously made request if the current one duplicates it
     * @param jobId            - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                         - (optional) parameter (if differs from 0) allowing to override the default
     *                           value of the corresponding parameter from the Configuration.
     * @return a pointer to the replication request
     */
    ReplicationRequestPtr replicate(std::string const& workerName,
                                    std::string const& sourceWorkerName,
                                    std::string const& database,
                                    unsigned int chunk,
                                    ReplicationRequestCallbackType onFinish=nullptr,
                                    int  priority=0,
                                    bool keepTracking=true,
                                    bool allowDuplicate=true,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for deleting a replica.
     *
     * @param workerName     - the name of a worker node where the replica will be deleted
     * @param database       - database name
     * @param chunk          - the chunk number
     * @param onFinish       - (optional) callback function to be called upon the completion of the request
     * @param priority       - (optional) priority level of the request
     * @param keepTracking   - (optional) keep tracking the request before it finishes or fails
     * @param allowDuplicate - (optional) follow a previously made request if the current one duplicates it
     * @param jobId          - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                       - (optional) parameter (if differs from 0) allowing to override the default
     *                         value of the corresponding parameter from the Configuration.
     * @return a pointer to the replication request
     */
    DeleteRequestPtr deleteReplica(std::string const& workerName,
                                   std::string const& database,
                                   unsigned int chunk,
                                   DeleteRequestCallbackType onFinish=nullptr,
                                   int  priority=0,
                                   bool keepTracking=true,
                                   bool allowDuplicate=true,
                                   std::string const& jobId="",
                                   unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for finding a replica.
     *
     * PERFORMANCE NOTE: enabling 'computeCheckSum' will require reading each file.
     *                   Hence this will slow down the operation, and it may also
     *                   affcet the overall perfromance of Qserv on the corresponding
     *                   worker node.
     *
     * @param workerName      - the name of a worker node where the replica is located
     * @param database        - database name
     * @param chunk           - the chunk number
     * @param onFinish        - (optional) callback function to be called upon the completion of the request
     * @param priority        - (optional) priority level of the request
     * @param computeCheckSum - (optional) tell worker server to compute check/control sum on each file
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                         - (optional) parameter (if differs from 0) allowing to override the default
     *                           value of the corresponding parameter from the Configuration.
     * @return a pointer to the replication request
     */
    FindRequestPtr findReplica(std::string const& workerName,
                               std::string const& database,
                               unsigned int chunk,
                               FindRequestCallbackType onFinish=nullptr,
                               int  priority=0,
                               bool computeCheckSum=false,
                               bool keepTracking=true,
                               std::string const& jobId="",
                               unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for finding replicas in a scope of a database.
     *
     * @param workerName      - the name of a worker node where the replicas are located
     * @param database        - database name
     * @param saveReplicaInfo - (optional) save replica info in a database
     * @param onFinish        - (optional) callback function to be called upon the completion of the request
     * @param priority        - (optional) priority level of the request
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the replication request
     */
    FindAllRequestPtr findAllReplicas(
                                std::string const& workerName,
                                std::string const& database,
                                bool saveReplicaInfo=true,
                                FindAllRequestCallbackType onFinish=nullptr,
                                int  priority=0,
                                bool keepTracking=true,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the stop request
     */
    StopReplicationRequestPtr stopReplication(
                                        std::string const& workerName,
                                        std::string const& targetRequestId,
                                        StopReplicationRequestCallbackType onFinish=nullptr,
                                        bool keepTracking=true,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the stop request
     */
    StopDeleteRequestPtr stopReplicaDelete(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StopDeleteRequestCallbackType onFinish=nullptr,
                                    bool keepTracking=true,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the stop request
     */
    StopFindRequestPtr stopReplicaFind(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StopFindRequestCallbackType onFinish=nullptr,
                                bool keepTracking=true,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the stop request
     */
    StopFindAllRequestPtr stopReplicaFindAll(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StopFindAllRequestCallbackType onFinish=nullptr,
                                    bool keepTracking=true,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the status inquery request
     */
    StatusReplicationRequestPtr statusOfReplication(
                                        std::string const& workerName,
                                        std::string const& targetRequestId,
                                        StatusReplicationRequestCallbackType onFinish=nullptr,
                                        bool keepTracking=false,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the status inquery request
     */
    StatusDeleteRequestPtr statusOfDelete(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusDeleteRequestCallbackType onFinish=nullptr,
                                    bool keepTracking=false,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the status inquery request
     */
    StatusFindRequestPtr statusOfFind(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusFindRequestCallbackType onFinish=nullptr,
                                    bool keepTracking=false,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding (multiple) replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - (optional) callback function to be called upon completion of the operation
     * @param keepTracking    - (optional) keep tracking the request before it finishes or fails
     * @param jobId           - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                        - (optional) parameter (if differs from 0) allowing to override the default
     *                          value of the corresponding parameter from the Configuration.
     * @return a pointer to the status inquery request
     */
    StatusFindAllRequestPtr statusOfFindAll(
                                        std::string const& workerName,
                                        std::string const& targetRequestId,
                                        StatusFindAllRequestCallbackType onFinish=nullptr,
                                        bool keepTracking=false,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Tell the worker-side service to temporarily suspend processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - (optional) callback function to be called upon completion of the operation
     * @param jobId      - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - (optional) parameter (if differs from 0) allowing to override the default
     *                     value of the corresponding parameter from the Configuration.
     * @return a pointer to the request
     */
    ServiceSuspendRequestPtr suspendWorkerService(
                                        std::string const& workerName,
                                        ServiceSuspendRequestCallbackType onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Tell the worker-side service to resume processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - (optional) callback function to be called upon completion of the operation
     * @param jobId      - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - (optional) parameter (if differs from 0) allowing to override the default
     *                     value of the corresponding parameter from the Configuration.
     * @return a pointer to the request
     */
    ServiceResumeRequestPtr resumeWorkerService(
                                        std::string const& workerName,
                                        ServiceResumeRequestCallbackType onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);
    /**
     * Request the current status of the worker-side service
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - (optional) callback function to be called upon completion of the operation
     * @param jobId      - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - (optional) parameter (if differs from 0) allowing to override the default
     *                     value of the corresponding parameter from the Configuration.
     * @return a pointer to the request
     */
    ServiceStatusRequestPtr statusOfWorkerService(
                                        std::string const& workerName,
                                        ServiceStatusRequestCallbackType onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Request detailed info on which replication-related requests are known
     * to the worker-side service
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - (optional) callback function to be called upon completion of the operation
     * @param jobId      - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - (optional) parameter (if differs from 0) allowing to override the default
     *                     value of the corresponding parameter from the Configuration.
     * @return a pointer to the request
     */
    ServiceRequestsRequestPtr requestsOfWorkerService(
                                        std::string const& workerName,
                                        ServiceRequestsRequestCallbackType onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Cancel all queue or being processed replica-related requsts known
     * to the worker-side service and return detailed info on all known requests.
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - (optional) callback function to be called upon completion of the operation
     * @param jobId      - (optional) identifier of a job issed the request
     * @param requestExpirationIvalSec
     *                   - (optional) parameter (if differs from 0) allowing to override the default
     *                     value of the corresponding parameter from the Configuration.
     * @return a pointer to the request
     */
    ServiceDrainRequestPtr drainWorkerService(
                                        std::string const& workerName,
                                        ServiceDrainRequestCallbackType onFinish=nullptr,
                                        std::string const& jobId="",
                                        unsigned int requestExpirationIvalSec=0);

    /**
     * Return requests of a specific type
     *
     * @param requests - a collection to be populated with requests
     */
    template <class REQUEST_TYPE>
    void requestsOfType(std::vector<typename REQUEST_TYPE::Ptr>& requests) const {
        util::Lock lock(_mtx, context() + "requestsOfType");
        requests.clear();
        for (auto&& itr: _registry)
            if (typename REQUEST_TYPE::Ptr ptr =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request())) {
                requests.push_back(ptr);
            }
    }

    /**
     * @return the number of requests of a specific type
     */
    template <class REQUEST_TYPE>
    size_t numRequestsOfType() const {
        util::Lock lock(_mtx, context() + "numRequestsOfType");
        size_t result(0);
        for (auto&& itr: _registry) {
            if (typename REQUEST_TYPE::Ptr request =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request())) { ++result; }
        }
        return result;
    }

    /// @return the total number of requests of all kinds
    size_t numActiveRequests() const;

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, other services
     */
    explicit Controller(ServiceProvider::Ptr const& serviceProvider);

    /// @return the context string for debugging and diagnostic printouts
    std::string context() const;

    /**
     * Finalize the completion of the request. This method will notify
     * a requestor on the completion of the operation and it will also
     * remove the request from the server's registry.
     *
     * @param id - a unique identifier of a request
     */
    void finish(std::string const& id);

    /**
     * Make sure the server is runnning
     *
     * @throws std::runtime_error if the server is not running
     */
    void assertIsRunning() const;

private:

    /// The unique identity of the instance
    ControllerIdentity const _identity;

    /// The time (milliseconds since UNIX Epoch) when an instance of
    /// the Controller was created.
    uint64_t const _startTime;

    /// The provider of various services
    ServiceProvider::Ptr _serviceProvider;

    // The BOOST ASIO communication services & threads which run them

    size_t _numThreads;

    boost::asio::io_service _io_service;
    std::unique_ptr<boost::asio::io_service::work> _work;
    std::vector<std::shared_ptr<std::thread>> _threads;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable util::Mutex _mtx;

    /// The registry of the on-going requests.
    std::map<std::string,std::shared_ptr<ControllerRequestWrapper>> _registry;

    /// Worker messenger service
    std::shared_ptr<Messenger> _messenger;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONTROLLER_H
