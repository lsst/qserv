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
#include <map>
#include <memory>
#include <thread>
#include <vector>

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

/**
 * Class ControllerRequestWrapper is the base class for implementing requests
 * registry as a polymorphic collection to store active requests. Pure virtual
 * methods of the class will be overridden by request-type-specific implementations
 * (see structure RequestWrappeImplr<REQUEST_TYPE> in the .cc file) capturing
 * type-dependent pointer and a callback function.
 */
struct ControllerRequestWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ControllerRequestWrapper> Ptr;

    virtual ~ControllerRequestWrapper() = default;

    /// This subscriber notification method will be called upon a completion of a request
    virtual void notify() = 0;

    /// Return a pointer to the stored request object
    virtual std::shared_ptr<Request> request() const = 0;
};

/**
 * Structure ControllerIdentity encapsulates various attributes which identify
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
  * @note:
  *   All methods launching, stopping or checking status of requests
  *   require that the server to be running. Otherwise it will throw
  *   std::runtime_error. The current implementation of the server
  *   doesn't support (yet?) an operation queuing mechanism.
 *
  * @node
  *   Methods which take worker names as parameters will throw exception
  *   std::invalid_argument if the specified worker names are not found
  *   in the configuration.
  */
class Controller : public std::enable_shared_from_this<Controller> {

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
     * @param serviceProvider
     *   for configuration, other services
     *
     * @return
     *   pointer to an instance of the Class
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
    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }

    /// @return reference to the I/O service for ASYNC requests
    boost::asio::io_service& io_service() { return serviceProvider()->io_service(); }

    /**
     * Create and start a new request for creating a replica.
     *
     * The method will throw exception std::invalid_argument if the worker
     * names are equal.
     *
     * @param workerName
     *   the name of a worker node from which to copy the chunk
     *
     * @param sourceWorkerName
     *   the name of a worker node where the replica will be created
     *
     * @param database
     *   database name
     * 
     * @param chunk
     *   the chunk to be replicated
     * 
     * @param onFinish
     *   (optional) callback function to be called upon the completion of the request
     * 
     * @param priority
     *   (optional) priority level of the request
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param allowDuplicate
     *   (optional) follow a previously made request if the current one duplicates it
     * 
     * @param jobId
     *   (optional) identifier of a job issued the request
     * 
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    ReplicationRequestPtr replicate(std::string const& workerName,
                                    std::string const& sourceWorkerName,
                                    std::string const& database,
                                    unsigned int chunk,
                                    ReplicationRequestCallbackType const& onFinish=nullptr,
                                    int  priority=0,
                                    bool keepTracking=true,
                                    bool allowDuplicate=true,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for deleting a replica.
     *
     * @param workerName
     *   the name of a worker node where the replica will be deleted
     * 
     * @param database
     *   database name
     * 
     * @param chunk
     *   the chunk whose replica will be deleted
     * 
     * @param onFinish
     *   (optional) callback function to be called upon the completion of
     *   the request
     *
     * @param priority
     *   (optional) priority level of the request
     * 
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     * 
     * @param allowDuplicate
     *   (optional) follow a previously made request if the current one
     *   duplicates it
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override
     *   the default value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    DeleteRequestPtr deleteReplica(std::string const& workerName,
                                   std::string const& database,
                                   unsigned int chunk,
                                   DeleteRequestCallbackType const& onFinish=nullptr,
                                   int  priority=0,
                                   bool keepTracking=true,
                                   bool allowDuplicate=true,
                                   std::string const& jobId="",
                                   unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for finding a replica.
     *
     * @note
     *  On the performance of the operation: enabling 'computeCheckSum' will require
     *  reading each file. Hence this will slow down the operation, and it may also
     *  affect the overall performance of Qserv on the corresponding worker node.
     *
     * @param workerName
     *   the name of a worker node where the replica is located
     *
     * @param database
     *   database name
     *
     * @param chunk
     *   the chunk whose replicas will be looked for
     * 
     * @param onFinish
     *   (optional) callback function to be called upon the completion of
     *   the request
     *
     * @param priority
     *   (optional) priority level of the request
     * 
     * @param computeCheckSum
     *   (optional) tell worker server to compute check/control sum on each file
     * 
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     * 
     * @param jobId
     *   (optional) identifier of a job issued the request
     * 
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    FindRequestPtr findReplica(std::string const& workerName,
                               std::string const& database,
                               unsigned int chunk,
                               FindRequestCallbackType const& onFinish=nullptr,
                               int  priority=0,
                               bool computeCheckSum=false,
                               bool keepTracking=true,
                               std::string const& jobId="",
                               unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for finding replicas in a scope of a database.
     *
     * @param workerName
     *   the name of a worker node where the replicas are located
     * 
     * @param database
     *   database name
     * 
     * @param saveReplicaInfo
     *   (optional) save replica info in a database
     *
     * @param onFinish
     *   (optional) callback function to be called upon the completion of
     *   the request
     *
     * @param priority
     *   (optional) priority level of the request
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    FindAllRequestPtr findAllReplicas(std::string const& workerName,
                                      std::string const& database,
                                      bool saveReplicaInfo=true,
                                      FindAllRequestCallbackType const& onFinish=nullptr,
                                      int  priority=0,
                                      bool keepTracking=true,
                                      std::string const& jobId="",
                                      unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for testing the worker-side back-end.
     *
     * @param workerName
     *   the name of a worker node where the replicas are located
     * 
     * @param data
     *   data string to be send to and received from the worker
     *
     * @param delay
     *   execution time (milliseconds) of the request at worker
     *
     * @param onFinish
     *   (optional) callback function to be called upon the completion of
     *   the request
     *
     * @param priority
     *   (optional) priority level of the request
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    EchoRequestPtr echo(std::string const& workerName,
                        std::string const& data,
                        uint64_t delay,
                        EchoRequestCallbackType const& onFinish=nullptr,
                        int  priority=0,
                        bool keepTracking=true,
                        std::string const& jobId="",
                        unsigned int requestExpirationIvalSec=0);

    /**
     * Create and start a new request for executing queries against worker databases
     *
     * @param workerName
     *   the name of a worker node where the replicas are located
     * 
     * @param query
     *   the query to be executed
     *
     * @param user
     *   the name of a database account for connecting to the database service
     *
     * @param password
     *   a database for connecting to the database service
     *
     * @param maxRows
     *   (optional) limit for the maximum number of rows to be returned with the request.
     *   Laving the default value of the parameter to 0 will result in not imposing any
     *   explicit restrictions on a size of the result set. NOte that other, resource-defined
     *   restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *   objects, the amount of available memory, etc.
     *
     * @param onFinish
     *   (optional) callback function to be called upon the completion of
     *   the request
     *
     * @param priority
     *   (optional) priority level of the request
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    SqlRequestPtr sql(std::string const& workerName,
                      std::string const& worker,
                      std::string const& query,
                      std::string const& user,
                      std::string const& password,
                      uint64_t maxRows,
                      SqlRequestCallbackType const& onFinish=nullptr,
                      int  priority=0,
                      bool keepTracking=true,
                      std::string const& jobId="",
                      unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replication request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be stopped
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StopReplicationRequestPtr stopReplication(std::string const& workerName,
                                              std::string const& targetRequestId,
                                              StopReplicationRequestCallbackType const& onFinish=nullptr,
                                              bool keepTracking=true,
                                              std::string const& jobId="",
                                              unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replica deletion request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be stopped
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StopDeleteRequestPtr stopReplicaDelete(std::string const& workerName,
                                           std::string const& targetRequestId,
                                           StopDeleteRequestCallbackType const& onFinish=nullptr,
                                           bool keepTracking=true,
                                           std::string const& jobId="",
                                           unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replica lookup request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be stopped
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StopFindRequestPtr stopReplicaFind(std::string const& workerName,
                                       std::string const& targetRequestId,
                                       StopFindRequestCallbackType const& onFinish=nullptr,
                                       bool keepTracking=true,
                                       std::string const& jobId="",
                                       unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding replicas lookup request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be stopped
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StopFindAllRequestPtr stopReplicaFindAll(std::string const& workerName,
                                             std::string const& targetRequestId,
                                             StopFindAllRequestCallbackType const& onFinish=nullptr,
                                             bool keepTracking=true,
                                             std::string const& jobId="",
                                             unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding worker framework testing request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be stopped
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StopEchoRequestPtr stopEcho(std::string const& workerName,
                                std::string const& targetRequestId,
                                StopEchoRequestCallbackType const& onFinish=nullptr,
                                bool keepTracking=true,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Stop an outstanding database query request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be stopped
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StopSqlRequestPtr stopSql(std::string const& workerName,
                              std::string const& targetRequestId,
                              StopSqlRequestCallbackType const& onFinish=nullptr,
                              bool keepTracking=true,
                              std::string const& jobId="",
                              unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replication request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be inspected
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of the operation
     * 
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StatusReplicationRequestPtr statusOfReplication(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusReplicationRequestCallbackType const& onFinish=nullptr,
                                    bool keepTracking=false,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replica deletion request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be inspected
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StatusDeleteRequestPtr statusOfDelete(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StatusDeleteRequestCallbackType const& onFinish=nullptr,
                                bool keepTracking=false,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding replica lookup request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be inspected
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of the operation
     * 
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     * 
     * @param jobId
     *   (optional) identifier of a job issued the request
     * 
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StatusFindRequestPtr statusOfFind(
                            std::string const& workerName,
                            std::string const& targetRequestId,
                            StatusFindRequestCallbackType const& onFinish=nullptr,
                            bool keepTracking=false,
                            std::string const& jobId="",
                            unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding (multiple) replicas lookup request.
     *
     * @param workerName
     *    the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *    an identifier of a request to be inspected
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StatusFindAllRequestPtr statusOfFindAll(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StatusFindAllRequestCallbackType const& onFinish=nullptr,
                                bool keepTracking=false,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding worker framework testing request.
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be inspected
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StatusEchoRequestPtr statusOfEcho(
                            std::string const& workerName,
                            std::string const& targetRequestId,
                            StatusEchoRequestCallbackType const& onFinish=nullptr,
                            bool keepTracking=false,
                            std::string const& jobId="",
                            unsigned int requestExpirationIvalSec=0);

    /**
     * Check the on-going status of an outstanding databae query
     *
     * @param workerName
     *   the name of a worker node where the request was launched
     *
     * @param targetRequestId
     *   an identifier of a request to be inspected
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param keepTracking
     *   (optional) keep tracking the request before it finishes or fails
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    StatusSqlRequestPtr statusOfSql(
                            std::string const& workerName,
                            std::string const& targetRequestId,
                            StatusSqlRequestCallbackType const& onFinish=nullptr,
                            bool keepTracking=false,
                            std::string const& jobId="",
                            unsigned int requestExpirationIvalSec=0);

    /**
     * Tell the worker-side service to temporarily suspend processing requests
     *
     * @param workerName
     *   the name of a worker node where the service runs
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    ServiceSuspendRequestPtr suspendWorkerService(
                                std::string const& workerName,
                                ServiceSuspendRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Tell the worker-side service to resume processing requests
     *
     * @param workerName
     *   the name of a worker node where the service runs
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    ServiceResumeRequestPtr resumeWorkerService(
                                std::string const& workerName,
                                ServiceResumeRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);
    /**
     * Request the current status of the worker-side service
     *
     * @param workerName
     *   the name of a worker node where the service runs
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of
     *   the operation
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    ServiceStatusRequestPtr statusOfWorkerService(
                                std::string const& workerName,
                                ServiceStatusRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Request detailed info on which replication-related requests are known
     * to the worker-side service
     *
     * @param workerName
     *   the name of a worker node where the service runs
     *
     * @param onFinish
     *   (optional) callback function to be called upon completion of the operation
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    ServiceRequestsRequestPtr requestsOfWorkerService(
                                    std::string const& workerName,
                                    ServiceRequestsRequestCallbackType const& onFinish=nullptr,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    /**
     * Cancel all queue or being processed replica-related requests known
     * to the worker-side service and return detailed info on all known requests.
     *
     * @param workerName
     *   the name of a worker node where the service runs
     * 
     * @param onFinish
     *   (optional) callback function to be called upon completion of the operation
     *
     * @param jobId
     *   (optional) identifier of a job issued the request
     *
     * @param requestExpirationIvalSec
     *   (optional) parameter (if differs from 0) allowing to override the default
     *   value of the corresponding parameter from the Configuration.
     *
     * @return
     *   a pointer to the new request
     */
    ServiceDrainRequestPtr drainWorkerService(
                                std::string const& workerName,
                                ServiceDrainRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    /**
     * Return requests of a specific type
     *
     * @param requests
     *   a collection to be populated with requests
     */
    template <class REQUEST_TYPE>
    void requestsOfType(std::vector<typename REQUEST_TYPE::Ptr>& requests) const {
        util::Lock lock(_mtx, _context(__func__));
        requests.clear();
        for (auto&& itr: _registry)
            if (typename REQUEST_TYPE::Ptr ptr =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request())) {
                requests.push_back(ptr);
            }
    }

    /// @return the number of requests of a specific type
    template <class REQUEST_TYPE>
    size_t numRequestsOfType() const {
        util::Lock lock(_mtx, _context(__func__));
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

    /// @see Controller::create()
    explicit Controller(ServiceProvider::Ptr const& serviceProvider);

    /**
     * @param func
     *   (optional) the name of a method/function requested the context string
     *
     * @return
     *   the context string for debugging and diagnostic printouts
     */
    std::string _context(std::string const& func=std::string()) const;

    /**
     * Finalize the completion of the request. This method will notify
     * a requester on the completion of the operation and it will also
     * remove the request from the server's registry.
     *
     * @param id
     *   a unique identifier of a request
     */
    void _finish(std::string const& id);

    /**
     * Make sure the server is running
     *
     * @throws std::runtime_error
     *   if the server is not running
     */
    void _assertIsRunning() const;


    /// The unique identity of the instance
    ControllerIdentity const _identity;

    /// The time (milliseconds since UNIX Epoch) when an instance of
    /// the Controller was created.
    uint64_t const _startTime;

    /// The provider of various services
    ServiceProvider::Ptr const _serviceProvider;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable util::Mutex _mtx;

    /// The registry of the on-going requests.
    std::map<std::string, std::shared_ptr<ControllerRequestWrapper>> _registry;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONTROLLER_H
