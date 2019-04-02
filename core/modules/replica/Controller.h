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

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class ControllerImpl;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Structure ControllerIdentity encapsulates various attributes which identify
 * each instance of the Controller class. This information is meant to
 * be used in the multi-Controller setups to coordinate operations
 * between multiple instances and to avoid/resolve conflicts.
 */
struct ControllerIdentity {
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

    DeleteRequestPtr deleteReplica(std::string const& workerName,
                                   std::string const& database,
                                   unsigned int chunk,
                                   DeleteRequestCallbackType const& onFinish=nullptr,
                                   int  priority=0,
                                   bool keepTracking=true,
                                   bool allowDuplicate=true,
                                   std::string const& jobId="",
                                   unsigned int requestExpirationIvalSec=0);

    FindRequestPtr findReplica(std::string const& workerName,
                               std::string const& database,
                               unsigned int chunk,
                               FindRequestCallbackType const& onFinish=nullptr,
                               int  priority=0,
                               bool computeCheckSum=false,
                               bool keepTracking=true,
                               std::string const& jobId="",
                               unsigned int requestExpirationIvalSec=0);

    FindAllRequestPtr findAllReplicas(std::string const& workerName,
                                      std::string const& database,
                                      bool saveReplicaInfo=true,
                                      FindAllRequestCallbackType const& onFinish=nullptr,
                                      int  priority=0,
                                      bool keepTracking=true,
                                      std::string const& jobId="",
                                      unsigned int requestExpirationIvalSec=0);

    EchoRequestPtr echo(std::string const& workerName,
                        std::string const& data,
                        uint64_t delay,
                        EchoRequestCallbackType const& onFinish=nullptr,
                        int  priority=0,
                        bool keepTracking=true,
                        std::string const& jobId="",
                        unsigned int requestExpirationIvalSec=0);

    SqlRequestPtr sql(std::string const& workerName,
                      std::string const& query,
                      std::string const& user,
                      std::string const& password,
                      uint64_t maxRows,
                      SqlRequestCallbackType const& onFinish=nullptr,
                      int  priority=0,
                      bool keepTracking=true,
                      std::string const& jobId="",
                      unsigned int requestExpirationIvalSec=0);

    StopReplicationRequestPtr stopReplication(std::string const& workerName,
                                              std::string const& targetRequestId,
                                              StopReplicationRequestCallbackType const& onFinish=nullptr,
                                              bool keepTracking=true,
                                              std::string const& jobId="",
                                              unsigned int requestExpirationIvalSec=0);

    StopDeleteRequestPtr stopReplicaDelete(std::string const& workerName,
                                           std::string const& targetRequestId,
                                           StopDeleteRequestCallbackType const& onFinish=nullptr,
                                           bool keepTracking=true,
                                           std::string const& jobId="",
                                           unsigned int requestExpirationIvalSec=0);

    StopFindRequestPtr stopReplicaFind(std::string const& workerName,
                                       std::string const& targetRequestId,
                                       StopFindRequestCallbackType const& onFinish=nullptr,
                                       bool keepTracking=true,
                                       std::string const& jobId="",
                                       unsigned int requestExpirationIvalSec=0);

    StopFindAllRequestPtr stopReplicaFindAll(std::string const& workerName,
                                             std::string const& targetRequestId,
                                             StopFindAllRequestCallbackType const& onFinish=nullptr,
                                             bool keepTracking=true,
                                             std::string const& jobId="",
                                             unsigned int requestExpirationIvalSec=0);

    StopEchoRequestPtr stopEcho(std::string const& workerName,
                                std::string const& targetRequestId,
                                StopEchoRequestCallbackType const& onFinish=nullptr,
                                bool keepTracking=true,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    StopSqlRequestPtr stopSql(std::string const& workerName,
                              std::string const& targetRequestId,
                              StopSqlRequestCallbackType const& onFinish=nullptr,
                              bool keepTracking=true,
                              std::string const& jobId="",
                              unsigned int requestExpirationIvalSec=0);

    StatusReplicationRequestPtr statusOfReplication(
                                    std::string const& workerName,
                                    std::string const& targetRequestId,
                                    StatusReplicationRequestCallbackType const& onFinish=nullptr,
                                    bool keepTracking=false,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    StatusDeleteRequestPtr statusOfDelete(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StatusDeleteRequestCallbackType const& onFinish=nullptr,
                                bool keepTracking=false,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    StatusFindRequestPtr statusOfFind(
                            std::string const& workerName,
                            std::string const& targetRequestId,
                            StatusFindRequestCallbackType const& onFinish=nullptr,
                            bool keepTracking=false,
                            std::string const& jobId="",
                            unsigned int requestExpirationIvalSec=0);

    StatusFindAllRequestPtr statusOfFindAll(
                                std::string const& workerName,
                                std::string const& targetRequestId,
                                StatusFindAllRequestCallbackType const& onFinish=nullptr,
                                bool keepTracking=false,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    StatusEchoRequestPtr statusOfEcho(
                            std::string const& workerName,
                            std::string const& targetRequestId,
                            StatusEchoRequestCallbackType const& onFinish=nullptr,
                            bool keepTracking=false,
                            std::string const& jobId="",
                            unsigned int requestExpirationIvalSec=0);

    StatusSqlRequestPtr statusOfSql(
                            std::string const& workerName,
                            std::string const& targetRequestId,
                            StatusSqlRequestCallbackType const& onFinish=nullptr,
                            bool keepTracking=false,
                            std::string const& jobId="",
                            unsigned int requestExpirationIvalSec=0);

    ServiceSuspendRequestPtr suspendWorkerService(
                                std::string const& workerName,
                                ServiceSuspendRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    ServiceResumeRequestPtr resumeWorkerService(
                                std::string const& workerName,
                                ServiceResumeRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    ServiceStatusRequestPtr statusOfWorkerService(
                                std::string const& workerName,
                                ServiceStatusRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

    ServiceRequestsRequestPtr requestsOfWorkerService(
                                    std::string const& workerName,
                                    ServiceRequestsRequestCallbackType const& onFinish=nullptr,
                                    std::string const& jobId="",
                                    unsigned int requestExpirationIvalSec=0);

    ServiceDrainRequestPtr drainWorkerService(
                                std::string const& workerName,
                                ServiceDrainRequestCallbackType const& onFinish=nullptr,
                                std::string const& jobId="",
                                unsigned int requestExpirationIvalSec=0);

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

    size_t numActiveRequests() const;

    /**
     * Class RequestWrapper is the base class for implementing requests
     * registry as a polymorphic collection to store active requests. Pure virtual
     * methods of the class will be overridden by request-type-specific implementations
     * (see structure RequestWrappeImpl<REQUEST_TYPE> in the .cc file) capturing
     * type-dependent pointer and a callback function.
     */
    struct RequestWrapper {

        typedef std::shared_ptr<RequestWrapper> Ptr;

        virtual ~RequestWrapper() = default;

        /// on completion of a request
        virtual void notify() = 0;

        /// stored request object
        virtual std::shared_ptr<Request> request() const = 0;
    };

private:

    explicit Controller(ServiceProvider::Ptr const& serviceProvider);

    std::string _context(std::string const& func=std::string()) const;

    void _finish(std::string const& id);

    void _assertIsRunning() const;


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
