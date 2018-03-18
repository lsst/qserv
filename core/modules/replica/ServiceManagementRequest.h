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
#ifndef LSST_QSERV_REPLICA_SERVICE_MANAGEMENT_REQUEST_H
#define LSST_QSERV_REPLICA_SERVICE_MANAGEMENT_REQUEST_H

/// ServiceManagementRequest.h declares:
///
/// Common classes shared by all implementations:
///
///   class ServiceState
///   class ServiceSuspendRequestPolicy
///   class ServiceManagementRequestBase
///   class ServiceManagementRequest
///   class ServiceSuspendRequest
///   class ServiceResumeRequest
///   class ServiceStatusRequest
///
/// Request implementations based on multiplexed connectors provided by
/// base class RequestMessenger:
///
///   class ServiceManagementRequestBaseM
///   class ServiceManagementRequestM
///   class ServiceSuspendRequestM
///   class ServiceResumeRequestM
///   class ServiceStatusRequestM
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/RequestMessenger.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Messenger;

/**
 * This structure encapsulates various parameters representing the state
 * of the remote request processing service. The parameters are available
 * upon the completion of the request.
 */
struct ServiceState {

    // Its state
    enum State {
        SUSPEND_IN_PROGRESS = 0,
        SUSPENDED           = 1,
        RUNNING             = 2
    };
    State state;

    /// Return string representation of the state
    std::string state2string() const {
        switch (state) {
            case SUSPEND_IN_PROGRESS: return "SUSPEND_IN_PROGRESS";
            case SUSPENDED:           return "SUSPENDED";
            case RUNNING:             return "RUNNING";
        }
        return "";
    }

    /// The backend technology
    std::string technology;

    /// When the service started (milliseconds since UNIX Epoch)
    uint64_t startTime;

    // Counters for requests known to the service since its last start

    uint32_t numNewRequests;
    uint32_t numInProgressRequests;
    uint32_t numFinishedRequests;

    std::vector<proto::ReplicationServiceResponseInfo> newRequests;
    std::vector<proto::ReplicationServiceResponseInfo> inProgressRequests;
    std::vector<proto::ReplicationServiceResponseInfo> finishedRequests;

    /// Set parameter values from a protobuf object
    void set (const proto::ReplicationServiceResponse &message);
};

/// Overloaded streaming operator for type ServiceState
std::ostream& operator<< (std::ostream &os, const ServiceState &ss);


// ========================================================================
//   Customizations for specific request types require dedicated policies
// ========================================================================

struct ServiceSuspendRequestPolicy {
    static char const* requestTypeName() {
        return "SERVICE_SUSPEND";
    }
    static proto::ReplicationServiceRequestType requestType () {
        return proto::ReplicationServiceRequestType::SERVICE_SUSPEND;
    }
};
struct ServiceResumeRequestPolicy {
    static char const* requestTypeName() {
        return "SERVICE_RESUME";
    }
    static proto::ReplicationServiceRequestType requestType() {
        return proto::ReplicationServiceRequestType::SERVICE_RESUME;
    }
};
struct ServiceStatusRequestPolicy {
    static char const* requestTypeName() {
        return "SERVICE_STATUS";
    }
    static proto::ReplicationServiceRequestType requestType() {
        return proto::ReplicationServiceRequestType::SERVICE_STATUS;
    }
};
struct ServiceRequestsRequestPolicy {
    static char const* requestTypeName() {
        return "SERVICE_REQUESTS";
    }
    static proto::ReplicationServiceRequestType requestType() {
        return proto::ReplicationServiceRequestType::SERVICE_REQUESTS;
    }
};
struct ServiceDrainRequestPolicy {
    static char const* requestTypeName() {
        return "SERVICE_DRAIN";
    }
    static proto::ReplicationServiceRequestType requestType() {
        return proto::ReplicationServiceRequestType::SERVICE_DRAIN;
    }
};

/**
  * Class ServiceManagementRequestBaseM is the base class for a family of requests
  * managing worker-side replication service. The only variable parameter of this
  * class is a specific type of the managemenyt request.
  *
  * Note that this class can't be instantiate directly. It serves as an implementation
  * of the protocol. All final customizations and type-specific operations are
  * provided via a generic subclass.
  */
class ServiceManagementRequestBaseM
    :   public RequestMessenger {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementRequestBaseM> pointer;

    // Default construction and copy semantics are prohibited

    ServiceManagementRequestBaseM() = delete;
    ServiceManagementRequestBaseM(ServiceManagementRequestBaseM const&) = delete;
    ServiceManagementRequestBaseM& operator=(ServiceManagementRequestBaseM const&) = delete;

    /// Destructor
    ~ServiceManagementRequestBaseM() override = default;

    /**
     * Get the state of the worker-side service
     *
     * This method will throw exception std::logic_error if the request's primary state
     * is not 'FINISHED' and its extended state is neither 'SUCCESS" or 'SERVER_ERROR'.
     */
    ServiceState const& getServiceState() const;

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    ServiceManagementRequestBaseM(ServiceProvider::pointer const& serviceProvider,
                                  boost::asio::io_service& io_service,
                                  char const*              requestTypeName,
                                  std::string const&       worker,
                                  lsst::qserv::proto::ReplicationServiceRequestType requestType,
                                  std::shared_ptr<Messenger> const& messenger);
private:

    /**
      * Implement the method declared in the base class
      *
      * @see Request::startImpl()
      */
    void startImpl() final;

    /**
     * Process the worker response to the requested operation.
     *
     * @param success - the flag indicating if the operation was successfull
     * @param message - a response from the worker service (if success is 'true')
     */
    void analyze(bool success,
                 proto::ReplicationServiceResponse const& message);

private:

    /// Request type
    proto::ReplicationServiceRequestType _requestType;

    /// Detailed status of the worker-side service obtained upon completion of
    /// the management request.
    ServiceState _serviceState;
};


/**
  * Generic class ServiceManagementRequestM extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class ServiceManagementRequestM
    :   public ServiceManagementRequestBaseM {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementRequestM<POLICY>> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are prohibited

    ServiceManagementRequestM() = delete;
    ServiceManagementRequestM(ServiceManagementRequestM const&) = delete;
    ServiceManagementRequestM& operator=(ServiceManagementRequestM const&) = delete;

    /// Destructor
    ~ServiceManagementRequestM() final = default;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one to be affectd by the request)
     * @param io_service       - network communication service
     * @param onFinish         - an optional callback function to be called upon a completion of
     *                           the request.
     * @param messenger       - an interface for communicating with workers
     */
    static pointer create(ServiceProvider::pointer const& serviceProvider,
                          boost::asio::io_service& io_service,
                          std::string const& worker,
                          callback_type onFinish,
                          std::shared_ptr<Messenger> const& messenger) {

        return ServiceManagementRequestM<POLICY>::pointer(
            new ServiceManagementRequestM<POLICY>(
                serviceProvider,
                io_service,
                POLICY::requestTypeName(),
                worker,
                POLICY::requestType(),
                onFinish,
                messenger));
    }

private:

    /**
     * Construct the request
     */
    ServiceManagementRequestM(ServiceProvider::pointer const& serviceProvider,
                              boost::asio::io_service& io_service,
                              char const* requestTypeName,
                              std::string const& worker,
                              lsst::qserv::proto::ReplicationServiceRequestType requestType,
                              callback_type onFinish,
                              std::shared_ptr<Messenger> const& messenger)
        :   ServiceManagementRequestBaseM(serviceProvider,
                                          io_service,
                                          requestTypeName,
                                          worker,
                                          requestType,
                                          messenger),
            _onFinish(onFinish) {
    }

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * bu the base class.
     */
    void notify() final {
        if (_onFinish != nullptr) {
            ServiceManagementRequestM<POLICY>::pointer self =
                shared_from_base<ServiceManagementRequestM<POLICY>>();
            _onFinish(self);
        }
    }

private:

    /// Registered callback to be called when the operation finishes
    callback_type _onFinish;
};

typedef ServiceManagementRequestBaseM ServiceManagementRequestBase;

typedef ServiceManagementRequestM<ServiceSuspendRequestPolicy>  ServiceSuspendRequest;
typedef ServiceManagementRequestM<ServiceResumeRequestPolicy>   ServiceResumeRequest;
typedef ServiceManagementRequestM<ServiceStatusRequestPolicy>   ServiceStatusRequest;
typedef ServiceManagementRequestM<ServiceRequestsRequestPolicy> ServiceRequestsRequest;
typedef ServiceManagementRequestM<ServiceDrainRequestPolicy>    ServiceDrainRequest;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SERVICE_MANAGEMENT_REQUEST_H
