/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_SERVICE_MANAGEMENT_REQUEST_BASE_H
#define LSST_QSERV_REPLICA_SERVICE_MANAGEMENT_REQUEST_BASE_H

/// ServiceManagementRequestBase.h declares:
///
/// Common classes shared by all implementations:
///
///   class ServiceState
///   class ServiceManagementRequestBase
///
/// (see individual class documentation for more information)

// System headers
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

/**
  * Class ServiceManagementRequestBase is the base class for a family of requests
  * managing worker-side replication service. The only variable parameter of this
  * class is a specific type of the managemenyt request.
  *
  * Note that this class can't be instantiate directly. It serves as an implementation
  * of the protocol. All final customizations and type-specific operations are
  * provided via a generic subclass.
  */
class ServiceManagementRequestBase
    :   public RequestMessenger {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementRequestBase> Ptr;

    // Default construction and copy semantics are prohibited

    ServiceManagementRequestBase() = delete;
    ServiceManagementRequestBase(ServiceManagementRequestBase const&) = delete;
    ServiceManagementRequestBase& operator=(ServiceManagementRequestBase const&) = delete;

    /// Destructor
    ~ServiceManagementRequestBase() override = default;

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
    ServiceManagementRequestBase(ServiceProvider::Ptr const& serviceProvider,
                                 boost::asio::io_service& io_service,
                                 char const* requestTypeName,
                                 std::string const& worker,
                                 proto::ReplicationServiceRequestType requestType,
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

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SERVICE_MANAGEMENT_REQUEST_BASE_H
