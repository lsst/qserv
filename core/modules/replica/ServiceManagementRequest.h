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
#ifndef LSST_QSERV_REPLICA_SERVICEMANAGEMENTREQUEST_H
#define LSST_QSERV_REPLICA_SERVICEMANAGEMENTREQUEST_H

/// ServiceManagementRequest.h declares:
////
///   class ServiceSuspendRequestPolicy
///   class ServiceManagementRequest
///   class ServiceSuspendRequest
///   class ServiceResumeRequest
///   class ServiceStatusRequest
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <future>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/ServiceManagementRequestBase.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Messenger;

// ========================================================================
//   Customizations for specific request types require dedicated policies
// ========================================================================

struct ServiceSuspendRequestPolicy {
    static char const*                          requestName();
    static proto::ReplicationServiceRequestType requestType();
};

struct ServiceResumeRequestPolicy {
    static char const*                          requestName();
    static proto::ReplicationServiceRequestType requestType();
};

struct ServiceStatusRequestPolicy {
    static char const*                          requestName();
    static proto::ReplicationServiceRequestType requestType();
};

struct ServiceRequestsRequestPolicy {
    static char const*                          requestName();
    static proto::ReplicationServiceRequestType requestType();
};

struct ServiceDrainRequestPolicy {
    static char const*                          requestName();
    static proto::ReplicationServiceRequestType requestType();
};

/**
  * Generic class ServiceManagementRequest extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class ServiceManagementRequest
    :   public ServiceManagementRequestBase {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementRequest<POLICY>> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    ServiceManagementRequest() = delete;
    ServiceManagementRequest(ServiceManagementRequest const&) = delete;
    ServiceManagementRequest& operator=(ServiceManagementRequest const&) = delete;

    ~ServiceManagementRequest() final = default;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - provides various services for the application
     * @param worker           - identifier of a worker node (the one to be affectd by the request)
     * @param io_service       - network communication service (BOOST ASIO)
     * @param onFinish         - callback function to be called upon a completion of the request
     * @param messenger        - messenging service for workers
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      CallbackType onFinish,
                      std::shared_ptr<Messenger> const& messenger) {

        return ServiceManagementRequest<POLICY>::Ptr(
            new ServiceManagementRequest<POLICY>(
                serviceProvider,
                io_service,
                POLICY::requestName(),
                worker,
                POLICY::requestType(),
                onFinish,
                messenger));
    }

private:

    /**
     * Construct the request
     *
     * @see ServiceManagementRequest::create()
     */
    ServiceManagementRequest(ServiceProvider::Ptr const& serviceProvider,
                             boost::asio::io_service& io_service,
                             char const* requestName,
                             std::string const& worker,
                             proto::ReplicationServiceRequestType requestType,
                             CallbackType onFinish,
                             std::shared_ptr<Messenger> const& messenger)
        :   ServiceManagementRequestBase(serviceProvider,
                                         io_service,
                                         requestName,
                                         worker,
                                         requestType,
                                         messenger),
            _onFinish(onFinish) {
    }

    /**
     * @see Request::notifyImpl()
     */
    void notifyImpl() final {
        if (_onFinish) {
            _onFinish(shared_from_base<ServiceManagementRequest<POLICY>>());
        }
    }

private:

    /// Registered callback to be called when the operation finishes
    CallbackType _onFinish;
};

typedef ServiceManagementRequest<ServiceSuspendRequestPolicy>  ServiceSuspendRequest;
typedef ServiceManagementRequest<ServiceResumeRequestPolicy>   ServiceResumeRequest;
typedef ServiceManagementRequest<ServiceStatusRequestPolicy>   ServiceStatusRequest;
typedef ServiceManagementRequest<ServiceRequestsRequestPolicy> ServiceRequestsRequest;
typedef ServiceManagementRequest<ServiceDrainRequestPolicy>    ServiceDrainRequest;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SERVICEMANAGEMENTREQUEST_H
