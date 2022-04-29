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
#ifndef LSST_QSERV_REPLICA_SERVICEMANAGEMENTREQUEST_H
#define LSST_QSERV_REPLICA_SERVICEMANAGEMENTREQUEST_H

/**
 * This header declares a collection of the worker servers management request
 * classes as part of the Controller-side Replication Framework.
 *
 * @see class ServiceSuspendRequestPolicy
 * @see class ServiceManagementRequest
 * @see class ServiceSuspendRequest
 * @see class ServiceResumeRequest
 * @see class ServiceStatusRequest
 */

// System headers
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/protocol.pb.h"
#include "replica/ServiceManagementRequestBase.h"

// Forward declarations
namespace lsst { namespace qserv { namespace replica {
class Messenger;
}}}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst { namespace qserv { namespace replica {

// ========================================================================
//   Customizations for specific request types require dedicated policies
// ========================================================================

class ServiceSuspendRequestPolicy {
public:
    static char const* requestName();
    static ProtocolServiceRequestType requestType();
};

class ServiceResumeRequestPolicy {
public:
    static char const* requestName();
    static ProtocolServiceRequestType requestType();
};

class ServiceStatusRequestPolicy {
public:
    static char const* requestName();
    static ProtocolServiceRequestType requestType();
};

class ServiceRequestsRequestPolicy {
public:
    static char const* requestName();
    static ProtocolServiceRequestType requestType();
};

class ServiceDrainRequestPolicy {
public:
    static char const* requestName();
    static ProtocolServiceRequestType requestType();
};

class ServiceReconfigRequestPolicy {
public:
    static char const* requestName();
    static ProtocolServiceRequestType requestType();
};

/**
 * Generic class ServiceManagementRequest extends its base class
 * to allow further policy-based customization of specific requests.
 */
template <typename POLICY>
class ServiceManagementRequest : public ServiceManagementRequestBase {
public:
    /// Inject the into a namespace of the class
    typedef POLICY Policy;

    typedef std::shared_ptr<ServiceManagementRequest<POLICY>> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

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
     * @param serviceProvider provides various services for the application
     * @param io_service network communication service (BOOST ASIO)
     * @param worker identifier of a worker node (the one to be affected by the request)
     * @param priority a priority level of the request
     * @param onFinish callback function to be called upon a completion of the request
     * @param messenger messenger service for workers
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& worker, CallbackType const& onFinish, int priority,
                      std::shared_ptr<Messenger> const& messenger) {
        return ServiceManagementRequest<POLICY>::Ptr(new ServiceManagementRequest<POLICY>(
                serviceProvider, io_service, POLICY::requestName(), worker, POLICY::requestType(), priority,
                onFinish, messenger));
    }

protected:
    void notify(util::Lock const& lock) final {
        notifyDefaultImpl<ServiceManagementRequest<POLICY>>(lock, _onFinish);
    }

private:
    ServiceManagementRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                             char const* requestName, std::string const& worker,
                             ProtocolServiceRequestType requestType, int priority,
                             CallbackType const& onFinish, std::shared_ptr<Messenger> const& messenger)
            : ServiceManagementRequestBase(serviceProvider, io_service, requestName, worker, requestType,
                                           priority, messenger),
              _onFinish(onFinish) {}

    // Input parameters

    CallbackType _onFinish;
};

typedef ServiceManagementRequest<ServiceSuspendRequestPolicy> ServiceSuspendRequest;
typedef ServiceManagementRequest<ServiceResumeRequestPolicy> ServiceResumeRequest;
typedef ServiceManagementRequest<ServiceStatusRequestPolicy> ServiceStatusRequest;
typedef ServiceManagementRequest<ServiceRequestsRequestPolicy> ServiceRequestsRequest;
typedef ServiceManagementRequest<ServiceDrainRequestPolicy> ServiceDrainRequest;
typedef ServiceManagementRequest<ServiceReconfigRequestPolicy> ServiceReconfigRequest;

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SERVICEMANAGEMENTREQUEST_H
