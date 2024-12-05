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
#include "replica/proto/protocol.pb.h"
#include "replica/requests/ServiceManagementRequestBase.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

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
     * Class-specific parameters are documented below:
     * @param priority The priority level of the request.
     *
     * @see The very base class Request for the description of the common parameters
     *   of all subclasses.
     *
     * @return A pointer to the created object.
     */
    static Ptr createAndStart(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                              CallbackType const& onFinish = nullptr, int priority = PRIORITY_VERY_HIGH,
                              std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0) {
        auto ptr = ServiceManagementRequest<POLICY>::Ptr(new ServiceManagementRequest<POLICY>(
                controller, POLICY::requestName(), workerName, POLICY::requestType(), priority, onFinish));
        ptr->start(jobId, requestExpirationIvalSec);
        return ptr;
    }

protected:
    void notify(replica::Lock const& lock) final {
        notifyDefaultImpl<ServiceManagementRequest<POLICY>>(lock, _onFinish);
    }

private:
    ServiceManagementRequest(std::shared_ptr<Controller> const& controller, char const* requestName,
                             std::string const& workerName, ProtocolServiceRequestType requestType,
                             int priority, CallbackType const& onFinish)
            : ServiceManagementRequestBase(controller, requestName, workerName, requestType, priority),
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

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SERVICEMANAGEMENTREQUEST_H
