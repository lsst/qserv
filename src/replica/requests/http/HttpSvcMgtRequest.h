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
#ifndef LSST_QSERV_REPLICA_HTTPSVCMGTREQUEST_H
#define LSST_QSERV_REPLICA_HTTPSVCMGTREQUEST_H

/**
 * This header declares a collection of the worker servers management request
 * classes as part of the Controller-side Replication Framework.
 *
 * @see class HttpSvcMgtSuspendRequestPolicy
 * @see class HttpSvcMgtResumeRequestPolicy
 * @see class HttpSvcMgtStatusRequestPolicy
 * @see class HttpSvcMgtRequestsRequestPolicy
 * @see class HttpSvcMgtDrainRequestPolicy
 * @see class HttpSvcMgtReconfigRequestPolicy
 */

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/proto/Protocol.h"
#include "replica/requests/http/HttpSvcMgtRequestBase.h"
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

class HttpSvcMgtSuspendRequestPolicy {
public:
    static char const* requestName();
    static protocol::ServiceRequestType requestType();
};

class HttpSvcMgtResumeRequestPolicy {
public:
    static char const* requestName();
    static protocol::ServiceRequestType requestType();
};

class HttpSvcMgtStatusRequestPolicy {
public:
    static char const* requestName();
    static protocol::ServiceRequestType requestType();
};

class HttpSvcMgtRequestsRequestPolicy {
public:
    static char const* requestName();
    static protocol::ServiceRequestType requestType();
};

class HttpSvcMgtDrainRequestPolicy {
public:
    static char const* requestName();
    static protocol::ServiceRequestType requestType();
};

class HttpSvcMgtReconfigRequestPolicy {
public:
    static char const* requestName();
    static protocol::ServiceRequestType requestType();
};

/**
 * Generic class HttpSvcMgtRequest extends its base class SvcMgtRequestBase
 * to allow further policy-based customization of specific requests.
 */
template <typename POLICY>
class HttpSvcMgtRequest : public HttpSvcMgtRequestBase {
public:
    /// Inject the into a namespace of the class
    typedef POLICY Policy;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<HttpSvcMgtRequest<POLICY>>)> CallbackType;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * Class-specific parameters are documented below:
     * @param priority The priority level of the request.
     * @see The very base class Request for the description of the common parameters of all subclasses.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<HttpSvcMgtRequest<POLICY>> createAndStart(
            std::shared_ptr<Controller> const& controller, std::string const& workerName,
            CallbackType const& onFinish = nullptr, int priority = PRIORITY_VERY_HIGH,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0) {
        auto ptr = std::shared_ptr<HttpSvcMgtRequest<POLICY>>(new HttpSvcMgtRequest<POLICY>(
                controller, POLICY::requestName(), workerName, POLICY::requestType(), priority, onFinish));
        ptr->start(jobId, requestExpirationIvalSec);
        return ptr;
    }

    HttpSvcMgtRequest() = delete;
    HttpSvcMgtRequest(HttpSvcMgtRequest const&) = delete;
    HttpSvcMgtRequest& operator=(HttpSvcMgtRequest const&) = delete;

    virtual ~HttpSvcMgtRequest() final = default;

protected:
    void notify(replica::Lock const& lock) final {
        notifyDefaultImpl<HttpSvcMgtRequest<POLICY>>(lock, _onFinish);
    }

private:
    HttpSvcMgtRequest(std::shared_ptr<Controller> const& controller, char const* requestName,
                      std::string const& workerName, protocol::ServiceRequestType requestType, int priority,
                      CallbackType const& onFinish)
            : HttpSvcMgtRequestBase(controller, requestName, workerName, requestType, priority),
              _onFinish(onFinish) {}

    // Input parameters

    CallbackType _onFinish;
};

typedef HttpSvcMgtRequest<HttpSvcMgtSuspendRequestPolicy> HttpSvcSuspendRequest;
typedef HttpSvcMgtRequest<HttpSvcMgtResumeRequestPolicy> HttpSvcResumeRequest;
typedef HttpSvcMgtRequest<HttpSvcMgtStatusRequestPolicy> HttpSvcStatusRequest;
typedef HttpSvcMgtRequest<HttpSvcMgtRequestsRequestPolicy> HttpSvcRequestsRequest;
typedef HttpSvcMgtRequest<HttpSvcMgtDrainRequestPolicy> HttpSvcDrainRequest;
typedef HttpSvcMgtRequest<HttpSvcMgtReconfigRequestPolicy> HttpSvcReconfigRequest;

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPSVCMGTREQUEST_H
