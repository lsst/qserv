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
#ifndef LSST_QSERV_REPLICA_HTTPREQUEST_H
#define LSST_QSERV_REPLICA_HTTPREQUEST_H

// System headers
#include <memory>
#include <string>
#include <tuple>

// Third party headers
#include "boost/system/error_code.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"
#include "replica/requests/RequestI.h"
#include "replica/util/Mutex.h"

// Forward declarations
namespace lsst::qserv::http {
class AsyncReq;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpRequest is a base class for a family of requests within
 * the master Replication Controller service.
 *
 * The class is not intended to be used directly. Instead, it should be subclassed
 * by a specific request type. Objects of the subclassed request type are created
 * by a factory method "createAndStart" that is provided by each such class.
 */
class HttpRequest : public RequestI {
public:
    HttpRequest() = delete;
    HttpRequest(HttpRequest const&) = delete;
    HttpRequest& operator=(HttpRequest const&) = delete;

    virtual ~HttpRequest() = default;

protected:
    /**
     * Construct the request with the pointer to the services provider.
     *
     * @note options 'keepTracking' and 'disposeRequired'
     *  have effect for specific request only.
     *
     * @param controller The Controller associated with the request.
     * @param type The type name of a request (used informally for debugging, and it's
     *  also stored in the persistent state of the Replication system).
     * @param workerName The name of a worker.
     * @param priority A value of the parameter may affect an execution order of
     *  the request by the worker service. It may also affect an order requests
     *  are processed locally. Higher number means higher priority.
     * @param keepTracking Keep tracking the request before it finishes or fails
     * @param disposeRequired The flag indicating of the worker-side request
     *  disposal is needed for a particular request. Normally, it's required for
     *  requests which are queued by workers in its processing queues.
     */
    HttpRequest(std::shared_ptr<Controller> const& controller, std::string const& type,
                std::string const& workerName, int priority, bool keepTracking, bool disposeRequired);

    virtual void awaken(boost::system::error_code const& ec) final;

    /**
     * Send an asynchronous request to a worker
     * @note Upon a completion of the request, the _analyze() method will be called to process the response.
     * @param lock a lock on HttpRequest::mtx must be acquired before calling this method
     * @param target the target URL path on the worker
     * @param method the HTTP method to use for the request (http::Method)
     * @param requestJson the JSON payload to send with the request (nlohmann::json)
     */
    void send(replica::Lock const& lock, std::string const& target, http::Method method,
              nlohmann::json const& requestJson = nlohmann::json::object());

    /**
     * Process the completion of the requested operation. The implementation should analyze the response
     * and take appropriate actions based on the result.
     * @param req The asynchronous request object representing the response from a worker
     *  (if the request was initiated).
     * @param error The optional error response from the messenger (if any).
     */
    virtual void analyze(std::shared_ptr<http::AsyncReq> req, std::string error) = 0;

    /**
     * Retrieve the response data from the HTTP request.
     * @param lock A lock on HttpRequest::mtx must be acquired before calling this method.
     * @param func The name of the calling function (for logging purposes).
     * @param req The HTTP request object.
     * @param error The error message, if any.
     * @return A tuple where the first element is 'true' if the request has suceeded, 'false'
     *  otherwise, and the second element is the parsed JSON response data.
     */
    std::tuple<bool, nlohmann::json> getResponseData(replica::Lock const& lock, std::string const& func,
                                                     std::shared_ptr<http::AsyncReq> const& req,
                                                     std::string const& error);
    /**
     * Initiate the request disposal at the worker server. This method is automatically
     * called upon succesfull completion of requests for which the flag 'disposeRequired'
     * was set during request object construction. However, the streaming requests
     * that are designed to make more than one trip to the worker under the same request
     * identifier may also explicitly call this method upon completing intermediate
     * requests. That is normally done to expedite the garbage collection of the worker
     * requests and prevent excessive memory build up (or keeping other resources)
     * at the worker.
     * @param lock The lock on HttpRequest::mtx must be acquired before calling this method.
     * @param priority The desired priority level of the operation.
     * @param onFinish The optional callback to be called upon the completion of
     *  the request disposal operation.
     */
    void dispose(replica::Lock const& lock, int priority, OnDisposeCallbackType const& onFinish = nullptr);
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPREQUEST_H
