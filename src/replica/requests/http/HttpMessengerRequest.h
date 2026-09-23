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
#ifndef LSST_QSERV_REPLICA_HTTPMESSENGERREQUEST_H
#define LSST_QSERV_REPLICA_HTTPMESSENGERREQUEST_H

// System headers
#include <atomic>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"

// Forward declarations
namespace lsst::qserv::http {
class AsyncReq;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class HttpMessengerConnector;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpMessengerRequest is essentially a wrapper class around http::AsyncReq.
 * The wrapper is needed for the following reasons:
 * - injecting request identifier, priority and the key type which are needed by the PriorityQueue.
 * - delayed instantiation and start of the request when the baseUrl becomes available and
 *   when the messenger service pulls the request from the queue for processing.
 * - A guarantee that the onFinish callback will be invoked exactly once for each request,
 *   even if the request is canceled before it is sent.
 *
 * The implementation of the class is thread-safe to ensure the process() method can be called
 * just once and concurrently with other operations.
 */
class HttpMessengerRequest : public std::enable_shared_from_this<HttpMessengerRequest> {
public:
    /// The type of the unique identifier for the request, required by the PriorityQueue.
    using key_type = std::string;

    /**
     * Callback function type for handling the completion of an HTTP request.
     * Parameters of the callback are:
     * - A shared pointer to the AsyncReq object representing the HTTP request.
     *   The pointer will be null if the request was not sent.
     * - A string containing an error message if the request failed, or an empty string
     *   if it succeeded.
     */
    typedef std::function<void(std::shared_ptr<http::AsyncReq>, std::string)> OnFinishCallback;

    /**
     * Construct the object in the default failed (member 'success') state. Hence, there is no
     * need to set this state explicitly unless a request turns out to be
     * a success.
     * @param connector  The shared pointer to the HttpMessengerConnector managing this request.
     * @param io_service  The I/O service for communication (the caller must ensure it remains
     *  valid for the lifetime of the request).
     * @param workerName  The name of the worker handling the request.
     * @param id  A unique identifier of the request.
     * @param priority  The priority level of a request.
     * @param resource  The resource path of the HTTP request.
     * @param method  The HTTP method of the request.
     * @param requestJson  The JSON object with the request content.
     * @throw std::invalid_argument  If the onFinish callback is not provided.
     */
    HttpMessengerRequest(std::shared_ptr<HttpMessengerConnector> const& connector,
                         boost::asio::io_service& io_service, std::string const& id, int priority,
                         std::string const& resource, http::Method method, nlohmann::json const& requestJson,
                         OnFinishCallback onFinish, unsigned int timeoutSec);

    HttpMessengerRequest() = delete;
    HttpMessengerRequest(HttpMessengerRequest const&) = delete;
    HttpMessengerRequest& operator=(HttpMessengerRequest const&) = delete;

    ~HttpMessengerRequest() = default;

    // Public attributes of the HTTP request which are required by the PriorityQueue.

    std::string id() const { return _id; }
    int priority() const { return _priority; }

    /**
     * Initiate the asynchronous processing of the HTTP request and notify a requestor via the onFinish
     * callback on a failure or success.
     * @note The method may throw exceptions on failures to create or start the HTTP request.
     *  In both cases the onFinish callback will be invoked to notify the requestor of the failure.
     *  However, the method will return false if the request is already in progress.
     * @param baseUrl  The base URL for the HTTP request.
     * @return  False if the request is already in progress, true otherwise.
     */
    bool process(std::string const& baseUrl);

    /**
     * Cancel the request if it is still in progress.
     * @note The onFinish callback will be invoked with a null pointer for the AsyncReq
     *  object if the request was not in progress.
     */
    void cancel();

    /**
     * Notify the requestor via the onFinish callback with the current error message.
     * The method is triggered by HttpMessengerConnector when it's notified on the completion
     * of the request.
     */
    void notify();

private:
    /// The connector that created this request. The pointer is needed to notify the connector
    /// about the request's completion. After that the pointer may no longer be valid.
    std::shared_ptr<HttpMessengerConnector> _connector;

    boost::asio::io_service& _io_service;
    std::string const _id;
    int const _priority;
    std::string const _resource;
    http::Method const _method;
    std::string const _data;
    OnFinishCallback _onFinish;  ///< The callback gets cleared once invoked.
    unsigned int const _timeoutSec;
    std::size_t const _maxResponseBodySize;

    /// The suffix encodes parameters of the API ("version", "instance_id") which are sent in the URL
    /// of the HTTP request. Values of these parameters are derived from the connector's configuration.
    std::string const _urlSuffix;

    /// Mutex to protect access to the state of the request.
    std::mutex _mtx;

    /// The object will be created when the method process() is called.
    std::shared_ptr<http::AsyncReq> _asyncReq;

    /// The error message if the request failed. It will be sent to the onFinish callback.
    std::string _error;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGERREQUEST_H
