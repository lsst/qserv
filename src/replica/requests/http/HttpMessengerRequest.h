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
    /**
     * Generate unique request identifiers in a thread-safe manner.
     */
    class IdGenerator {
    public:
        std::uint64_t next() {
            std::uint64_t current = _counter.load();
            while (true) {
                if (current == std::numeric_limits<std::uint64_t>::max()) {
                    throw std::overflow_error("HTTP messenger request ID space exhausted");
                }
                if (_counter.compare_exchange_weak(current, current + 1)) return current + 1;
            }
        }

    private:
        std::atomic<std::uint64_t> _counter{0};
    };

    /// The type of the unique identifier for the request, required by the PriorityQueue.
    using key_type = std::uint64_t;

    /**
     * Callback function type for handling the completion of an HTTP request.
     * Parameters of the callback are:
     * - The request's identifier.
     * - A shared pointer to the AsyncReq object representing the HTTP request.
     *   The pointer will be null if the request was not sent.
     */
    typedef std::function<void(std::uint64_t, std::shared_ptr<http::AsyncReq>)> OnFinishCallback;

    /**
     * Construct the object in the default failed (member 'success') state. Hence, there is no
     * need to set this state explicitly unless a request turns out to be
     * a success.
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
    HttpMessengerRequest(boost::asio::io_service& io_service,
                         std::uint64_t id, int priority, std::string const& resource, http::Method method,
                         nlohmann::json const& requestJson, OnFinishCallback onFinish,
                         unsigned int timeoutMs);

    HttpMessengerRequest() = delete;
    HttpMessengerRequest(HttpMessengerRequest const&) = delete;
    HttpMessengerRequest& operator=(HttpMessengerRequest const&) = delete;

    ~HttpMessengerRequest() = default;

    // Public attributes of the HTTP request which are required by the PriorityQueue.

    std::uint64_t id() const { return _id; }
    int priority() const { return _priority; }

    /**
     * Process the HTTP request and notify a requestor via the onFinish callback
     * on a failure or success.
     * @param baseUrl  The base URL for the HTTP request.
     * @throw std::logic_error  If the request is already in progress.
     */
    void process(std::string const& baseUrl);

    /**
     * Cancel the request if it is still in progress.
     * @note The onFinish callback will be invoked with a null pointer for the AsyncReq
     *  object if the request was not in progress.
     */
    void cancel();

private:
    // Parameters of the request.
    boost::asio::io_service& _io_service;
    std::uint64_t const _id;
    int const _priority;
    std::string const _resource;
    http::Method const _method;
    std::string const _data;
    OnFinishCallback _onFinish; ///< The callback gets cleared once invoked.
    unsigned int const _timeoutMs;

    /// Mutex to protect access to the state of the request.
    std::mutex _mtx;

    /// The object will be created when the method process() is called.
    std::shared_ptr<http::AsyncReq> _asyncReq;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGERREQUEST_H
