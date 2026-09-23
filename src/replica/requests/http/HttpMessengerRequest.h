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
#include <stdexcept>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpMessengerRequest represents requests processed by the messenger.
 */
class HttpMessengerRequest {
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
     *   - The request's identifier.
     *   - A boolean flag indicating success or failure of the request.
     *   - The error message in case of failure.
     *   - The JSON object representing the response payload.
     */
    typedef std::function<void(std::uint64_t, bool, std::string const&, nlohmann::json const&)>
            OnFinishCallback;

    /**
     * Construct the object in the default failed (member 'success') state. Hence, there is no
     * need to set this state explicitly unless a request turns out to be
     * a success.
     * @param id  A unique identifier of the request.
     * @param priority  The priority level of a request.
     * @param resource  The resource path of the HTTP request.
     * @param method  The HTTP method of the request.
     * @param requestJson  The JSON object with the request content.
     */
    HttpMessengerRequest(std::uint64_t id, int priority, std::string const& resource, http::Method method,
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
     * Process the HTTP request and notify a requestor via the onFinish callback.
     * @note The notification is sent within the context of a thread that processes the request.
     * @param baseUrl  The base URL for the HTTP request.
     */
    void process(std::string const& baseUrl);

    /**
     * Request cancellation only affects the active requests by atempting to prevent
     * then from calling the onFinish callback.
     * @note This is the best-effort approach and does not guarantee that the onFinish callback
     *  will not be invoked. An originator of the request should be prepared to handle this scenario.
     */
    void cancel() { _canceled = true; }

private:
    std::uint64_t _id;
    int _priority;
    std::string _resource;
    http::Method _method;
    nlohmann::json _requestJson;
    OnFinishCallback _onFinish;
    unsigned int _timeoutMs;
    std::atomic<bool> _canceled{false};
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGERREQUEST_H
