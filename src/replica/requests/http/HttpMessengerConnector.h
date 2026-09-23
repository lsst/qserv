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
#ifndef LSST_QSERV_REPLICA_HTTPMESSENGERCONNECTOR_H
#define LSST_QSERV_REPLICA_HTTPMESSENGERCONNECTOR_H

// System headers
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"
#include "replica/config/ConfigWorker.h"
#include "replica/util/PriorityQueue.h"

// Forward declarations
namespace lsst::qserv::http {
class ClientConnPool;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class Config;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpMessengerRequest represents requests processed by the messenger.
 */
class HttpMessengerRequest {
public:
    /**
     * Callback function type for handling the completion of an HTTP request.
     * Parameters of the callback are:
     *   - The request 'id'.
     *   - A boolean indicating success or failure of the request.
     *   - The error message in case of failure.
     *   - The JSON object representing the response payload.
     */
    typedef std::function<void(std::string const&, bool, std::string const&, nlohmann::json const&)>
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
    HttpMessengerRequest(std::string const& id, int priority, std::string const& resource,
                         http::Method method, nlohmann::json const& requestJson, OnFinishCallback onFinish,
                         unsigned int timeoutMs);

    HttpMessengerRequest() = delete;
    HttpMessengerRequest(HttpMessengerRequest const&) = delete;
    HttpMessengerRequest& operator=(HttpMessengerRequest const&) = delete;

    ~HttpMessengerRequest() = default;

    // Public attributes of the HTTP request which are required by the PriorityQueue.

    std::string const& id() const { return _id; }
    int priority() const { return _priority; }

    /**
     * Process the HTTP request and notify a requestor via the onFinish callback.
     * @param baseUrl  The base URL for the HTTP request.
     * @param connPool  The connection pool for managing HTTP client connections.
     */
    void process(std::string const& baseUrl, std::shared_ptr<http::ClientConnPool> const& connPool);

    /**
     * Request cancellation only affects the active requests by atempting to prevent
     * then from calling the onFinish callback.
     * @note This is the best-effort approach and does not guarantee that the onFinish callback
     *   will not be invoked. An originator of the request should be prepared to handle this scenario.
     */
    void cancel() { _canceled = true; }

private:
    std::string _id;
    int _priority;
    std::string _resource;
    http::Method _method;
    nlohmann::json _requestJson;
    OnFinishCallback _onFinish;
    unsigned int _timeoutMs;
    std::atomic<bool> _canceled{false};
};

/**
 * Class HttpMessengerConnector provides a communication interface for sending/receiving
 * messages to and from worker services over the HTTP protocol.
 */
class HttpMessengerConnector : public std::enable_shared_from_this<HttpMessengerConnector> {
public:
    /**
     * Callback function type for handling the completion of an HTTP request.
     * @see HttpMessengerRequest::OnFinishCallback
     */
    typedef HttpMessengerRequest::OnFinishCallback OnFinishCallback;

    HttpMessengerConnector() = delete;
    HttpMessengerConnector(HttpMessengerConnector const&) = delete;
    HttpMessengerConnector& operator=(HttpMessengerConnector const&) = delete;

    ~HttpMessengerConnector() = default;

    /**
     * Create a new connector with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param config  The configuration service of the Replication Framework.
     * @param workerName  The name of a worker.
     * @return  A pointer to the created object.
     */
    static std::shared_ptr<HttpMessengerConnector> create(std::shared_ptr<Config> const& config,
                                                          std::string const& workerName);

    /**
     * Cancel all requests and stop operations.
     */
    void stop();

    /**
     * Initiate sending a message.
     *
     * The response message will be initialized only in case of successful completion
     * of the request. The method may throw exception std::logic_error if
     * the MessangerConnector already has another request registered with the same
     * request 'id'.
     *
     * @param resource  The resource path for the HTTP request.
     * @param id  A unique identifier of a request.
     * @param priority  The priority of the request.
     * @param method  The HTTP method to be used for the request.
     * @param requestJson  The JSON object representing the request payload.
     * @param timeoutMs  The timeout for the request in milliseconds. A value of 0 assumes
     *  the default timeout.
     * @param method  The HTTP method to be used for the request.
     * @param requestJson  The JSON object representing the request payload.
     * @param timeoutMs  The timeout for the request in milliseconds. A value of 0 assumes
     *  the default timeout.
     * @note The request identifier and its priority are expected to be included in
     *  the 'requestJson' payload.
     * @throw std::invalid_argument  For invalid parameters such as an empty resource path,
     *  misformed or incomplete request (JSON payload).
     * @throw std::logic_error  If the messenger already has another request registered with
     *  the same request 'id'.
     */
    void send(std::string const& id, int priority, std::string const& resource, http::Method method,
              nlohmann::json const& requestJson, OnFinishCallback onFinish, unsigned int timeoutMs = 0);

    /**
     * Cancel an outstanding request.
     *
     * @param id  A unique identifier of a request.
     * @throw std::invalid_argument  If the request identifier is empty.
     */
    void cancel(std::string const& id);

    /**
     * Check if a requst is known to the Messenger.
     * @param id  A unique identifier of a request.
     * @return  'true' if the specified request is known to the Messenger.
     * @throw std::invalid_argument  If the request identifier is empty.
     */
    bool exists(std::string const& id) const;

private:
    HttpMessengerConnector(std::shared_ptr<Config> const& config, std::string const& workerName);

    /// Initialize the state and start request processing threads.
    void _init();

    /**
     * Start processing requests from the request queue.
     *
     * This method is intended to be run by each thread.
     * It continuously processes requests from the queue until the service is stopped.
     */
    void _processRequests();

    /// @return The context string for the current worker (used for reporting errors and logging).
    std::string _context() const;

    // Input parameters

    ConfigWorker const _worker;

    // State variables of the service

    /// The base URL for the worker.
    std::string const _baseUrl;

    /// The number of threads for processing requests.
    /// The same number is used to initialize the connection pool.
    std::size_t const _numThreads;

    /// The connection pool for managing HTTP client connections.
    std::shared_ptr<http::ClientConnPool> const _connPool;

    /// The threads responsible for processing requests.
    std::vector<std::unique_ptr<std::thread>> _threads;

    /// The priority queue for managing outstanding requests.
    replica::PriorityQueue<HttpMessengerRequest> _requestQueue;

    /// The map of active requests keyed by their unique identifiers.
    std::map<std::string, std::shared_ptr<HttpMessengerRequest>> _activeRequests;

    /// Flag to indicate if the service should stop.
    std::atomic<bool> _stopService{false};

    /// This mutex is for synchronizing access to the request queue and other shared resources.
    mutable std::mutex _mtx;

    /// The condition variable used to signal changes in the request queue.
    std::condition_variable _cv;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGERCONNECTOR_H
