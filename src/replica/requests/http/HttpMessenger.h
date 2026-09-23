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
#ifndef LSST_QSERV_REPLICA_HTTPMESSENGER_H
#define LSST_QSERV_REPLICA_HTTPMESSENGER_H

// System headers
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"
#include "replica/util/Mutex.h"
#include "replica/requests/http/HttpMessengerRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class Config;
class HttpMessengerConnector;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpMessenger provides a communication interface for sending requests
 * to the worker services over the HTTP protocol. Features include:
 * - Queuing requests with priority handling
 * - Parallel processing of multiple requests
 * - Connection pooling
 * - Client-defined timeouts on handling requests
 * - Asynchronous notification on the completion or failure of requests
 * - Request cancellation with best-effort guarantee
 *
 * @note Request cancellation is best-effort and not guaranteed.
 *  If the cancellation has affected a request that was still in the queue then no
 *  'onFinish' callback will be triggered. Requests which are in progress may still
 *  complete and trigger the 'onFinish' callback. In order to properly handle this
 *  scenario, the caller should ignore the callback after initiating the cancellation.
 *
 * @note The implementation tracks workers registered in Config and automatically updates
 *  a list of their HTTP connections. Workers are not required to be registered in advance;
 *  the messenger will handle dynamic updates.
 *
 * @note The size of the thread pool used for handling requests is configurable and affects
 *  the level of parallelism. Each worker connection has its own instance of the thread pool.
 *  The relevant configuration parameter is ("controller","http-messenger-num-thread").
 *  Notification on the completion or failure of requests is handled asynchronously via
 *  the 'onFinish' callback. The callbacks are invoked asynchronously in the context of
 *  the thread pool. It means that requestors should process the callbacks as quickly as possible
 *  to avoid blocking other requests.
 */
class HttpMessenger : public std::enable_shared_from_this<HttpMessenger> {
public:
    /**
     * Callback function type for handling the completion of an HTTP request.
     * @see HttpMessengerRequest::OnFinishCallback
     */
    typedef HttpMessengerRequest::OnFinishCallback OnFinishCallback;

    HttpMessenger() = delete;
    HttpMessenger(HttpMessenger const&) = delete;
    HttpMessenger& operator=(HttpMessenger const&) = delete;

    ~HttpMessenger() noexcept;

    /**
     * Create a new messenger with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param config  The Config service of the Replication Framework.
     * @return  A pointer to the created object.
     */
    static std::shared_ptr<HttpMessenger> create(std::shared_ptr<Config> const& config);

    /**
     * Cancel all queued and in-progress requests and stop operations. When called
     * from a messenger worker callback, shutdown is scheduled asynchronously to
     * avoid waiting for sibling callbacks on the same worker pool.
     */
    void stop();

    /**
     * Initiate sending a request
     *
     * @param workerName  The name of a worker.
     * @param priority  The priority of the request.
     * @param resource  The resource path for the HTTP request.
     * @param method  The HTTP method to be used for the request.
     * @param requestJson  The optional JSON object representing the request payload.
     *  The object will be ignored in the case of a GET request.
     * @param onFinish  An asynchronous callback function called upon a completion
     *  or failure of the operation.
     * @param timeoutMs  The optional timeout for the request in milliseconds. A value of 0 assumes
     *  the default timeout.
     * @throw std::invalid_argument  For invalid parameters such as an empty worker name,
     *  empty resource path, misformed or incomplete request (JSON payload).
     * @throw std::logic_error  If the messenger already has another request registered with
     *  the same identifier, or if the service has been stopped.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     * @return  The unique identifier of the request which can be used to track or cancel the request.
     */
    std::uint64_t send(std::string const& workerName, int priority, std::string const& resource,
                       http::Method method, nlohmann::json const& requestJson, OnFinishCallback onFinish,
                       unsigned int timeoutMs = 0);

    /**
     * Cancel an outstanding request if it's still in the queue.
     *
     * @param workerName  The name of a worker.
     * @param id  A unique identifier of a request.
     * @return  The completion status of the operation.
     * @throw std::invalid_argument  If the worker name is empty or the request identifier is 0.
     * @throw std::logic_error  If the messenger does not have a request registered with the specified
     * identifier, or if the service has been stopped.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     */
    void cancel(std::string const& workerName, std::uint64_t id);

    /**
     * Check if a request with the specified identifier exists in the request queue
     * or is currently active.
     *
     * @param workerName The name of a worker.
     * @param id  The unique identifier of a request returned by the send() method.
     * @throw std::invalid_argument  If the worker name is empty or the request identifier is 0.
     * @throw std::logic_error  If the service has been stopped.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     */
    bool exists(std::string const& workerName, std::uint64_t id);

private:
    HttpMessenger(std::shared_ptr<Config> const& config);

    /**
     * Locate and return a connector for the specified worker.
     * @param workerName  The name of a worker.
     * @return  A pointer to the connector.
     * @throw std::invalid_argument  If the worker name is empty.
     * @throw std::logic_error  If the service has been stopped.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     */
    std::shared_ptr<HttpMessengerConnector> _connector(std::string const& workerName);

    /// @return The context string for the given worker (used for reporting errors and logging).
    static std::string _context(std::string const& workerName);

    // Input parameters

    std::shared_ptr<Config> const _config;

    /// Generates unique request identifiers in a thread-safe manner.
    std::shared_ptr<HttpMessengerRequest::IdGenerator> _requestIdGenerator;

    /// Flag to indicate if the service should stop.
    std::atomic<bool> _stopService{false};

    /// The mutex for implementing the synchronized management of the connections.
    mutable replica::Mutex _mtx;

    /// Connection providers for individual workers
    std::map<std::string, std::shared_ptr<HttpMessengerConnector>> _workerConnector;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGER_H
