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
#include <functional>
#include <map>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"
#include "replica/util/Mutex.h"
#include "replica/requests/http/HttpMessengerConnector.h"

// Forward declarations
namespace lsst::qserv::replica {
class Config;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpMessenger provides a communication interface for sending/receiving messages
 * to and from worker services over the HTTP protocol. Features include:
 * - Queuing requests with priority handling
 * - Parallel processing of many requests
 * - Connection pooling
 * - Client-defined timeouts on handling requests
 * - Asynchronous notification on the completion or failure of requests
 * - Request cancellation with best-effort guarantee
 *
 * @note Request cancellation is best-effort and not guaranteed.
 *  If the cancellation has affected a request that was still in the queue then no
 *  'onFinish' callback will be triggered. Requests which are in progress may still
 *  complete and trigger the 'onFinish' callback. In order to properly handle this
 *  scenario, the caller should ignore the callback.
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

    ~HttpMessenger() = default;

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
     * Cancel all requests and stop operations.
     */
    void stop();

    /**
     * Initiate sending a message
     *
     * The response message will be initialized only in case of successful completion
     * of the request.
     *
     * @param workerName  The name of a worker.
     * @param id  A unique identifier of a request.
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
     *  empty identifier or resource path, misformed or incomplete request (JSON payload).
     * @throw std::logic_error  If the messenger already has another request registered with
     *  the same identifier.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     */
    void send(std::string const& workerName, std::string const& id, int priority, std::string const& resource,
              http::Method method, nlohmann::json const& requestJson, OnFinishCallback onFinish,
              unsigned int timeoutMs = 0);

    /**
     * Cancel an outstanding request if it's still in the queue.
     *
     * @param workerName  The name of a worker.
     * @param id  A unique identifier of a request.
     * @return  The completion status of the operation.
     * @throw std::invalid_argument  If the worker name or the request identifier is empty.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     */
    void cancel(std::string const& workerName, std::string const& id);

    /**
     * Return 'true' if the specified request is known to the Messenger.
     *
     * @param workerName The name of a worker.
     * @param id  A unique identifier of a request.
     * @throw std::invalid_argument  If the worker name or the request identifier is empty.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     */
    bool exists(std::string const& workerName, std::string const& id);

private:
    HttpMessenger(std::shared_ptr<Config> const& config);

    /**
     * Locate and return a connector for the specified worker.
     * @param workerName  The name of a worker.
     * @return  A pointer to the connector.
     * @throw std::invalid_argument  If the worker name is empty.
     * @throw ConfigUnknownWorker  If the specified worker name is not known to the configuration.
     */
    std::shared_ptr<HttpMessengerConnector> const& _connector(std::string const& workerName);

    /// @return The context string for the given worker (used for reporting errors and logging).
    static std::string _context(std::string const& workerName);

    // Input parameters

    std::shared_ptr<Config> const _config;

    /// The mutex for implementing the synchronized management of the connections.
    mutable replica::Mutex _mtx;

    /// Connection providers for individual workers
    std::map<std::string, std::shared_ptr<HttpMessengerConnector>> _workerConnector;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGER_H
