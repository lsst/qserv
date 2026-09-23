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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"
#include "replica/config/ConfigWorker.h"
#include "replica/requests/http/HttpMessengerRequest.h"
#include "replica/util/PriorityQueue.h"

// Forward declarations
namespace lsst::qserv::http {
class AsyncReq;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class Config;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpMessengerConnector provides a communication interface for sending/receiving
 * messages to and from worker services over the HTTP protocol.
 */
class HttpMessengerConnector : public std::enable_shared_from_this<HttpMessengerConnector> {
public:
    /**
     * Callback function type for handling the completion of an HTTP request.
     * Parameters of the callback are:
     * - A shared pointer to the AsyncReq object representing the HTTP request.
     *   The pointer will be null if the request was not sent.
     * - A string containing an error message if the request failed, or an empty string
     *   if it succeeded.
     */
    typedef std::function<void(std::shared_ptr<http::AsyncReq>, std::string)> OnFinishCallback;

    HttpMessengerConnector() = delete;
    HttpMessengerConnector(HttpMessengerConnector const&) = delete;
    HttpMessengerConnector& operator=(HttpMessengerConnector const&) = delete;

    ~HttpMessengerConnector() noexcept;

    /**
     * Create a new connector with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param config  The configuration service of the Replication Framework.
     * @param io_service  The I/O service for communication.
     * @param workerName  The name of a worker.
     * @return  A pointer to the created object.
     */
    static std::shared_ptr<HttpMessengerConnector> create(std::shared_ptr<Config> const& config,
                                                          boost::asio::io_service& io_service,
                                                          std::string const& workerName);

    std::shared_ptr<Config> const& config() const { return _config; }

    /**
     * Cancel all queued or in-progress requests.
     */
    void cancelAll();

    /**
     * Initiate sending a message.
     *
     * The response message will be initialized only in case of successful completion
     * of the request. The method may throw exception std::logic_error if
     * the MessengerConnector already has another request registered with the same
     * request 'id'.
     *
     * @param id  The unique identifier of the request.
     * @param priority  The priority of the request.
     * @param resource  The resource path for the HTTP request.
     * @param method  The HTTP method to be used for the request.
     * @param requestJson  The JSON object representing the request payload.
     * @param onFinish  The callback to be invoked upon request completion.
     * @param timeoutSec  The timeout for the request in milliseconds. A value of 0 assumes
     *  the default timeout.
     * @note The request identifier and its priority are expected to be included in
     *  the 'requestJson' payload.
     * @throw std::invalid_argument  For invalid parameters such as an empty resource path,
     *  misformed or incomplete request (JSON payload).
     */
    void send(std::string const& id, int priority, std::string const& resource, http::Method method,
              nlohmann::json const& requestJson, OnFinishCallback onFinish, unsigned int timeoutSec = 0);

    /**
     * Cancel an outstanding request.
     *
     * @param id  A unique identifier of a request.
     * @throw std::invalid_argument  If the request identifier is empty.
     */
    void cancel(std::string const& id);

    /**
     * Check if a request with the specified identifier exists in the request queue
     * or is currently active.
     * @param id  A unique identifier of a request.
     * @return  'true' if the specified request exists in the request queue or is currently active.
     * @throw std::invalid_argument  If the request identifier is empty.
     */
    bool exists(std::string const& id) const;

private:
    HttpMessengerConnector(std::shared_ptr<Config> const& config, boost::asio::io_service& io_service,
                           std::string const& workerName);

    /**
     * Fetches the next request (if any) from the request queue, moves the request into
     * the active requests map, and starts its processing.
     * @note This method is intended to be run on the submittion and completion of HTTP requests.
     *  The method won't do anything if the number of active requests has reached the maximum allowed
     *  by the configuration parameter ("controller", "http-messenger-num-requests").
     *  A value of the parameter is checked reach time the method is invoked to allow
     *  with any changes to the configuration parameter being immediately respected.
     * @param lock  A lock guard to ensure thread-safe access to shared resources.
     * @throw ConfigUnknownWorker  If the worker specified by '_workerName' is not known
     *  in the configuration. This exception should trigger an abort of the request processing
     *  to prevent further processing.
     */
    void _processNextRequest(std::lock_guard<std::mutex> const& lock);

    /// Declare HttpMessengerRequest as a friend class to allow it to call _processingFinished directly.
    friend class HttpMessengerRequest;

    /**
     * Notify the connector that the processing of a request has finished.
     * The method is called by the HttpMessengerRequest object upon completion (success or failure)
     * of the request. The method will notify a client by calling a callback in the request.
     * @param id  A unique identifier of a request.
     */
    void _processingFinished(std::string const& id);

    /**
     * Construct the base URL for the worker from the configuration and worker name.
     * @return  The base URL for the worker.
     * @throw ConfigUnknownWorker  If the worker specified by '_workerName' is not known
     *  in the configuration. This exception should trigger an abort of the request processing
     *  to prevent further processing.
     */
    std::string _baseUrl() const;

    /// @return The context string for the current worker (used for reporting errors and logging).
    std::string _context() const;

    // Input parameters

    std::shared_ptr<Config> const _config;
    boost::asio::io_service& _io_service;
    std::string const _workerName;

    // State variables of the service

    /// The priority queue for managing outstanding requests.
    replica::PriorityQueue<HttpMessengerRequest> _requestQueue;

    /// The map of active requests keyed by their unique identifiers.
    std::map<std::string, std::shared_ptr<HttpMessengerRequest>> _activeRequests;

    /// This mutex is for synchronizing access to the request queue and other shared resources.
    mutable std::mutex _mtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGERCONNECTOR_H
