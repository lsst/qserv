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
#include "replica/requests/http/HttpMessengerRequest.h"
#include "replica/util/PriorityQueue.h"

// Forward declarations
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
     * @see HttpMessengerRequest::OnFinishCallback
     */
    typedef HttpMessengerRequest::OnFinishCallback OnFinishCallback;

    HttpMessengerConnector() = delete;
    HttpMessengerConnector(HttpMessengerConnector const&) = delete;
    HttpMessengerConnector& operator=(HttpMessengerConnector const&) = delete;

    /// The non-trivial desctruction is required to stop threads and join
    ~HttpMessengerConnector();

    /**
     * Create a new connector with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param config  The configuration service of the Replication Framework.
     * @param workerName  The name of a worker.
     * @param requestIdGenerator  The generator for unique request identifiers.
     * @return  A pointer to the created object.
     */
    static std::shared_ptr<HttpMessengerConnector> create(
            std::shared_ptr<Config> const& config, std::string const& workerName,
            std::shared_ptr<HttpMessengerRequest::IdGenerator> const& requestIdGenerator);

    /// @return True if the current thread is processing a request for any connector.
    static bool isCurrentThreadWorker() noexcept;

    /**
     * Cancel all requests and stop operations. When called from a worker callback,
     * shutdown is scheduled asynchronously to avoid joining sibling workers from
     * within the worker pool.
     */
    void stop();

    /**
     * Initiate sending a message.
     *
     * The response message will be initialized only in case of successful completion
     * of the request. The method may throw exception std::logic_error if
     * the MessengerConnector already has another request registered with the same
     * request 'id'.
     *
     * @param resource  The resource path for the HTTP request.
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
     * @throw std::logic_error  If the service has been stopped.
     * @return  The unique identifier of the request which can be used to track or cancel the request.
     */
    std::uint64_t send(int priority, std::string const& resource, http::Method method,
                       nlohmann::json const& requestJson, OnFinishCallback onFinish,
                       unsigned int timeoutMs = 0);

    /**
     * Cancel an outstanding request.
     *
     * @param id  A unique identifier of a request.
     * @throw std::invalid_argument  If the request identifier is 0.
     */
    void cancel(std::uint64_t id);

    /**
     * Check if a request with the specified identifier exists in the request queue
     * or is currently active.
     * @param id  A unique identifier of a request.
     * @return  'true' if the specified request exists in the request queue or is currently active.
     * @throw std::invalid_argument  If the request identifier is 0.
     */
    bool exists(std::uint64_t id) const;

private:
    HttpMessengerConnector(std::shared_ptr<Config> const& config, std::string const& workerName,
                           std::shared_ptr<HttpMessengerRequest::IdGenerator> const& requestIdGenerator);

    /// Initialize the state and start request processing threads.
    void _initThreadPool();

    /**
     * Process the next request from the request queue.
     * This method is intended to be run by each thread.
     * @return  'true' if the request was processed successfully, 'false' if the thread
     *  that invoked the method should stop.
     */
    bool _processNextRequest();

    /**
     * Implement to stop the service and cancel all outstanding requests.
     */
    void _stop();

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
    std::string const _workerName;
    std::shared_ptr<HttpMessengerRequest::IdGenerator> const _requestIdGenerator;

    // State variables of the service

    /// The number of threads for processing requests.
    /// The same number is used to initialize the connection pool.
    std::size_t const _numThreads;

    /// The threads responsible for processing requests.
    std::vector<std::unique_ptr<std::thread>> _threads;

    /// The priority queue for managing outstanding requests.
    replica::PriorityQueue<HttpMessengerRequest> _requestQueue;

    /// The map of active requests keyed by their unique identifiers.
    std::map<std::uint64_t, std::shared_ptr<HttpMessengerRequest>> _activeRequests;

    /// Flag to indicate if the service should stop.
    std::atomic<bool> _stopService{false};

    /// This mutex is for synchronizing access to the request queue and other shared resources.
    mutable std::mutex _mtx;

    /// Serializes joins while allowing stop() to be called from a worker callback.
    std::mutex _joinMtx;

    /// The condition variable used to signal changes in the request queue.
    std::condition_variable _cv;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPMESSENGERCONNECTOR_H
