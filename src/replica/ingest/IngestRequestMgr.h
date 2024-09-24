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
#ifndef LSST_QSERV_REPLICA_INGESTREQUESTMGR_H
#define LSST_QSERV_REPLICA_INGESTREQUESTMGR_H

// System headers
#include <condition_variable>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>

// Forward declarations
namespace lsst::qserv::replica {
class IngestRequest;
class IngestResourceMgr;
class ServiceProvider;
class TransactionContribInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Exceptions of this class are thrown when no requests matching the desired
 * criteria were found in the request manager collections.
 */
class IngestRequestNotFound : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/**
 * Exceptions of this class are thrown when no request will be found available
 * when attempting to get the one via the timed version of the method
 * IngestRequestMgr::next(). See the documentation for the method for further details.
 */
class IngestRequestTimerExpired : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/**
 * Class IngestRequestMgr The manager maintains a collection of the ASYNC requests
 * processed by the Ingest system. Each worker service has an instance of the class.
 *
 * The corresponding REST services interact with the manager to order initiating
 * various operations, such as submitting new requests, inspecting a status of existing
 * requests, cancelling queued (or the on-going) requests on behalf of the user ingest
 * workflows. Requests are represented in the manager's implementation using class
 * 'IngestRequest'.
 *
 * Requests are processed by a pool of threads maintained by the Ingest system.
 * The threads pull requests from the manager and return them back after finishing
 * processing them. Threads may also return requests in case of request cancellation
 * or any failures to process the requests.
 *
 * In the implementation of the manager, there are three collections of requests:
 * - a queue of the input requests that are ready to be processed
 * - an ordered (by the unique identifiers of requests) collection of requests that
 *   are in-progress (being processed by the threads)
 * - an ordered (by the unique identifiers of requests) collection of the output
 *   requests that have been processed (cancelled or failed).
 *
 * Requests are processed in the same (FIFO) order they're registered in the manager.
 *
 * Requests stored in the output collection stay in there either until they're claimed
 * by the REST services, or until they expire (if requests have an explicitly set
 * expiration timeout), or if they're cleared by the expiration timer which is set
 * globally for all requests by the manager.
 *
 * @note All public methods of the class are thread-safe (synchronized).
 * @note The class can be also used for unit testing w/o making any side effects
 *  (like attempting to connect to the Replication system's database or other
 *  remote services). In order to instaniate instances of the class for unit testing,
 *  one has to call the special factory method IngestRequestMgr::test(). This
 *  method will make an object that has the empty ServiceProvider and the empty
 *  worker name.
 */
class IngestRequestMgr : public std::enable_shared_from_this<IngestRequestMgr> {
public:
    typedef std::shared_ptr<IngestRequestMgr> Ptr;

    /**
     * The factory method for instantiating the manager.
     * @param serviceProvider The provider is needed to access various services of
     *   the Replication system's framework, such as the Configuration service,
     *   the Database service, etc.
     * @param workerName The name of a worker this service is acting upon.
     * @return A newly created instance of the manager.
     */
    static std::shared_ptr<IngestRequestMgr> create(std::shared_ptr<ServiceProvider> const& serviceProvider,
                                                    std::string const& workerName);

    /**
     * The factory method for instantiating the dummy manager for unit testing.
     * @param resourceMgr The optional resource manager of the test instances is allowed
     *  to be configured externally. If the default value is assumed (the null pointer)
     *  then the empty manager will be created that won't allow imposing any
     *  constraints during request scheduling.
     * @return A newly created instance of the manager.
     */
    static std::shared_ptr<IngestRequestMgr> test(
            std::shared_ptr<IngestResourceMgr> const& resourceMgr = std::shared_ptr<IngestResourceMgr>());

    /**
     * Find a request by its identifier.
     *
     * - The method will first search the request in its transient collections.
     *   If no request will be found in any then the search will continue to
     *   the database.
     *
     * @param id The unique identifier of a request in question.
     * @return The descriptor of the found request.
     * @throw IngestRequestNotFound If the request was not found.
     */
    TransactionContribInfo find(unsigned int id);

    /**
     * Submit a new ingest request.
     *
     * - The request will be registered in the input queue.
     * - A state of the request will be validated before the registration.
     *   The method may also throw exceptions if the request won't be in a proper state.
     *
     * @param request The request to be registered.
     */
    void submit(std::shared_ptr<IngestRequest> const& request);

    /**
     * Cancel a request by its unique identifier if the request is still queued or processed.
     *
     * - The request has to exist and be found in any of those collections.
     * - Completed or previously cancelled requests can't be cancelled.
     * - Requests in the final stage of the processing while the data are already being
     *   ingested into the corresponding MySQL table can't be cancelled as well.
     * - Upon successful completion of the operation the status of the request
     *   will be set to TransactionContribInfo::Status::CANCELLED. Requests that
     *   had been found completed by the time when the cancellation request was
     *   made the current status of the request will be retained. Cancellation operations
     *   for requests in the latter states are also considered as successful.
     *   It's up to a caller of the method to use the request lookup method find() and
     *   inspect the returned object descriptor to see the actual status of the request.
     * - The method may also throw exception should any problem happened while
     *   the method using other services (the Replication system's database, etc.).
     *
     * @param id The unique identifier of a request to be cancelled.
     * @return 'true' if the request was found and successfully cancelled (or marked for cancellation).
     */
    bool cancel(unsigned int id);

    /**
     * Retrieves the next request from the input queue or block the calling
     * thread before such requests will be available (submitted).
     *
     * - This method is used by the threads from a thread pool that is meant
     *   for processing ASYNC requests.
     * - Requests returned by this method will be moved from the input queue into
     *   the in-progress collection.
     *
     * @return An object representing the request.
     */
    std::shared_ptr<IngestRequest> next();

    /**
     * The timed version of a method that retrieves the next request from the input queue.
     *
     * The method will block the calling thread for the specified duration of time
     * while no request will be avaiable (submitted) in the input queue. If no request
     * will be still available upon an expiration of the wait period the method will
     * throw an exception.
     *
     * @param ivalMsec A duration of time not to exceed while waiting for a request to be available.
     * @return An object representing the request.
     * @throws std::invalid_argument If the specifid interval is 0.
     * @throws IngestRequestTimerExpired If no request will be found in the queue
     *   before an expiration of the specified timeout.
     */
    std::shared_ptr<IngestRequest> next(std::chrono::milliseconds const& ivalMsec);

    /**
     * Report a request that has been processed (or failed to be processed, explicitly
     * cancelled, or expired.).
     *
     * - This method is used by the threads from a thread pool that is meant
     *   for processing ASYNC requests.
     * - Requests returned by this method will be moved from the in-progress collection
     *   into the output collection.
     *
     * @param id The unique identifier of a request to be reported.
     * @throw IngestRequestNotFound If the request is unknown to the manager.
     */
    void completed(unsigned int id);

    /**
     * Return the number of the queued requests for the specified database
     * (if any was provided). Otherwise return all requests in this state.
     * @param databaseName The optional name of a database to be inspected.
     * @return size_t
     */
    size_t inputQueueSize(std::string const& databaseName = std::string()) const;

    /**
     * Return the number of in-progress requests for the specified database
     * (if any was provided). Otherwise return all requests in this state.
     * @param databaseName The optional name of a database to be inspected.
     * @return size_t
     */
    size_t inProgressQueueSize(std::string const& databaseName = std::string()) const;

private:
    /// @see method IngestRequestMgr::create()
    IngestRequestMgr(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& workerName);

    /// @see method IngestRequestMgr::test()
    IngestRequestMgr(std::shared_ptr<IngestResourceMgr> const& resourceMgr);

    /**
     * Find the next request that's suitable for processing based on availability
     * of requests and existing resource constraints.
     * The request (if any will be found) will be pulled from the corresponding
     * input queue and moved into the in-progress queue.
     *
     * @param lock The lock to be acquired before calling the method.
     * @return The request found in the queue or the empty pointer if
     *  no suitable request was found.
     */
    std::shared_ptr<IngestRequest> _next(std::unique_lock<std::mutex> const& lock);

    /**
     * Update the concurrency limit from the configuration (if needed)
     * @param lock The lock to be acquired before calkling the method.
     * @param database The name of the affected database.
     * @return 'true' if the concurrency has increased.
     */
    bool _updateMaxConcurrency(std::unique_lock<std::mutex> const& lock, std::string const& database);

    // Input parameters
    std::shared_ptr<ServiceProvider> const _serviceProvider;
    std::string const _workerName;
    std::shared_ptr<IngestResourceMgr> const _resourceMgr;

    /// The mutex for enforcing thread safety of the class's public API and
    /// internal operations.
    mutable std::mutex _mtx;

    /// The condition variable for notifying requests processing threads waiting for
    /// the next request that is ready to be processed.
    std::condition_variable _cv;

    /// Input queues of databases. Each active database has its own queue. The newest
    /// elements are added to the back of the queues.
    std::map<std::string, std::list<std::shared_ptr<IngestRequest>>> _input;

    /// Requests that are being processed by the threads are indexed by their unique
    /// identifiers.
    std::map<unsigned int, std::shared_ptr<IngestRequest>> _inProgress;

    /// The maximum number of concurrent requests to be processed for a database.
    /// A value of 0 means there is no limit.
    std::map<std::string, unsigned int> _maxConcurrency;

    /// The current number of the concurrent requests being processed for databases.
    std::map<std::string, unsigned int> _concurrency;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTREQUESTMGR_H
