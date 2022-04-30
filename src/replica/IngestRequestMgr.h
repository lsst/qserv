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
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>

// Qserv headers
#include "replica/ServiceProvider.h"

// Forward declarations
namespace lsst::qserv::replica {
class IngestRequest;
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
 * Class IngestRequestMgr The manager maintains a collection of the ASYNC requests
 * processed by the Ingest system. Each worker service has an instance of the class.
 *
 * The corresponding REST services interact with the manager to order initiating
 * various operations, such as submiting new requests, inspecting a status of existing
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
    static IngestRequestMgr::Ptr create(ServiceProvider::Ptr const& serviceProvider,
                                        std::string const& workerName);

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
     * Cancel a request by its unique identifier.
     *
     * - The request has to exist and be found in any of three collections.
     * - Requests found in the output queue can't be cancelled.
     * - Requests in the final stage of the processing while the data are already being
     *   ingested into the corresponding MySQL table can't be cancelled as well.
     * - Upon the successfull completion of the operation the status of the request
     *   will be set to TransactionContribInfo::Status::CANCELLED. Requests that
     *   had been found completed by the time when the cancellation request was
     *   made the current status of the request will be retained. Cancellation operations
     *   for requests in the latter states are also considered as successfull.
     *   It's up to a caller of the method to inspect the returned object descriptor to
     *   see the actual status of the request.
     * - The method may also throw exception should any problem happened while
     *   the method using other services (the Replication system's database, etc.).
     *
     * @param id The unique identifier of a request to be cancelled.
     * @return The updated descriptor of the request.
     * @throw std::invalid_argument If the request is unknown to the manager.
     */
    TransactionContribInfo cancel(unsigned int id);

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
     * Report a request that has been processed (or failed to be processed, explicitly
     * cancelled, or expired.).
     *
     * - This method is used by the threads from a thread pool that is meant
     *   for processing ASYNC requests.
     * - Requests returned by this method will be moved from the in-progress collection
     *   into the output collection.
     *
     * @param id The unique identifier of a request to be reported.
     * @throw std::invalid_argument If the request is unknown to the manager.
     */
    void completed(unsigned int id);

private:
    /// @see method IngestRequestMgr::creeate()
    IngestRequestMgr(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName);

    // Input parameters
    ServiceProvider::Ptr const _serviceProvider;
    std::string const _workerName;

    /// The mutex for enforcing thread safety of the class's public API and
    /// internal operations.
    mutable std::mutex _mtx;

    /// The condition variable for notifying requests processing threads waiting for
    /// the next request that is ready to be processed.
    std::condition_variable _cv;

    std::list<std::shared_ptr<IngestRequest>> _input;
    std::map<unsigned int, std::shared_ptr<IngestRequest>> _inProgress;
    std::map<unsigned int, std::shared_ptr<IngestRequest>> _output;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTREQUESTMGR_H
