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
#ifndef LSST_QSERV_REPLICA_WORKERPROCESSOR_H
#define LSST_QSERV_REPLICA_WORKERPROCESSOR_H

// System headers
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

// Qserv headers
#include "replica/proto/protocol.pb.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Mutex.h"
#include "replica/worker/WorkerProcessorThread.h"
#include "replica/worker/WorkerRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class WorkerRequestFactory;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerProcessor is a front-end interface for processing
 * requests from remote clients within worker-side services.
 */
class WorkerProcessor : public std::enable_shared_from_this<WorkerProcessor> {
public:
    typedef std::shared_ptr<WorkerProcessor> Ptr;

    // The thread-based processor class is allowed to access the internal API
    friend class WorkerProcessorThread;

    /**
     * Structure PriorityQueueTyp extends the standard priority queue for pointers
     * to the new (unprocessed) requests.
     *
     * Its design relies upon the inheritance to get access to the protected
     * data members 'c' representing the internal container of the base queue
     * in order to implement the iterator protocol.
     */
    struct PriorityQueueType
            : std::priority_queue<WorkerRequest::Ptr, std::vector<WorkerRequest::Ptr>, WorkerRequestCompare> {
        /// @return iterator to the beginning of the container
        decltype(c.begin()) begin() { return c.begin(); }

        /// @return iterator to the end of the container
        decltype(c.end()) end() { return c.end(); }

        /**
         * Remove a request from the queue by its identifier
         *
         * @param id an identifier of a request
         * @return 'true' if the object was actually removed
         */
        bool remove(std::string const& id) {
            auto itr = std::find_if(c.begin(), c.end(),
                                    [&id](WorkerRequest::Ptr const& ptr) { return ptr->id() == id; });
            if (itr != c.end()) {
                c.erase(itr);
                std::make_heap(c.begin(), c.end(), comp);
                return true;
            }
            return false;
        }
    };

    /// Current state of the request processing engine
    enum State {
        STATE_IS_RUNNING,   // all threads are running
        STATE_IS_STOPPING,  // stopping all threads
        STATE_IS_STOPPED    // not started
    };

    /// @return the string representation of the state
    static std::string state2string(State state);

    /**
     * The factory method for objects of the class
     *
     * @param serviceProvider provider is needed to access the Configuration of
     *   a setup in order to get a number of the processing threads to be launched
     *   by the processor.
     * @param requestFactory reference to a factory of requests (for instantiating
     *   request objects)
     * @param worker the name of a worker
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, WorkerRequestFactory const& requestFactory,
                      std::string const& worker);

    WorkerProcessor() = delete;
    WorkerProcessor(WorkerProcessor const&) = delete;
    WorkerProcessor& operator=(WorkerProcessor const&) = delete;

    ~WorkerProcessor() = default;

    /// @return the state of the processor
    State state() const { return _state; }

    /// @return the string representation of the state
    std::string state2string() const { return state2string(state()); }

    /// Begin processing requests
    void run();

    /// Stop processing all requests, and stop all threads
    void stop();

    /// Drain (cancel) all queued and in-progress requests
    void drain();

    /// Reload Configuration
    void reconfig();

    /**
     * Enqueue the replication request for processing
     *
     * @param id an identifier of a request
     * @param priority the priority level of a request
     * @param request the Protobuf object received from a client
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     */
    void enqueueForReplication(std::string const& id, int32_t priority, unsigned int requestExpirationIvalSec,
                               ProtocolRequestReplicate const& request, ProtocolResponseReplicate& response);

    /**
     * Enqueue the replica deletion request for processing
     *
     * @param id an identifier of a request
     * @param priority the priority level of a request
     * @param request the Protobuf object received from a client
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     */
    void enqueueForDeletion(std::string const& id, int32_t priority, unsigned int requestExpirationIvalSec,
                            ProtocolRequestDelete const& request, ProtocolResponseDelete& response);

    /**
     * Enqueue the replica lookup request for processing
     *
     * @param id an identifier of a request
     * @param priority the priority level of a request
     * @param request the Protobuf object received from a client
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     */
    void enqueueForFind(std::string const& id, int32_t priority, unsigned int requestExpirationIvalSec,
                        ProtocolRequestFind const& request, ProtocolResponseFind& response);

    /**
     * Enqueue the multi-replica lookup request for processing
     *
     * @param id an identifier of a request
     * @param priority the priority level of a request
     * @param request the Protobuf object received from a client
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     */
    void enqueueForFindAll(std::string const& id, int32_t priority, unsigned int requestExpirationIvalSec,
                           ProtocolRequestFindAll const& request, ProtocolResponseFindAll& response);

    /**
     * Enqueue the worker-side testing request for processing
     *
     * @param id an identifier of a request
     * @param priority the priority level of a request
     * @param request the Protobuf object received from a client
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     */
    void enqueueForEcho(std::string const& id, int32_t priority, unsigned int requestExpirationIvalSec,
                        ProtocolRequestEcho const& request, ProtocolResponseEcho& response);

    /**
     * Enqueue a request for querying the worker database
     *
     * @param id an identifier of a request
     * @param priority the priority level of a request
     * @param request the Protobuf object received from a client
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     */
    void enqueueForSql(std::string const& id, int32_t priority, unsigned int requestExpirationIvalSec,
                       ProtocolRequestSql const& request, ProtocolResponseSql& response);

    /**
     * Enqueue a request for extracting the "director" index data from
     * the director tables.
     *
     * @param id an identifier of a request
     * @param priority the priority level of a request
     * @param request the Protobuf object received from a client
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     */
    void enqueueForDirectorIndex(std::string const& id, int32_t priority,
                                 unsigned int requestExpirationIvalSec,
                                 ProtocolRequestDirectorIndex const& request,
                                 ProtocolResponseDirectorIndex& response);

    /**
     * Set default values to protocol response which has 3 mandatory fields:
     *
     *   status
     *   status_ext
     *   performance
     *
     * @param response the Protobuf object to be updated
     * @param status primary completion status of a request
     * @param extendedStatus extended completion status of a request
     */
    template <class PROTOCOL_RESPONSE_TYPE>
    static void setDefaultResponse(PROTOCOL_RESPONSE_TYPE& response, ProtocolStatus status,
                                   ProtocolStatusExt extendedStatus) {
        WorkerPerformance performance;
        performance.setUpdateStart();
        performance.setUpdateFinish();
        response.set_allocated_performance(performance.info().release());

        response.set_status(status);
        response.set_status_ext(extendedStatus);
    }

    /**
     * Dequeue replication request
     *
     * If the request is not being processed yet then it will be simply removed
     * from the ready-to-be-processed queue. If it's being processed an attempt
     * to cancel processing will be made. If it has already processed this will
     * be reported.
     */
    template <typename RESPONSE_MSG_TYPE>
    void dequeueOrCancel(ProtocolRequestStop const& request, RESPONSE_MSG_TYPE& response) {
        replica::Lock lock(_mtx, _context(__func__));

        // Set this response unless an exact request (same type and identifier)
        // will be found.
        setDefaultResponse(response, ProtocolStatus::BAD, ProtocolStatusExt::INVALID_ID);

        if (WorkerRequest::Ptr const ptr = _dequeueOrCancelImpl(lock, request.id())) {
            try {
                // Set request-specific fields. Note exception handling for scenarios
                // when request identifiers won't match actual types of requests
                _setInfo(ptr, response);

                // The status field is present in all response types
                response.set_status(ptr->status());
                response.set_status_ext(ptr->extendedStatus());

            } catch (std::logic_error const& ex) {
                ;
            }
        }
    }

    /**
     * Return the status of an on-going replication request
     */
    template <typename RESPONSE_MSG_TYPE>
    void checkStatus(ProtocolRequestStatus const& request, RESPONSE_MSG_TYPE& response) {
        replica::Lock lock(_mtx, _context(__func__));

        // Set this response unless an exact request (same type and identifier)
        // will be found.
        setDefaultResponse(response, ProtocolStatus::BAD, ProtocolStatusExt::INVALID_ID);

        if (WorkerRequest::Ptr const ptr = _checkStatusImpl(lock, request.id())) {
            try {
                // Set request-specific fields. Note exception handling for scenarios
                // when request identifiers won't match actual types of requests
                _setInfo(ptr, response);

                // The status field is present in all response types
                response.set_status(ptr->status());
                response.set_status_ext(ptr->extendedStatus());

            } catch (std::logic_error const&) {
                ;
            }
        }
    }

    /**
     * Find the request in any queue, and "garbage collect" it to release resources
     * associated with the request. If the request is still in the "in-progress"
     * state then it will be "drained" before disposing. If the request isn't found
     * in any queue then nothing will happen (no exception thrown, no side effects).
     *
     * @param id an identifier of a request affected by the operation
     * @return 'true' if the request was found and actually removed from any queue
     */
    bool dispose(std::string const& id);

    /**
     * Fill in processor's state and counters into a response object to be sent
     * back to a remote client.
     *
     * @param response the Protobuf object to be initialized and ready to be sent
     *   back to the client
     * @param id an identifier of an original request this response is being sent
     * @param status desired status to set
     * @param extendedReport to return detailed info on all known replica-related requests
     */
    void setServiceResponse(ProtocolServiceResponse& response, std::string const& id, ProtocolStatus status,
                            bool extendedReport = false);

    size_t numNewRequests() const;
    size_t numInProgressRequests() const;
    size_t numFinishedRequests() const;

private:
    WorkerProcessor(ServiceProvider::Ptr const& serviceProvider, WorkerRequestFactory const& requestFactory,
                    std::string const& worker);

    static std::string _classMethodContext(std::string const& func);

    /**
     * Return the next request which is ready to be processed
     * and if then one found assign it to the specified thread. The request
     * will be removed from the ready-to-be-processed queue.
     *
     * If the one is available within the specified timeout then such request
     * will be moved into the in-progress queue, assigned to the processor thread
     * and returned to a caller. Otherwise an empty pointer (pointing to nullptr)
     * will be returned.
     *
     * This method is supposed to be called by one of the processing threads
     * when it becomes available.
     *
     * @note this method will block for a duration of time not exceeding
     *   the client-specified timeout unless it's set to 0. In the later
     *   case the method will block indefinitely.
     * @param processorThread reference to a thread which fetches the next request
     * @param timeoutMilliseconds (optional) amount of time to wait before to finish if
     *   no suitable requests are available for processing
     */
    WorkerRequest::Ptr _fetchNextForProcessing(WorkerProcessorThread::Ptr const& processorThread,
                                               unsigned int timeoutMilliseconds = 0);

    /**
     * Implement the operation for the specified identifier if such request
     * is still known to the Processor. Return a reference to the request object
     * whose state will be properly updated.
     *
     * @param lock а lock on WorkerProcessor::_mtx to be acquired before
     *   calling this method
     * @param id an identifier of a request
     * @return a valid reference to the request object (if found)
     *   or a reference to nullptr otherwise.
     */
    WorkerRequest::Ptr _dequeueOrCancelImpl(replica::Lock const& lock, std::string const& id);

    /**
     * Find and return a reference to the request object.
     *
     * @param lock а lock on WorkerProcessor::_mtx to be acquired before
     *   calling this method
     * @param id an identifier of a request
     * @return a valid reference to the request object (if found)
     *   or a reference to nullptr otherwise.
     */
    WorkerRequest::Ptr _checkStatusImpl(replica::Lock const& lock, std::string const& id);

    /**
     * Extract the extra data from the request and put
     * it into the response object.
     *
     * @note This method expects a correct dynamic type of the request
     *   object. Otherwise it will throw the std::logic_error exception.
     */
    void _setInfo(WorkerRequest::Ptr const& request, ProtocolResponseReplicate& response);

    /**
     * Extract the extra data from the request and put it into the response object.
     *
     * @param request finished request
     * @param response Google Protobuf object to be initialized
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void _setInfo(WorkerRequest::Ptr const& request, ProtocolResponseDelete& response);

    /**
     * Extract the replica info (for one chunk) from the request and put
     * it into the response object.
     *
     * @param request finished request
     * @param response Google Protobuf object to be initialized
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void _setInfo(WorkerRequest::Ptr const& request, ProtocolResponseFind& response);

    /**
     * Extract the replica info (for multiple chunks) from the request and put
     * it into the response object.
     *
     * @param request finished request
     * @param response Google Protobuf object to be initialized
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void _setInfo(WorkerRequest::Ptr const& request, ProtocolResponseFindAll& response);

    /**
     * Extract the input data string received with the request and put
     * it back into the response object.
     *
     * @param request finished request
     * @param response Google Protobuf object to be initialized
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void _setInfo(WorkerRequest::Ptr const& request, ProtocolResponseEcho& response);

    /**
     * Extract the result set (if the query has succeeded) and put
     * it into the response object.
     *
     * @param request finished request
     * @param response Google Protobuf object to be initialized
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void _setInfo(WorkerRequest::Ptr const& request, ProtocolResponseSql& response);

    /**
     * Extract the result set (if the query has succeeded) and put
     * it into the response object.
     *
     * @param request finished request
     * @param response Google Protobuf object to be initialized
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void _setInfo(WorkerRequest::Ptr const& request, ProtocolResponseDirectorIndex& response);

    /**
     * Fill in the information object for the specified request based on its
     * actual type.
     *
     * @param request a pointer to the request
     * @param info a pointer to the Protobuf object to be filled
     * @throw std::logic_error for unsupported request types.
     */
    void _setServiceResponseInfo(WorkerRequest::Ptr const& request, ProtocolServiceResponseInfo* info) const;

    /**
     * Report a decision not to process a request
     *
     * This method is supposed to be called by one of the processing threads
     * after it fetches the next ready-to-process request and then decided
     * not to proceed with processing. Normally this should happen when
     * the thread was asked to stop. In that case the request will be put
     * back into the ready-to-be processed request and be picked up later
     * by some other thread.
     *
     * @param request a pointer to the request
     */
    void _processingRefused(WorkerRequest::Ptr const& request);

    /**
     * Report a request which has been processed or cancelled.
     *
     * The method is called by a thread which was processing the request.
     * The request will be moved into the corresponding queue. A proper
     * completion status is expected be stored within the request.
     *
     * @param request a pointer to the request
     */
    void _processingFinished(WorkerRequest::Ptr const& request);

    /**
     * For threads reporting their completion
     *
     * This method is used by threads to report a change in their state.
     * It's meant to be used during the gradual and asynchronous state transition
     * of this processor from the combined State::STATE_IS_STOPPING to
     * State::STATE_IS_STOPPED. The later is achieved when all threads are stopped.
     *
     * @param processorThread reference to the processing thread which finished
     */
    void _processorThreadStopped(WorkerProcessorThread::Ptr const& processorThread);

    std::string _context(std::string const& func = std::string()) const { return "PROCESSOR  " + func; }

    ServiceProvider::Ptr const _serviceProvider;
    WorkerRequestFactory const& _requestFactory;
    std::string const _worker;

    State _state;

    uint64_t _startTime;  /// When the processor started (milliseconds since UNIX Epoch)

    std::vector<WorkerProcessorThread::Ptr> _threads;

    mutable replica::Mutex _mtx;  /// Mutex guarding the queues

    PriorityQueueType _newRequests;

    std::map<std::string, WorkerRequest::Ptr> _inProgressRequests;
    std::map<std::string, WorkerRequest::Ptr> _finishedRequests;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERPROCESSOR_H
