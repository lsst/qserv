// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// WorkerProcessor.h declares:
///
/// class WorkerProcessor
/// (see individual class documentation for more information)

// System headers
#include <algorithm>
#include <list>
#include <memory>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerProcessorThread.h"
#include "replica/WorkerRequest.h"
#include "util/Mutex.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class WorkerRequestFactory;

/**
  * Class WorkerProcessor is a front-end interface for processing
  * requests of remote clients.
  */
class WorkerProcessor
    :   public std::enable_shared_from_this<WorkerProcessor> {

public:

    /// Pointer type for objects of the class
    typedef std::shared_ptr<WorkerProcessor> Ptr;

    // The thread-based processor class is allowed to access the internal API
    friend class WorkerProcessorThread;

    /**
     * Struct PriorityQueueTyp extends the standard priority queue for pointers
     * to the new (unprocessed) requests.
     * 
     * Its design relies upon the inheritance to get access to the protected
     * data members 'c' representing the internal container of the base queue
     * in order to implement the iterator protocol.
     */
    struct PriorityQueueType
        :   std::priority_queue<WorkerRequest::Ptr,
                                std::vector<WorkerRequest::Ptr>,
                                WorkerRequestCompare> {

        /// @return iterator to the beginning of the container
        decltype(c.begin()) begin() {
            return c.begin();
        }

        /// @return iterator to the end of the container
        decltype(c.end()) end() {
            return c.end();
        }

        /**
         * Remove a request from the queue by its identifier
         *
         * @param id - an identifier of a request
         *
         * @return 'true' if the object was actially removed
         */
        bool remove(std::string const& id) {
            auto itr = std::find_if (
                c.begin(),
                c.end(),
                [&id] (WorkerRequest::Ptr const& ptr) {
                    return ptr->id() == id;
                }
            );
            if (itr != c.end()) {
                c.erase(itr);
                std::make_heap(c.begin(), c.end(), comp);
                return true;
            }
            return false;
        }
    };

    /// Ordinary collection of pointers for requests in other (than new/unprocessed) state
    typedef std::list<WorkerRequest::Ptr> CollectionType;

    /// Current state of the request processing engine
    enum State {
        STATE_IS_RUNNING,    // all threads are running
        STATE_IS_STOPPING,   // stopping all threads
        STATE_IS_STOPPED     // not started
    };

    /// @return the string representation of the state
    static std::string state2string(State state);

    /**
     * The factory method for objects of the class
     *
     * @param serviceProvider - provider of various services
     * @param requestFactory  - reference to a factory of requests (for instantiating request objects)
     * @param worker          - the name of a worker
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      WorkerRequestFactory& requestFactory,
                      std::string const& worker);

    // Default construction and copy semantics are prohibited

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

    /**
     * Enqueue the replication request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForReplication(std::string const& id,
                               proto::ReplicationRequestReplicate const& request,
                               proto::ReplicationResponseReplicate& response);

    /**
     * Enqueue the replica deletion request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForDeletion(std::string const& id,
                            proto::ReplicationRequestDelete const& request,
                            proto::ReplicationResponseDelete& response);

    /**
     * Enqueue the replica lookup request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForFind(std::string const& id,
                        proto::ReplicationRequestFind const& request,
                        proto::ReplicationResponseFind& response);

    /**
     * Enqueue the multi-replica lookup request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForFindAll(std::string const& id,
                           proto::ReplicationRequestFindAll const& request,
                           proto::ReplicationResponseFindAll& response);

    /**
     * Set default values to protocol response which has 3 mandatory fields:
     *
     *   status
     *   status_ext
     *   performance
     *   
     * @param response       - the protobuf object to be updated
     * @param status         - primary completion status of a request
     * @param extendedStatus - extended completion status of a request
     */
    template <class PROTOCOL_RESPONSE_TYPE>
    static void setDefaultResponse(PROTOCOL_RESPONSE_TYPE& response,
                                   proto::ReplicationStatus status,
                                   proto::ReplicationStatusExt extendedStatus) {

        WorkerPerformance performance;
        performance.setUpdateStart();
        performance.setUpdateFinish();
        response.set_allocated_performance(performance.info());

        response.set_status(status);
        response.set_status_ext(extendedStatus);
    }

    /**
     * Dequeue replication request
     *
     * If the request is not being processed yet then it wil be simply removed
     * from the ready-to-be-processed queue. If it's being processed an attempt
     * to cancel processing will be made. If it has already processed this will
     * be reported.
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object representing the request
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    template <typename RESPONSE_MSG_TYPE>
    void dequeueOrCancel(std::string const& id,
                         proto::ReplicationRequestStop const& request,
                         RESPONSE_MSG_TYPE& response) {

        util::Lock lock(_mtx, context() + "dequeueOrCancel");

        // Set this response unless an exact request (same type and identifier)
        // will be found.
        setDefaultResponse(response,
                           proto::ReplicationStatus::BAD,
                           proto::ReplicationStatusExt::INVALID_ID);

        // Try to locate a request with specified identifier and make sure
        // its actual type matches expecations

        if (WorkerRequest::Ptr ptr = dequeueOrCancelImpl(lock, request.id())) {
            try {

                // Set request-specific fields. Note exception handling for scenarios
                // when request identifiers won't match actual types of requests
                setInfo(ptr, response);

                // The status field is present in all response types
                response.set_status(             translate(ptr->status()));
                response.set_status_ext(replica::translate(ptr->extendedStatus()));

            } catch (std::logic_error const& ex) {
                ;
            }
        }
    }

    /**
     * Return the status of an on-going replication request
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object representing the request
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    template <typename RESPONSE_MSG_TYPE>
    void checkStatus(std::string const& id,
                     proto::ReplicationRequestStatus const& request,
                     RESPONSE_MSG_TYPE& response) {

        util::Lock lock(_mtx, context() + "checkStatus");

        // Set this response unless an exact request (same type and identifier)
        // will be found.
        setDefaultResponse(response,
                           proto::ReplicationStatus::BAD,
                           proto::ReplicationStatusExt::INVALID_ID);

        // Try to locate a request with specified identifier and make sure
        // its actual type matches expecations

        if (WorkerRequest::Ptr ptr = checkStatusImpl(lock, request.id())) {
            try {

                // Set request-specific fields. Note exception handling for scenarios
                // when request identifiers won't match actual types of requests
                setInfo(ptr, response);

                // The status field is present in all response types
                response.set_status(             translate(ptr->status()));
                response.set_status_ext(replica::translate(ptr->extendedStatus()));

            } catch (std::logic_error const&) {
                ;
            }
        }
    }

    /**
     * Fill in processor's state and counters into a response object to be sent
     * back to a remote client.
     *
     * @param response       - the protobuf object to be initialized and ready
     *                         to be sent back to the client
     * @param id             - an identifier of an original request this response
     *                         is being sent
     * @param status         - desired status to set
     * @param extendedReport - to return detailed info on all known
     *                         replica-related requests
     */
    void setServiceResponse(proto::ReplicationServiceResponse& response,
                            std::string const& id,
                            proto::ReplicationServiceResponse::Status status,
                            bool extendedReport = false);

    ///@return total number of new unprocessed requests
    size_t numNewRequests() const;

    /// @return total number of requests which are being processed
    size_t numInProgressRequests() const;

    /// @return total number of completed (succeeded or otherwise) requests
    size_t numFinishedRequests() const;

private:

    /**
     * The constructor of the class is available to the functory method only
     *
     * @see WorkerProcessor::create
     */
    WorkerProcessor(ServiceProvider::Ptr const& serviceProvider,
                    WorkerRequestFactory& requestFactory,
                    std::string const& worker);

    /**
     * Translate the completion status for replication requests and return
     * its protobuf counterpart
     *
     * @param status - a completion status of a request processing object
     *
     * @return the matching completion status as per a protobuf definition
     */
    static proto::ReplicationStatus translate(WorkerRequest::CompletionStatus status);

    /**
     * Return the next request which is ready to be pocessed
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
     * ATTENTION: this method will block for a duration of time not exceeding
     *            the client-specified timeout unless it's set to 0. In the later
     *            case the method will block indefinitevely.
     *
     * @param processorThread     - reference to a thread which fetches the next request
     * @param timeoutMilliseconds - (optional) amount of time to wait before to finish if
     *                              no suitable requests are available for processing
     */
    WorkerRequest::Ptr fetchNextForProcessing(
                                WorkerProcessorThread::Ptr const& processorThread,
                                unsigned int timeoutMilliseconds=0);

    /**
     * Implement the operation for the specified identifier if such request
     * is still known to the Processor. Return a reference to the request object
     * whose state will be properly updated.
     *
     * @param lock - lock which must be acquired before calling this method 
     * @param id   - an identifier of a request
     *
     * @return - a valid reference to the request object (if found)
     *           or a reference to nullptr otherwise.
     */
    WorkerRequest::Ptr dequeueOrCancelImpl(util::Lock const& lock,
                                           std::string const& id);

    /**
     * Find and return a reference to the request object.
     *
     * @param lock - lock which must be acquired before calling this method 
     * @param id   - an identifier of a request
     *
     * @return - a valid reference to the request object (if found)
     *           or a reference to nullptr otherwise.
     */
    WorkerRequest::Ptr checkStatusImpl(util::Lock const& lock,
                                       std::string const& id);

    /**
     * Extract the extra data from the request and put
     * it into the response object.
     *
     * NOTE: This method expects a correct dynamic type of the request
     *       object. Otherwise it will throw the std::logic_error exception.
     */
    void setInfo(WorkerRequest::Ptr const& request,
                 proto::ReplicationResponseReplicate& response);

    /**
     * Extract the extra data from the request and put it into the response object.
     *
     * @param request  - finished request
     * @param response - Google Protobuf object to be initialized
     *
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void setInfo(WorkerRequest::Ptr const& request,
                 proto::ReplicationResponseDelete& response);

    /**
     * Extract the replica info (for one chunk) from the request and put
     * it into the response object.
     *
     * @param request  - finished request
     * @param response - Google Protobuf object to be initialized
     *
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void setInfo(WorkerRequest::Ptr const& request,
                 proto::ReplicationResponseFind& response);

    /**
     * Extract the replica info (for multiple chunks) from the request and put
     * it into the response object.
     *
     * @param request  - finished request
     * @param response - Google Protobuf object to be initialized
     *
     * @throws std::logic_error if the dynamic type of the request won't match expectations
     */
    void setInfo(WorkerRequest::Ptr const& request,
                 proto::ReplicationResponseFindAll& response);

    /**
     * Fill in the information object for the specified request based on its
     * actual type.
     *
     * @param request - a pointer to the request
     * @param info    - a pointer to the protobuf object to be filled
     *
     * @throw std::logic_error for unsupported request types.
     */
    void setServiceResponseInfo(WorkerRequest::Ptr const& request,
                                proto::ReplicationServiceResponseInfo* info) const;

    /**
     * Report a decision not to process a request
     *
     * This method ia supposed to be called by one of the processing threads
     * after it fetches the next ready-to-process request and then decided
     * not to proceed with processing. Normally this should happen when
     * the thread was asked to stop. In that case the request will be put
     * back into the ready-to-be processed request and be picked up later
     * by some other thread.
     *
     * @param request - a pointer to the request
     */
    void processingRefused(WorkerRequest::Ptr const& request);

    /**
     * Report a request which has been processed or cancelled.
     *
     * The method is called by a thread which was processing the request.
     * The request will be moved into the corresponding queue. A proper
     * completion status is expected be stored within the request.
     *
     * @param request - a pointer to the request
     */
    void processingFinished(WorkerRequest::Ptr const& request);

    /**
     * For threads reporting their completion
     *
     * This method is used by threads to report a change in their state.
     * It's meant to be used during the gradual and asynchronous state transition
     * of this processor from the combined State::STATE_IS_STOPPING to
     * State::STATE_IS_STOPPED. The later is achieved when all threads are stopped.
     *
     * @param processorThread - reference to the processing thread which finished
     */
    void processorThreadStopped(WorkerProcessorThread::Ptr const& processorThread);

    /// @return the context string
    std::string context() const { return "PROCESSOR  "; }

private:

    /// Services used by the processor
    ServiceProvider::Ptr _serviceProvider;

    /// A factory of request objects
    WorkerRequestFactory& _requestFactory;

    /// The name of the worker
    std::string _worker;

    /// Current state of the processor
    State _state;

    /// When the processor started (milliseconds since UNIX Epoch)
    uint64_t _startTime;

    /// A pool of threads for processing requests
    std::vector<WorkerProcessorThread::Ptr> _threads;

    /// Mutex guarding the queues
    mutable util::Mutex _mtx;

    /// New unprocessed requests
    PriorityQueueType _newRequests;

    /// Requests which are being processed
    CollectionType _inProgressRequests;

    /// Completed (succeeded or otherwise) requests
    CollectionType _finishedRequests;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERPROCESSOR_H
