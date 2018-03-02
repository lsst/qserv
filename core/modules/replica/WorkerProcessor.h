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
#ifndef LSST_QSERV_REPLICA_WORKER_PROCESSOR_H
#define LSST_QSERV_REPLICA_WORKER_PROCESSOR_H

/// WorkerProcessor.h declares:
///
/// class WorkerProcessor
/// (see individual class documentation for more information)

// System headers
#include <algorithm>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/WorkerProcessorThread.h"
#include "replica/WorkerRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class ServiceProvider;
class WorkerRequestFactory;

/**
  * Class WorkerProcessor is a front-end interface for processing
  * requests fro connected clients.
  */
class WorkerProcessor {

public:

    // The thread-based processor class is allowed to access the internal API
    friend class WorkerProcessorThread;

    /// The priority queue for pointers to the new (unprocessed) requests.
    /// Using inheritance to get access to the protected data members 'c'
    /// representing the internal container.
    struct PriorityQueueType
        :   std::priority_queue<WorkerRequest::pointer,
                                std::vector<WorkerRequest::pointer>,
                                WorkerRequestCompare> {

        /// The beginning of the container to allow the iterator protocol
        decltype(c.begin()) begin() {
            return c.begin();
        }

        /// The end of the container to allow the iterator protocol
        decltype(c.end()) end() {
            return c.end();
        }

        /// Remove a request from the queue by its identifier
        bool remove(std::string const& id) {
            auto itr = std::find_if (
                c.begin(),
                c.end(),
                [&id] (WorkerRequest::pointer const& ptr) {
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
    typedef std::list<WorkerRequest::pointer> CollectionType;

    /// Current state of the request processing engine
    enum State {
        STATE_IS_RUNNING,    // all threads are running
        STATE_IS_STOPPING,   // stopping all threads
        STATE_IS_STOPPED     // not started
    };

    /// Return the string representation of the status
    static std::string state2string (State state);

    // Default construction and copy semantics are prohibited

    WorkerProcessor() = delete;
    WorkerProcessor(WorkerProcessor const&) = delete;
    WorkerProcessor&operator=(WorkerProcessor const&) = delete;

    /**
     * The constructor of the class.
     */
    WorkerProcessor(ServiceProvider&      serviceProvider,
                    WorkerRequestFactory& requestFactory,
                    std::string const&    worker);

    /// Destructor
    ~WorkerProcessor() = default;

    /// @return the state of the processor
    State state() const { return _state; }

    /// Begin processing requests
    void run();

    /// Stop processing all requests, and stop all threads
    void stop();

    /**
     * Drain (cancel) all queued and in-progress requests
     */
    void drain();

    /**
     * Enqueue the replication request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForReplication(std::string const&                        id,
                               proto::ReplicationRequestReplicate const& request,
                               proto::ReplicationResponseReplicate&      response);

    /**
     * Enqueue the replica deletion request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForDeletion(std::string const&                     id,
                            proto::ReplicationRequestDelete const& request,
                            proto::ReplicationResponseDelete&      response);

    /**
     * Enqueue the replica lookup request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForFind(std::string const&                   id,
                        proto::ReplicationRequestFind const& request,
                        proto::ReplicationResponseFind&      response);

    /**
     * Enqueue the multi-replica lookup request for processing
     *
     * @param id       - an identifier of a request
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForFindAll(std::string const&                      id,
                           proto::ReplicationRequestFindAll const& request,
                           proto::ReplicationResponseFindAll&      response);

    /**
     * Set default values to protocol response which has 3 mandatory fields:
     *
     *   status
     *   status_ext
     *   performance
     */
    template <class PROTOCOL_RESPONSE_TYPE>
    static void setDefaultResponse(PROTOCOL_RESPONSE_TYPE&      response,
                                   proto::ReplicationStatus     status,
                                   proto::ReplicationStatusExt  extendedStatus) {
    
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
    void dequeueOrCancel(std::string const&                   id,
                         proto::ReplicationRequestStop const& request,
                         RESPONSE_MSG_TYPE&                   response) {

        // Set this response unless an exact request (same type and identifier)
        // will be found.
        setDefaultResponse(response,
                           proto::ReplicationStatus::BAD,
                           proto::ReplicationStatusExt::INVALID_ID);

        // Try to locate a request with specified identifier and make sure
        // its actual type matches expecations

        if (WorkerRequest::pointer ptr = dequeueOrCancelImpl(request.id())) {
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
    void checkStatus(std::string const&                     id,
                     proto::ReplicationRequestStatus const& request,
                     RESPONSE_MSG_TYPE&                     response) {

        // Set this response unless an exact request (same type and identifier)
        // will be found.
        setDefaultResponse(response,
                           proto::ReplicationStatus::BAD,
                           proto::ReplicationStatusExt::INVALID_ID);

        // Try to locate a request with specified identifier and make sure
        // its actual type matches expecations

        if (WorkerRequest::pointer ptr = checkStatusImpl(request.id())) {
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

    /// Number of new unprocessed requests
    size_t numNewRequests() const;

    /// Number of requests which are being processed
    size_t numInProgressRequests() const;

    /// Number of completed (succeeded or otherwise) requests
    size_t numFinishedRequests() const;

private:

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
     * If the one is available witin the specified timeout then such request
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
     */
    WorkerRequest::pointer fetchNextForProcessing(
                                WorkerProcessorThread::pointer const& processorThread,
                                unsigned int                          timeoutMilliseconds=0);

    /**
     * Implement the operation for the specified identifier if such request
     * is still known to the Processor. Return a reference to the request object
     * whose state will be properly updated.
     *
     * @return - a valid reference to the request object (if found)
     *           or a reference to nullptr otherwise. 
     */
    WorkerRequest::pointer dequeueOrCancelImpl(std::string const& id);

    /** Find and return a reference to the request object.
     * 
     * @return - a valid reference to the request object (if found)
     *           or a reference to nullptr otherwise. 
     */
    WorkerRequest::pointer checkStatusImpl(std::string const& id);

    /**
     * Extract the extra data from the request and put
     * it into the response object.
     *
     * NOTE: This method expects a correct dynamic type of the request
     *       object. Otherwise it will throw the std::logic_error exception.
     */
    void setInfo(WorkerRequest::pointer const&        request,
                 proto::ReplicationResponseReplicate& response);

    /**
     * Extract the extra data from the request and put
     * it into the response object.
     *
     * NOTE: This method expects a correct dynamic type of the request
     *       object. Otherwise it will throw the std::logic_error exception.
     */
    void setInfo(WorkerRequest::pointer const&     request,
                 proto::ReplicationResponseDelete& response);

    /**
     * Extract the replica info (for one chunk) from the request and put
     * it into the response object.
     *
     * NOTE: This method expects a correct dynamic type of the request
     *       object. Otherwise it will throw the std::logic_error exception.
     */
    void setInfo(WorkerRequest::pointer const&   request,
                 proto::ReplicationResponseFind& response);
 
    /**
     * Extract the replica info (for multiple chunks) from the request and put
     * it into the response object.
     *
     * NOTE: This method expects a correct dynamic type of the request
     *       object. Otherwise it will throw the std::logic_error exception.
     */
    void setInfo(WorkerRequest::pointer const&      request,
                 proto::ReplicationResponseFindAll& response);

    /**
     * Fill in the information object for the specified request based on its
     * actual type.
     *
     * The method will throw std::logic_error for unsupported request types.
     *
     * @param request - a reference to the request
     * @param info    - a pointer to the protobuf object to be filled
     */
    void setServiceResponseInfo(WorkerRequest::pointer const&          request,
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
     */
    void processingRefused(WorkerRequest::pointer const& request);

    /**
     * Report a request which has been processed or cancelled.
     *
     * The method is called by a thread which was processing the request.
     * The request will be moved into the corresponding queue. A proper
     * completion status is expected be stored witin the request.
     */
    void processingFinished(WorkerRequest::pointer const& request);

    /**
     * For threads reporting their completion
     *
     * This method is used by threads to report a change in their state.
     * It's meant to be used during the gradual and asynchronous state transition
     * of this processor from the combined State::STATE_IS_STOPPING to
     * State::STATE_IS_STOPPED. The later is achieved when all threads are stopped.
     */
    void processorThreadStopped(WorkerProcessorThread::pointer const& processorThread);

    /// Return the context string
    std::string context() const { return "PROCESSOR  "; }

private:

    /// Services used by the processor
    ServiceProvider& _serviceProvider;

    /// A factory of request objects
    WorkerRequestFactory &_requestFactory;

    /// The name of the worker
    std::string _worker;

    /// Current state of the processor
    State _state;

    /// When the processor started (milliseconds since UNIX Epoch)
    uint64_t _startTime;

    /// A pool of threads for processing requests
    std::vector<WorkerProcessorThread::pointer> _threads;
    
    /// Mutex guarding the queues
    mutable std::mutex _mtx;

    /// New unprocessed requests
    PriorityQueueType _newRequests;

    /// Requests which are being processed
    CollectionType _inProgressRequests;

    /// Completed (succeeded or otherwise) requests
    CollectionType _finishedRequests;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_PROCESSOR_H