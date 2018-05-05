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

// Class header
#include "replica/WorkerProcessor.h"

// System headers
#include <algorithm>
#include <iterator>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerDeleteRequest.h"
#include "replica/WorkerFindRequest.h"
#include "replica/WorkerFindAllRequest.h"
#include "replica/WorkerReplicationRequest.h"
#include "replica/WorkerRequestFactory.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerProcessor");

namespace replica = ::lsst::qserv::replica;
namespace proto   = ::lsst::qserv::proto;

template <class PROTOCOL_RESPONSE_TYPE,
          class PROTOCOL_REQUEST_TYPE>
bool ifDuplicateRequest(PROTOCOL_RESPONSE_TYPE& response,
                        replica::WorkerRequest::Ptr const& p,
                        PROTOCOL_REQUEST_TYPE const& request) {

    bool isDuplicate = false;

    if (replica::WorkerReplicationRequest::Ptr ptr =
        std::dynamic_pointer_cast<replica::WorkerReplicationRequest>(p)) {
        isDuplicate =
            (ptr->database() == request.database()) and
            (ptr->chunk()    == request.chunk());

    } else if (replica::WorkerDeleteRequest::Ptr ptr =
             std::dynamic_pointer_cast<replica::WorkerDeleteRequest>(p)) {
        isDuplicate =
            (ptr->database() == request.database()) and
            (ptr->chunk()    == request.chunk());
    }
    if (isDuplicate) {
        replica::WorkerProcessor::setDefaultResponse(
            response,
            proto::ReplicationStatus::BAD,
            proto::ReplicationStatusExt::DUPLICATE);
        response.set_duplicate_request_id(p->id());
    }
    return isDuplicate;
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

std::string WorkerProcessor::state2string(State state) {
    switch (state) {
        case STATE_IS_RUNNING:  return "STATE_IS_RUNNING";
        case STATE_IS_STOPPING: return "STATE_IS_STOPPING";
        case STATE_IS_STOPPED:  return "STATE_IS_STOPPED";
    }
    throw std::logic_error("unhandled state " + std::to_string(state) +
                           " in WorkerProcessor::state2string()");
}

proto::ReplicationStatus WorkerProcessor::translate(WorkerRequest::CompletionStatus status) {
    switch (status) {
        case WorkerRequest::STATUS_NONE:          return proto::ReplicationStatus::QUEUED;
        case WorkerRequest::STATUS_IN_PROGRESS:   return proto::ReplicationStatus::IN_PROGRESS;
        case WorkerRequest::STATUS_IS_CANCELLING: return proto::ReplicationStatus::IS_CANCELLING;
        case WorkerRequest::STATUS_CANCELLED:     return proto::ReplicationStatus::CANCELLED;
        case WorkerRequest::STATUS_SUCCEEDED:     return proto::ReplicationStatus::SUCCESS;
        case WorkerRequest::STATUS_FAILED:        return proto::ReplicationStatus::FAILED;
        default:
            throw std::logic_error(
                "unhandled status " + WorkerRequest::status2string(status) +
                " at WorkerProcessor::translate()");
    }
}

WorkerProcessor::WorkerProcessor(ServiceProvider::Ptr const& serviceProvider,
                                 WorkerRequestFactory& requestFactory,
                                 std::string const& worker)
    :   _serviceProvider(serviceProvider),
        _requestFactory(requestFactory),
        _worker(worker),
        _state(STATE_IS_STOPPED),
        _startTime(PerformanceUtils::now()) {
}

void WorkerProcessor::run() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "run");

    LOCK_GUARD;

    if (_state == STATE_IS_STOPPED) {

        size_t const numThreads = _serviceProvider->config()->workerNumProcessingThreads();
        if (not numThreads) {
            throw std::out_of_range(
                        "invalid configuration parameter for the number of processing threads. "
                        "The value of the parameter must be greater than 0");
        }

        // Create threads if needed
        if (_threads.empty()) {
            for (size_t i=0; i < numThreads; ++i) {
                _threads.push_back(WorkerProcessorThread::create(*this));
            }
        }

        // Tell each thread to run

        for (auto&& t: _threads) {
            t->run();
        }
        _state = STATE_IS_RUNNING;
    }
}

void WorkerProcessor::stop() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "stop");

    LOCK_GUARD;

    if (_state == STATE_IS_RUNNING) {

        // Tell each thread to stop.

        for (auto&& t: _threads) {
            t->stop();
        }

        // Begin transitioning to the final state via this intermediate one.
        // The transition will finish asynchronious when all threads will report
        // desired changes in their states.

        _state = STATE_IS_STOPPING;
    }
}

void WorkerProcessor::drain() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "drain");

    // Collect identifiers of requests from both queues under the guard
    std::list<std::string> ids;
    {
        LOCK_GUARD;

        for (auto&& ptr: _newRequests)        { ids.push_back(ptr->id()); }
        for (auto&& ptr: _inProgressRequests) { ids.push_back(ptr->id()); }
    }

    // Dequeue requests w/o the guard to avoid a dedlock because
    // the dequeuing method will request the lock.
    for (auto&& id: ids) dequeueOrCancelImpl(id);
}

void WorkerProcessor::enqueueForReplication(
                            std::string const& id,
                            proto::ReplicationRequestReplicate const& request,
                            proto::ReplicationResponseReplicate& response) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForReplication"
        << "  id: "     << id
        << "  db: "     << request.database()
        << "  chunk: "  << request.chunk()
        << "  worker: " << request.worker());

    LOCK_GUARD;

    // Verify a scope of the request to ensure it won't duplicate or interfere (with)
    // existing requests in the active (non-completed) queues. A reason why we're ignoring
    // the completed is that this replica may have already been deleted from this worker.

    for (auto&& ptr: _newRequests) {
        if (::ifDuplicateRequest(response,
                                 ptr,
                                 request)) { return; }
    }
    for (auto&& ptr: _inProgressRequests) {
        if (::ifDuplicateRequest(response,
                                 ptr,
                                 request)) { return; }
    }

    // The code below may catch exceptions if other parameters of the requites
    // won't pass further validation against the present configuration of the request
    // procesisng service.
    try {
        auto const ptr =
            _requestFactory.createReplicationRequest(
                _worker,
                id,
                request.priority(),
                request.database(),
                request.chunk(),
                request.worker());

        _newRequests.push(ptr);

        response.set_status(proto::ReplicationStatus::QUEUED);
        response.set_status_ext(proto::ReplicationStatusExt::NONE);
        response.set_allocated_performance(ptr->performance().info());

        setInfo(ptr, response);

    } catch (std::invalid_argument const& ec) {
        LOGS(_log, LOG_LVL_ERROR, context() << "enqueueForReplication  " << ec.what());

        setDefaultResponse(response,
                           proto::ReplicationStatus::BAD,
                           proto::ReplicationStatusExt::INVALID_PARAM);
    }
}

void WorkerProcessor::enqueueForDeletion(std::string const& id,
                                         proto::ReplicationRequestDelete const& request,
                                         proto::ReplicationResponseDelete& response) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForDeletion"
        << "  id: "    << id
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk());

    LOCK_GUARD;

    // Verify a scope of the request to ensure it won't duplicate or interfere (with)
    // existing requests in the active (non-completed) queues. A reason why we're ignoring
    // the completed is that this replica may have already been deleted from this worker.

    for (auto&& ptr : _newRequests) {
        if (::ifDuplicateRequest(response,
                                 ptr,
                                 request)) { return; }
    }
    for (auto&& ptr : _inProgressRequests) {
        if (::ifDuplicateRequest(response,
                                 ptr,
                                 request)) { return; }
    }

    // The code below may catch exceptions if other parameters of the requites
    // won't pass further validation against the present configuration of the request
    // procesisng service.
    try {
        auto const ptr =
            _requestFactory.createDeleteRequest(
                _worker,
                id,
                request.priority(),
                request.database(),
                request.chunk());

        _newRequests.push(ptr);

        response.set_status(               proto::ReplicationStatus::QUEUED);
        response.set_status_ext(           proto::ReplicationStatusExt::NONE);
        response.set_allocated_performance(ptr->performance().info());

        setInfo(ptr, response);

    } catch (std::invalid_argument const& ec) {
        LOGS(_log, LOG_LVL_ERROR, context() << "enqueueForDeletion  " << ec.what());

        setDefaultResponse(response,
                           proto::ReplicationStatus::BAD,
                           proto::ReplicationStatusExt::INVALID_PARAM);
    }
}

void WorkerProcessor::enqueueForFind(std::string const& id,
                                     proto::ReplicationRequestFind const& request,
                                     proto::ReplicationResponseFind& response) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForFind"
        << "  id: "    << id
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk()
        << "  compute_cs: " << (request.compute_cs() ? "true" : "false"));

    LOCK_GUARD;

    auto const ptr =
        _requestFactory.createFindRequest(
            _worker,
            id,
            request.priority(),
            request.database(),
            request.chunk(),
            request.compute_cs());

    _newRequests.push(ptr);

    response.set_status(               proto::ReplicationStatus::QUEUED);
    response.set_status_ext(           proto::ReplicationStatusExt::NONE);
    response.set_allocated_performance(ptr->performance().info());

    setInfo(ptr, response);
}

void WorkerProcessor::enqueueForFindAll(std::string const&                      id,
                                        proto::ReplicationRequestFindAll const& request,
                                        proto::ReplicationResponseFindAll&      response) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForFindAll"
        << "  id: " << id
        << "  db: " << request.database());

    LOCK_GUARD;

    // TODO: run the sanity check to ensure no such request is found in any
    //       of the queue. Return 'DUPLICATE' error status if teh one is found.

    auto const ptr =
        _requestFactory.createFindAllRequest(
            _worker,
            id,
            request.priority(),
            request.database());

    _newRequests.push(ptr);

    response.set_status(               proto::ReplicationStatus::QUEUED);
    response.set_status_ext(           proto::ReplicationStatusExt::NONE);
    response.set_allocated_performance(ptr->performance().info());

    setInfo(ptr, response);
}

WorkerRequest::Ptr WorkerProcessor::dequeueOrCancelImpl(std::string const& id) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "dequeueOrCancelImpl" << "  id: " << id);

    LOCK_GUARD;

    // Still waiting in the queue?

    for (auto&& ptr: _newRequests) {
        if (ptr->id() == id) {

            // Cancel it and move it into the final queue in case if a client
            // won't be able to receive the desired status of the request due to
            // a protocol failure, etc.

            ptr->cancel();

            switch (ptr->status()) {

                case WorkerRequest::STATUS_CANCELLED:

                    _newRequests.remove(id);
                    _finishedRequests.push_back(ptr);

                    return ptr;

                default:
                    throw std::logic_error(
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::dequeueOrCancelImpl among new requests");
            }
        }
    }

    // Is it already being processed?

    for (auto&& ptr: _inProgressRequests) {
        if (ptr->id() == id) {

            // Tell the request to begin the cancelling protocol. The protocol
            // will take care of moving the request into the final queue when
            // the cancellation will finish.
            //
            // At the ment time we just notify the client about the cancelattion status
            // of the request and let it come back later to check the updated status.

            ptr->cancel();

            switch (ptr->status()) {

                // These are the most typical states for request in this queue

                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_IS_CANCELLING:

                // The following two states are also allowed here because
                // in-progress requests are still alowed to progress to the completed
                // states before reporting their new state via method:
                //    WorkerProcessor::processingFinished()
                // Sometimes, the request just can't finish this in time due to
                // LOCK_GUARD held by the current method. We shouldn't worry
                // about this situation here. The request will be moved into the next
                // queue as soon as LOCK_GUAR will be released.

                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw std::logic_error(
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::dequeueOrCancelImpl among in-progress requests");
            }
        }
    }

    // Has it finished?

    for (auto&& ptr: _finishedRequests) {
        if (ptr->id() == id) {

            // There is nothing else we can do here other than just
            // reporting the completion status of the request. It's up to a client
            // to figure out what to do about this situation.

            switch (ptr->status()) {

                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw std::logic_error(
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::dequeueOrCancelImpl.");
            }
        }
    }

    // No request found!
    return WorkerRequest::Ptr();
}

WorkerRequest::Ptr WorkerProcessor::checkStatusImpl(std::string const& id) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "checkStatusImpl" << "  id: " << id);

    LOCK_GUARD;

    // Still waiting in the queue?

    for (auto&& ptr: _newRequests) {
        if (ptr->id() == id) {
            switch (ptr->status()) {

                // This state requirement is strict for the non-active requsts
                case WorkerRequest::STATUS_NONE:
                    return ptr;

                default:
                    throw std::logic_error(
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::checkStatusImpl among new requests");
            }
        }
    }

    // Is it already being processed?

    for (auto&& ptr: _inProgressRequests) {
        if (ptr->id() == id) {
            switch (ptr->status()) {

                // These are the most typical states for request in this queue

                case WorkerRequest::STATUS_IS_CANCELLING:
                case WorkerRequest::STATUS_IN_PROGRESS:

                // The following three states are also allowed here because
                // in-progress requests are still alowed to progress to the completed
                // states before reporting their new state via method:
                //    WorkerProcessor::processingFinished()
                // Sometimes, the request just can't finish this in time due to
                // LOCK_GUARD held by the current method. We shouldn't worry
                // about this situation here. The request will be moved into the next
                // queue as soon as LOCK_GUAR will be released.

                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw std::logic_error(
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::checkStatusImpl among in-progress requests");
            }
        }
    }


    // Has it finished?

    for (auto&& ptr: _finishedRequests) {
        if (ptr->id() == id) {
            switch (ptr->status()) {

                /* This state requirement is strict for the completed requsts */
                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw std::logic_error(
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::checkStatusImpl among finished requests");
            }
        }
    }

    // No request found!

    return WorkerRequest::Ptr();
}

void WorkerProcessor::setServiceResponse(
                            proto::ReplicationServiceResponse& response,
                            std::string const& id,
                            proto::ReplicationServiceResponse::Status status,
                            bool extendedReport) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setServiceResponse");

    LOCK_GUARD;

    response.set_status(    status);
    response.set_technology(_requestFactory.technology());
    response.set_start_time(_startTime);

    switch (state()) {

        case WorkerProcessor::State::STATE_IS_RUNNING:
            response.set_service_state(proto::ReplicationServiceResponse::RUNNING);
            break;

        case WorkerProcessor::State::STATE_IS_STOPPING:
            response.set_service_state(proto::ReplicationServiceResponse::SUSPEND_IN_PROGRESS);
            break;

        case WorkerProcessor::State::STATE_IS_STOPPED:
            response.set_service_state(proto::ReplicationServiceResponse::SUSPENDED);
            break;
    }
    response.set_num_new_requests(        _newRequests.size());
    response.set_num_in_progress_requests(_inProgressRequests.size());
    response.set_num_finished_requests(   _finishedRequests.size());

    if (extendedReport) {
        for (auto&& request: _newRequests) {
            setServiceResponseInfo(request,
                                   response.add_new_requests());
        }
        for (auto&& request: _inProgressRequests) {
            setServiceResponseInfo(request,
                                   response.add_in_progress_requests());
        }
        for (auto&& request: _finishedRequests) {
            setServiceResponseInfo(request,
                                   response.add_finished_requests());
        }
    }
}

void WorkerProcessor::setServiceResponseInfo(
                            WorkerRequest::Ptr const& request,
                            proto::ReplicationServiceResponseInfo* info) const {

    if (
        auto const ptr = std::dynamic_pointer_cast<WorkerReplicationRequest>(request)) {

        info->set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);
        info->set_id(          ptr->id());
        info->set_priority(    ptr->priority());
        info->set_database(    ptr->database());
        info->set_chunk(       ptr->chunk());
        info->set_worker(      ptr->sourceWorker());

    } else if (
        auto const ptr = std::dynamic_pointer_cast<WorkerDeleteRequest>(request)) {

        info->set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_DELETE);
        info->set_id(          ptr->id());
        info->set_priority(    ptr->priority());
        info->set_database(    ptr->database());
        info->set_chunk(       ptr->chunk());

    } else if (
        auto const ptr = std::dynamic_pointer_cast<WorkerFindRequest>(request)) {

        info->set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND);
        info->set_id(          ptr->id());
        info->set_priority(    ptr->priority());
        info->set_database(    ptr->database());
        info->set_chunk(       ptr->chunk());

    } else if (
        auto const ptr = std::dynamic_pointer_cast<WorkerFindAllRequest>(request)) {

        info->set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);
        info->set_id(          ptr->id());
        info->set_priority(    ptr->priority());
        info->set_database(    ptr->database());

    } else {
        throw std::logic_error(
            "unsupported request type: " + request->type() + " id: " + request->id() +
            " at WorkerProcessor::setServiceResponseInfo");
    }
}

size_t WorkerProcessor::numNewRequests() const {
    LOCK_GUARD;
    return _newRequests.size();
}

size_t WorkerProcessor::numInProgressRequests() const {
    LOCK_GUARD;
    return _inProgressRequests.size();
}

size_t WorkerProcessor::numFinishedRequests() const {
    LOCK_GUARD;
    return _finishedRequests.size();
}

WorkerRequest::Ptr WorkerProcessor::fetchNextForProcessing(
                                    WorkerProcessorThread::Ptr const& processorThread,
                                    unsigned int timeoutMilliseconds) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "fetchNextForProcessing"
        << "  thread: " << processorThread->id()
        << "  timeout: " << timeoutMilliseconds);

    // For generating random intervals within the maximum range of seconds
    // requested by a client.

    util::BlockPost blockPost(0, timeoutMilliseconds);

    unsigned int totalElapsedTime = 0;
    while (totalElapsedTime < timeoutMilliseconds) {

        // IMPORTANT: make sure no wait is happening within the same
        // scope where the thread safe block is defined. Otherwise
        // the queue will be locked for all threads for the duration of
        // the wait.

        {
            LOCK_GUARD;

            if (not _newRequests.empty()) {

                WorkerRequest::Ptr request = _newRequests.top();
                _newRequests.pop();

                request->setStatus(WorkerRequest::STATUS_IN_PROGRESS);
                _inProgressRequests.push_back(request);

                return request;
            }
        }
        totalElapsedTime += blockPost.wait();
    }

    // Return null pointer since noting has been found within the specified
    // timeout.

    return WorkerRequest::Ptr();
}

void
WorkerProcessor::processingRefused(WorkerRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "processingRefused" << "  id: " << request->id());

    LOCK_GUARD;

    // Update request's state before moving it back into
    // the input queue.

    request->setStatus(WorkerRequest::STATUS_NONE);
    _inProgressRequests.remove_if(
        [&request] (WorkerRequest::Ptr const& ptr) {
            return ptr->id() == request->id();
        }
    );
    _newRequests.push(request);
}

void WorkerProcessor::processingFinished(WorkerRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "processingFinished"
        << "  id: " << request->id()
        << "  status: " << WorkerRequest::status2string(request->status()));

    LOCK_GUARD;

    // Then move it forward into the finished queue.

    _inProgressRequests.remove_if(
        [&request] (WorkerRequest::Ptr const& ptr) {
            return ptr->id() == request->id();
        }
    );
    _finishedRequests.push_back(request);
}

void WorkerProcessor::processorThreadStopped(
                            WorkerProcessorThread::Ptr const& processorThread) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "processorThreadStopped" << "  thread: "
         << processorThread->id());

    LOCK_GUARD;

    if (_state == STATE_IS_STOPPING) {

        // Complete state transition if all threds are stopped

        for (auto&& t: _threads) {
            if (t->isRunning()) { return; }
        }
        _state = STATE_IS_STOPPED;
    }
}

void WorkerProcessor::setInfo(WorkerRequest::Ptr const& request,
                              proto::ReplicationResponseReplicate& response) {

    if (not request) return;

    auto ptr = std::dynamic_pointer_cast<WorkerReplicationRequest>(request);
    if (not ptr) {
        throw std::logic_error(
                "incorrect dynamic type of request id: " + request->id() +
                " in WorkerProcessor::setInfo(WorkerReplicationRequest)");
    }

    // Return the performance of the target request

    response.set_allocated_target_performance(ptr->performance().info());

    // Note the ownership transfer of an intermediate protobuf object obtained
    // from  ReplicaInfo object in the call below. The protobuf
    // runtime will take care of deleting the intermediate objects.

    response.set_allocated_replica_info(ptr->replicaInfo().info());

    // Same comment on the ownership transfer applies here

    auto protoRequestPtr = new proto::ReplicationRequestReplicate();

    protoRequestPtr->set_priority(ptr->priority());
    protoRequestPtr->set_database(ptr->database());
    protoRequestPtr->set_chunk(   ptr->chunk());
    protoRequestPtr->set_worker(  ptr->sourceWorker());

    response.set_allocated_request(protoRequestPtr);
}

void WorkerProcessor::setInfo(WorkerRequest::Ptr const& request,
                             proto::ReplicationResponseDelete& response) {

    auto ptr = std::dynamic_pointer_cast<WorkerDeleteRequest>(request);
    if (not ptr) {
        throw std::logic_error(
                "incorrect dynamic type of request id: " + request->id() +
                " in WorkerProcessor::setInfo(WorkerDeleteRequest)");
    }

    // Return the performance of the target request

    response.set_allocated_target_performance(ptr->performance().info());

    // Note the ownership transfer of an intermediate protobuf object obtained
    // from ReplicaInfo object in the call below. The protobuf runtime will take
    // care of deleting the intermediate object.

    response.set_allocated_replica_info(ptr->replicaInfo().info());

    // Same comment on the ownership transfer applies here

    auto protoRequestPtr = new proto::ReplicationRequestDelete();

    protoRequestPtr->set_priority(ptr->priority());
    protoRequestPtr->set_database(ptr->database());
    protoRequestPtr->set_chunk(   ptr->chunk());

    response.set_allocated_request(protoRequestPtr);
}

void WorkerProcessor::setInfo(WorkerRequest::Ptr const&   request,
                              proto::ReplicationResponseFind& response) {

    auto ptr = std::dynamic_pointer_cast<WorkerFindRequest>(request);
    if (not ptr) {
        throw std::logic_error("incorrect dynamic type of request id: " + request->id() +
                               " in WorkerProcessor::setInfo(WorkerFindRequest)");
    }

    // Return the performance of the target request

    response.set_allocated_target_performance(ptr->performance().info());

    // Note the ownership transfer of an intermediate protobuf object obtained
    // from ReplicaInfo object in the call below. The protobuf runtime will take
    // care of deleting the intermediate object.

    response.set_allocated_replica_info(ptr->replicaInfo().info());

    // Same comment on the ownership transfer applies here

    auto protoRequestPtr = new proto::ReplicationRequestFind();

    protoRequestPtr->set_priority(  ptr->priority());
    protoRequestPtr->set_database(  ptr->database());
    protoRequestPtr->set_chunk(     ptr->chunk());
    protoRequestPtr->set_compute_cs(ptr->computeCheckSum());

    response.set_allocated_request(protoRequestPtr);
}

void WorkerProcessor::setInfo(WorkerRequest::Ptr const& request,
                              proto::ReplicationResponseFindAll& response) {

    auto ptr = std::dynamic_pointer_cast<WorkerFindAllRequest>(request);
    if (not ptr) {
        throw std::logic_error("incorrect dynamic type of request id: " + request->id() +
                               " in WorkerProcessor::setInfo(WorkerFindAllRequest)");
    }

    // Return the performance of the target request

    response.set_allocated_target_performance(ptr->performance().info());

    // Note that a new Info object is allocated and appended to
    // the 'replica_info_many' series at each step of the iteration below.
    // The protobuf runtime will take care of deleting those objects.

    for (auto&& replicaInfo: ptr->replicaInfoCollection()) {
        proto::ReplicationReplicaInfo* info = response.add_replica_info_many();
        replicaInfo.setInfo(info);
    }

    // Same comment on the ownership transfer applies here

    auto protoRequestPtr = new proto::ReplicationRequestFindAll();

    protoRequestPtr->set_priority(ptr->priority());
    protoRequestPtr->set_database(ptr->database());

    response.set_allocated_request(protoRequestPtr);
}

}}} // namespace lsst::qserv::replica
