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
#include "replica/WorkerEchoRequest.h"
#include "replica/WorkerFindRequest.h"
#include "replica/WorkerFindAllRequest.h"
#include "replica/WorkerReplicationRequest.h"
#include "replica/WorkerRequestFactory.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerProcessor");

using namespace lsst::qserv::replica;

template <class PROTOCOL_RESPONSE_TYPE,
          class PROTOCOL_REQUEST_TYPE>
bool ifDuplicateRequest(PROTOCOL_RESPONSE_TYPE& response,
                        WorkerRequest::Ptr const& p,
                        PROTOCOL_REQUEST_TYPE const& request) {

    bool isDuplicate = false;

    if (WorkerReplicationRequest::Ptr ptr =
        dynamic_pointer_cast<WorkerReplicationRequest>(p)) {
        isDuplicate =
            (ptr->database() == request.database()) and
            (ptr->chunk()    == request.chunk());

    } else if (WorkerDeleteRequest::Ptr ptr =
             dynamic_pointer_cast<WorkerDeleteRequest>(p)) {
        isDuplicate =
            (ptr->database() == request.database()) and
            (ptr->chunk()    == request.chunk());
    }
    if (isDuplicate) {
        WorkerProcessor::setDefaultResponse(
            response,
            ProtocolStatus::BAD,
            ProtocolStatusExt::DUPLICATE);
        response.set_duplicate_request_id(p->id());
    }
    return isDuplicate;
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

string WorkerProcessor::state2string(State state) {
    switch (state) {
        case STATE_IS_RUNNING:  return "STATE_IS_RUNNING";
        case STATE_IS_STOPPING: return "STATE_IS_STOPPING";
        case STATE_IS_STOPPED:  return "STATE_IS_STOPPED";
    }
    throw logic_error(
            "WorkerProcessor::" + string(__func__) +"  unhandled state " + to_string(state));
}


ProtocolStatus WorkerProcessor::translate(WorkerRequest::CompletionStatus status) {
    switch (status) {
        case WorkerRequest::STATUS_NONE:          return ProtocolStatus::QUEUED;
        case WorkerRequest::STATUS_IN_PROGRESS:   return ProtocolStatus::IN_PROGRESS;
        case WorkerRequest::STATUS_IS_CANCELLING: return ProtocolStatus::IS_CANCELLING;
        case WorkerRequest::STATUS_CANCELLED:     return ProtocolStatus::CANCELLED;
        case WorkerRequest::STATUS_SUCCEEDED:     return ProtocolStatus::SUCCESS;
        case WorkerRequest::STATUS_FAILED:        return ProtocolStatus::FAILED;
        default:
            throw logic_error(
                    "WorkerProcessor::" + string(__func__) +
                    "unhandled status " + WorkerRequest::status2string(status));
    }
}


WorkerProcessor::Ptr WorkerProcessor::create(ServiceProvider::Ptr const& serviceProvider,
                                             WorkerRequestFactory const& requestFactory,
                                             string const& worker) {
    return Ptr(new WorkerProcessor(serviceProvider,
                                   requestFactory,
                                   worker));
}


WorkerProcessor::WorkerProcessor(ServiceProvider::Ptr const& serviceProvider,
                                 WorkerRequestFactory const& requestFactory,
                                 string const& worker)
    :   _serviceProvider(serviceProvider),
        _requestFactory(requestFactory),
        _worker(worker),
        _state(STATE_IS_STOPPED),
        _startTime(PerformanceUtils::now()) {
}


void WorkerProcessor::run() {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__);

    util::Lock lock(_mtx, _context() + __func__);

    if (_state == STATE_IS_STOPPED) {

        size_t const numThreads = _serviceProvider->config()->workerNumProcessingThreads();
        if (not numThreads) {
            throw out_of_range(
                    "WorkerProcessor::" + string(__func__) +
                    "invalid configuration parameter for the number of processing threads. "
                    "The value of the parameter must be greater than 0");
        }

        // Create threads if needed
        if (_threads.empty()) {
            auto const self = shared_from_this();
            for (size_t i=0; i < numThreads; ++i) {
                _threads.push_back(WorkerProcessorThread::create(self));
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

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__);

    util::Lock lock(_mtx, _context() + __func__);

    if (_state == STATE_IS_RUNNING) {

        // Tell each thread to stop.

        for (auto&& t: _threads) {
            t->stop();
        }

        // Begin transitioning to the final state via this intermediate one.
        // The transition will finish asynchronous when all threads will report
        // desired changes in their states.

        _state = STATE_IS_STOPPING;
    }
}


void WorkerProcessor::drain() {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__);

    util::Lock lock(_mtx, _context() + __func__);

    // Collect identifiers of requests to be affected by the operation
    list<string> ids;

    for (auto&& ptr: _newRequests)        ids.push_back(ptr->id());
    for (auto&& ptr: _inProgressRequests) ids.push_back(ptr->id());

    for (auto&& id: ids) _dequeueOrCancelImpl(lock, id);
}


void WorkerProcessor::enqueueForReplication(
                            string const& id,
                            ProtocolRequestReplicate const& request,
                            ProtocolResponseReplicate& response) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
        << "  id: "     << id
        << "  db: "     << request.database()
        << "  chunk: "  << request.chunk()
        << "  worker: " << request.worker());

    util::Lock lock(_mtx, _context() + __func__);

    // Verify a scope of the request to ensure it won't duplicate or interfere (with)
    // existing requests in the active (non-completed) queues. A reason why we're ignoring
    // the completed is that this replica may have already been deleted from this worker.

    for (auto&& ptr: _newRequests) {
        if (::ifDuplicateRequest(response, ptr, request)) return;
    }
    for (auto&& ptr: _inProgressRequests) {
        if (::ifDuplicateRequest(response, ptr,request)) return;
    }

    // The code below may catch exceptions if other parameters of the requites
    // won't pass further validation against the present configuration of the request
    // processing service.
    try {
        auto const ptr = _requestFactory.createReplicationRequest(
            _worker,
            id,
            request.priority(),
            request.database(),
            request.chunk(),
            request.worker()
        );
        _newRequests.push(ptr);

        response.set_status(ProtocolStatus::QUEUED);
        response.set_status_ext(ProtocolStatusExt::NONE);
        response.set_allocated_performance(ptr->performance().info());

        _setInfo(ptr, response);

    } catch (invalid_argument const& ec) {
        LOGS(_log, LOG_LVL_ERROR, _context() << __func__ << "  " << ec.what());

        setDefaultResponse(response,
                           ProtocolStatus::BAD,
                           ProtocolStatusExt::INVALID_PARAM);
    }
}


void WorkerProcessor::enqueueForDeletion(string const& id,
                                         ProtocolRequestDelete const& request,
                                         ProtocolResponseDelete& response) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
        << "  id: "    << id
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk());

    util::Lock lock(_mtx, _context() + __func__);

    // Verify a scope of the request to ensure it won't duplicate or interfere (with)
    // existing requests in the active (non-completed) queues. A reason why we're ignoring
    // the completed is that this replica may have already been deleted from this worker.

    for (auto&& ptr : _newRequests) {
        if (::ifDuplicateRequest(response, ptr, request)) return;
    }
    for (auto&& ptr : _inProgressRequests) {
        if (::ifDuplicateRequest(response, ptr, request)) return;
    }

    // The code below may catch exceptions if other parameters of the requites
    // won't pass further validation against the present configuration of the request
    // processing service.
    try {
        auto const ptr = _requestFactory.createDeleteRequest(
            _worker,
            id,
            request.priority(),
            request.database(),
            request.chunk()
        );
        _newRequests.push(ptr);

        response.set_status(ProtocolStatus::QUEUED);
        response.set_status_ext(ProtocolStatusExt::NONE);
        response.set_allocated_performance(ptr->performance().info());

        _setInfo(ptr, response);

    } catch (invalid_argument const& ec) {
        LOGS(_log, LOG_LVL_ERROR, _context() << __func__ << "  " << ec.what());

        setDefaultResponse(response,
                           ProtocolStatus::BAD,
                           ProtocolStatusExt::INVALID_PARAM);
    }
}


void WorkerProcessor::enqueueForFind(string const& id,
                                     ProtocolRequestFind const& request,
                                     ProtocolResponseFind& response) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
        << "  id: "    << id
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk()
        << "  compute_cs: " << (request.compute_cs() ? "true" : "false"));

    util::Lock lock(_mtx, _context() + __func__);

    // The code below may catch exceptions if other parameters of the requites
    // won't pass further validation against the present configuration of the request
    // processing service.
    try {
        auto const ptr = _requestFactory.createFindRequest(
            _worker,
            id,
            request.priority(),
            request.database(),
            request.chunk(),
            request.compute_cs()
        );
        _newRequests.push(ptr);
    
        response.set_status(ProtocolStatus::QUEUED);
        response.set_status_ext(ProtocolStatusExt::NONE);
        response.set_allocated_performance(ptr->performance().info());
    
        _setInfo(ptr, response);

    } catch (invalid_argument const& ec) {
        LOGS(_log, LOG_LVL_ERROR, _context() << __func__ << "  " << ec.what());

        setDefaultResponse(response,
                           ProtocolStatus::BAD,
                           ProtocolStatusExt::INVALID_PARAM);
    }
}


void WorkerProcessor::enqueueForFindAll(string const& id,
                                        ProtocolRequestFindAll const& request,
                                        ProtocolResponseFindAll& response) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
        << "  id: " << id
        << "  db: " << request.database());

    util::Lock lock(_mtx, _context() + __func__);

    // The code below may catch exceptions if other parameters of the requites
    // won't pass further validation against the present configuration of the request
    // processing service.
    try {
        auto const ptr = _requestFactory.createFindAllRequest(
            _worker,
            id,
            request.priority(),
            request.database()
        );
        _newRequests.push(ptr);
    
        response.set_status(ProtocolStatus::QUEUED);
        response.set_status_ext(ProtocolStatusExt::NONE);
        response.set_allocated_performance(ptr->performance().info());
    
        _setInfo(ptr, response);

    } catch (invalid_argument const& ec) {
        LOGS(_log, LOG_LVL_ERROR, _context() << __func__ << "  " << ec.what());

        setDefaultResponse(response,
                           ProtocolStatus::BAD,
                           ProtocolStatusExt::INVALID_PARAM);
    }
}


void WorkerProcessor::enqueueForEcho(string const& id,
                                     ProtocolRequestEcho const& request,
                                     ProtocolResponseEcho& response) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
        << "  id: " << id
        << "  data.size: " << request.data().size()
        << "  delay: " << request.delay());

    util::Lock lock(_mtx, _context() + __func__);

    // Instant response if no delay was requested

    if (0 == request.delay()) {

        WorkerPerformance performance;
        performance.setUpdateStart();
        performance.setUpdateFinish();

        response.set_status(ProtocolStatus::SUCCESS);
        response.set_status_ext(ProtocolStatusExt::NONE);
        response.set_allocated_performance(performance.info());
        response.set_data(request.data());

        return;
    }

    // The code below may catch exceptions if other parameters of the requites
    // won't pass further validation against the present configuration of the request
    // processing service.

    try {
        auto const ptr = _requestFactory.createEchoRequest(
            _worker,
            id,
            request.priority(),
            request.data(),
            request.delay()
        );
        _newRequests.push(ptr);
    
        response.set_status(ProtocolStatus::QUEUED);
        response.set_status_ext(ProtocolStatusExt::NONE);
        response.set_allocated_performance(ptr->performance().info());
    
        _setInfo(ptr, response);

    } catch (invalid_argument const& ec) {
        LOGS(_log, LOG_LVL_ERROR, _context() << __func__ << "  " << ec.what());

        setDefaultResponse(response,
                           ProtocolStatus::BAD,
                           ProtocolStatusExt::INVALID_PARAM);
    }
}


WorkerRequest::Ptr WorkerProcessor::_dequeueOrCancelImpl(util::Lock const& lock,
                                                         string const& id) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  id: " << id);

    // Still waiting in the queue?
    //
    // ATTENTION: the loop variable is a copy of (not a reference to) a shared
    // pointer to allow removing (if needed) the corresponding entry from the
    // input collection while retaining a valid copy of the pointer to be placed
    // into the next stage  collection.

    for (auto ptr: _newRequests) {
        if (ptr->id() == id) {

            // Cancel it and move it into the final queue in case if a client
            // won't be able to receive the desired status of the request due to
            // a protocol failure, etc.

            ptr->cancel();

            switch (ptr->status()) {

                case WorkerRequest::STATUS_CANCELLED: {

                    _newRequests.remove(id);
                    _finishedRequests.push_back(ptr);

                    return ptr;
                }
                default:
                    throw logic_error(
                            "WorkerProcessor::" + string(__func__) + "  unexpected request status " +
                            WorkerRequest::status2string(ptr->status()) + " in new requests");
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
            // At the meant time we just notify the client about the cancellation status
            // of the request and let it come back later to check the updated status.

            ptr->cancel();

            switch (ptr->status()) {

                // These are the most typical states for request in this queue

                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_IS_CANCELLING:

                // The following two states are also allowed here because
                // in-progress requests are still allowed to progress to the completed
                // states before reporting their new state via method:
                //    WorkerProcessor::_processingFinished()
                // Sometimes, the request just can't finish this in time due to
                // util::Lock lock(_mtx) held by the current method. We shouldn't worry
                // about this situation here. The request will be moved into the next
                // queue as soon as util::Lock lock(_mtx) will be released.

                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw logic_error(
                            "WorkerProcessor::" + string(__func__) + "  unexpected request status " +
                            WorkerRequest::status2string(ptr->status()) + " in in-progress requests");
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
                    throw logic_error(
                            "WorkerProcessor::" + string(__func__) + "  unexpected request status " +
                            WorkerRequest::status2string(ptr->status()) + " in finished requests");
            }
        }
    }

    // No request found!
    return WorkerRequest::Ptr();
}


WorkerRequest::Ptr WorkerProcessor::_checkStatusImpl(util::Lock const& lock,
                                                     string const& id) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  id: " << id);

    // Still waiting in the queue?

    for (auto&& ptr: _newRequests) {
        if (ptr->id() == id) {
            switch (ptr->status()) {

                // This state requirement is strict for the non-active requests
                case WorkerRequest::STATUS_NONE:
                    return ptr;

                default:
                    throw logic_error(
                            "WorkerProcessor::" + string(__func__) + "  unexpected request status " +
                            WorkerRequest::status2string(ptr->status()) + " in new requests");
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
                // in-progress requests are still allowed to progress to the completed
                // states before reporting their new state via method:
                //    WorkerProcessor::_processingFinished()
                // Sometimes, the request just can't finish this in time due to
                // util::Lock lock(_mtx) held by the current method. We shouldn't worry
                // about this situation here. The request will be moved into the next
                // queue as soon as util::Lock lock(_mtx) will be released.

                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw logic_error(
                            "WorkerProcessor::" + string(__func__) + "  unexpected request status " +
                            WorkerRequest::status2string(ptr->status()) + " in in-progress requests");
            }
        }
    }

    // Has it finished?

    for (auto&& ptr: _finishedRequests) {
        if (ptr->id() == id) {
            switch (ptr->status()) {

                /* This state requirement is strict for the completed requests */
                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw logic_error(
                            "WorkerProcessor::" + string(__func__) + "  unexpected request status " +
                            WorkerRequest::status2string(ptr->status()) + " in finished requests");
            }
        }
    }

    // No request found!

    return WorkerRequest::Ptr();
}


void WorkerProcessor::setServiceResponse(
                            ProtocolServiceResponse& response,
                            string const& id,
                            ProtocolServiceResponse::Status status,
                            bool extendedReport) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__);

    util::Lock lock(_mtx, _context() + __func__);

    response.set_status(    status);
    response.set_technology(_requestFactory.technology());
    response.set_start_time(_startTime);

    switch (state()) {

        case WorkerProcessor::State::STATE_IS_RUNNING:
            response.set_service_state(ProtocolServiceResponse::RUNNING);
            break;

        case WorkerProcessor::State::STATE_IS_STOPPING:
            response.set_service_state(ProtocolServiceResponse::SUSPEND_IN_PROGRESS);
            break;

        case WorkerProcessor::State::STATE_IS_STOPPED:
            response.set_service_state(ProtocolServiceResponse::SUSPENDED);
            break;
    }
    response.set_num_new_requests(        _newRequests.size());
    response.set_num_in_progress_requests(_inProgressRequests.size());
    response.set_num_finished_requests(   _finishedRequests.size());

    if (extendedReport) {
        for (auto&& request: _newRequests) {
            _setServiceResponseInfo(request,
                                    response.add_new_requests());
        }
        for (auto&& request: _inProgressRequests) {
            _setServiceResponseInfo(request,
                                    response.add_in_progress_requests());
        }
        for (auto&& request: _finishedRequests) {
            _setServiceResponseInfo(request,
                                    response.add_finished_requests());
        }
    }
}


void WorkerProcessor::_setServiceResponseInfo(
        WorkerRequest::Ptr const& request,
        ProtocolServiceResponseInfo* info) const {

    if (nullptr != dynamic_pointer_cast<WorkerReplicationRequest>(request)) {
        info->set_queued_type(ProtocolQueuedRequestType::REPLICA_CREATE);
    } else if (nullptr != dynamic_pointer_cast<WorkerDeleteRequest>(request)) {
        info->set_queued_type(ProtocolQueuedRequestType::REPLICA_DELETE);
    } else if (nullptr != dynamic_pointer_cast<WorkerFindRequest>(request)) {
        info->set_queued_type(ProtocolQueuedRequestType::REPLICA_FIND);
    } else if (nullptr != dynamic_pointer_cast<WorkerFindAllRequest>(request)) {
        info->set_queued_type(ProtocolQueuedRequestType::REPLICA_FIND_ALL);
    } else if (nullptr != dynamic_pointer_cast<WorkerEchoRequest>(request)) {
        info->set_queued_type(ProtocolQueuedRequestType::TEST_ECHO);
    } else {
        throw logic_error(
                "WorkerProcessor::" + string(__func__) +
                "  unsupported request type: " + request->type() + " id: " + request->id());
    }
    info->set_id(request->id());
    info->set_priority(request->priority());
}


size_t WorkerProcessor::numNewRequests() const {
    util::Lock lock(_mtx, _context() + __func__);
    return _newRequests.size();
}


size_t WorkerProcessor::numInProgressRequests() const {
    util::Lock lock(_mtx, _context() + __func__);
    return _inProgressRequests.size();
}


size_t WorkerProcessor::numFinishedRequests() const {
    util::Lock lock(_mtx, _context() + __func__);
    return _finishedRequests.size();
}


WorkerRequest::Ptr WorkerProcessor::_fetchNextForProcessing(
        WorkerProcessorThread::Ptr const& processorThread,
        unsigned int timeoutMilliseconds) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
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
            util::Lock lock(_mtx, _context() + __func__);

            if (not _newRequests.empty()) {

                WorkerRequest::Ptr request = _newRequests.top();
                _newRequests.pop();

                request->start();
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


void WorkerProcessor::_processingRefused(WorkerRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  id: " << request->id());

    util::Lock lock(_mtx, _context() + __func__);

    // Update request's state before moving it back into
    // the input queue.

    request->stop();

    _inProgressRequests.remove_if(
        [&request] (WorkerRequest::Ptr const& ptr) {
            return ptr->id() == request->id();
        }
    );
    _newRequests.push(request);
}


void WorkerProcessor::_processingFinished(WorkerRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__
        << "  id: " << request->id()
        << "  status: " << WorkerRequest::status2string(request->status()));

    util::Lock lock(_mtx, _context() + __func__);

    // Then move it forward into the finished queue.

    _inProgressRequests.remove_if(
        [&request] (WorkerRequest::Ptr const& ptr) {
            return ptr->id() == request->id();
        }
    );
    _finishedRequests.push_back(request);
}


void WorkerProcessor::_processorThreadStopped(WorkerProcessorThread::Ptr const& processorThread) {

    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  thread: "
         << processorThread->id());

    util::Lock lock(_mtx, _context() + __func__);

    if (_state == STATE_IS_STOPPING) {

        // Complete state transition if all threads are stopped

        for (auto&& t: _threads) {
            if (t->isRunning()) return;
        }
        _state = STATE_IS_STOPPED;
    }
}


void WorkerProcessor::_setInfo(WorkerRequest::Ptr const& request,
                               ProtocolResponseReplicate& response) {

    if (nullptr == request) return;

    auto ptr = dynamic_pointer_cast<WorkerReplicationRequest>(request);
    if (not ptr) {
        throw logic_error(
                "WorkerProcessor::" + string(__func__) + "(WorkerReplicationRequest)"
                "  incorrect dynamic type of request id: " + request->id());
    }
    ptr->setInfo(response);
}


void WorkerProcessor::_setInfo(WorkerRequest::Ptr const& request,
                               ProtocolResponseDelete& response) {

    auto ptr = dynamic_pointer_cast<WorkerDeleteRequest>(request);
    if (not ptr) {
        throw logic_error(
                "WorkerProcessor::" + string(__func__) + "(WorkerDeleteRequest)"
                "  incorrect dynamic type of request id: " + request->id());
    }
    ptr->setInfo(response);
}


void WorkerProcessor::_setInfo(WorkerRequest::Ptr const&   request,
                               ProtocolResponseFind& response) {

    auto ptr = dynamic_pointer_cast<WorkerFindRequest>(request);
    if (not ptr) {
        throw logic_error(
                "WorkerProcessor::" + string(__func__) + "(WorkerFindRequest)"
                "  incorrect dynamic type of request id: " + request->id());
    }
    ptr->setInfo(response);
}


void WorkerProcessor::_setInfo(WorkerRequest::Ptr const& request,
                               ProtocolResponseFindAll& response) {

    auto ptr = dynamic_pointer_cast<WorkerFindAllRequest>(request);
    if (not ptr) {
        throw logic_error(
                "WorkerProcessor::" + string(__func__) + "(WorkerFindAllRequest)"
                "  incorrect dynamic type of request id: " + request->id());
    }
    ptr->setInfo(response);
}


void WorkerProcessor::_setInfo(WorkerRequest::Ptr const& request,
                               ProtocolResponseEcho& response) {

    auto ptr = dynamic_pointer_cast<WorkerEchoRequest>(request);
    if (not ptr) {
        throw logic_error(
                "WorkerProcessor::" + string(__func__) + "(WorkerEchoRequest)"
                "  incorrect dynamic type of request id: " + request->id());
    }
    ptr->setInfo(response);
}

}}} // namespace lsst::qserv::replica
