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
#include "replica/Request.h"

// System headers
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Controller.h"
#include "replica/DatabaseServices.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Request");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

std::string Request::state2string(State state) {
    switch (state) {
        case CREATED:     return "CREATED";
        case IN_PROGRESS: return "IN_PROGRESS";
        case FINISHED:    return "FINISHED";
    }
    throw std::logic_error(
                    "incomplete implementation of method Request::state2string(State)");
}

std::string Request::state2string(ExtendedState state) {
    switch (state) {
        case NONE:                 return "NONE";
        case SUCCESS:              return "SUCCESS";
        case CLIENT_ERROR:         return "CLIENT_ERROR";
        case SERVER_BAD:           return "SERVER_BAD";
        case SERVER_ERROR:         return "SERVER_ERROR";
        case SERVER_QUEUED:        return "SERVER_QUEUED";
        case SERVER_IN_PROGRESS:   return "SERVER_IN_PROGRESS";
        case SERVER_IS_CANCELLING: return "SERVER_IS_CANCELLING";
        case SERVER_CANCELLED:     return "SERVER_CANCELLED";
        case EXPIRED:              return "EXPIRED";
        case CANCELLED:            return "CANCELLED";
    }
    throw std::logic_error(
                    "incomplete implementation of method Request::state2string(ExtendedState)");
}

Request::Request(ServiceProvider& serviceProvider,
                 boost::asio::io_service& io_service,
                 std::string const& type,
                 std::string const& worker,
                 int  priority,
                 bool keepTracking,
                 bool allowDuplicate)
    :   _serviceProvider(serviceProvider),
        _type(type),
        _id(Generators::uniqueId()),
        _worker(worker),
        _priority(priority),
        _keepTracking(keepTracking),
        _allowDuplicate(allowDuplicate),
        _state(CREATED),
        _extendedState(NONE),
        _extendedServerStatus(ExtendedCompletionStatus::EXT_STATUS_NONE),
        _performance(),
        _bufferPtr(new ProtocolBuffer(serviceProvider.config()->requestBufferSizeBytes())),
        _workerInfo(serviceProvider.config()->workerInfo(worker)),
        _timerIvalSec(serviceProvider.config()->retryTimeoutSec()),
        _timer(io_service),
        _requestExpirationIvalSec(serviceProvider.config()->controllerRequestTimeoutSec()),
        _requestExpirationTimer(io_service) {

        _serviceProvider.assertWorkerIsValid(worker);
}

std::string const& Request::remoteId() const {
    return _duplicateRequestId.empty() ? _id : _duplicateRequestId;
}

void Request::start(std::shared_ptr<Controller> const& controller,
                    std::string const& jobId,
                    unsigned int requestExpirationIvalSec) {

    LOCK_GUARD;

    assertState(CREATED);

    // Change the expiration ival if requested
    if (requestExpirationIvalSec) {
        _requestExpirationIvalSec = requestExpirationIvalSec;
    }
    LOGS(_log, LOG_LVL_DEBUG, context() << "start  _requestExpirationIvalSec: "
         << _requestExpirationIvalSec);

    // Build optional associaitons with the corresponding Controller and the job
    //
    // NOTE: this is done only once, the first time a non-trivial value
    // of each parameter is presented to the method.

    if (not _controller    and     controller)    { _controller = controller; }
    if (    _jobId.empty() and not jobId.empty()) { _jobId      = jobId; }

    _performance.setUpdateStart();

    if (_requestExpirationIvalSec) {
        _requestExpirationTimer.cancel();
        _requestExpirationTimer.expires_from_now(boost::posix_time::seconds(_requestExpirationIvalSec));
        _requestExpirationTimer.async_wait(
            boost::bind(
                &Request::expired,
                shared_from_this(),
                boost::asio::placeholders::error
            )
        );
    }

    // Let a subclass to proceed with its own sequence of actions
    startImpl();

    _controller->serviceProvider().databaseServices()->saveState(shared_from_this());
}

std::string const& Request::jobId() const {
    if (_state == State::CREATED) {
        throw std::logic_error(
            "the Job Id is not available because the request has not started yet");
    }
    return _jobId;
}

void Request::expired(boost::system::error_code const& ec) {

    LOCK_GUARD;

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) { return; }

    // Also ignore this event if the request is over
    if (_state == State::FINISHED) { return; }

    // Pringt this only after those rejections made above
    LOGS(_log, LOG_LVL_DEBUG, context() << "expired");

    finish(EXPIRED);
}

void Request::cancel() {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel");

    finish(CANCELLED);
}

void Request::finish(ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finish");

    // Check if it's not too late for this operation
    if (_state == FINISHED) { return; }

    // Set new state to make sure all event handlers will recognize
    // this scenario and avoid making any modifications to the request's state.
    setState(FINISHED, extendedState);

    // Close all operations on BOOST ASIO if needed
    _requestExpirationTimer.cancel();

    // Let a subclass to run its own finalization if needed
    finishImpl();

    // We have to update the timestamp before invoking a user provided
    // callback on the completion of the operation.
    _performance.setUpdateFinish();

    _controller->serviceProvider().databaseServices()->saveState(shared_from_this());

    // This will invoke user-defined notifiers (if any)
    notify();
}


bool Request::isAborted(boost::system::error_code const& ec) const {

    if (ec == boost::asio::error::operation_aborted) {
        LOGS(_log, LOG_LVL_DEBUG, context() << "isAborted  ** ABORTED **");
        return true;
    }
    return false;
}

void Request::assertState(State state) const {
    if (state != _state) {
        throw std::logic_error(
            "wrong state " + state2string(state) + " instead of " + state2string(_state));
    }
}

void Request::setState(State state,
                       ExtendedState extendedState)
{
    LOGS(_log, LOG_LVL_DEBUG, context() << "setState  " << state2string(state, extendedState));

    _state         = state;
    _extendedState = extendedState;

    _controller->serviceProvider().databaseServices()->saveState(shared_from_this());
}
    
}}} // namespace lsst::qserv::replica