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
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/LockUtils.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

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

std::string Request::state2string(State state,
                                  ExtendedState extendedState) {
    return state2string(state) + "::" + state2string(extendedState);
}

std::string Request::state2string(State state,
                                  ExtendedState extendedState,
                                  replica::ExtendedCompletionStatus serverStatus) {
    return state2string(state, extendedState) + "::" + replica::status2string(serverStatus);
}

Request::Request(ServiceProvider::Ptr const& serviceProvider,
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
        _bufferPtr(new ProtocolBuffer(serviceProvider->config()->requestBufferSizeBytes())),
        _workerInfo(serviceProvider->config()->workerInfo(worker)),
        _timerIvalSec(serviceProvider->config()->retryTimeoutSec()),
        _timer(io_service),
        _requestExpirationIvalSec(serviceProvider->config()->controllerRequestTimeoutSec()),
        _requestExpirationTimer(io_service) {

        _serviceProvider->assertWorkerIsValid(worker);
}
std::string Request::context() const {
    return "REQUEST " + id() + "  " + type() +
           "  " + state2string(state(), extendedState()) +
           "::" + replica::status2string(extendedServerStatus()) + "  ";
}

std::string const& Request::remoteId() const {
    return _duplicateRequestId.empty() ? _id : _duplicateRequestId;
}

void Request::start(std::shared_ptr<Controller> const& controller,
                    std::string const& jobId,
                    unsigned int requestExpirationIvalSec) {

    LOCK(_mtx, context() + "start");

    assertState(CREATED, context() + "start");

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

    if (not _controller    and     controller)    _controller = controller;
    if (    _jobId.empty() and not jobId.empty()) _jobId      = jobId;

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

    savePersistentState();
}

std::string const& Request::jobId() const {
    if (_state == State::CREATED) {
        throw std::logic_error(
            "the Job Id is not available because the request has not started yet");
    }
    return _jobId;
}

void Request::expired(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "expired"
         << (ec == boost::asio::error::operation_aborted ? "  ** ABORTED **" : ""));

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "expired");

    if (_state == State::FINISHED) return;

    finish(EXPIRED);

    // Invoke a subclass-specific notification
    notify();
}

void Request::cancel() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel");

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "cancel");

    if (_state == FINISHED) return;

    finish(CANCELLED);

    // Invoke a subclass-specific notification
    notify();
}

void Request::finish(ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finish");

    ASSERT_LOCK(_mtx, context() + "finish");

    // Check if it's not too late for this operation
    if (_state == FINISHED) return;

    // We have to update the timestamp before making a state transition
    // to ensure a client gets a consistent view onto the object's state.
    _performance.setUpdateFinish();

    // Set new state to make sure all event handlers will recognize
    // this scenario and avoid making any modifications to the request's state.
    setState(FINISHED, extendedState);

    // Stop the timer if the one is still running
    _requestExpirationTimer.cancel();

    // Let a subclass to run its own finalization if needed
    finishImpl();

    savePersistentState();
}


bool Request::isAborted(boost::system::error_code const& ec) const {

    if (ec == boost::asio::error::operation_aborted) {
        LOGS(_log, LOG_LVL_DEBUG, context() << "isAborted  ** ABORTED **");
        return true;
    }
    return false;
}

void Request::assertState(State state,
                          std::string const& context) const {

    if (state != _state) {
        throw std::logic_error(
            context + ": wrong state " + state2string(state) + " instead of " + state2string(_state));
    }
}

void Request::setState(State state,
                       ExtendedState extendedState)
{
    LOGS(_log, LOG_LVL_DEBUG, context() << "setState  " << state2string(state, extendedState));

    ASSERT_LOCK(_mtx, context() + "setState");

    // ATTENTION: ensure the top-level state is the last to change in
    // in the transient state transition in order to guarantee a consistent
    // view on to the object's state from the clients' prospective.

    _extendedState = extendedState;
    _state = state;

    savePersistentState();
}

}}} // namespace lsst::qserv::replica
