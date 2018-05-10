/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/QservMgtRequest.h"

// System headers
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/Common.h"
#include "replica/DatabaseServices.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK(MUTEX) std::lock_guard<std::mutex> lock(MUTEX)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

std::string QservMgtRequest::state2string(State state) {
    switch (state) {
        case State::CREATED:     return "CREATED";
        case State::IN_PROGRESS: return "IN_PROGRESS";
        case State::FINISHED:    return "FINISHED";
    }
    throw std::logic_error(
                    "incomplete implementation of method QservMgtRequest::state2string(State)");
}

std::string QservMgtRequest::state2string(ExtendedState state) {
    switch (state) {
        case ExtendedState::NONE:                 return "NONE";
        case ExtendedState::SUCCESS:              return "SUCCESS";
        case ExtendedState::CLIENT_ERROR:         return "CLIENT_ERROR";
        case ExtendedState::SERVER_BAD:           return "SERVER_BAD";
        case ExtendedState::SERVER_IN_USE:        return "SERVER_IN_USE";
        case ExtendedState::SERVER_ERROR:         return "SERVER_ERROR";
        case ExtendedState::EXPIRED:              return "EXPIRED";
        case ExtendedState::CANCELLED:            return "CANCELLED";
    }
    throw std::logic_error(
                    "incomplete implementation of method QservMgtRequest::state2string(ExtendedState)");
}

QservMgtRequest::QservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                                 boost::asio::io_service& io_service,
                                 std::string const& type,
                                 std::string const& worker)
    :   _serviceProvider(serviceProvider),
        _type(type),
        _id(Generators::uniqueId()),
        _worker(worker),
        _state(State::CREATED),
        _extendedState(ExtendedState::NONE),
        _serverError() ,
        _performance(),
        _jobId(""),
        _service(nullptr),
        _requestExpirationIvalSec(_serviceProvider->config()->xrootdTimeoutSec()),
        _requestExpirationTimer(io_service) {

        _serviceProvider->assertWorkerIsValid(_worker);
}

std::string const& QservMgtRequest::serverError() const {
    static std::string const context = "QservMgtRequest::serverError()  ";
    if (_state != State::FINISHED) {
        throw std::logic_error(
                        context + "not allowed to call this method while the request hasn't finished");
    }
    return _serverError;
}

void QservMgtRequest::start(XrdSsiService* service,
                            std::string const& jobId,
                            unsigned int requestExpirationIvalSec) {
    LOCK(_mtx);

    assertState(State::CREATED, "QservMgtRequest::start");

    // Change the expiration ival if requested
    if (requestExpirationIvalSec) {
        _requestExpirationIvalSec = requestExpirationIvalSec;
    }
    LOGS(_log, LOG_LVL_DEBUG, context() << "start  _requestExpirationIvalSec: "
         << _requestExpirationIvalSec);

    // Build optional associaitons with the corresponding service and the job
    //
    // NOTE: this is done only once, the first time a non-trivial value
    // of each parameter is presented to the method.

    if (not _service and service) _service = service;
    if (not _service) {
        throw std::invalid_argument("QservMgtRequest::start  null pointer for XrdSsiService");
    }
    if (_jobId.empty() and not jobId.empty()) _jobId = jobId;

    _performance.setUpdateStart();

    if (_requestExpirationIvalSec) {
        _requestExpirationTimer.cancel();
        _requestExpirationTimer.expires_from_now(boost::posix_time::seconds(_requestExpirationIvalSec));
        _requestExpirationTimer.async_wait(
            boost::bind(
                &QservMgtRequest::expired,
                shared_from_this(),
                boost::asio::placeholders::error
            )
        );
    }

    // Let a subclass to proceed with its own sequence of actions
    startImpl();

    _serviceProvider->databaseServices()->saveState(*this);
}

std::string const& QservMgtRequest::jobId() const {
    if (_state == State::CREATED) {
        throw std::logic_error(
            "the Job Id is not available because the request has not started yet");
    }
    return _jobId;
}

void QservMgtRequest::expired(boost::system::error_code const& ec) {

    LOCK(_mtx);

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;

    // Also ignore this event if the request is over
    if (_state == State::FINISHED) return;

    // Pringt this only after those rejections made above
    LOGS(_log, LOG_LVL_DEBUG, context() << "expired");

    finish(ExtendedState::EXPIRED);
}

void QservMgtRequest::cancel() {

    LOCK(_mtx);

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel");

    finish(ExtendedState::CANCELLED);
}

void QservMgtRequest::finish(ExtendedState extendedState,
                             std::string const& serverError) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finish");

    // Check if it's not too late for this operation
    if (_state == State::FINISHED) return;

    // Set the optional server error state as well
    //
    // IMPORTANT: this needs to be done before performing the state
    // transition to insure clients will get a consistent view onto
    // the object state.
    _serverError = serverError;

    // Set new state to make sure all event handlers will recognize
    // this scenario and avoid making any modifications to the request's state.
    setState(State::FINISHED, extendedState);

    // Close all operations on BOOST ASIO if needed
    _requestExpirationTimer.cancel();

    // Let a subclass to run its own finalization if needed
    finishImpl();

    // We have to update the timestamp before invoking a user provided
    // callback on the completion of the operation.
    _performance.setUpdateFinish();

    _serviceProvider->databaseServices()->saveState(*this);

    // This will invoke user-defined notifiers (if any)
    notify();
}

void QservMgtRequest::assertState(State state,
                                  std::string const& context) const {
    if (state != _state) {
        throw std::logic_error(
            context + ": wrong state " + state2string(state) + " instead of " + state2string(_state));
    }
}

void QservMgtRequest::setState(State state,
                               ExtendedState extendedState)
{
    LOGS(_log, LOG_LVL_DEBUG, context() << "setState  " << state2string(state, extendedState));

    // IMPORTANT: the top-level state is the last to be set when performing
    // the state transition to insure clients will get a consistent view onto
    // the object state.

    _extendedState = extendedState;
    _state = state;

    _serviceProvider->databaseServices()->saveState(*this);
}

}}} // namespace lsst::qserv::replica
