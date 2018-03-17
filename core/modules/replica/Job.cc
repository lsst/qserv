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
#include "replica/Job.h"

// System headers
#include <stdexcept>
#include <utility>      // std::swap

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/Common.h"            // Generators::uniqueId()
#include "replica/DatabaseServices.h"
#include "replica/Performance.h"       // PerformanceUtils::now()
#include "replica/QservMgtServices.h"
#include "replica/RemoveReplicaQservMgtRequest.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Job");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

std::string Job::state2string(State state) {
    switch (state) {
        case CREATED:     return "CREATED";
        case IN_PROGRESS: return "IN_PROGRESS";
        case FINISHED:    return "FINISHED";
    }
    throw std::logic_error(
                "incomplete implementation of method Job::state2string(State)");
}

std::string
Job::state2string(ExtendedState state) {
    switch (state) {
        case NONE:         return "NONE";
        case SUCCESS:      return "SUCCESS";
        case FAILED:       return "FAILED";
        case QSERV_FAILED: return "QSERV_FAILED";
        case QSERV_IN_USE: return "QSERV_IN_USE";
        case EXPIRED:      return "EXPIRED";
        case CANCELLED:    return "CANCELLED";
    }
    throw std::logic_error(
                "incomplete implementation of method Job::state2string(ExtendedState)");
}

Job::Job(Controller::pointer const& controller,
         std::string const& parentJobId,
         std::string const& type,
         Options const& options)
    :   _id(Generators::uniqueId()),
        _controller(controller),
        _parentJobId(parentJobId),
        _type(type),
        _options(options),
        _state(State::CREATED),
        _extendedState(ExtendedState::NONE),
        _beginTime(0),
        _endTime(0),
        _heartbeatTimerIvalSec(_controller->serviceProvider()->config()->jobHeartbeatTimeoutSec()),
        _heartbeatTimer(_controller->io_service()),
        _expirationIvalSec(_controller->serviceProvider()->config()->jobTimeoutSec()),
        _expirationTimer(_controller->io_service()) {
}

Job::Options Job::setOptions(Options const& newOptions) {
    Options options(newOptions);
    std::swap(_options, options);
    return options;
}

std::string Job::context() const {
    return  "JOB [id=" + _id + ", type=" + _type +
            ", state=" + state2string(_state, _extendedState) + "]  ";
}

void Job::start() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "start");

    do {
        LOCK_GUARD;

        assertState(State::CREATED);

        // IMPORTANT: update these before proceeding to the implementation
        // because the later may create children jobs whose performance
        // counters must be newer, and whose saved state within the database
        // may depend on this job's state.
        _beginTime = PerformanceUtils::now();
        _controller->serviceProvider()->databaseServices()->saveState(shared_from_this());

        // Delegate the rest to the specific implementation
        startImpl();

        // Allow the job to be fully accomplished right away
        if (_state == State::FINISHED) { break; }

        // The only other state which is allowed here
        assertState(State::IN_PROGRESS);

        // Start timers if configured
        startHeartbeatTimer();
        startExpirationTimer();

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify(); }
}

void Job::cancel() {
    {
        // Limit the scope of this lock here to allow deadlock-free
        // callbacks to clients.
        LOCK_GUARD;

        finish(ExtendedState::CANCELLED);
    }
    notify();
}

void Job::finish(ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finish");

    // Also ignore this event if the request is over
    if (_state == State::FINISHED) { return; }

    // Stop timers if they're still running
    _heartbeatTimer.cancel();
    _expirationTimer.cancel();

    // Invoke a subclass specific cancellation sequence of actions if anything
    // bad has happen.
    if (extendedState != ExtendedState::SUCCESS) {
        cancelImpl();
    }

    // Set new state to make sure all event handlers will recognize
    // this scenario and avoid making any modifications to the request's state.
    setState(State::FINISHED, extendedState);

    _controller->serviceProvider()->databaseServices()->saveState(shared_from_this());
}

void Job::qservAddReplica(unsigned int chunk,
                          std::string const& databaseFamily,
                          std::string const& worker,
                          AddReplicaQservMgtRequest::callback_type onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "** START ** Qserv notification on ADD replica:"
         << ", chunk="          << chunk
         << ", databaseFamily=" << databaseFamily
         << "  worker="         << worker);

    auto self = shared_from_this();

    _controller->serviceProvider()->qservMgtServices()->addReplica(
        chunk,
        databaseFamily,
        worker,
        [self,onFinish] (AddReplicaQservMgtRequest::pointer const& request) {

            LOGS(_log, LOG_LVL_DEBUG, self->context()
                 << "** FINISH ** Qserv notification on ADD replica:"
                 << "  chunk="          << request->chunk()
                 << ", databaseFamily=" << request->databaseFamily()
                 << ", worker="         << request->worker()
                 << ", state="          << request->state2string(request->state())
                 << ", extendedState="  << request->state2string(request->extendedState())
                 << ". serverError="    << request->serverError());

            // Pass through the result to a caller
            if (onFinish) {
                onFinish(request);
            }
        },
        _id
    );
}

void Job::qservRemoveReplica(unsigned int chunk,
                             std::string const& databaseFamily,
                             std::string const& worker,
                             bool force,
                             RemoveReplicaQservMgtRequest::callback_type onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "** START ** Qserv notification on REMOVE replica:"
         << "  chunk="          << chunk
         << ", databaseFamily=" << databaseFamily
         << ", worker="         << worker
         << ", force="          << (force ? "true" : "false"));

    auto self = shared_from_this();

    _controller->serviceProvider()->qservMgtServices()->removeReplica(
        chunk,
        databaseFamily,
        worker,
        force,
        [self,onFinish] (RemoveReplicaQservMgtRequest::pointer const& request) {

            LOGS(_log, LOG_LVL_DEBUG, self->context()
                 << "** FINISH ** Qserv notification on REMOVE replica:"
                 << "  chunk="          << request->chunk()
                 << ", databaseFamily=" << request->databaseFamily()
                 << ", worker="         << request->worker()
                 << ", force="          << (request->force() ? "true" : "false")
                 << ", state="          << request->state2string(request->state())
                 << ", extendedState="  << request->state2string(request->extendedState())
                 << ". serverError="    << request->serverError());

            // Pass through the result to the caller if requested
            if (onFinish) {
                onFinish(request);
            }
        },
        _id
    );
}

void Job::assertState(State state) const {
    if (state != _state) {
        throw std::logic_error(
            "wrong state " + state2string(state) + " instead of " + state2string(_state));
    }
}

void Job::setState(State state,
                   ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setState  state=" << state2string(state, extendedState));

    _state         = state;
    _extendedState = extendedState;

    if (_state == State::FINISHED) {
        _endTime = PerformanceUtils::now();
    }
    _controller->serviceProvider()->databaseServices()->saveState(shared_from_this());
}

void Job::startHeartbeatTimer() {
    if (_heartbeatTimerIvalSec) {
        _heartbeatTimer.cancel();
        _heartbeatTimer.expires_from_now(boost::posix_time::seconds(_heartbeatTimerIvalSec));
        _heartbeatTimer.async_wait(
            boost::bind(
                &Job::heartbeat,
                shared_from_this(),
                boost::asio::placeholders::error
            )
        );
    }
}

void Job::heartbeat(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "heartbeat");

    LOCK_GUARD;

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) { return; }

    // Also ignore this event if the request is over
    if (_state == State::FINISHED) { return; }

    // Update the job entry in the database
    _controller->serviceProvider()->databaseServices()->updateHeartbeatTime(shared_from_this());

    // Start another interval
    startHeartbeatTimer();
}

void Job::startExpirationTimer() {
    if (_expirationIvalSec) {
        _expirationTimer.cancel();
        _expirationTimer.expires_from_now(boost::posix_time::seconds(_expirationIvalSec));
        _expirationTimer.async_wait(
            boost::bind(
                &Job::expired,
                shared_from_this(),
                boost::asio::placeholders::error
            )
        );
    }
}

void Job::expired(boost::system::error_code const& ec) {
    {
        // Limit the scope of this lock here to allow deadlock-free
        // callbacks to clients.
        LOCK_GUARD;

        // Ignore this event if the timer was aborted
        if (ec == boost::asio::error::operation_aborted) { return; }

        finish(ExtendedState::EXPIRED);
    }
    notify();
}

}}} // namespace lsst::qserv::replica
