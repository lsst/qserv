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
#include <sstream>
#include <stdexcept>
#include <utility>      // std::swap

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/Common.h"            // Generators::uniqueId()
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/LockUtils.h"
#include "replica/Performance.h"       // PerformanceUtils::now()
#include "replica/QservMgtServices.h"
#include "replica/RemoveReplicaQservMgtRequest.h"
#include "replica/ServiceProvider.h"
#include "util/IterableFormatter.h"


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

Job::Job(Controller::Ptr const& controller,
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
        _heartbeatTimerPtr(nullptr),
        _expirationIvalSec(_controller->serviceProvider()->config()->jobTimeoutSec()),
        _expirationTimerPtr(nullptr) {
}

Job::Options Job::setOptions(Options const& newOptions) {
    Options options(newOptions);
    std::swap(_options, options);
    return options;
}

std::string Job::context() const {
    return  "JOB     " + _id + "  " + _type +
            "  " + state2string(_state, _extendedState) + "  ";
}

void Job::start() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "start");

    LOCK(_mtx, context() + "start");

    assertState(State::CREATED, context() + "start");

    // IMPORTANT: update these before proceeding to the implementation
    // because the later may create children jobs whose performance
    // counters must be newer, and whose saved state within the database
    // may depend on this job's state.
    _beginTime = PerformanceUtils::now();
    _controller->serviceProvider()->databaseServices()->saveState(*this);

    // Start timers if configured
    startHeartbeatTimer();
    startExpirationTimer();

    // Delegate the rest to the specific implementation
    startImpl();

    // Allow the job to be fully accomplished right away
    if (_state == State::FINISHED) {

        // Subclass specific notification
        notify();

        return;
    }

    // Otherwise, the only other state which is allowed here is this
    assertState(State::IN_PROGRESS, context() + "start");
}

void Job::cancel() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel"
         << "  _state="         << state2string(_state)
         << ", _extendedState=" << state2string(_extendedState));

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "cancel");

    if (_state == State::FINISHED) return;

    finish(ExtendedState::CANCELLED);

    // Subclass specific notification
    notify();
}

void Job::finish(ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finish"
         << "  _state="         << state2string(_state)
         << ", _extendedState=" << state2string(_extendedState)
         << ", (new)extendedState=" << state2string(extendedState));

    ASSERT_LOCK(_mtx, context() + "finish");

    // Also ignore this event if the request is over
    if (_state == State::FINISHED) return;

    // *IMPORTANT*: Set new state *BEFORE* calling subclass-specific cancellation
    // protocol to make sure all event handlers will recognize this scenario and
    // avoid making any modifications to the request's state.
    setState(State::FINISHED, extendedState);

    // Invoke a subclass specific cancellation sequence of actions if anything
    // bad has happen.
    if (extendedState != ExtendedState::SUCCESS) {
        cancelImpl();
    }
    _controller->serviceProvider()->databaseServices()->saveState(*this);

    // Stop timers if they're still running
    if(_heartbeatTimerPtr) _heartbeatTimerPtr->cancel();
    if(_expirationTimerPtr) _expirationTimerPtr->cancel();
}

void Job::qservAddReplica(unsigned int chunk,
                          std::vector<std::string> const& databases,
                          std::string const& worker,
                          AddReplicaQservMgtRequest::CallbackType onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "** START ** Qserv notification on ADD replica:"
         << ", chunk="     << chunk
         << ", databases=" << util::printable(databases)
         << "  worker="    << worker);

    ASSERT_LOCK(_mtx, context() + "qservAddReplica");

    auto self = shared_from_this();

    _controller->serviceProvider()->qservMgtServices()->addReplica(
        chunk,
        databases,
        worker,
        [self,onFinish] (AddReplicaQservMgtRequest::Ptr const& request) {

            LOGS(_log, LOG_LVL_DEBUG, self->context()
                 << "** FINISH ** Qserv notification on ADD replica:"
                 << "  chunk="         << request->chunk()
                 << ", databases="     << util::printable(request->databases())
                 << ", worker="        << request->worker()
                 << ", state="         << request->state2string(request->state())
                 << ", extendedState=" << request->state2string(request->extendedState())
                 << ". serverError="   << request->serverError());

            // Pass through the result to a caller
            if (onFinish) {
                onFinish(request);
            }
        },
        _id
    );
}

void Job::qservRemoveReplica(unsigned int chunk,
                             std::vector<std::string> const& databases,
                             std::string const& worker,
                             bool force,
                             RemoveReplicaQservMgtRequest::CallbackType onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "** START ** Qserv notification on REMOVE replica:"
         << "  chunk="     << chunk
         << ", databases=" << util::printable(databases)
         << ", worker="    << worker
         << ", force="     << (force ? "true" : "false"));

    ASSERT_LOCK(_mtx, context() + "qservRemoveReplica");

    auto self = shared_from_this();

    _controller->serviceProvider()->qservMgtServices()->removeReplica(
        chunk,
        databases,
        worker,
        force,
        [self,onFinish] (RemoveReplicaQservMgtRequest::Ptr const& request) {

            LOGS(_log, LOG_LVL_DEBUG, self->context()
                 << "** FINISH ** Qserv notification on REMOVE replica:"
                 << "  chunk="         << request->chunk()
                 << ", databases="     << util::printable(request->databases())
                 << ", worker="        << request->worker()
                 << ", force="         << (request->force() ? "true" : "false")
                 << ", state="         << request->state2string(request->state())
                 << ", extendedState=" << request->state2string(request->extendedState())
                 << ". serverError="   << request->serverError());

            // Pass through the result to the caller if requested
            if (onFinish) {
                onFinish(request);
            }
        },
        _id
    );
}

void Job::assertState(State state,
                      std::string const& context) const {
    if (state != _state) {
        throw std::logic_error(
            context + ": wrong state " + state2string(state) + " instead of " + state2string(_state));
    }
}

void Job::setState(State state,
                   ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setState  state=" << state2string(state, extendedState));

    ASSERT_LOCK(_mtx, context() + "setState");

    // ATTENTION: changing the top-level state to FINISHED should be last step
    // in the transient state transition in order to ensure a consistent view
    // onto the combined state.

    if (_state == State::FINISHED) {
        _endTime = PerformanceUtils::now();
    }
    _extendedState = extendedState;
    _state = state;

    _controller->serviceProvider()->databaseServices()->saveState(*this);
}

void Job::startHeartbeatTimer() {

    ASSERT_LOCK(_mtx, context() + "startHeartbeatTimer");

    if (_heartbeatTimerIvalSec) {

        LOGS(_log, LOG_LVL_DEBUG, context() << "startHeartbeatTimer");

        // The time needs to be initialized each time when a new interval
        // is about to begin. Otherwise it will strt firing immediately.
        _heartbeatTimerPtr.reset(
            new boost::asio::deadline_timer(
                _controller->io_service(),
                boost::posix_time::seconds(_heartbeatTimerIvalSec)));

        _heartbeatTimerPtr->async_wait(
            boost::bind(
                &Job::heartbeat,
                shared_from_this(),
                boost::asio::placeholders::error
            )
        );
    }
}

void Job::heartbeat(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "heartbeat: "
         << (ec == boost::asio::error::operation_aborted ? "** ABORTED **" : ""));

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "heartbeat");

    if (_state == State::FINISHED) return;

    // Update the job entry in the database
    _controller->serviceProvider()->databaseServices()->updateHeartbeatTime(*this);

    // Start another interval
    startHeartbeatTimer();

}

void Job::startExpirationTimer() {

    ASSERT_LOCK(_mtx, context() + "startExpirationTimer");

    if (_expirationIvalSec) {

        LOGS(_log, LOG_LVL_DEBUG, context() << "startExpirationTimer");

        // The time needs to be initialized each time when a new interval
        // is about to begin. Otherwise it will strt firing immediately.
        _expirationTimerPtr.reset(
            new boost::asio::deadline_timer(
                _controller->io_service(),
                boost::posix_time::seconds(_expirationIvalSec)));

        _expirationTimerPtr->async_wait(
            boost::bind(
                &Job::expired,
                shared_from_this(),
                boost::asio::placeholders::error
            )
        );
    }
}

void Job::expired(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "expired: "
         << (ec == boost::asio::error::operation_aborted ? "** ABORTED **" : ""));

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;
         
    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "expired");

    if (_state == State::FINISHED) return;

    finish(ExtendedState::EXPIRED);

    // Subclass specific notification
    notify();
}

}}} // namespace lsst::qserv::replica
