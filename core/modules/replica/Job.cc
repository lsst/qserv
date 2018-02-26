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

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Common.h"            // Generators::uniqueId()
#include "replica/Performance.h"       // PerformanceUtils::now()
#include "replica/ServiceProvider.h"


// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Job");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

std::string
Job::state2string (State state) {
    switch (state) {
        case CREATED:     return "CREATED";
        case IN_PROGRESS: return "IN_PROGRESS";
        case FINISHED:    return "FINISHED";
    }
    throw std::logic_error (
                "incomplete implementation of method Job::state2string(State)");
}

std::string
Job::state2string (ExtendedState state) {
    switch (state) {
        case NONE:      return "NONE";
        case SUCCESS:   return "SUCCESS";
        case FAILED:    return "FAILED";
        case EXPIRED:   return "EXPIRED";
        case CANCELLED: return "CANCELLED";
    }
    throw std::logic_error (
                "incomplete implementation of method Job::state2string(ExtendedState)");
}

Job::Job (Controller::pointer const& controller,
          std::string const&         type,
          int                        priority,
          bool                       exclusive,
          bool                       preemptable)

    :   _id          (Generators::uniqueId()),
        _controller  (controller),
        _type        (type),
        _priority    (priority),
        _exclusive   (exclusive),
        _preemptable (preemptable),

        _state         (State::CREATED),
        _extendedState (ExtendedState::NONE),

        _beginTime (0),
        _endTime   (0) {
}

Job::~Job () {
}

std::string
Job::context () const {
    return "JOB [id=" + _id + ", type=" + _type + ", state=" + state2string(_state, _extendedState) + "]  ";
}

void
Job::start () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "start");

    do {
        LOCK_GUARD;

        assertState(State::CREATED);
        startImpl();

        _beginTime = PerformanceUtils::now();
        _controller->serviceProvider().databaseServices()->saveState (shared_from_this());

        // Allow the job to be fully accomplished right away
        if (_state == State::FINISHED) break;

        assertState(State::IN_PROGRESS);

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED)
        notify();
}

void
Job::cancel () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel");

    // Invoke a subclass specific cancellation sequence of actions
    {
        LOCK_GUARD;

        assertState(State::IN_PROGRESS);
        cancelImpl();
        assertState(State::FINISHED);
    }

    // The callabacks are called w/o the lock guard to avoid deadlocking
    // in case if subscribers will attempt to use the public API of this
    // class or its subclasses.

    notify();

    _controller->serviceProvider().databaseServices()->saveState (shared_from_this());
}

void
Job::assertState (State state) const {
    if (state != _state)
        throw std::logic_error (
            "wrong state " + state2string(state) + " instead of " + state2string(_state));
}

void
Job::setState (State         state,
               ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setState  state=" << state2string(state, extendedState));

    _state         = state;
    _extendedState = extendedState;
    
    if (_state == State::FINISHED)
        _endTime = PerformanceUtils::now();

    _controller->serviceProvider().databaseServices()->saveState (shared_from_this());
}
    
}}} // namespace lsst::qserv::replica