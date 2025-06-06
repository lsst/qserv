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
#include "replica/jobs/Job.h"

// System headers
#include <functional>
#include <sstream>
#include <stdexcept>
#include <utility>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/qserv/QservMgtServices.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"  // Generators::uniqueId()
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Job");

}  // namespace

namespace lsst::qserv::replica {

atomic<size_t> Job::_numClassInstances(0);

string Job::state2string(State state) {
    switch (state) {
        case CREATED:
            return "CREATED";
        case IN_PROGRESS:
            return "IN_PROGRESS";
        case FINISHED:
            return "FINISHED";
    }
    throw logic_error("incomplete implementation of method Job::state2string(State)");
}

string Job::state2string(ExtendedState state) {
    switch (state) {
        case NONE:
            return "NONE";
        case SUCCESS:
            return "SUCCESS";
        case CONFIG_ERROR:
            return "CONFIG_ERROR";
        case FAILED:
            return "FAILED";
        case QSERV_FAILED:
            return "QSERV_FAILED";
        case QSERV_CHUNK_IN_USE:
            return "QSERV_CHUNK_IN_USE";
        case BAD_RESULT:
            return "BAD_RESULT";
        case TIMEOUT_EXPIRED:
            return "TIMEOUT_EXPIRED";
        case CANCELLED:
            return "CANCELLED";
    }
    throw logic_error("Job::" + string(__func__) + "  incomplete implementation of method");
}

json Job::Progress::toJson() const { return json::object({{"complete", complete}, {"total", total}}); }

Job::Job(Controller::Ptr const& controller, string const& parentJobId, string const& type, int priority)
        : _id(Generators::uniqueId()),
          _controller(controller),
          _parentJobId(parentJobId),
          _type(type),
          _priority(priority),
          _state(State::CREATED),
          _extendedState(ExtendedState::NONE),
          _beginTime(0),
          _endTime(0),
          _heartbeatTimerIvalSec(controller->serviceProvider()->config()->get<unsigned int>(
                  "controller", "job-heartbeat-sec")),
          _expirationIvalSec(controller->serviceProvider()->config()->get<unsigned int>("controller",
                                                                                        "job-timeout-sec")) {
    // This report is used solely for debugging purposes to allow tracking
    // potential memory leaks within applications.
    ++_numClassInstances;
    LOGS(_log, LOG_LVL_DEBUG, context() << "constructed  instances: " << _numClassInstances);
}

Job::~Job() {
    --_numClassInstances;
    LOGS(_log, LOG_LVL_DEBUG, context() << "destructed   instances: " << _numClassInstances);
}

string Job::state2string() const {
    replica::Lock lock(_mtx, context() + __func__);
    return state2string(state(), extendedState());
}

list<pair<string, string>> Job::persistentLogData() const {
    LOGS(_log, LOG_LVL_DEBUG, context());
    if (state() == State::FINISHED) return list<pair<string, string>>();
    throw logic_error("Job::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

string Job::context() const {
    return "JOB     " + id() + "  " + type() + "  " + state2string(state(), extendedState()) + "  ";
}

void Job::start() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    replica::Lock lock(_mtx, context() + __func__);
    _assertState(lock, State::CREATED, context() + __func__);

    // IMPORTANT: update these before proceeding to the implementation
    // because the later may create children jobs whose performance
    // counters must be newer, and whose saved state within the database
    // may depend on this job's state.
    _beginTime = util::TimeUtils::now();
    controller()->serviceProvider()->databaseServices()->saveState(*this);

    // Start timers if configured
    _startHeartbeatTimer(lock);
    _startExpirationTimer(lock);

    // Delegate the rest to the specific implementation
    startImpl(lock);

    // Allow the job to be fully accomplished right away
    if (state() == State::FINISHED) return;

    // Otherwise, the only other state which is allowed here is this
    setState(lock, State::IN_PROGRESS);
}

Job::Progress Job::progress() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    replica::Lock lock(_mtx, context() + __func__);
    return Progress{_finished ? 1ULL : 0ULL, 1ULL};
}

void Job::wait() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    if (_finished) return;
    unique_lock<mutex> onFinishLock(_onFinishMtx);
    _onFinishCv.wait(onFinishLock, [&] { return _finished.load(); });
}

void Job::wait(chrono::milliseconds const& ivalMsec, WaitMonitorFunc const& func) {
    string const context_ = context() + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);
    if (_finished) return;
    if (ivalMsec.count() == 0) throw invalid_argument(context_ + "callaback interval can't be 0.");
    if (func == nullptr) throw invalid_argument(context_ + "callaback function not provided.");
    auto const self = shared_from_this();
    unique_lock<mutex> onFinishLock(_onFinishMtx);
    while (!_onFinishCv.wait_for(onFinishLock, ivalMsec, [&] { return _finished.load(); })) {
        // Unlock and relock is needed to prevent the deadlock in case if the called function
        // will be interacting with the public API of the job.
        onFinishLock.unlock();
        func(self);
        onFinishLock.lock();
    }
}

void Job::cancel() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;
    finish(lock, ExtendedState::CANCELLED);
}

void Job::finish(replica::Lock const& lock, ExtendedState newExtendedState) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  newExtendedState=" << state2string(newExtendedState));

    // Also ignore this event if the request is over
    if (state() == State::FINISHED) return;

    // *IMPORTANT*: Set new state *BEFORE* calling subclass-specific cancellation
    // protocol to make sure all event handlers will recognize this scenario and
    // avoid making any modifications to the request's state.
    setState(lock, State::FINISHED, newExtendedState);

    // Invoke a subclass specific cancellation sequence of actions if anything
    // bad has happen.
    if (newExtendedState != ExtendedState::SUCCESS) cancelImpl(lock);
    controller()->serviceProvider()->databaseServices()->saveState(*this);

    // Stop timers if they're still running
    if (_heartbeatTimerPtr) _heartbeatTimerPtr->cancel();
    if (_expirationTimerPtr) _expirationTimerPtr->cancel();
    notify(lock);

    // Unblock threads (if any) waiting on the synchronization call
    // to method Job::wait()
    _finished = true;
    _onFinishCv.notify_all();
}

void Job::_assertState(replica::Lock const& lock, State desiredState, string const& context) const {
    if (desiredState != state()) {
        throw logic_error(context + ": wrong state " + state2string(state()) + " instead of " +
                          state2string(desiredState));
    }
}

void Job::setState(replica::Lock const& lock, State newState, ExtendedState newExtendedState) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  new state=" << state2string(newState, newExtendedState));

    // ATTENTION: changing the top-level state to FINISHED should be last step
    // in the transient state transition in order to ensure a consistent view
    // onto the combined state.
    if (newState == State::FINISHED) _endTime = util::TimeUtils::now();
    {
        unique_lock<mutex> onFinishLock(_onFinishMtx);
        _extendedState = newExtendedState;
        _state = newState;
    }
    controller()->serviceProvider()->databaseServices()->saveState(*this);
}

void Job::_startHeartbeatTimer(replica::Lock const& lock) {
    if (_heartbeatTimerIvalSec) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
        // The timer needs to be initialized each time a new interval
        // is about to begin. Otherwise it will immediately expire when
        // async_wait() will be called.
        _heartbeatTimerPtr.reset(
                new boost::asio::deadline_timer(controller()->serviceProvider()->io_service(),
                                                boost::posix_time::seconds(_heartbeatTimerIvalSec)));
        _heartbeatTimerPtr->async_wait(bind(&Job::_heartbeat, shared_from_this(), _1));
    }
}

void Job::_heartbeat(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  "
                   << (ec == boost::asio::error::operation_aborted ? "** ABORTED **" : ""));

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    // Update the job entry in the database
    controller()->serviceProvider()->databaseServices()->updateHeartbeatTime(*this);

    // Start another interval
    _startHeartbeatTimer(lock);
}

void Job::_startExpirationTimer(replica::Lock const& lock) {
    if (0 != _expirationIvalSec) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
        // The timer needs to be initialized each time a new interval
        // is about to begin. Otherwise it will immediately expire when
        // async_wait() will be called.
        _expirationTimerPtr.reset(
                new boost::asio::deadline_timer(controller()->serviceProvider()->io_service(),
                                                boost::posix_time::seconds(_expirationIvalSec)));
        _expirationTimerPtr->async_wait(bind(&Job::_expired, shared_from_this(), _1));
    }
}

void Job::_expired(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  "
                   << (ec == boost::asio::error::operation_aborted ? "** ABORTED **" : ""));

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;
    finish(lock, ExtendedState::TIMEOUT_EXPIRED);
}

}  // namespace lsst::qserv::replica
