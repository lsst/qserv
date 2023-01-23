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
#include "replica/Request.h"

// System headers
#include <algorithm>
#include <functional>
#include <sstream>
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Request");

}  // namespace

namespace lsst::qserv::replica {

atomic<size_t> Request::_numClassInstances(0);

string Request::state2string(State state) {
    switch (state) {
        case CREATED:
            return "CREATED";
        case IN_PROGRESS:
            return "IN_PROGRESS";
        case FINISHED:
            return "FINISHED";
    }
    throw logic_error("Request::" + string(__func__) + "(State)  incomplete implementation");
}

string Request::state2string(ExtendedState state) {
    switch (state) {
        case NONE:
            return "NONE";
        case SUCCESS:
            return "SUCCESS";
        case CLIENT_ERROR:
            return "CLIENT_ERROR";
        case SERVER_BAD:
            return "SERVER_BAD";
        case SERVER_ERROR:
            return "SERVER_ERROR";
        case SERVER_CREATED:
            return "SERVER_CREATED";
        case SERVER_QUEUED:
            return "SERVER_QUEUED";
        case SERVER_IN_PROGRESS:
            return "SERVER_IN_PROGRESS";
        case SERVER_IS_CANCELLING:
            return "SERVER_IS_CANCELLING";
        case SERVER_CANCELLED:
            return "SERVER_CANCELLED";
        case TIMEOUT_EXPIRED:
            return "TIMEOUT_EXPIRED";
        case CANCELLED:
            return "CANCELLED";
    }
    throw logic_error("Request::" + string(__func__) + "(ExtendedState)  incomplete implementation");
}

string Request::state2string(State state, ExtendedState extendedState) {
    return state2string(state) + "::" + state2string(extendedState);
}

string Request::state2string(State state, ExtendedState extendedState,
                             replica::ProtocolStatusExt serverStatus) {
    return state2string(state, extendedState) + "::" + replica::status2string(serverStatus);
}

Request::Request(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                 string const& type, string const& worker, int priority, bool keepTracking,
                 bool allowDuplicate, bool disposeRequired)
        : _serviceProvider(serviceProvider),
          _type(type),
          _id(Generators::uniqueId()),
          _worker(worker),
          _priority(priority),
          _keepTracking(keepTracking),
          _allowDuplicate(allowDuplicate),
          _disposeRequired(disposeRequired),
          _state(CREATED),
          _extendedState(NONE),
          _extendedServerStatus(ProtocolStatusExt::NONE),
          _bufferPtr(new ProtocolBuffer(
                  serviceProvider->config()->get<size_t>("common", "request-buf-size-bytes"))),
          _workerInfo(serviceProvider->config()->workerInfo(worker)),
          _timerIvalSec(serviceProvider->config()->get<unsigned int>("common", "request-retry-interval-sec")),
          _timer(io_service),
          _requestExpirationIvalSec(
                  serviceProvider->config()->get<unsigned int>("controller", "request-timeout-sec")),
          _requestExpirationTimer(io_service) {
    _serviceProvider->config()->assertWorkerIsValid(worker);

    // This report is used solely for debugging purposes to allow tracking
    // potential memory leaks within applications.
    ++_numClassInstances;
    LOGS(_log, LOG_LVL_DEBUG, context() << "constructed  instances: " << _numClassInstances);
}

Request::~Request() {
    --_numClassInstances;
    LOGS(_log, LOG_LVL_DEBUG, context() << "destructed   instances: " << _numClassInstances);
}

string Request::state2string() const {
    replica::Lock lock(_mtx, context() + __func__);
    return state2string(state(), extendedState()) + "::" + replica::status2string(extendedServerStatus());
}

string Request::context() const {
    return "REQUEST " + id() + "  " + type() + "  " + state2string(state(), extendedState()) +
           "::" + replica::status2string(extendedServerStatus()) + "  ";
}

string const& Request::remoteId() const { return _duplicateRequestId.empty() ? _id : _duplicateRequestId; }

unsigned int Request::nextTimeIvalMsec() {
    auto result = _currentTimeIvalMsec;
    _currentTimeIvalMsec = min(2 * _currentTimeIvalMsec, 1000 * timerIvalSec());
    return result;
}

Performance Request::performance() const {
    replica::Lock lock(_mtx, context() + __func__);
    return performance(lock);
}

Performance Request::performance(replica::Lock const& lock) const { return _performance; }

string Request::toString(bool extended) const {
    ostringstream oss;
    oss << context() << "\n"
        << "  worker: " << worker() << "\n"
        << "  priority: " << priority() << "\n"
        << "  keepTracking: " << bool2str(keepTracking()) << "\n"
        << "  allowDuplicate: " << bool2str(allowDuplicate()) << "\n"
        << "  disposeRequired: " << bool2str(disposeRequired()) << "\n"
        << "  remoteId: " << remoteId() << "\n"
        << "  performance: " << performance() << "\n";
    if (extended) {
        for (auto&& kv : extendedPersistentState()) {
            oss << "  " << kv.first << ": " << kv.second << "\n";
        }
    }
    return oss.str();
}

void Request::start(shared_ptr<Controller> const& controller, string const& jobId,
                    unsigned int requestExpirationIvalSec) {
    replica::Lock lock(_mtx, context() + __func__);

    assertState(lock, CREATED, context() + __func__);

    // Change the expiration interval if requested
    if (requestExpirationIvalSec) {
        _requestExpirationIvalSec = requestExpirationIvalSec;
    }
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  _requestExpirationIvalSec: " << _requestExpirationIvalSec);

    // Build optional associations with the corresponding Controller and the job
    //
    // NOTE: this is done only once, the first time a non-trivial value
    // of each parameter is presented to the method.

    if (not _controller and controller) _controller = controller;
    if (_jobId.empty() and not jobId.empty()) _jobId = jobId;

    _performance.setUpdateStart();

    if (_requestExpirationIvalSec) {
        _requestExpirationTimer.cancel();
        _requestExpirationTimer.expires_from_now(boost::posix_time::seconds(_requestExpirationIvalSec));
        _requestExpirationTimer.async_wait(bind(&Request::expired, shared_from_this(), _1));
    }

    // Let a subclass to proceed with its own sequence of actions

    startImpl(lock);

    // Finalize state transition before saving the persistent state

    setState(lock, IN_PROGRESS);
}

void Request::wait() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (_finished) return;

    unique_lock<mutex> onFinishLock(_onFinishMtx);
    _onFinishCv.wait(onFinishLock, [&] { return _finished; });
}

string const& Request::jobId() const {
    if (state() == State::CREATED) {
        throw logic_error("Request::" + string(__func__) +
                          "  the Job Id is not available because the request has not started yet");
    }
    return _jobId;
}

void Request::expired(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << (ec == boost::asio::error::operation_aborted ? "  ** ABORTED **" : ""));

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    finish(lock, TIMEOUT_EXPIRED);
}

void Request::cancel() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == FINISHED) return;

    finish(lock, CANCELLED);
}

void Request::keepTrackingOrFinish(replica::Lock const& lock, ExtendedState extendedState) {
    if (keepTracking()) {
        timer().expires_from_now(boost::posix_time::milliseconds(nextTimeIvalMsec()));
        timer().async_wait(bind(&Request::awaken, shared_from_this(), _1));
    } else {
        finish(lock, extendedState);
    }
}

void Request::awaken(boost::system::error_code const& ec) {
    throw runtime_error(context() + string(__func__) + "  the default implementation is not allowed.");
}

void Request::finish(replica::Lock const& lock, ExtendedState extendedState) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Check if it's not too late for this operation
    if (state() == FINISHED) return;

    // We have to update the timestamp before making a state transition
    // to ensure a client gets a consistent view onto the object's state.
    _performance.setUpdateFinish();

    // Set new state to make sure all event handlers will recognize
    // this scenario and avoid making any modifications to the request's state.
    setState(lock, FINISHED, extendedState);

    // Stop the timer if the one is still running
    _requestExpirationTimer.cancel();

    // Let a subclass to run its own finalization if needed
    finishImpl(lock);
    notify(lock);

    // Unblock threads (if any) waiting on the synchronization call
    // to method Request::wait()
    _finished = true;
    _onFinishCv.notify_all();
}

bool Request::isAborted(boost::system::error_code const& ec) const {
    if (ec == boost::asio::error::operation_aborted) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  ** ABORTED **");
        return true;
    }
    return false;
}

void Request::assertState(replica::Lock const& lock, State desiredState, string const& context) const {
    if (desiredState != state()) {
        throw logic_error(context + ": wrong state " + state2string(state()) + " instead of " +
                          state2string(desiredState));
    }
}

void Request::setState(replica::Lock const& lock, State newState, ExtendedState newExtendedState) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  " << state2string(newState, newExtendedState));

    // ATTENTION: ensure the top-level state is the last to change in
    // in the transient state transition in order to guarantee a consistent
    // view on to the object's state from the client's prospective.
    {
        unique_lock<mutex> onFinishLock(_onFinishMtx);
        _extendedState = newExtendedState;
        _state = newState;
    }
    savePersistentState(lock);
}

}  // namespace lsst::qserv::replica
