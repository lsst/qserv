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

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

atomic<size_t> QservMgtRequest::_numClassInstances(0);


string QservMgtRequest::state2string(State state) {
    switch (state) {
        case State::CREATED:     return "CREATED";
        case State::IN_PROGRESS: return "IN_PROGRESS";
        case State::FINISHED:    return "FINISHED";
    }
    throw logic_error(
           "QservMgtRequest::" + string(__func__) + "(State)  incomplete implementation");
}


string QservMgtRequest::state2string(ExtendedState state) {
    switch (state) {
        case ExtendedState::NONE:                return "NONE";
        case ExtendedState::SUCCESS:             return "SUCCESS";
        case ExtendedState::CONFIG_ERROR:        return "CONFIG_ERROR";
        case ExtendedState::SERVER_BAD:          return "SERVER_BAD";
        case ExtendedState::SERVER_CHUNK_IN_USE: return "SERVER_CHUNK_IN_USE";
        case ExtendedState::SERVER_ERROR:        return "SERVER_ERROR";
        case ExtendedState::SERVER_BAD_RESPONSE: return "SERVER_BAD_RESPONSE";
        case ExtendedState::TIMEOUT_EXPIRED:     return "TIMEOUT_EXPIRED";
        case ExtendedState::CANCELLED:           return "CANCELLED";
    }
    throw logic_error(
            "QservMgtRequest::" + string(__func__) + "(ExtendedState)  incomplete implementation");
}


QservMgtRequest::QservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                                 string const& type,
                                 string const& worker)
    :   _serviceProvider(serviceProvider),
        _type(type),
        _id(Generators::uniqueId()),
        _worker(worker),
        _state(State::CREATED),
        _extendedState(ExtendedState::NONE),
        _service(nullptr),
        _requestExpirationIvalSec(serviceProvider->config()->xrootdTimeoutSec()),
        _requestExpirationTimer(serviceProvider->io_service()) {

    // This report is used solely for debugging purposes to allow tracking
    // potential memory leaks within applications.
    ++_numClassInstances;
    LOGS(_log, LOG_LVL_DEBUG, context() << "constructed  instances: " << _numClassInstances);
}


QservMgtRequest::~QservMgtRequest() {
    --_numClassInstances;
    LOGS(_log, LOG_LVL_DEBUG, context() << "destructed   instances: " << _numClassInstances);
}


string QservMgtRequest::state2string() const {
    util::Lock lock(_mtx, context() + __func__);
    return state2string(state(), extendedState()) + "::" + serverError(lock);
}


string QservMgtRequest::serverError() const {
    util::Lock lock(_mtx, context() + __func__);
    return serverError(lock);
}


string QservMgtRequest::serverError(util::Lock const& lock) const {
    return _serverError;
}


string QservMgtRequest::context() const {
    return id() +
        "  " + type() +
        "  " + state2string(state(), extendedState()) +
        "  ";
}


Performance QservMgtRequest::performance() const {
    util::Lock lock(_mtx, context() + __func__);
    return performance(lock);
}


Performance QservMgtRequest::performance(util::Lock const& lock) const {
    return _performance;
}


void QservMgtRequest::start(XrdSsiService* service,
                            string const& jobId,
                            unsigned int requestExpirationIvalSec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    util::Lock lock(_mtx, "QservMgtRequest::" + string(__func__));

    assertState(State::CREATED, "QservMgtRequest::" + string(__func__));

    // This needs to be updated to override the default value of the counter
    // which was created upon the object construction.

    _performance.setUpdateStart();

    // Check if configuration parameters are valid

    if (not (serviceProvider()->config()->isKnownWorker(worker()) and
             (service != nullptr))) {

        LOGS(_log, LOG_LVL_ERROR, context() << __func__
             << "  ** MISCONFIGURED ** "
             << " worker: '" << worker() << "'"
             << " XrdSsiService pointer: " << service);

        setState(lock,
                 State::FINISHED,
                 ExtendedState::CONFIG_ERROR);
        
        notify(lock);
        return;
    }
  
    // Build associations with the corresponding service and
    // the job (optional)

    _service = service;
    _jobId = jobId;
      
    // Change the default values of the expiration ival if requested
    // before starting the timer.
  
    if (0 != requestExpirationIvalSec) {
        _requestExpirationIvalSec = requestExpirationIvalSec;
    }
    if (0 != _requestExpirationIvalSec) {
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

    // Let a subclass to proceed with its own sequence of actions before
    // finalizing state transition and updating the persistent state.

    startImpl(lock);

    if (state() == State::FINISHED) return;

    setState(lock,
             State::IN_PROGRESS);
}


void QservMgtRequest::wait() {
 
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;

    unique_lock<mutex> onFinishLock(_onFinishMtx);
    _onFinishCv.wait(onFinishLock, [this] {
        return state() == State::FINISHED;
    });
}


string const& QservMgtRequest::jobId() const {
    if (_state == State::CREATED) {
        throw logic_error(
                "Job::" + string(__func__) +
                "  the Job Id is not available because the request has not started yet");
    }
    return _jobId;
}


void QservMgtRequest::expired(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__
         << (ec == boost::asio::error::operation_aborted ? "  ** ABORTED **" : ""));

    // Ignore this event if the timer was aborted

    if (ec == boost::asio::error::operation_aborted) return;

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    finish(lock, ExtendedState::TIMEOUT_EXPIRED);
}


void QservMgtRequest::cancel() {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    finish(lock, ExtendedState::CANCELLED);
}


void QservMgtRequest::finish(util::Lock const& lock,
                             ExtendedState extendedState,
                             string const& serverError) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Set the optional server error state as well
    //
    // IMPORTANT: this needs to be done before performing the state
    // transition to insure clients will get a consistent view onto
    // the object state.

    _serverError = serverError;

    // Set new state to make sure all event handlers will recognize
    // this scenario and avoid making any modifications to the request's state.

    setState(lock,
             State::FINISHED,
             extendedState);

    // Close all operations on BOOST ASIO if needed

    _requestExpirationTimer.cancel();

    // Let a subclass to run its own finalization if needed

    finishImpl(lock);

    // We have to update the timestamp before invoking a user provided
    // callback on the completion of the operation.

    _performance.setUpdateFinish();

    serviceProvider()->databaseServices()->saveState(*this, _performance, _serverError);

    notify(lock);

    // Unblock threads (if any) waiting on the synchronization call
    // to method QservMgtRequest::wait()

    _onFinishCv.notify_all();
}


void QservMgtRequest::assertState(State desiredState,
                                  string const& context) const {
    if (desiredState != state()) {
        throw logic_error(
                context + ": wrong state " + state2string(state()) + " instead of " +
                state2string(desiredState));
    }
}


void QservMgtRequest::setState(util::Lock const& lock,
                               State newState,
                               ExtendedState newExtendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  "
         << state2string(newState, newExtendedState));

    // IMPORTANT: the top-level state is the last to be set when performing
    // the state transition to insure clients will get a consistent view onto
    // the object state.
    {
        unique_lock<mutex> onFinishLock(_onFinishMtx);
        _extendedState = newExtendedState;
        _state = newState;
    }
    serviceProvider()->databaseServices()->saveState(*this, _performance, _serverError);
}

}}} // namespace lsst::qserv::replica
