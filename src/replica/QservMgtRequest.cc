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
#include <unordered_map>

// Qserv headers
#include "http/MetaModule.h"
#include "replica/Configuration.h"
#include "replica/Common.h"
#include "replica/DatabaseServices.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

atomic<size_t> QservMgtRequest::_numClassInstances(0);

string QservMgtRequest::state2string(State state) {
    switch (state) {
        case State::CREATED:
            return "CREATED";
        case State::IN_PROGRESS:
            return "IN_PROGRESS";
        case State::FINISHED:
            return "FINISHED";
    }
    throw logic_error("QservMgtRequest::" + string(__func__) + "(State)  incomplete implementation");
}

string QservMgtRequest::state2string(ExtendedState state) {
    switch (state) {
        case ExtendedState::NONE:
            return "NONE";
        case ExtendedState::SUCCESS:
            return "SUCCESS";
        case ExtendedState::CONFIG_ERROR:
            return "CONFIG_ERROR";
        case ExtendedState::BODY_LIMIT_ERROR:
            return "BODY_LIMIT_ERROR";
        case ExtendedState::SERVER_BAD:
            return "SERVER_BAD";
        case ExtendedState::SERVER_CHUNK_IN_USE:
            return "SERVER_CHUNK_IN_USE";
        case ExtendedState::SERVER_ERROR:
            return "SERVER_ERROR";
        case ExtendedState::SERVER_BAD_RESPONSE:
            return "SERVER_BAD_RESPONSE";
        case ExtendedState::TIMEOUT_EXPIRED:
            return "TIMEOUT_EXPIRED";
        case ExtendedState::CANCELLED:
            return "CANCELLED";
    }
    throw logic_error("QservMgtRequest::" + string(__func__) + "(ExtendedState)  incomplete implementation");
}

QservMgtRequest::QservMgtRequest(ServiceProvider::Ptr const& serviceProvider, string const& type,
                                 string const& worker)
        : _serviceProvider(serviceProvider),
          _type(type),
          _id(Generators::uniqueId()),
          _worker(worker),
          _state(State::CREATED),
          _extendedState(ExtendedState::NONE) {
    // This report is used solely for debugging purposes to allow tracking
    // potential memory leaks within applications.
    ++_numClassInstances;
    LOGS(_log, LOG_LVL_TRACE, context() << "constructed  instances: " << _numClassInstances);
}

QservMgtRequest::~QservMgtRequest() {
    --_numClassInstances;
    LOGS(_log, LOG_LVL_TRACE, context() << "destructed   instances: " << _numClassInstances);
}

string QservMgtRequest::state2string() const {
    replica::Lock const lock(_mtx, context() + __func__);
    return state2string(state(), extendedState()) + "::" + serverError(lock);
}

string QservMgtRequest::serverError() const {
    replica::Lock const lock(_mtx, context() + __func__);
    return serverError(lock);
}

string QservMgtRequest::serverError(replica::Lock const& lock) const { return _serverError; }

string QservMgtRequest::context() const {
    return id() + "  " + type() + "  " + state2string(state(), extendedState()) + "  ";
}

Performance QservMgtRequest::performance() const {
    replica::Lock const lock(_mtx, context() + __func__);
    return performance(lock);
}

Performance QservMgtRequest::performance(replica::Lock const& lock) const { return _performance; }

void QservMgtRequest::start(string const& jobId, unsigned int requestExpirationIvalSec) {
    string const context_ = context() + __func__;
    LOGS(_log, LOG_LVL_TRACE, context_);

    replica::Lock const lock(_mtx, context_);
    _assertNotStarted(__func__);

    // This needs to be updated to override the default value of the counter
    // which was created upon the object construction.
    _performance.setUpdateStart();

    // Check if configuration parameters are valid
    auto const config = _serviceProvider->config();
    if (!config->isKnownWorker(_worker)) {
        LOGS(_log, LOG_LVL_ERROR, context_ << " ** MISCONFIGURED ** unknown worker: '" << _worker << "'.");
        _setState(lock, State::FINISHED, ExtendedState::CONFIG_ERROR);
        notify(lock);
        return;
    }

    // Build an association with the corresponding parent job (optional).
    _jobId = jobId;

    // Adjust the default value of the expiration ival (if requested) before
    // creating and starting the request.
    unsigned int actualExpirationIvalSec = requestExpirationIvalSec;
    if (0 == actualExpirationIvalSec) {
        actualExpirationIvalSec = config->get<unsigned int>("xrootd", "request-timeout-sec");
    }

    createHttpReqImpl(lock);

    _httpRequest->setExpirationIval(actualExpirationIvalSec);
    _httpRequest->start();

    _setState(lock, State::IN_PROGRESS);
}

void QservMgtRequest::wait() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    if (_state == State::FINISHED) return;
    unique_lock<mutex> onFinishLock(_onFinishMtx);
    _onFinishCv.wait(onFinishLock, [&] { return _finished; });
}

string const& QservMgtRequest::jobId() const {
    _assertStarted(__func__);
    return _jobId;
}

json const& QservMgtRequest::info() const {
    if (!((_state == State::FINISHED) && (_extendedState == ExtendedState::SUCCESS))) {
        throw logic_error("QservMgtRequest::" + string(__func__) +
                          "  no info available in state: " + state2string(_state, _extendedState));
    }
    return _info;
}

void QservMgtRequest::cancel() {
    _assertStarted(__func__);
    // No HTTP request would be sent if the request creation failed for some
    // reason (like misconfiguration, etc.). Hence, there is nothing to cancel.
    {
        replica::Lock const lock(_mtx, context() + __func__);
        if (_httpRequest == nullptr) return;
    }
    _httpRequest->cancel();
}

void QservMgtRequest::createHttpReq(replica::Lock const& lock, string const& service, string const& query) {
    _assertNotStarted(__func__);
    string const target = service + query + string(query.empty() ? "?" : "&") + "id" + _id +
                          "&instance_id=" + _serviceProvider->instanceId() + "&worker=" + _worker +
                          "&version=" + to_string(http::MetaModule::version);
    _httpRequest = http::AsyncReq::create(
            _serviceProvider->io_service(),
            [self = shared_from_this()](shared_ptr<http::AsyncReq> const&) { self->_processResponse(); },
            http::Method::GET, _getHostPortTracker(), target);
}

void QservMgtRequest::createHttpReq(replica::Lock const& lock, http::Method method, string const& target,
                                    json const& body) {
    _assertNotStarted(__func__);
    json data = body;
    data["id"] = _id;
    data["instance_id"] = _serviceProvider->instanceId();
    data["worker"] = _worker;
    data["auth_key"] = _serviceProvider->authKey();
    data["admin_auth_key"] = _serviceProvider->adminAuthKey();
    data["version"] = http::MetaModule::version;
    unordered_map<string, string> const headers = {{"Content-Type", "application/json"}};
    _httpRequest = http::AsyncReq::create(
            _serviceProvider->io_service(),
            [self = shared_from_this()](shared_ptr<http::AsyncReq> const&) { self->_processResponse(); },
            method, _getHostPortTracker(), target, data.dump(), headers);
}

void QservMgtRequest::finish(replica::Lock const& lock, ExtendedState extendedState,
                             string const& serverError) {
    string const context_ = context() + __func__;
    LOGS(_log, LOG_LVL_DEBUG, context_ << " serverError:" << serverError);

    // Set the optional server error state as well.
    // IMPORTANT: this needs to be done before performing the state transition to insure
    //            clients will get a consistent view onto the object state.
    _serverError = serverError;

    // Set new state to make sure all event handlers will recognize
    // this scenario and avoid making any modifications to the request's state.
    _setState(lock, State::FINISHED, extendedState);

    // We have to update the timestamp before invoking a user provided
    // callback on the completion of the operation.
    _performance.setUpdateFinish();

    // TODO: temporarily disabled while this class is not supported by
    //       the persistent backend.
    //
    // _serviceProvider->databaseServices()->saveState(*this, _performance, _serverError);

    notify(lock);

    // Unblock threads (if any) waiting on the synchronization call to the method wait().
    _finished = true;
    _onFinishCv.notify_all();
}

http::AsyncReq::GetHostPort QservMgtRequest::_getHostPortTracker() const {
    return [config = _serviceProvider->config(),
            worker = _worker](http::AsyncReq::HostPort const&) -> http::AsyncReq::HostPort {
        auto const info = config->workerInfo(worker);
        return http::AsyncReq::HostPort{info.qservWorker.host.addr, info.qservWorker.port};
    };
}

void QservMgtRequest::_processResponse() {
    string const context_ = context() + string(__func__) + " ";
    if (_state == State::FINISHED) return;
    replica::Lock const lock(_mtx, context_);
    if (_state == State::FINISHED) return;

    switch (_httpRequest->state()) {
        case http::AsyncReq::State::FINISHED:
            try {
                _info = json::parse(_httpRequest->responseBody());
                if (_info.at("success").get<int>() == 0) {
                    string const msg = "worker reported error: " + _info.at("error").get<string>();
                    auto extendedState = ExtendedState::SERVER_BAD;
                    // Check for optional markers in the optional extended error section that might
                    // clarify an actual reason behind the failure.
                    if (auto const itr = _info.find("error_ext"); itr != _info.end()) {
                        json const errorExt = *itr;
                        if (errorExt.contains("in_use")) {
                            extendedState = ExtendedState::SERVER_CHUNK_IN_USE;
                        } else if (errorExt.contains("invalid_param")) {
                            extendedState = ExtendedState::CONFIG_ERROR;
                        }
                    }
                    finish(lock, extendedState, msg);
                } else {
                    // Let a subclass do the optional result validation and post processing.
                    // Note that the subclass has the final say on the completion status
                    // of the request.
                    finish(lock, dataReady(lock, _info));
                }
            } catch (exception const& ex) {
                string const msg = "failed to parse/process worker response, ex: " + string(ex.what());
                finish(lock, ExtendedState::SERVER_BAD_RESPONSE, msg);
            }
            break;
        case http::AsyncReq::State::FAILED:
            finish(lock, ExtendedState::SERVER_ERROR,
                   _httpRequest->errorMessage() + ", code: " + to_string(_httpRequest->responseCode()));
            break;
        case http::AsyncReq::State::BODY_LIMIT_ERROR:
            finish(lock, ExtendedState::BODY_LIMIT_ERROR,
                   _httpRequest->errorMessage() + ", code: " + to_string(_httpRequest->responseCode()));
            break;
        case http::AsyncReq::State::CANCELLED:
            finish(lock, ExtendedState::CANCELLED);
            break;
        case http::AsyncReq::State::EXPIRED:
            finish(lock, ExtendedState::TIMEOUT_EXPIRED);
            break;
        default:
            throw runtime_error(context_ + "unsupported state of the HTTP _httpRequest: " +
                                http::AsyncReq::state2str(_httpRequest->state()));
    }
}

void QservMgtRequest::_assertStarted(string const& func, bool mustBeStarted) const {
    string const context_ = context() + func;
    LOGS(_log, LOG_LVL_TRACE, context_);
    if (mustBeStarted) {
        if (State::CREATED == _state) {
            throw logic_error(context_ + " the request was not started.");
        }
    } else {
        if (State::CREATED != _state) {
            throw logic_error(context_ + " the request was already started.");
        }
    }
}

void QservMgtRequest::_setState(replica::Lock const& lock, State newState, ExtendedState newExtendedState) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__ << "  " << state2string(newState, newExtendedState));

    // IMPORTANT: the top-level state is the last to be set when performing
    // the state transition to ensure clients will get a consistent view onto
    // the object state.
    {
        unique_lock<mutex> onFinishLock(_onFinishMtx);
        _extendedState = newExtendedState;
        _state = newState;
    }
    _serviceProvider->databaseServices()->saveState(*this, _performance, _serverError);
}

}  // namespace lsst::qserv::replica
