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
#include "replica/worker/WorkerHttpRequest.h"

// System headers
#include <limits>
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/services/ServiceProvider.h"
#include "util/common.h"

// LSST headers
#include "lsst/log/Log.h"

#define CONTEXT context("WorkerHttpRequest", __func__)

using namespace std;
using namespace std::placeholders;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerHttpRequest");
}  // namespace

namespace lsst::qserv::replica {

replica::Mutex WorkerHttpRequest::mtxDataFolderOperations;
string WorkerHttpRequest::_workerFqdn;
replica::Mutex WorkerHttpRequest::_mtxWorkerFqdn;

string WorkerHttpRequest::getWorkerFqdn(unsigned int timeoutSec) {
    replica::Lock lock(_mtxWorkerFqdn, "WorkerHttpRequest::" + string(__func__));
    if (_workerFqdn.empty()) {
        // Note that util::get_current_host_fqdn_wait will throw std::runtime_error
        // if it fails to get a response from the DNS server within the specified timeout.
        // The non-empty value is guaranteed to be returned on success.
        _workerFqdn = util::get_current_host_fqdn_wait(timeoutSec);
    }
    return _workerFqdn;
}

WorkerHttpRequest::WorkerHttpRequest(shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
                                     string const& type, protocol::QueuedRequestHdr const& hdr,
                                     protocol::RequestParams const& params,
                                     ExpirationCallbackType const& onExpired)
        : _serviceProvider(serviceProvider),
          _worker(worker),
          _type(type),
          _hdr(hdr),
          _params(params),
          _onExpired(onExpired),
          _expirationTimeoutSec(hdr.timeout == 0 ? serviceProvider->config()->get<unsigned int>(
                                                           "controller", "request-timeout-sec")
                                                 : hdr.timeout),
          _expirationTimerPtr(new boost::asio::deadline_timer(serviceProvider->io_service())) {}

WorkerHttpRequest::~WorkerHttpRequest() { dispose(); }

void WorkerHttpRequest::checkIfCancelling(replica::Lock const& lock, string const& context_) {
    switch (status()) {
        case protocol::Status::IN_PROGRESS:
            break;
        case protocol::Status::IS_CANCELLING:
            setStatus(lock, protocol::Status::CANCELLED);
            throw WorkerHttpRequestCancelled();
        default:
            throw logic_error(CONTEXT + " not allowed while in status: " + protocol::toString(status()));
    }
}

void WorkerHttpRequest::init() {
    LOGS(_log, LOG_LVL_TRACE, CONTEXT);
    replica::Lock lock(mtx, CONTEXT);
    if (status() != protocol::Status::CREATED) return;

    // Start the expiration timer
    if (_expirationTimeoutSec != 0) {
        _expirationTimerPtr->cancel();
        _expirationTimerPtr->expires_from_now(boost::posix_time::seconds(_expirationTimeoutSec));
        _expirationTimerPtr->async_wait(bind(&WorkerHttpRequest::_expired, shared_from_this(), _1));
        LOGS(_log, LOG_LVL_TRACE,
             CONTEXT << " started timer with _expirationTimeoutSec: " << _expirationTimeoutSec);
    }
}

void WorkerHttpRequest::start() {
    LOGS(_log, LOG_LVL_TRACE, CONTEXT);
    replica::Lock lock(mtx, CONTEXT);
    switch (status()) {
        case protocol::Status::CREATED:
            setStatus(lock, protocol::Status::IN_PROGRESS);
            break;
        default:
            throw logic_error(CONTEXT + " not allowed while in status: " + protocol::toString(status()));
    }
}

void WorkerHttpRequest::cancel() {
    LOGS(_log, LOG_LVL_TRACE, CONTEXT);
    replica::Lock lock(mtx, CONTEXT);
    switch (status()) {
        case protocol::Status::QUEUED:
        case protocol::Status::CREATED:
        case protocol::Status::CANCELLED:
            setStatus(lock, protocol::Status::CANCELLED);
            break;
        case protocol::Status::IN_PROGRESS:
        case protocol::Status::IS_CANCELLING:
            setStatus(lock, protocol::Status::IS_CANCELLING);
            break;

        // Nothing to be done to the completed requests
        case protocol::Status::SUCCESS:
        case protocol::Status::BAD:
        case protocol::Status::FAILED:
            break;
    }
}

void WorkerHttpRequest::rollback() {
    LOGS(_log, LOG_LVL_TRACE, CONTEXT);
    replica::Lock lock(mtx, CONTEXT);
    switch (status()) {
        case protocol::Status::CREATED:
        case protocol::Status::IN_PROGRESS:
            setStatus(lock, protocol::Status::CREATED);
            break;
        case protocol::Status::IS_CANCELLING:
            setStatus(lock, protocol::Status::CANCELLED);
            throw WorkerHttpRequestCancelled();
            break;
        default:
            throw logic_error(CONTEXT + " not allowed while in status: " + protocol::toString(status()));
    }
}

void WorkerHttpRequest::stop() {
    LOGS(_log, LOG_LVL_TRACE, CONTEXT);
    replica::Lock lock(mtx, CONTEXT);
    setStatus(lock, protocol::Status::CREATED);
}

void WorkerHttpRequest::dispose() noexcept {
    LOGS(_log, LOG_LVL_TRACE, CONTEXT);
    replica::Lock lock(mtx, CONTEXT);

    // No timer object exists if the request was created using the simplified constructor
    // for the unit testing. And the expiration timer won't be started if the expiration
    // timeout is set to 0.
    if (_expirationTimerPtr == nullptr || _expirationTimeoutSec == 0) return;
    try {
        _expirationTimerPtr->cancel();
        _expirationTimerPtr.reset();
        LOGS(_log, LOG_LVL_TRACE,
             CONTEXT << " cancelled timer with _expirationTimeoutSec: " << _expirationTimeoutSec);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, CONTEXT << " request expiration couldn't be cancelled, ex: " << ex.what());
    }
}

json WorkerHttpRequest::toJson(bool includeResultIfFinished) const {
    LOGS(_log, LOG_LVL_TRACE, CONTEXT);

    // ATTENTION: the rest of the code relies on the fact that the status of a request is
    // immutable in the final states and on the state transition rules described in
    // the implementation of the method setStatus(). This allowes to capture a consistent snapshot
    // of the request's status in this method without locking the request's mutex. The locking
    // is undersirable here because request execution for some requests may have a long lifetime
    // which will block the request status inqueries for a long time.
    protocol::Status const status = _status.load();

    // The error message and extended state are only relevant for the requests in the final
    // states of BAD or FAILED. In all other cases, the extended status is expected
    // to be NONE and the error message is expected to be empty.
    string error;
    protocol::StatusExt extendedStatus = protocol::StatusExt::NONE;
    switch (status) {
        case protocol::Status::BAD:
        case protocol::Status::FAILED:
            error = _error;
            extendedStatus = _extendedStatus.load();
            break;
        default:
            break;
    }

    // No thread synchronization is needed to read the data of a request in the final state
    // of SUCCESS because the request and its result are expected to be immutable by design.
    bool const resultIsAvailable = (includeResultIfFinished && status == protocol::Status::SUCCESS);

    json req = _hdr.toJson();
    req["params"] = _params.toJson();

    return {{"req", req},
            {"resp", json::object({{"type", _type},
                                   {"expiration_timeout_sec", _expirationTimeoutSec},
                                   {"status", protocol::toString(status)},
                                   {"status_ext", protocol::toString(extendedStatus)},
                                   {"error", error},
                                   {"result", resultIsAvailable ? getResult() : json::object()},
                                   {"perf", _performance.toJson()}})}};
}

string WorkerHttpRequest::context(string const& className, string const& func) const {
    return id() + " " + _type + " " + protocol::toString(status()) + " " + className + "::" + func;
}

void WorkerHttpRequest::setStatus(replica::Lock const& lock, protocol::Status status,
                                  protocol::StatusExt extendedStatus, string const& error) {
    LOGS(_log, LOG_LVL_TRACE,
         CONTEXT << " " << protocol::toString(_status, _extendedStatus) << " -> "
                 << protocol::toString(status, extendedStatus));
    switch (status) {
        case protocol::Status::CREATED:
            _performance.start_time = 0;
            _performance.finish_time = 0;
            break;
        case protocol::Status::IN_PROGRESS:
            _performance.setUpdateStart();
            _performance.finish_time = 0;
            break;
        case protocol::Status::IS_CANCELLING:
            break;
        case protocol::Status::CANCELLED:

            // Set the start time to some meaningful value in case if the request was
            // cancelled while sitting in the input queue
            if (0 == _performance.start_time) _performance.setUpdateStart();
            _performance.setUpdateFinish();
            break;

        case protocol::Status::SUCCESS:
        case protocol::Status::FAILED:
            _performance.setUpdateFinish();
            break;
        default:
            throw logic_error(CONTEXT + " unhandled status: " + protocol::toString(status));
    }

    // ATTENTION: the top-level status is the last to be modified in
    // the state transition to ensure clients will see a consistent state
    // of the object.
    _error = error;
    _extendedStatus = extendedStatus;
    _status = status;
}

void WorkerHttpRequest::_expired(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_TRACE,
         CONTEXT << (ec == boost::asio::error::operation_aborted ? " ** ABORTED **" : ""));

    replica::Lock lock(mtx, CONTEXT);

    // Clearing the stored callback after finishing the up-stream notification
    // has two purposes:
    //
    // 1. it guaranties no more than one time notification
    // 2. it breaks the up-stream dependency on a caller object if a shared
    //    pointer to the object was mentioned as the lambda-function's closure

    // Ignore this event if the timer was aborted
    if (ec != boost::asio::error::operation_aborted) {
        if (_onExpired != nullptr) {
            serviceProvider()->io_service().post(bind(move(_onExpired), _hdr.id));
        }
    }
    _onExpired = nullptr;
}

}  // namespace lsst::qserv::replica
