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
#include "replica/WorkerRequest.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/SuccessRateGenerator.h"
#include "util/BlockPost.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerRequest");

/// Maximum duration for the request execution
unsigned int const maxDurationMillisec = 10000;

/// Random interval for the incremental execution
lsst::qserv::util::BlockPost incrementIvalMillisec(1000, 2000);

/// Random generator of success/failure rates
lsst::qserv::replica::SuccessRateGenerator successRateGenerator(0.9);

}  // namespace

namespace lsst::qserv::replica {

util::Mutex WorkerRequest::_mtxDataFolderOperations;

string WorkerRequest::status2string(ProtocolStatus status) { return ProtocolStatus_Name(status); }

string WorkerRequest::status2string(ProtocolStatus status, ProtocolStatusExt extendedStatus) {
    return status2string(status) + "::" + replica::status2string(extendedStatus);
}

WorkerRequest::WorkerRequest(ServiceProvider::Ptr const& serviceProvider, string const& worker,
                             string const& type, string const& id, int priority,
                             ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec)
        : _serviceProvider(serviceProvider),
          _worker(worker),
          _type(type),
          _id(id),
          _priority(priority),
          _onExpired(onExpired),
          _requestExpirationIvalSec(
                  requestExpirationIvalSec == 0
                          ? serviceProvider->config()->get<unsigned int>("controller", "request-timeout-sec")
                          : requestExpirationIvalSec),
          _requestExpirationTimer(serviceProvider->io_service()),
          _status(ProtocolStatus::CREATED),
          _extendedStatus(ProtocolStatusExt::NONE),
          _performance(),
          _durationMillisec(0) {}

WorkerRequest::~WorkerRequest() { dispose(); }

WorkerRequest::ErrorContext WorkerRequest::reportErrorIf(bool errorCondition,
                                                         ProtocolStatusExt extendedStatus,
                                                         string const& errorMsg) {
    WorkerRequest::ErrorContext errorContext;
    if (errorCondition) {
        errorContext.failed = true;
        errorContext.extendedStatus = extendedStatus;
        LOGS(_log, LOG_LVL_ERROR, context() << "execute" << errorMsg);
    }
    return errorContext;
}

void WorkerRequest::init() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));

    if (status() != ProtocolStatus::CREATED) return;

    // Start the expiration timer

    if (_requestExpirationIvalSec != 0) {
        _requestExpirationTimer.cancel();
        _requestExpirationTimer.expires_from_now(boost::posix_time::seconds(_requestExpirationIvalSec));
        _requestExpirationTimer.async_wait(bind(&WorkerRequest::_expired, shared_from_this(), _1));

        LOGS(_log, LOG_LVL_DEBUG,
             context() << __func__ << "  started expiration timer with "
                       << " _requestExpirationIvalSec: " << _requestExpirationIvalSec);
    }
}

void WorkerRequest::start() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));

    switch (status()) {
        case ProtocolStatus::CREATED:
            setStatus(lock, ProtocolStatus::IN_PROGRESS);
            break;
        default:
            throw logic_error(context(__func__) +
                              "  not allowed while in status: " + WorkerRequest::status2string(status()));
    }
}

bool WorkerRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));

    // Simulate request 'processing' for some maximum duration of time (milliseconds)
    // while making a progress through increments of random duration of time.
    // Success/failure modes will be also simulated using the corresponding generator.

    switch (status()) {
        case ProtocolStatus::IN_PROGRESS:
            break;
        case ProtocolStatus::IS_CANCELLING:
            setStatus(lock, ProtocolStatus::CANCELLED);
            throw WorkerRequestCancelled();
        default:
            throw logic_error(context(__func__) +
                              "  not allowed while in status: " + WorkerRequest::status2string(status()));
    }

    _durationMillisec += ::incrementIvalMillisec.wait();

    if (_durationMillisec < ::maxDurationMillisec) return false;

    setStatus(lock, ::successRateGenerator.success() ? ProtocolStatus::SUCCESS : ProtocolStatus::FAILED);
    return true;
}

void WorkerRequest::cancel() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));

    switch (status()) {
        case ProtocolStatus::QUEUED:
        case ProtocolStatus::CREATED:
        case ProtocolStatus::CANCELLED:
            setStatus(lock, ProtocolStatus::CANCELLED);
            break;
        case ProtocolStatus::IN_PROGRESS:
        case ProtocolStatus::IS_CANCELLING:
            setStatus(lock, ProtocolStatus::IS_CANCELLING);
            break;

        // Nothing to be done to the completed requests
        case ProtocolStatus::SUCCESS:
        case ProtocolStatus::BAD:
        case ProtocolStatus::FAILED:
            break;
    }
}

void WorkerRequest::rollback() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));

    switch (status()) {
        case ProtocolStatus::CREATED:
        case ProtocolStatus::IN_PROGRESS:
            setStatus(lock, ProtocolStatus::CREATED);
            break;
        case ProtocolStatus::IS_CANCELLING:
            setStatus(lock, ProtocolStatus::CANCELLED);
            throw WorkerRequestCancelled();
            break;
        default:
            throw logic_error(context(__func__) +
                              "  not allowed while in status: " + WorkerRequest::status2string(status()));
    }
}

void WorkerRequest::stop() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));
    setStatus(lock, ProtocolStatus::CREATED);
}

void WorkerRequest::dispose() noexcept {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));
    if (_requestExpirationIvalSec != 0) {
        try {
            _requestExpirationTimer.cancel();
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_DEBUG,
                 context(__func__) << "  request expiration couldn't be cancelled, ex: " << ex.what());
        }
    }
}

void WorkerRequest::setStatus(util::Lock const& lock, ProtocolStatus status,
                              ProtocolStatusExt extendedStatus) {
    LOGS(_log, LOG_LVL_DEBUG,
         context(__func__) << "  " << WorkerRequest::status2string(_status, _extendedStatus) << " -> "
                           << WorkerRequest::status2string(status, extendedStatus));

    switch (status) {
        case ProtocolStatus::CREATED:
            _performance.start_time = 0;
            _performance.finish_time = 0;
            break;
        case ProtocolStatus::IN_PROGRESS:
            _performance.setUpdateStart();
            _performance.finish_time = 0;
            break;
        case ProtocolStatus::IS_CANCELLING:
            break;
        case ProtocolStatus::CANCELLED:

            // Set the start time to some meaningful value in case if the request was
            // cancelled while sitting in the input queue

            if (0 == _performance.start_time) _performance.setUpdateStart();

            _performance.setUpdateFinish();
            break;

        case ProtocolStatus::SUCCESS:
        case ProtocolStatus::FAILED:
            _performance.setUpdateFinish();
            break;
        default:
            throw logic_error(context(__func__) + "  unhandled status: " + to_string(status));
    }

    // ATTENTION: the top-level status is the last to be modified in
    // the state transition to ensure clients will see a consistent state
    // of the object.

    _extendedStatus = extendedStatus;
    _status = status;
}

void WorkerRequest::_expired(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << (ec == boost::asio::error::operation_aborted ? "  ** ABORTED **" : ""));
    util::Lock lock(_mtx, context(__func__));

    // Clearing the stored callback after finishing the up-stream notification
    // has two purposes:
    //
    // 1. it guaranties no more than one time notification
    // 2. it breaks the up-stream dependency on a caller object if a shared
    //    pointer to the object was mentioned as the lambda-function's closure

    // Ignore this event if the timer was aborted
    if (ec != boost::asio::error::operation_aborted) {
        if (_onExpired != nullptr) {
            serviceProvider()->io_service().post(bind(move(_onExpired), _id));
        }
    }
    _onExpired = nullptr;
}

}  // namespace lsst::qserv::replica
