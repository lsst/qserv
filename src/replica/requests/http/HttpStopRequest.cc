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
#include "replica/requests/http/HttpStopRequest.h"

// System headers
#include <stdexcept>
#include <sstream>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/proto/Protocol.h"
#include "replica/requests/http/HttpMessenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpStopRequest");
bool const disposeRequiredNo = false;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpStopRequest> HttpStopRequest::createAndStart(shared_ptr<Controller> const& controller,
                                                            string const& workerName,
                                                            string const& targetRequestId,
                                                            CallbackType const& onFinish, int priority,
                                                            bool keepTracking, string const& jobId,
                                                            unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpStopRequest>(
            new HttpStopRequest(controller, workerName, targetRequestId, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpStopRequest::HttpStopRequest(shared_ptr<Controller> const& controller, string const& workerName,
                                 string const& targetRequestId, CallbackType const& onFinish, int priority,
                                 bool keepTracking)
        : HttpRequest(controller, "REQUEST_STATUS", workerName, priority, keepTracking, ::disposeRequiredNo),
          _targetRequestId(targetRequestId),
          _onFinish(onFinish) {}

string HttpStopRequest::toString(bool extended) const {
    ostringstream oss;
    oss << RequestI::toString(extended) << "  targetRequestId: " + targetRequestId() << "\n";
    if ((state() == State::FINISHED) && (extendedState() == ExtendedState::SUCCESS)) {
        oss << "  targetStatus: " << protocol::toString(targetRequestStatus()) << "\n";
        oss << "  targetStatusExt: " << protocol::toString(targetRequestExtendedStatus()) << "\n";
        oss << "  targetPerformance: " << targetRequestPerformance() << "\n";
    } else {
        oss << "  targetStatus: N/A\n";
        oss << "  targetStatusExt: N/A\n";
        oss << "  targetPerformance: N/A\n";
    }
    return oss.str();
}

protocol::Status HttpStopRequest::targetRequestStatus() const {
    _assertIsSucceeded();
    return _targetStatus;
}

protocol::StatusExt HttpStopRequest::targetRequestExtendedStatus() const {
    _assertIsSucceeded();
    return _targetExtendedServerStatus;
}

Performance const& HttpStopRequest::targetRequestPerformance() const {
    _assertIsSucceeded();
    return _targetPerformance;
}

void HttpStopRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpStopRequest>(lock, _onFinish);
}

void HttpStopRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}
void HttpStopRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    send(lock, "/worker/replica/stop/" + _targetRequestId, http::Method::PUT);
}

void HttpStopRequest::analyze(shared_ptr<http::AsyncReq> req, string error) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  error=" << error);

    if (state() == State::FINISHED) return;
    replica::Lock lock(mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    auto [success, message] = getResponseData(lock, __func__, req, error);
    if (!success) return;

    // Parse the message and extract request status and metadata. Any failure will be interpreted
    // as a client error. Note that status, extended status and the performance data returned by
    // the server are related to the target request and not to the status request itself.
    // The only abnormality of the implementation is the performance data which is shared between
    // the status request and the target request. This problem is considered acceptable for the current
    // implementation.
    try {
        json const& resp = message.at("resp");
        _targetStatus = protocol::toStatus(resp.at("status").get<string>());
        _targetExtendedServerStatus = protocol::toStatusExt(resp.at("status_ext").get<string>());
        mutablePerformance().update(resp.at("perf").get<json>());
        _targetPerformance = performance();
        finish(lock, SUCCESS);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse response: " << ex.what());
        finish(lock, CLIENT_ERROR);
    }
}

void HttpStopRequest::_assertIsSucceeded() const {
    if (state() != State::FINISHED) {
        throw logic_error("HttpStopRequest::" + string(__func__) +
                          "  the status request hasn't finished yet");
    }
    if (extendedState() != ExtendedState::SUCCESS) {
        throw logic_error("HttpStopRequest::" + string(__func__) + "  the status request has failed");
    }
}

}  // namespace lsst::qserv::replica
