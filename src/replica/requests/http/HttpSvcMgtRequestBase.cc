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
#include "replica/requests/http/HttpSvcMgtRequestBase.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/AsyncReq.h"
#include "http/Method.h"
#include "replica/contr/Controller.h"
#include "replica/requests/http/HttpMessenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "nlohmann/json.hpp"

using namespace std;
using namespace nlohmann;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpSvcMgtRequest");

bool const keepTrackingNo = false;
bool const disposeRequiredNo = false;

}  // namespace

namespace lsst::qserv::replica {

void HttpSvcMgtRequestBase::extendedPrinter(shared_ptr<HttpSvcMgtRequestBase> const& ptr) {
    RequestI::defaultPrinter(ptr);
    cout << ptr->getServiceState();
}

ServiceStateInfo const& HttpSvcMgtRequestBase::getServiceState() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    switch (RequestI::state()) {
        case RequestI::State::FINISHED:
            switch (RequestI::extendedState()) {
                case RequestI::ExtendedState::SUCCESS:
                case RequestI::ExtendedState::SERVER_ERROR:
                    return _serviceState;
                default:
                    break;
            }
        default:
            break;
    }
    throw logic_error("SvcMgtRequestBase::" + string(__func__) +
                      "  not allowed in the current state of the request");
}

HttpSvcMgtRequestBase::HttpSvcMgtRequestBase(shared_ptr<Controller> const& controller,
                                             char const* requestName, string const& workerName,
                                             protocol::ServiceRequestType requestType, int priority)
        : HttpRequest(controller, requestName, workerName, priority, ::keepTrackingNo, ::disposeRequiredNo),
          _requestType(requestType) {}

void HttpSvcMgtRequestBase::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    switch (_requestType) {
        case protocol::ServiceRequestType::SERVICE_SUSPEND:
            send(lock, "/worker/service/suspend", http::Method::PUT);
            break;
        case protocol::ServiceRequestType::SERVICE_RESUME:
            send(lock, "/worker/service/resume", http::Method::PUT);
            break;
        case protocol::ServiceRequestType::SERVICE_STATUS:
            send(lock, "/worker/service/status", http::Method::GET);
            break;
        case protocol::ServiceRequestType::SERVICE_REQUESTS:
            send(lock, "/worker/service/requests", http::Method::GET);
            break;
        case protocol::ServiceRequestType::SERVICE_DRAIN:
            send(lock, "/worker/service/drain", http::Method::PUT);
            break;
        case protocol::ServiceRequestType::SERVICE_RECONFIG:
            send(lock, "/worker/service/reconfig", http::Method::PUT);
            break;
        default:
            throw logic_error("SvcMgtRequestBase::" + string(__func__) +
                              "  not allowed for request type: " + to_string(static_cast<int>(_requestType)));
    }
}

void HttpSvcMgtRequestBase::analyze(shared_ptr<http::AsyncReq> req, string error) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  error=" << error);

    if (state() == State::FINISHED) return;
    replica::Lock lock(mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    auto [success, message] = getResponseData(lock, __func__, req, error);
    if (!success) return;

    // Parse the message and extract request status and metadata. Any failure will be interpreted
    // as a client error. Note that the result of the request is contained in the "result" field
    // of the response. The result is extracted only if the request successfully finished
    // with protocol::Status::SUCCESS.
    protocol::Status status;
    try {
        json const& resp = message.at("resp");
        status = protocol::toStatus(resp.at("status").get<string>());
        setExtendedServerStatus(lock, protocol::toStatusExt(resp.at("status_ext").get<string>()));
        mutablePerformance().update(resp.at("perf").get<json>());
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse response: " << ex.what());
        finish(lock, CLIENT_ERROR);
        return;
    }
    switch (status) {
        case protocol::Status::SUCCESS:
            try {
                json const& result = message.at("resp").at("result");
                _serviceState.set(result);
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse result: " << ex.what());
                finish(lock, CLIENT_ERROR);
                return;
            }
            finish(lock, SUCCESS);
            break;
        default:
            finish(lock, SERVER_ERROR);
            break;
    }
}

void HttpSvcMgtRequestBase::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

}  // namespace lsst::qserv::replica
