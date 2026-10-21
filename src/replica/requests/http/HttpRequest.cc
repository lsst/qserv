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
#include "replica/requests/http/HttpRequest.h"

// System headers
#include <algorithm>
#include <functional>
#include <sstream>
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "http/AsyncReq.h"
#include "qhttp/Status.h"
#include "replica/contr/Controller.h"
#include "replica/requests/http/HttpMessenger.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpRequest");

}  // namespace

namespace lsst::qserv::replica {

HttpRequest::HttpRequest(shared_ptr<Controller> const& controller, string const& type,
                         string const& workerName, int priority, bool keepTracking, bool disposeRequired)
        : RequestI(controller, type, workerName, priority, keepTracking, disposeRequired) {}

tuple<bool, json> HttpRequest::getResponseData(replica::Lock const& lock, string const& func,
                                               shared_ptr<http::AsyncReq> const& req, string const& error) {
    ExtendedState extendedState = ExtendedState::CLIENT_ERROR;
    string msg;
    if (!req) {
        msg = "No request object";
        if (!error.empty()) msg += ", error=" + error;
    } else if (!error.empty()) {
        msg = "Error: " + error;
    } else if (req->state() != http::AsyncReq::State::FINISHED) {
        msg = "http::AsyncReq failed with state=" + http::AsyncReq::state2str(req->state()) +
              ", error=" + req->errorMessage();
    } else if (req->responseCode() != qhttp::Status::STATUS_OK) {
        extendedState = ExtendedState::SERVER_ERROR;
        msg = "Unexpected HTTP code=" + qhttp::status2str(req->responseCode());
    } else if (req->responseBodySize() == 0) {
        msg = "Empty response data";
    } else {
        try {
            json const message = json::parse(req->responseBody());
            if (message.at("success").get<int>()) {
                return {true, message};
            }
            extendedState = ExtendedState::SERVER_ERROR;
            msg = "Worker reported failure, error: " + message.at("error").get<string>() +
                  ", error_ext: " + message.at("error_ext").dump();
        } catch (exception const& ex) {
            msg = "Failed to parse JSON response, ex: " + string(ex.what());
        }
    }
    LOGS(_log, LOG_LVL_ERROR, context() << func << "  " << msg);
    finish(lock, extendedState);
    return {false, json()};
}

void HttpRequest::awaken(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " ec.value=" << ec.value() << " ec.message=" << ec.message());

    if (isAborted(ec)) return;
    if (state() == State::FINISHED) return;
    replica::Lock lock(mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    try {
        send(lock, "/worker/request/track/" + id(), http::Method::GET);
    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  Exception: " << ex.what());
        finish(lock, ExtendedState::CLIENT_ERROR);
    }
}

void HttpRequest::send(replica::Lock const& lock, string const& target, http::Method method,
                       json const& requestJson) {
    _target = target;
    _method = method;
    _requestJson = requestJson;

    controller()->httpMessenger()->send(
            workerName(), id(), priority(), target, method, requestJson,
            [self = shared_from_base<HttpRequest>()](shared_ptr<http::AsyncReq> req, string error) {
                self->_analyze(req, error);
            },
            requestExpirationIvalSec());
}

void HttpRequest::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Ignore any exceptions thrown by the HTTP messenger.
    try {
        controller()->httpMessenger()->cancel(workerName(), id());
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, context() << __func__ << " id: " << id() << ", ex:  " << ex.what());
    }
}

void HttpRequest::dispose(replica::Lock const& lock, int priority, OnDisposeCallbackType const& onFinish) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    json const data = json::object({{"ids", json::array({id()})}});

    // Ignore any exceptions thrown by the HTTP messenger.
    try {
        controller()->httpMessenger()->send(
                workerName(), id(), priority, "/worker/request/dispose", http::Method::PUT, data,
                [onFinish](shared_ptr<http::AsyncReq> req, std::string error) {
                    if (!onFinish) return;
                    // The most comprehensive check for a successful HTTP response includes:
                    // 1. No error string.
                    // 2. The request object exists.
                    // 3. The request has finished.
                    // 4. The response code is HTTP 200 OK.
                    // 5. The response body is not empty.
                    // 6. The response body contains a valid JSON with a "success" field set to a non-zero
                    // value (true)
                    //    and a "resp" field with a "status" of SUCCESS.
                    // See: https://qserv.lsst.io/dev/api/repl-worker.html#response-objects
                    bool success = error.empty() && req &&
                                   (req->state() == http::AsyncReq::State::FINISHED) &&
                                   (req->responseCode() == qhttp::Status::STATUS_OK) &&
                                   (req->responseBodySize() > 0);
                    if (success) {
                        try {
                            auto const message = json::parse(req->responseBody());
                            success = message.at("success").get<int>() &&
                                      (protocol::toStatus(
                                               message.at("resp").get<json>().at("status").get<string>()) ==
                                       protocol::Status::SUCCESS);
                        } catch (...) {
                            success = false;
                        }
                    }
                    onFinish(success);
                },
                requestExpirationIvalSec());
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, context() << __func__ << " id: " << id() << ", ex:  " << ex.what());
    }
}

void HttpRequest::_analyze(shared_ptr<http::AsyncReq> req, string error) {
    // Enact the request retry protocol to handle cases when request could not be
    // completed successfully due to intermittent network hiccups or connection problems.
    //
    // Note that this decision can't be made by http::AsyncReq alone, because it doesn't have
    // the context of the request tracking protocol and the state of the request within
    // the replication system.
    if (req != nullptr && req->state() == http::AsyncReq::State::FAILED_RX) {
        if (state() == State::FINISHED) return;
        replica::Lock lock(mtx, context() + __func__);
        if (state() == State::FINISHED) return;

        send(lock, _target, _method, _requestJson);

    } else {
        analyze(req, error);
    }
}

}  // namespace lsst::qserv::replica
