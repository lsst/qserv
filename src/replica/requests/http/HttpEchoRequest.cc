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
#include "replica/requests/http/HttpEchoRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/Method.h"
#include "http/AsyncReq.h"
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/proto/Protocol.h"
#include "replica/requests/Messenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ProtocolBuffer.h"
#include "replica/util/ReplicaInfo.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpEchoRequest");
bool const disposeRequired = true;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpEchoRequest> HttpEchoRequest::createAndStart(shared_ptr<Controller> const& controller,
                                                            string const& workerName, string const& data,
                                                            uint64_t delay, CallbackType const& onFinish,
                                                            int priority, bool keepTracking,
                                                            std::string const& jobId,
                                                            unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpEchoRequest>(
            new HttpEchoRequest(controller, workerName, data, delay, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpEchoRequest::HttpEchoRequest(shared_ptr<Controller> const& controller, string const& workerName,
                                 string const& data, uint64_t delay, CallbackType const& onFinish,
                                 int priority, bool keepTracking)
        : HttpRequest(controller, "TEST_ECHO", workerName, priority, keepTracking, ::disposeRequired),
          _data(data),
          _delay(delay),
          _onFinish(onFinish) {}

string const& HttpEchoRequest::responseData() const { return _responseData; }

void HttpEchoRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  worker: " << workerName() << " data.length: " << data().size()
                   << " delay: " << delay());

    json const requestJson = {{"id", id()},
                              {"priority", priority()},
                              {"timeout", requestExpirationIvalSec()},
                              {"params", {{"delay", delay()}, {"data", data()}}}};
    send(lock, "/worker/echo", http::Method::POST, requestJson);
}

void HttpEchoRequest::analyze(shared_ptr<http::AsyncReq> req, string error) {
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
                _responseData = message.at("resp").at("result").get<string>();
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse result: " << ex.what());
                finish(lock, CLIENT_ERROR);
                return;
            }
            finish(lock, SUCCESS);
            break;
        case protocol::Status::CREATED:
            keepTrackingOrFinish(lock, SERVER_CREATED);
            break;
        case protocol::Status::QUEUED:
            keepTrackingOrFinish(lock, SERVER_QUEUED);
            break;
        case protocol::Status::IN_PROGRESS:
            keepTrackingOrFinish(lock, SERVER_IN_PROGRESS);
            break;
        case protocol::Status::IS_CANCELLING:
            keepTrackingOrFinish(lock, SERVER_IS_CANCELLING);
            break;
        case protocol::Status::BAD:
            finish(lock, SERVER_BAD);
            break;
        case protocol::Status::FAILED:
            finish(lock, SERVER_ERROR);
            break;
        case protocol::Status::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;
        default:
            throw logic_error("HttpEchoRequest::" + string(__func__) + "  unknown status '" +
                              protocol::toString(status) + "' received from server");
    }
}

void HttpEchoRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<HttpEchoRequest>(lock, _onFinish);
}

void HttpEchoRequest::savePersistentState(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> HttpEchoRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("data_length_bytes", to_string(data().size()));
    result.emplace_back("delay_milliseconds", to_string(delay()));
    return result;
}

}  // namespace lsst::qserv::replica
