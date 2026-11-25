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
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ProtocolBuffer.h"
#include "replica/util/ReplicaInfo.h"

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "nlohmann/json.hpp"

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

void HttpEchoRequest::analyze(replica::Lock const& lock, shared_ptr<http::AsyncReq> req, string error) {
    auto const onSuccess = [&](replica::Lock const& lock, json const& message) {
        try {
            _responseData = message.at("resp").at("result").get<string>();
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse result: " << ex.what());
            finish(lock, CLIENT_ERROR);
            return;
        }
        finish(lock, SUCCESS);
    };
    standardAnalyze(lock, req, error, onSuccess);
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
