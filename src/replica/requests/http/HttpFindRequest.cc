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
#include "replica/requests/http/HttpFindRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/Method.h"
#include "http/AsyncReq.h"
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/proto/Protocol.h"
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
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpFindRequest");
bool const disposeRequired = true;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpFindRequest> HttpFindRequest::createAndStart(shared_ptr<Controller> const& controller,
                                                            string const& workerName, string const& database,
                                                            unsigned int chunk, CallbackType const& onFinish,
                                                            int priority, bool computeCheckSum,
                                                            bool keepTracking, string const& jobId,
                                                            unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpFindRequest>(new HttpFindRequest(
            controller, workerName, database, chunk, onFinish, priority, computeCheckSum, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpFindRequest::HttpFindRequest(shared_ptr<Controller> const& controller, string const& workerName,
                                 string const& database, unsigned int chunk, CallbackType const& onFinish,
                                 int priority, bool computeCheckSum, bool keepTracking)
        : HttpRequest(controller, "REPLICA_FIND", workerName, priority, keepTracking, ::disposeRequired),
          _database(database),
          _chunk(chunk),
          _computeCheckSum(computeCheckSum),
          _onFinish(onFinish) {}

ReplicaInfo const& HttpFindRequest::responseData() const { return _replicaInfo; }

void HttpFindRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " "
                   << " worker: " << workerName() << " database: " << database() << " chunk: " << chunk()
                   << " computeCheckSum: " << (computeCheckSum() ? "true" : "false"));

    json const requestJson = {
            {"id", id()},
            {"priority", priority()},
            {"timeout", requestExpirationIvalSec()},
            {"params",
             {{"database", database()}, {"chunk", chunk()}, {"compute_cs", computeCheckSum() ? 1 : 0}}}};
    send(lock, "/worker/replica/find", http::Method::POST, requestJson);
}

void HttpFindRequest::analyze(replica::Lock const& lock, shared_ptr<http::AsyncReq> req, string error) {
    auto const onSuccess = [&](replica::Lock const& lock, json const& message) {
        try {
            _replicaInfo = ReplicaInfo(message.at("resp").at("result"));
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse result: " << ex.what());
            finish(lock, CLIENT_ERROR);
            return;
        }
        controller()->serviceProvider()->databaseServices()->saveReplicaInfo(_replicaInfo);
        finish(lock, SUCCESS);
    };
    standardAnalyze(lock, req, error, onSuccess);
}

void HttpFindRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<HttpFindRequest>(lock, _onFinish);
}

void HttpFindRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> HttpFindRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("chunk", to_string(chunk()));
    return result;
}

}  // namespace lsst::qserv::replica
