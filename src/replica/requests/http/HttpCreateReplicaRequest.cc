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
#include "replica/requests/http/HttpCreateReplicaRequest.h"

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
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpCreateReplicaRequest");
bool const disposeRequired = true;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpCreateReplicaRequest> HttpCreateReplicaRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& sourceWorkerName,
        string const& database, unsigned int chunk, CallbackType const& onFinish, int priority,
        bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpCreateReplicaRequest>(new HttpCreateReplicaRequest(
            controller, workerName, sourceWorkerName, database, chunk, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpCreateReplicaRequest::HttpCreateReplicaRequest(shared_ptr<Controller> const& controller,
                                                   string const& workerName, string const& sourceWorkerName,
                                                   string const& database, unsigned int chunk,
                                                   CallbackType const& onFinish, int priority,
                                                   bool keepTracking)
        : HttpRequest(controller, "REPLICA_CREATE", workerName, priority, keepTracking, ::disposeRequired),
          _database(database),
          _chunk(chunk),
          _sourceWorkerName(sourceWorkerName),
          _onFinish(onFinish),
          _replicaInfo() {}

void HttpCreateReplicaRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The delayed assertions are needed to prevent throwing exceptions from
    // within constructors.
    controller()->serviceProvider()->config()->assertWorkerIsValid(sourceWorkerName());
    controller()->serviceProvider()->config()->assertWorkersAreDifferent(sourceWorkerName(), workerName());

    ConfigWorker const sourceConfigWorker =
            controller()->serviceProvider()->config()->worker(sourceWorkerName());

    json const requestJson = {{"id", id()},
                              {"priority", priority()},
                              {"timeout", requestExpirationIvalSec()},
                              {"params",
                               {{"database", database()},
                                {"chunk", chunk()},
                                {"worker", sourceWorkerName()},
                                {"worker_host", sourceConfigWorker.fsHost.addr},
                                {"worker_port", sourceConfigWorker.fsPort}}}};
    send(lock, "/worker/replica/create", http::Method::POST, requestJson);
}

void HttpCreateReplicaRequest::analyze(replica::Lock const& lock, shared_ptr<http::AsyncReq> req,
                                       string error) {
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

void HttpCreateReplicaRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<HttpCreateReplicaRequest>(lock, _onFinish);
}

void HttpCreateReplicaRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> HttpCreateReplicaRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("chunk", to_string(chunk()));
    result.emplace_back("source_worker", sourceWorkerName());
    return result;
}

}  // namespace lsst::qserv::replica
