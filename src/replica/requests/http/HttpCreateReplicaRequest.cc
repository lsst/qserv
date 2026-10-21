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

void HttpCreateReplicaRequest::analyze(shared_ptr<http::AsyncReq> req, string error) {
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
                _replicaInfo = ReplicaInfo(message.at("resp").at("result"));
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse result: " << ex.what());
                finish(lock, CLIENT_ERROR);
                return;
            }
            controller()->serviceProvider()->databaseServices()->saveReplicaInfo(_replicaInfo);
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
            throw logic_error("HttpCreateReplicaRequest::" + string(__func__) + "  unknown status '" +
                              protocol::toString(status) + "' received from server");
    }
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
