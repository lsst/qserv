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
#include "replica/requests/http/HttpFindAllRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/AsyncReq.h"
#include "http/Method.h"
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/mysql/DatabaseMySQLExceptions.h"
#include "replica/proto/Protocol.h"
#include "replica/requests/http/HttpMessenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpFindAllRequest");
bool const disposeRequired = true;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpFindAllRequest> HttpFindAllRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        bool saveReplicaInfo, CallbackType const& onFinish, int priority, bool keepTracking,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpFindAllRequest>(new HttpFindAllRequest(
            controller, workerName, database, saveReplicaInfo, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpFindAllRequest::HttpFindAllRequest(shared_ptr<Controller> const& controller, string const& workerName,
                                       string const& database, bool saveReplicaInfo,
                                       CallbackType const& onFinish, int priority, bool keepTracking)
        : HttpRequest(controller, "REPLICA_FIND_ALL", workerName, priority, keepTracking, ::disposeRequired),
          _database(database),
          _saveReplicaInfo(saveReplicaInfo),
          _onFinish(onFinish) {}

const ReplicaInfoCollection& HttpFindAllRequest::responseData() const { return _replicaInfoCollection; }

void HttpFindAllRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    json const requestJson = {{"id", id()},
                              {"priority", priority()},
                              {"timeout", requestExpirationIvalSec()},
                              {"params", {{"database", database()}}}};
    send(lock, "/worker/replica/find-all", http::Method::POST, requestJson);
}

void HttpFindAllRequest::analyze(shared_ptr<http::AsyncReq> req, string error) {
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
                for (json const& replicaInfo : result.at("replica_info_many")) {
                    _replicaInfoCollection.emplace_back(replicaInfo);
                }
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse result: " << ex.what());
                finish(lock, CLIENT_ERROR);
                return;
            }
            try {
                if (saveReplicaInfo()) {
                    controller()->serviceProvider()->databaseServices()->saveReplicaInfoCollection(
                            workerName(), database(), _replicaInfoCollection);
                }
                finish(lock, SUCCESS);
            } catch (database::mysql::ER_NO_REFERENCED_ROW_2_ const& ex) {
                // If the database was already removed from the configuration this error
                // should be ignored as the intent of the request has been achieved.
                if (!controller()->serviceProvider()->config()->isKnownDatabase(database())) {
                    LOGS(_log, LOG_LVL_WARN,
                         context() << __func__ << " database '" << database()
                                   << "' is no longer valid per the configuration - "
                                      "the replica info saving will be skipped");
                    finish(lock, SUCCESS);
                } else {
                    LOGS(_log, LOG_LVL_ERROR,
                         context() << __func__ << " failed to save replica info collection into a database: "
                                   << ex.what());
                    finish(lock, CLIENT_ERROR);
                }
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR,
                     context() << __func__
                               << " failed to save replica info collection into a database: " << ex.what());
                finish(lock, CLIENT_ERROR);
            }
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
            if (!controller()->serviceProvider()->config()->isKnownDatabase(database())) {
                LOGS(_log, LOG_LVL_WARN,
                     context() << __func__ << " database '" << database()
                               << "' is no longer valid per the configuration - "
                                  "the replica info saving will be skipped");
                // Make sure to reset the extended server status before finishing the request
                // with SUCCESS.
                setExtendedServerStatus(lock, protocol::StatusExt::NONE);
                finish(lock, SUCCESS);
            } else {
                finish(lock, SERVER_ERROR);
            }
            break;
        case protocol::Status::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;
        default:
            throw logic_error("HttpFindAllRequest::" + string(__func__) + " unknown status '" +
                              protocol::toString(status) + "' received from server");
    }
}

void HttpFindAllRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<HttpFindAllRequest>(lock, _onFinish);
}

void HttpFindAllRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> HttpFindAllRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    return result;
}

}  // namespace lsst::qserv::replica
