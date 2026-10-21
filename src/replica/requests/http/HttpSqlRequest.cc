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
#include "replica/requests/http/HttpSqlRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/Method.h"
#include "http/AsyncReq.h"
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/requests/http/HttpMessenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "util/String.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpSqlRequest");
bool const disposeRequired = true;
}  // namespace

namespace lsst::qserv::replica {

void HttpSqlRequest::extendedPrinter(shared_ptr<HttpSqlRequest> const& ptr) {
    RequestI::defaultPrinter(ptr);
    for (auto&& itr : ptr->responseData().queryResultSet) {
        auto&& scope = itr.first;
        auto&& resultSet = itr.second;
        if (resultSet.hasResult) {
            string const caption = "RESULT SET [" + scope + "]";
            string const indent = "";
            auto const table = resultSet.toColumnTable(caption, indent);
            bool const topSeparator = false;
            bool const bottomSeparator = false;
            bool const repeatedHeader = false;
            size_t const pageSize = 0;
            table.print(cout, topSeparator, bottomSeparator, pageSize, repeatedHeader);
        }
    }
}

HttpSqlRequest::HttpSqlRequest(shared_ptr<Controller> const& controller, std::string const& requestName,
                               protocol::SqlRequestType type, string const& databaseName, bool batchMode,
                               json const& params, string const& workerName, uint64_t maxRows, int priority,
                               bool keepTracking, http::BinaryEncodingMode binaryEncodingMode)
        : HttpRequest(controller, requestName, workerName, priority, keepTracking, ::disposeRequired),
          _params(params),
          _maxRows(maxRows),
          _binaryEncodingMode(binaryEncodingMode) {
    _params["sql_request_type"] = protocol::toString(type);
    _params["database"] = databaseName;
    _params["batch_mode"] = batchMode ? 1 : 0;
    _params["max_rows"] = maxRows;
    _params["binary_encoding"] = http::binaryEncoding2string(binaryEncodingMode);
}

SqlResultSet const& HttpSqlRequest::responseData() const { return _responseData; }

list<pair<string, string>> HttpSqlRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    for (auto& [key, val] : _params.items()) {
        result.emplace_back(key, val.dump());
    }
    return result;
}

void HttpSqlRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    json const requestJson = {{"id", id()},
                              {"priority", priority()},
                              {"timeout", requestExpirationIvalSec()},
                              {"params", _params}};
    send(lock, "/worker/sql", http::Method::POST, requestJson);
}

void HttpSqlRequest::analyze(shared_ptr<http::AsyncReq> req, string error) {
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
                _responseData.set(message.at("resp").at("result"), _binaryEncodingMode);
                _responseData.performanceSec =
                        (util::TimeUtils::now() - performance(lock).c_create_time) / 1000.;
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
            throw logic_error("HttpSqlRequest::" + string(__func__) + "  unknown status '" +
                              protocol::toString(status) + "' received from server");
    }
}

void HttpSqlRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

}  // namespace lsst::qserv::replica
