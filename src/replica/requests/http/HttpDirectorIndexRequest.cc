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
#include "replica/requests/http/HttpDirectorIndexRequest.h"

// System headers
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <system_error>

// Qserv headers
#include "http/AsyncReq.h"
#include "http/Method.h"
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/proto/Protocol.h"
#include "replica/requests/Messenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ProtocolBuffer.h"

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "nlohmann/json.hpp"

using namespace std;
using namespace nlohmann;
namespace fs = std::filesystem;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpDirectorIndexRequest");
bool const disposeRequired = true;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpDirectorIndexRequest> HttpDirectorIndexRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        string const& directorTable, unsigned int chunk, bool hasTransactions, TransactionId transactionId,
        CallbackType const& onFinish, int priority, bool keepTracking, string const& jobId,
        unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpDirectorIndexRequest>(
            new HttpDirectorIndexRequest(controller, workerName, database, directorTable, chunk,
                                         hasTransactions, transactionId, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpDirectorIndexRequest::HttpDirectorIndexRequest(shared_ptr<Controller> const& controller,
                                                   string const& workerName, string const& database,
                                                   string const& directorTable, unsigned int chunk,
                                                   bool hasTransactions, TransactionId transactionId,
                                                   CallbackType const& onFinish, int priority,
                                                   bool keepTracking)
        : HttpRequest(controller, "INDEX", workerName, priority, keepTracking, ::disposeRequired),
          _database(database),
          _directorTable(directorTable),
          _chunk(chunk),
          _hasTransactions(hasTransactions),
          _transactionId(transactionId),
          _onFinish(onFinish) {
    _responseData.workerName = workerName;
    _responseData.fileName =
            controller->serviceProvider()->config()->get<string>("database", "qserv-master-tmp-dir") + "/" +
            database + "_" + directorTable + "_" + to_string(chunk) +
            (hasTransactions ? "_p" + to_string(transactionId) : "");
}

HttpDirectorIndexRequest::~HttpDirectorIndexRequest() {
    // Make the best attempt to get rid of the temporary file. Ignore any errors
    // for now. Just report them.
    std::error_code ec;
    fs::remove(fs::path(_responseData.fileName), ec);
    if (ec.value() != 0) {
        LOGS(_log, LOG_LVL_WARN,
             context() << "::" << __func__ << "  "
                       << "failed to remove the temporary file '" << _responseData.fileName);
    }
}

DirectorIndexRequestInfo const& HttpDirectorIndexRequest::responseData() const { return _responseData; }

void HttpDirectorIndexRequest::startImpl(replica::Lock const& lock) {
    json const requestJson = {{"id", id()},
                              {"priority", priority()},
                              {"timeout", requestExpirationIvalSec()},
                              {"params",
                               {{"database", database()},
                                {"director_table", directorTable()},
                                {"has_transaction", hasTransactions() ? 1 : 0},
                                {"transaction_id", transactionId()},
                                {"chunk", chunk()}}}};
    send(lock, "/worker/director-index", http::Method::POST, requestJson);
}

void HttpDirectorIndexRequest::analyze(shared_ptr<http::AsyncReq> req, string error) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  error=" << error);

    if (state() == State::FINISHED) return;
    replica::Lock lock(mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    // If the request is currently reading data from the remote file, delegate
    // to the appropriate handler.
    if (_readingData) {
        _writeDataIntoFileAndFinish(lock, req, error);
        return;
    }

    // Otherwise, analyze the response as usual.
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

        // The response object may carry a possible MySQL error code in case
        // if the request failed to be processed by the worker.
        _responseData.error = message.at("resp").at("error").get<string>();
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse response: " << ex.what());
        finish(lock, CLIENT_ERROR);
        return;
    }
    switch (status) {
        case protocol::Status::SUCCESS: {
            try {
                json const& result = message.at("resp").at("result");
                _responseData.remoteFileResource = result.at("file_resource").get<string>();
                _responseData.fileSizeBytes = result.at("file_size_bytes").get<size_t>();
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, context() << __func__ << " failed to parse result: " << ex.what());
                finish(lock, CLIENT_ERROR);
                return;
            }

            // Initiate a request for retrieving data from the worker into the temporary file.
            try {
                send(lock, _responseData.remoteFileResource, http::Method::GET);
                _readingData = true;
            } catch (std::exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  Exception: " << ex.what());
                finish(lock, CLIENT_ERROR);
            }
            break;
        }
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
            if (!_responseData.error.empty()) {
                LOGS(_log, LOG_LVL_ERROR,
                     context() << __func__ << " request failed with error: " << _responseData.error);
            }
            finish(lock, SERVER_ERROR);
            break;
        case protocol::Status::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;
        default:
            throw logic_error("HttpDirectorIndexRequest::" + string(__func__) + "  unknown status '" +
                              protocol::toString(status) + "' received from server");
    }
}

void HttpDirectorIndexRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpDirectorIndexRequest>(lock, _onFinish);
}

void HttpDirectorIndexRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> HttpDirectorIndexRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("director_table", directorTable());
    result.emplace_back("chunk", to_string(chunk()));
    result.emplace_back("has_transactions", bool2str(hasTransactions()));
    result.emplace_back("transaction_id", to_string(transactionId()));
    return result;
}

void HttpDirectorIndexRequest::_writeDataIntoFileAndFinish(replica::Lock const& lock,
                                                           shared_ptr<http::AsyncReq> req, string error) {
    // Delete the remote file regardless of an outcome of the file retrieval request. Ignore any exceptions.
    try {
        send(lock, _responseData.remoteFileResource, http::Method::DELETE);
    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  Exception: " << ex.what());
    }

    // Analyze the response.
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
    }
    if (!msg.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  " << msg);
        finish(lock, extendedState);
        return;
    }

    // Write data into the file
    ofstream file(_responseData.fileName, ios::binary | ios::out | ios::trunc);
    if (!file.good()) {
        LOGS(_log, LOG_LVL_ERROR,
             context() << __func__ << " failed to open/create/append file: " + _responseData.fileName);
        finish(lock, ExtendedState::CLIENT_ERROR);
        return;
    }
    file << req->responseBody();
    file.close();
    finish(lock, SUCCESS);
}

}  // namespace lsst::qserv::replica
