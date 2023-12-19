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
#include "replica/ingest/TransactionContrib.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/mysql/DatabaseMySQLTypes.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

string TransactionContribInfo::typeSelector2str(TypeSelector typeSelector) {
    switch (typeSelector) {
        case TypeSelector::SYNC:
            return "SYNC";
        case TypeSelector::ASYNC:
            return "ASYNC";
        case TypeSelector::SYNC_OR_ASYNC:
            return "SYNC_OR_ASYNC";
    };
    throw runtime_error("DatabaseServices::" + string(__func__) + "  unhandled type selector " +
                        to_string(static_cast<int>(typeSelector)));
}

map<TransactionContribInfo::Status, string> const TransactionContribInfo::_transactionContribStatus2str = {
        {TransactionContribInfo::Status::IN_PROGRESS, "IN_PROGRESS"},
        {TransactionContribInfo::Status::CREATE_FAILED, "CREATE_FAILED"},
        {TransactionContribInfo::Status::START_FAILED, "START_FAILED"},
        {TransactionContribInfo::Status::READ_FAILED, "READ_FAILED"},
        {TransactionContribInfo::Status::LOAD_FAILED, "LOAD_FAILED"},
        {TransactionContribInfo::Status::CANCELLED, "CANCELLED"},
        {TransactionContribInfo::Status::FINISHED, "FINISHED"}};

map<string, TransactionContribInfo::Status> const TransactionContribInfo::_transactionContribStr2status = {
        {"IN_PROGRESS", TransactionContribInfo::Status::IN_PROGRESS},
        {"CREATE_FAILED", TransactionContribInfo::Status::CREATE_FAILED},
        {"START_FAILED", TransactionContribInfo::Status::START_FAILED},
        {"READ_FAILED", TransactionContribInfo::Status::READ_FAILED},
        {"LOAD_FAILED", TransactionContribInfo::Status::LOAD_FAILED},
        {"CANCELLED", TransactionContribInfo::Status::CANCELLED},
        {"FINISHED", TransactionContribInfo::Status::FINISHED}};

vector<TransactionContribInfo::Status> const TransactionContribInfo::_transactionContribStatusCodes = {
        TransactionContribInfo::Status::IN_PROGRESS,  TransactionContribInfo::Status::CREATE_FAILED,
        TransactionContribInfo::Status::START_FAILED, TransactionContribInfo::Status::READ_FAILED,
        TransactionContribInfo::Status::LOAD_FAILED,  TransactionContribInfo::Status::CANCELLED,
        TransactionContribInfo::Status::FINISHED};

string const& TransactionContribInfo::status2str(TransactionContribInfo::Status status) {
    auto itr = _transactionContribStatus2str.find(status);
    if (itr == _transactionContribStatus2str.cend()) {
        throw invalid_argument("DatabaseServices::" + string(__func__) +
                               "  unknown status code: " + to_string(static_cast<int>(status)));
    }
    return itr->second;
}

TransactionContribInfo::Status TransactionContribInfo::str2status(string const& str) {
    auto itr = _transactionContribStr2status.find(str);
    if (itr == _transactionContribStr2status.cend()) {
        throw invalid_argument("DatabaseServices::" + string(__func__) + "  unknown status name: " + str);
    }
    return itr->second;
}

std::vector<TransactionContribInfo::Status> const& TransactionContribInfo::statusCodes() {
    return _transactionContribStatusCodes;
}

TransactionContribInfo::FailedRetry TransactionContribInfo::resetForRetry(
        TransactionContribInfo::Status newStatus, bool newAsyncMode) {
    status = newStatus;
    async = newAsyncMode;
    retryAllowed = false;
    TransactionContribInfo::FailedRetry retry;
    swap(retry.numBytes, numBytes);
    swap(retry.numRows, numRows);
    swap(retry.startTime, startTime);
    swap(retry.readTime, readTime);
    swap(retry.tmpFile, tmpFile);
    swap(retry.httpError, httpError);
    swap(retry.systemError, systemError);
    swap(retry.error, error);
    return retry;
}

json TransactionContribInfo::toJson() const {
    json info;
    info["id"] = id;
    info["transaction_id"] = transactionId;
    info["worker"] = worker;
    info["database"] = database;
    info["table"] = table;
    info["chunk"] = chunk;
    info["overlap"] = isOverlap ? 1 : 0;
    info["url"] = url;
    info["charset_name"] = charsetName;

    info["async"] = async ? 1 : 0;

    info["dialect_input"] = dialectInput.toJson();

    info["http_method"] = http::method2string(httpMethod);
    info["http_data"] = httpData;
    info["http_headers"] = json(httpHeaders);

    info["max_retries"] = maxRetries;
    info["num_failed_retries"] = numFailedRetries;
    json failedRetriesJson = json::array();
    for (auto&& r : failedRetries) {
        json failedRetryJson = json::object();
        failedRetryJson["num_bytes"] = r.numBytes;
        failedRetryJson["num_rows"] = r.numRows;
        failedRetryJson["start_time"] = r.startTime;
        failedRetryJson["read_time"] = r.readTime;
        failedRetryJson["tmp_file"] = r.tmpFile;
        failedRetryJson["http_error"] = r.httpError;
        failedRetryJson["system_error"] = r.systemError;
        failedRetryJson["error"] = r.error;
        failedRetriesJson.push_back(failedRetryJson);
    }
    info["failed_retries"] = failedRetriesJson;

    info["num_bytes"] = numBytes;
    info["num_rows"] = numRows;

    info["create_time"] = createTime;
    info["start_time"] = startTime;
    info["read_time"] = readTime;
    info["load_time"] = loadTime;

    info["status"] = TransactionContribInfo::status2str(status);
    info["tmp_file"] = tmpFile;
    info["http_error"] = httpError;
    info["system_error"] = systemError;
    info["error"] = error;
    info["retry_allowed"] = retryAllowed ? 1 : 0;
    info["max_num_warnings"] = maxNumWarnings;
    info["num_warnings"] = numWarnings;
    info["num_rows_loaded"] = numRowsLoaded;

    json warningsJson = json::array();
    for (auto&& w : warnings) {
        warningsJson.push_back(w.toJson());
    }
    info["warnings"] = warningsJson;

    return info;
}

}  // namespace lsst::qserv::replica
