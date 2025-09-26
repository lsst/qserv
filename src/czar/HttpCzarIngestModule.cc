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
#include "czar/HttpCzarIngestModule.h"

// System headers
#include <algorithm>
#include <stdexcept>
#include <vector>
#include <unordered_map>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "http/AsyncReq.h"
#include "http/Auth.h"
#include "http/BinaryEncoding.h"
#include "http/Exceptions.h"
#include "http/RequestBodyJSON.h"
#include "qmeta/UserTables.h"
#include "qmeta/UserTableIngestRequest.h"
#include "qhttp/Status.h"

using namespace std;
namespace asio = boost::asio;
namespace cconfig = lsst::qserv::cconfig;
namespace http = lsst::qserv::http;
using json = nlohmann::json;

namespace lsst::qserv::czar {

void HttpCzarIngestModule::process(asio::io_service& io_service, string const& context,
                                   httplib::Request const& req, httplib::Response& resp,
                                   string const& subModuleName, http::AuthType const authType) {
    HttpCzarIngestModule module(io_service, context, req, resp);
    module.execute(subModuleName, authType);
}

HttpCzarIngestModule::HttpCzarIngestModule(asio::io_service& io_service, string const& context,
                                           httplib::Request const& req, httplib::Response& resp)
        : http::ChttpModule(cconfig::CzarConfig::instance()->httpAuthContext(), req, resp),
          HttpCzarIngestModuleBase(io_service),
          _context(context) {}

string HttpCzarIngestModule::context() const { return _context; }

json HttpCzarIngestModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    if (subModuleName == "INGEST-DATA")
        return _ingestData();
    else if (subModuleName == "DELETE-DATABASE")
        return _deleteDatabase();
    else if (subModuleName == "DELETE-TABLE")
        return _deleteTable();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpCzarIngestModule::_ingestData() {
    debug(__func__);
    checkApiVersion(__func__, 49);

    auto const databaseName = body().required<string>("database");
    auto const tableName = body().required<string>("table");
    auto const charsetName = body().optional<string>("charset_name", "latin1");
    auto const collationName = body().optional<string>("collation_name", "latin1_swedish_ci");
    setTimeoutSec(max(1U, body().optional<unsigned int>("timeout", timeoutSec())));

    // This is needed for decoding values of the binary columns should they be present
    // in the table schema.
    http::BinaryEncodingMode const binaryEncodingMode =
            http::parseBinaryEncoding(body().optional<string>("binary_encoding", "hex"));

    debug(__func__, "database: '" + databaseName + "'");
    debug(__func__, "table: '" + tableName + "'");
    debug(__func__, "binary_encoding: '" + http::binaryEncoding2string(binaryEncodingMode) + "'");
    debug(__func__, "timeout: " + to_string(timeoutSec()));

    verifyUserDatabaseName(__func__, databaseName);
    verifyUserTableName(__func__, tableName);

    // Table schema is required to be an array of column descriptors
    if (!body().has("schema")) {
        throw http::Error(__func__, "table schema definition is missing in the request");
    }
    json const& schema = body().objJson.at("schema");
    if (!schema.is_array()) {
        throw http::Error(__func__, "table schema found in the request is not the JSON array");
    }
    if (schema.empty()) {
        throw http::Error(__func__, "table schema in the request is empty");
    }

    // Rows are expected to be supplied in the JSON array
    if (!body().has("rows")) {
        throw http::Error(__func__, "a collection of rows is missing in the request");
    }
    json const& rows = body().objJson.at("rows");
    if (!rows.is_array()) {
        throw http::Error(__func__, "a collection of rows found in the request is not the JSON array");
    }
    if (rows.empty()) {
        throw http::Error(__func__, "a collection of rows in the request is empty");
    }

    // The index definitions are optional and are expected to be an array of index descriptors.
    json indexes = json::array();
    if (body().has("indexes")) {
        indexes = body().objJson.at("indexes");
        if (!indexes.is_array()) {
            throw http::Error(__func__, "index definitions found in the request is not the JSON array");
        }
    }

    // Register the request in the QMeta database.
    qmeta::UserTables userTables(cconfig::CzarConfig::instance()->getMySqlQmetaConfig());
    qmeta::UserTableIngestRequest request;
    request.database = databaseName;
    request.table = tableName;
    request.tableType = qmeta::UserTableIngestRequest::TableType::FULLY_REPLICATED;
    request.isTemporary = true;
    request.dataFormat = qmeta::UserTableIngestRequest::DataFormat::JSON;
    request.schema = schema;
    request.indexes = indexes;
    request.extended["charset"] = charsetName;
    request.extended["collation"] = collationName;
    request.extended["binary_encoding"] = http::binaryEncoding2string(binaryEncodingMode);

    request = userTables.registerRequest(request);
    debug(__func__, "registered a new ingest request, id: " + to_string(request.id));

    // Ingest statistics
    std::uint32_t thisTransactionId = 0;
    std::atomic<std::uint32_t> numChunks{0};
    std::atomic<std::uint64_t> numRows{0};
    std::atomic<std::uint64_t> numBytes{0};

    // Push the data to all workers and monitor the progress.
    try {
        list<pair<string, string>> const warnings = ingestData(
                databaseName, tableName, charsetName, collationName, schema, indexes,
                [&](uint32_t transactionId) -> map<string, string> {
                    thisTransactionId = transactionId;

                    // Send table data to all eligible workers and wait for the responses.
                    // Note that requests are sent in parallel, and the duration of each such request
                    // is limited by the timeout parameter.
                    json dataJson = json::object(
                            {{"transaction_id", transactionId},
                             {"table", tableName},
                             {"charset_name", charsetName},
                             {"chunk", 0},
                             {"overlap", 0},
                             {"rows", rows},
                             {"binary_encoding", http::binaryEncoding2string(binaryEncodingMode)}});
                    setProtocolFields(dataJson);
                    auto const data = dataJson.dump();
                    map<string, shared_ptr<http::AsyncReq>> workerRequests;
                    for (auto const& workerId : getWorkerIds()) {
                        auto const req = asyncRequestWorker(workerId, data);
                        req->start();
                        workerRequests[workerId] = req;
                    }
                    for (auto const& [workerId, req] : workerRequests) {
                        req->wait();
                    }

                    // Process workers' responses.
                    map<string, string> errors;
                    for (auto const& [workerId, req] : workerRequests) {
                        if (req->responseCode() != qhttp::STATUS_OK) {
                            errors[workerId] = "http_code: " + to_string(req->responseCode());
                            continue;
                        }
                        json resp;
                        try {
                            resp = json::parse(req->responseBody());
                        } catch (exception const& ex) {
                            errors[workerId] = "ex: " + string(ex.what());
                            continue;
                        }
                        if (resp.at("success").get<int>() == 0) {
                            errors[workerId] = "error: " + resp.at("error").get<string>();
                        } else {
                            // Update ingest statistics. Values of the counters reported by workers
                            // are expected to be the same for the fully replicated tables.
                            // Though the last statement is not checked or enforced by the current
                            // implementation updating ingest statistics for each worker allows to get the
                            // values even if only one worker completes the request successfully.
                            json const& contrib = resp.at("contrib");
                            numRows = contrib.at("num_rows").get<std::uint64_t>();
                            numBytes = contrib.at("num_bytes").get<std::uint64_t>();
                        }
                    }
                    return errors;
                });

        // Make sure any warnings reported during the ingest are returned to the caller.
        for (auto const& warning : warnings) {
            warn(warning.first, warning.second);
        }
        request = userTables.ingestFinished(request.id, qmeta::UserTableIngestRequest::Status::COMPLETED,
                                            string(), thisTransactionId, numChunks, numRows, numBytes);
        debug(__func__, "ingest request completed, id: " + to_string(request.id));
    } catch (exception const& ex) {
        request = userTables.ingestFinished(request.id, qmeta::UserTableIngestRequest::Status::FAILED,
                                            ex.what(), thisTransactionId, numChunks, numRows, numBytes);
        error(__func__, "ingest request failed, id: " + to_string(request.id) + ", error: " + ex.what());
        throw;
    }
    return json();
}

json HttpCzarIngestModule::_deleteDatabase() {
    debug(__func__);
    checkApiVersion(__func__, 34);

    auto const databaseName = params().at("database");
    setTimeoutSec(max(1U, body().optional<unsigned int>("timeout", timeoutSec())));

    debug(__func__, "database: '" + databaseName + "'");
    debug(__func__, "timeout: " + to_string(timeoutSec()));

    verifyUserDatabaseName(__func__, databaseName);
    deleteDatabase(databaseName);

    // Mark all relevant tables of the database as deleted in the registry.
    qmeta::UserTables userTables(cconfig::CzarConfig::instance()->getMySqlQmetaConfig());
    userTables.databaseDeleted(databaseName);
    return json();
}

json HttpCzarIngestModule::_deleteTable() {
    debug(__func__);
    checkApiVersion(__func__, 34);

    auto const databaseName = params().at("database");
    auto const tableName = params().at("table");
    setTimeoutSec(max(1U, body().optional<unsigned int>("timeout", timeoutSec())));

    debug(__func__, "database: '" + databaseName + "'");
    debug(__func__, "table: '" + tableName + "'");
    debug(__func__, "timeout: " + to_string(timeoutSec()));

    verifyUserDatabaseName(__func__, databaseName);
    verifyUserTableName(__func__, tableName);
    deleteTable(databaseName, tableName);

    // Mark the table as deleted in the registry.
    qmeta::UserTables userTables(cconfig::CzarConfig::instance()->getMySqlQmetaConfig());
    bool const extended = false;
    try {
        userTables.tableDeleted(userTables.findRequest(databaseName, tableName, extended).id);
    } catch (qmeta::IngestRequestNotFound const&) {
        // Ignore the error if the request is not found.
    }
    return json();
}

}  // namespace lsst::qserv::czar
