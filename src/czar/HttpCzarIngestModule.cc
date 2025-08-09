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

    // Make changes to the persistent state of Qserv and the Replicaton/Ingest system.
    // Post warnings if any reported by the method.
    list<pair<string, string>> const warnings = ingestData(
            databaseName, tableName, charsetName, collationName, schema, indexes,
            [&](uint32_t transactionId) -> map<string, string> {
                // Send table data to all eligible workers and wait for the responses.
                // Note that requests are sent in parallel, and the duration of each such request
                // is limited by the timeout parameter.
                json dataJson =
                        json::object({{"transaction_id", transactionId},
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
                    auto const request = asyncRequestWorker(workerId, data);
                    request->start();
                    workerRequests[workerId] = request;
                }
                for (auto const& [workerId, request] : workerRequests) {
                    request->wait();
                }

                // Process responses from workers.
                map<string, string> errors;
                for (auto const& [workerId, request] : workerRequests) {
                    if (request->responseCode() != qhttp::STATUS_OK) {
                        errors[workerId] = "http_code: " + to_string(request->responseCode());
                        continue;
                    }
                    json response;
                    try {
                        response = json::parse(request->responseBody());
                    } catch (exception const& ex) {
                        errors[workerId] = "ex: " + string(ex.what());
                        continue;
                    }
                    if (response.at("success").get<int>() == 0) {
                        errors[workerId] = "error: " + response.at("error").get<string>();
                    }
                }
                return errors;
            });
    for (auto const& warning : warnings) {
        warn(warning.first, warning.second);
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
    return json();
}

}  // namespace lsst::qserv::czar
