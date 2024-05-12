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

// Third party headers
#include "boost/algorithm/string.hpp"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "http/AsyncReq.h"
#include "http/BinaryEncoding.h"
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/RequestBody.h"
#include "qhttp/Request.h"
#include "qhttp/Status.h"

using namespace std;
namespace asio = boost::asio;
namespace cconfig = lsst::qserv::cconfig;
namespace http = lsst::qserv::http;
using json = nlohmann::json;

namespace {

/// Database names provided by users must start with this prefix.
string const userDatabaseNamesPrefix = "user_";

/// @throw http::Error if the name is too short, or it doesn't start with the required prefix.
void verifyUserDatabaseName(string const& func, string const& databaseName) {
    if ((databaseName.size() <= userDatabaseNamesPrefix.size()) ||
        !boost::iequals(databaseName.substr(0, userDatabaseNamesPrefix.size()), userDatabaseNamesPrefix)) {
        auto err = "database name doesn't start with the prefix: " + userDatabaseNamesPrefix;
        throw http::Error(func, err);
    }
}

/// Table names provided by users can not start with the Qserv-specific prefix.
string const qservTableNamesPrefix = "qserv";

/// @throw http::Error if the name is empty or starts with the reserved prefix.
void verifyUserTableName(string const& func, string const& tableName) {
    if (tableName.empty()) throw http::Error(func, "table name is empty");
    if (boost::iequals(tableName.substr(0, qservTableNamesPrefix.size()), qservTableNamesPrefix)) {
        auto err = "table name starts with the reserved prefix: " + qservTableNamesPrefix;
        throw http::Error(func, err);
    }
}

size_t countDirectors(const json& database) {
    size_t numDirectors = 0;
    for (const auto& table : database.at("tables")) {
        if (table.at("is_director").get<int>() != 0) {
            ++numDirectors;
        }
    }
    return numDirectors;
}

// These parameters correspond to the present partitioning model of 150k chunks.
// In reality the parameters are not needed for ingesting regular tables which is
// is the only kind of tables supported by the current implementation of the module.
// Some value of the parameters are still required by the Replication/Ingest system's API.
unsigned int defaultNumStripes = 340;
unsigned int defaultNumSubStripes = 3;
float defaultOverlap = 0.01667;

string const defaultDirectorTableName = "qserv_director";
unsigned int const defaultChunkId = 0;

void setProtocolFields(json& data) {
    data["version"] = http::MetaModule::version;
    data["instance_id"] = cconfig::CzarConfig::instance()->replicationInstanceId();
    data["auth_key"] = cconfig::CzarConfig::instance()->replicationAuthKey();
    data["admin_auth_key"] = cconfig::CzarConfig::instance()->replicationAdminAuthKey();
}

}  // namespace

namespace lsst::qserv::czar {

void HttpCzarIngestModule::process(asio::io_service& io_service, string const& context,
                                   shared_ptr<qhttp::Request> const& req,
                                   shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                   http::AuthType const authType) {
    HttpCzarIngestModule module(io_service, context, req, resp);
    module.execute(subModuleName, authType);
}

HttpCzarIngestModule::HttpCzarIngestModule(asio::io_service& io_service, string const& context,
                                           shared_ptr<qhttp::Request> const& req,
                                           shared_ptr<qhttp::Response> const& resp)
        : http::ModuleBase(cconfig::CzarConfig::instance()->replicationAuthKey(),
                           cconfig::CzarConfig::instance()->replicationAdminAuthKey(), req, resp),
          _io_service(io_service),
          _context(context),
          _registryBaseUrl("http://" + cconfig::CzarConfig::instance()->replicationRegistryHost() + ":" +
                           to_string(cconfig::CzarConfig::instance()->replicationRegistryPort())) {}

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
    checkApiVersion(__func__, 35);

    auto const databaseName = body().required<string>("database");
    auto const tableName = body().required<string>("table");
    _timeoutSec = max(1U, body().optional<unsigned int>("timeout", _timeoutSec));

    // This is needed for decoding values of the binary columns should they be present
    // in the table schema.
    http::BinaryEncodingMode const binaryEncodingMode =
            http::parseBinaryEncoding(body().optional<string>("binary_encoding", "hex"));

    debug(__func__, "database: '" + databaseName + "'");
    debug(__func__, "table: '" + tableName + "'");
    debug(__func__, "binary_encoding: '" + http::binaryEncoding2string(binaryEncodingMode) + "'");
    debug(__func__, "timeout: " + to_string(_timeoutSec));

    ::verifyUserDatabaseName(__func__, databaseName);
    ::verifyUserTableName(__func__, tableName);

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

    // Begin making changes in the persistent state of Qserv and the Replicaton/Ingest system

    _unpublishOrCreateDatabase(databaseName);
    _createTable(databaseName, tableName, schema);

    uint32_t transactionId = 0;
    try {
        transactionId = _startTransaction(databaseName);
    } catch (exception const& ex) {
        _deleteTable(databaseName, tableName);
        throw;
    }

    // Send table data to all eligible workers and wait for the responses.
    // Note that requests are sent in parallel, and the duration of each such request
    // is limited by the timeout parameter.
    json dataJson = json::object({{"transaction_id", transactionId},
                                  {"table", tableName},
                                  {"chunk", 0},
                                  {"overlap", 0},
                                  {"rows", rows},
                                  {"binary_encoding", http::binaryEncoding2string(binaryEncodingMode)}});
    ::setProtocolFields(dataJson);
    auto const data = dataJson.dump();
    map<string, shared_ptr<http::AsyncReq>> workerRequests;
    for (auto const& workerId : _getWorkerIds()) {
        auto const request = _asyncRequestWorker(workerId, data);
        request->start();
        workerRequests[workerId] = request;
    }
    for (auto const& [workerId, request] : workerRequests) {
        request->wait();
    }

    // Process responses from workers.
    map<string, string> workerErrors;
    for (auto const& [workerId, request] : workerRequests) {
        if (request->responseCode() != qhttp::STATUS_OK) {
            workerErrors[workerId] = "http_code: " + to_string(request->responseCode());
            continue;
        }
        json response;
        try {
            response = json::parse(request->responseBody());
        } catch (exception const& ex) {
            workerErrors[workerId] = "ex: " + string(ex.what());
            continue;
        }
        if (response.at("success").get<int>() == 0) {
            workerErrors[workerId] = "error: " + response.at("error").get<string>();
        }
    }
    if (!workerErrors.empty()) {
        _abortTransaction(transactionId);
        _deleteTable(databaseName, tableName);
        json const errorExt = json::object({{"worker_errors", workerErrors}});
        throw http::Error(__func__, "error(s) reported by workers", errorExt);
    }

    // Success: commit the transaction and publish the database.
    _commitTransaction(transactionId);
    _publishDatabase(databaseName);

    // The post-ingest steps are optional. They are allowed to fail without affecting the success
    // of the ingest. A warning will be reported in the response in case of a failure.
    _createIndexes(__func__, databaseName, tableName, indexes);
    _countRows(__func__, databaseName, tableName);

    return json();
}

json HttpCzarIngestModule::_deleteDatabase() {
    debug(__func__);
    checkApiVersion(__func__, 34);

    auto const databaseName = params().at("database");
    _timeoutSec = max(1U, body().optional<unsigned int>("timeout", _timeoutSec));

    debug(__func__, "database: '" + databaseName + "'");
    debug(__func__, "timeout: " + to_string(_timeoutSec));

    ::verifyUserDatabaseName(__func__, databaseName);
    _deleteDatabase(databaseName);
    return json();
}

json HttpCzarIngestModule::_deleteTable() {
    debug(__func__);
    checkApiVersion(__func__, 34);

    auto const databaseName = params().at("database");
    auto const tableName = params().at("table");
    _timeoutSec = max(1U, body().optional<unsigned int>("timeout", _timeoutSec));

    debug(__func__, "database: '" + databaseName + "'");
    debug(__func__, "table: '" + tableName + "'");
    debug(__func__, "timeout: " + to_string(_timeoutSec));

    ::verifyUserDatabaseName(__func__, databaseName);
    ::verifyUserTableName(__func__, tableName);
    _deleteTable(databaseName, tableName);
    return json();
}

vector<string> HttpCzarIngestModule::_getWorkerIds() {
    vector<string> workerIds;
    auto const workersJson = _requestController(http::Method::GET, "/replication/config");
    for (auto const& worker : workersJson.at("config").at("workers")) {
        bool const isEnabled = worker.at("is-enabled").get<int>() != 0;
        bool const isReadOnly = worker.at("is-read-only").get<int>() != 0;
        if (isEnabled && !isReadOnly) {
            workerIds.push_back(worker.at("name").get<string>());
        }
    }
    if (workerIds.empty()) {
        throw http::Error(__func__, "no workers found in this Qserv instance");
    }
    return workerIds;
}

void HttpCzarIngestModule::_unpublishOrCreateDatabase(const string& databaseName) {
    json const config = _requestController(http::Method::GET, "/replication/config").at("config");
    for (const auto& database : config.at("databases")) {
        if (boost::iequals(database.at("database").get<string>(), databaseName)) {
            if (database.at("is_published").get<int>() != 0) _unpublishDatabase(databaseName);
            if (::countDirectors(database) == 0) _createDirectorTable(databaseName);
            return;
        }
    }
    _createDatabase(databaseName);
    _createDirectorTable(databaseName);
}

void HttpCzarIngestModule::_createDatabase(string const& databaseName) {
    json data = json::object({{"database", databaseName},
                              {"num_stripes", ::defaultNumStripes},
                              {"num_sub_stripes", ::defaultNumSubStripes},
                              {"overlap", ::defaultOverlap}});
    _requestController(http::Method::POST, "/ingest/database", data);
}

void HttpCzarIngestModule::_deleteDatabase(string const& databaseName) {
    json data = json::object();
    _requestController(http::Method::DELETE, "/ingest/database/" + databaseName, data);
}

void HttpCzarIngestModule::_unpublishDatabase(string const& databaseName) {
    json data = json::object({{"publish", 0}});
    _requestController(http::Method::PUT, "/replication/config/database/" + databaseName, data);
}

void HttpCzarIngestModule::_publishDatabase(string const& databaseName) {
    json data = json::object();
    _requestController(http::Method::PUT, "/ingest/database/" + databaseName, data);
}

void HttpCzarIngestModule::_createTable(string const& databaseName, string const& tableName,
                                        json const& schema) {
    json data = json::object(
            {{"database", databaseName}, {"table", tableName}, {"is_partitioned", 0}, {"schema", schema}});
    _requestController(http::Method::POST, "/ingest/table/", data);
}

void HttpCzarIngestModule::_createDirectorTable(string const& databaseName) {
    json const schema = json::array({{{"name", "objectId"}, {"type", "BIGINT"}},
                                     {{"name", "ra"}, {"type", "DOUBLE"}},
                                     {{"name", "dec"}, {"type", "DOUBLE"}},
                                     {{"name", "chunkId"}, {"type", "INT UNSIGNED NOT NULL"}},
                                     {{"name", "subChunkId"}, {"type", "INT UNSIGNED NOT NULL"}}});
    json data = json::object(
            {{"description", "The mandatory director table of the catalog. The table may be empty."},
             {"fields_terminated_by", ","},
             {"database", databaseName},
             {"table", ::defaultDirectorTableName},
             {"is_partitioned", 1},
             {"is_director", 1},
             {"director_key", "objectId"},
             {"longitude_key", "ra"},
             {"latitude_key", "dec"},
             {"chunk_id_key", "chunkId"},
             {"sub_chunk_id_key", "subChunkId"},
             {"schema", schema}});
    _requestController(http::Method::POST, "/ingest/table/", data);
    _allocateChunk(databaseName, ::defaultChunkId);
}

void HttpCzarIngestModule::_deleteTable(string const& databaseName, string const& tableName) {
    json data = json::object();
    _requestController(http::Method::DELETE, "/ingest/table/" + databaseName + "/" + tableName, data);
}

uint32_t HttpCzarIngestModule::_startTransaction(string const& databaseName) {
    json data = json::object({{"database", databaseName}});
    auto const response = _requestController(http::Method::POST, "/ingest/trans", data);
    return response.at("databases").at(databaseName).at("transactions")[0].at("id").get<uint32_t>();
}

void HttpCzarIngestModule::_abortOrCommitTransaction(uint32_t id, bool abort) {
    json data = json::object();
    auto const service = "/ingest/trans/" + to_string(id) + "?abort=" + (abort ? "1" : "0");
    _requestController(http::Method::PUT, service, data);
}

json HttpCzarIngestModule::_allocateChunk(string const& databaseName, unsigned int chunkId) {
    json data = json::object({{"database", databaseName}, {"chunk", chunkId}});
    return _requestController(http::Method::POST, "/ingest/chunk", data);
}

void HttpCzarIngestModule::_createIndexes(string const& func, string const& databaseName,
                                          string const& tableName, json const& indexes) {
    for (auto const& indexDef : indexes) {
        if (!indexDef.is_object()) throw http::Error(func, "index definition is not a JSON object");
        try {
            json data = indexDef;
            data["database"] = databaseName;
            data["table"] = tableName;
            data["overlap"] = 0;
            _requestController(http::Method::POST, "/replication/sql/index", data);
        } catch (exception const& ex) {
            warn(func, "index creation failed: " + string(ex.what()));
        }
    }
}

void HttpCzarIngestModule::_countRows(string const& func, string const& databaseName,
                                      string const& tableName) {
    json data = json::object({{"database", databaseName},
                              {"table", tableName},
                              {"row_counters_state_update_policy", "ENABLED"},
                              {"row_counters_deploy_at_qserv", 1}});
    try {
        _requestController(http::Method::POST, "/ingest/table-stats", data);
    } catch (exception const& ex) {
        warn(func, "row count failed: " + string(ex.what()));
    }
}

string HttpCzarIngestModule::_controller() {
    if (_controllerBaseUrl.empty()) {
        auto const response = _requestRegistry(http::Method::GET, "/services");
        for (auto const& [id, controller] : response.at("services").at("controllers").items()) {
            if (id == "master") {
                _controllerBaseUrl = "http://" + controller.at("host-addr").get<string>() + ":" +
                                     to_string(controller.at("port").get<uint16_t>());
                return _controllerBaseUrl;
            }
        }
        throw http::Error(__func__, "no master controller found in the response");
    }
    return _controllerBaseUrl;
}

string HttpCzarIngestModule::_worker(string const& workerId) {
    if (_workerBaseUrls.empty()) {
        auto const response = _requestRegistry(http::Method::GET, "/services");
        for (auto const& [id, worker] : response.at("services").at("workers").items()) {
            auto const replicationWorker = worker.at("replication");
            _workerBaseUrls[id] = "http://" + replicationWorker.at("host-addr").get<string>() + ":" +
                                  to_string(replicationWorker.at("http-loader-port").get<uint16_t>());
        }
    }
    if (_workerBaseUrls.count(workerId) == 0) {
        throw http::Error(__func__, "no connection parameters for worker: " + workerId);
    }
    return _workerBaseUrls.at(workerId);
}

json HttpCzarIngestModule::_request(http::Method method, string const& url, json& data) {
    json const errorExt = json::object(
            {{"method", http::method2string(method)}, {"url", url}, {"timeout_sec", _timeoutSec}});
    auto const request = _asyncRequest(method, url, data);
    request->start();
    request->wait();
    if (request->responseCode() != qhttp::STATUS_OK) {
        throw http::Error(__func__, "http_code: " + to_string(request->responseCode()), errorExt);
    }
    json response;
    try {
        response = json::parse(request->responseBody());
        debug(__func__, "response: " + response.dump());
    } catch (exception const& ex) {
        throw http::Error(__func__, "ex: " + string(ex.what()), errorExt);
    }
    if (response.at("success").get<int>() == 0) {
        throw http::Error(__func__, "error: " + response.at("error").get<string>(), errorExt);
    }
    return response;
}

shared_ptr<http::AsyncReq> HttpCzarIngestModule::_asyncRequest(http::Method method, string const& url,
                                                               json& data) {
    shared_ptr<http::AsyncReq> request;
    if (method == http::Method::GET) {
        string const url_ = url + "?version=" + to_string(http::MetaModule::version) +
                            "&instance_id=" + cconfig::CzarConfig::instance()->replicationInstanceId();
        request = http::AsyncReq::create(_io_service, nullptr, method, url_);
    } else {
        ::setProtocolFields(data);
        unordered_map<string, string> const headers({{"Content-Type", "application/json"}});
        request = http::AsyncReq::create(_io_service, nullptr, method, url, data.dump(), headers);
    }
    request->setExpirationIval(_timeoutSec);
    return request;
}

shared_ptr<http::AsyncReq> HttpCzarIngestModule::_asyncPostRequest(string const& url, string const& data) {
    unordered_map<string, string> const headers({{"Content-Type", "application/json"}});
    auto const request = http::AsyncReq::create(_io_service, nullptr, http::Method::POST, url, data, headers);
    request->setExpirationIval(_timeoutSec);
    return request;
}

}  // namespace lsst::qserv::czar
