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
#include "czar/HttpCzarIngestModuleBase.h"

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
#include "http/Client.h"
#include "http/ClientConfig.h"
#include "http/ClientConnPool.h"
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/RequestBodyJSON.h"
#include "qhttp/Status.h"

using namespace std;
namespace asio = boost::asio;
namespace cconfig = lsst::qserv::cconfig;
namespace http = lsst::qserv::http;
using json = nlohmann::json;

namespace {

size_t countDirectors(const json& database) {
    size_t numDirectors = 0;
    for (const auto& table : database.at("tables")) {
        if (table.at("is_director").get<int>() != 0) {
            ++numDirectors;
        }
    }
    return numDirectors;
}

string const defaultDirectorTableName = "qserv_director";
int32_t const defaultChunkId = 0;

}  // namespace

namespace lsst::qserv::czar {

// These parameters correspond to the present partitioning model of 150k chunks.
unsigned int HttpCzarIngestModuleBase::defaultNumStripes = 340;
unsigned int HttpCzarIngestModuleBase::defaultNumSubStripes = 3;
float HttpCzarIngestModuleBase::defaultOverlap = 0.01667;

HttpCzarIngestModuleBase::HttpCzarIngestModuleBase(asio::io_service& io_service)
        : _io_service(io_service),
          _registryBaseUrl("http://" + cconfig::CzarConfig::instance()->replicationRegistryHost() + ":" +
                           to_string(cconfig::CzarConfig::instance()->replicationRegistryPort())) {}

list<pair<string, string>> HttpCzarIngestModuleBase::ingestData(
        string const& databaseName, string const& tableName, bool isPartitioned, string directorIdColName,
        string directorLongitudeColName, string directorLatitudeColName, string const& charsetName,
        string const& collationName, json const& schema, json const& indexes, set<int32_t> const& chunkIds,
        function<map<string, string>(uint32_t, map<int32_t, vector<string>> const&)> const&
                submitRequestsToWorkers) {
    // At lest one director table is needed per catalog for Qserv to function correctly.
    // The default director should be created unless the ingest is for a partitioned table
    // and the user has specified a director table (which is presently the only option
    // supported by the current implementation for the partitioned tables).
    bool const createDefaultDirectorTable = !isPartitioned;
    _unpublishOrCreateDatabase(databaseName, createDefaultDirectorTable);
    _createTable(databaseName, tableName, isPartitioned, directorIdColName, directorLongitudeColName,
                 directorLatitudeColName, charsetName, collationName, schema);

    uint32_t transactionId = 0;
    try {
        transactionId = _startTransaction(databaseName);
    } catch (exception const& ex) {
        deleteTable(databaseName, tableName);
        throw;
    }
    map<int32_t, vector<string>> chunk2workerIds;
    if (!chunkIds.empty()) {
        chunk2workerIds = _allocateChunks(chunkIds, databaseName);
    }

    map<string, string> const workerErrors = submitRequestsToWorkers(transactionId, chunk2workerIds);

    if (!workerErrors.empty()) {
        _abortTransaction(transactionId);
        deleteTable(databaseName, tableName);
        json const errorExt = json::object({{"worker_errors", workerErrors}});
        throw http::Error(__func__, "error(s) reported by workers", errorExt);
    }

    // Success: commit the transaction and publish the database.
    _commitTransaction(transactionId);
    _publishDatabase(databaseName);

    // The post-ingest steps are optional. They are allowed to fail without affecting the success
    // of the ingest. A warning will be reported in the response in case of a failure.
    list<pair<string, string>> warnings;
    _createIndexes(__func__, databaseName, tableName, indexes, warnings);
    _countRows(__func__, databaseName, tableName, warnings);
    return warnings;
}

void HttpCzarIngestModuleBase::verifyUserDatabaseName(string const& func, string const& databaseName) {
    string const userDatabaseNamesPrefix = "user_";
    if ((databaseName.size() <= userDatabaseNamesPrefix.size()) ||
        !boost::iequals(databaseName.substr(0, userDatabaseNamesPrefix.size()), userDatabaseNamesPrefix)) {
        auto err = "database name doesn't start with the prefix: " + userDatabaseNamesPrefix;
        throw http::Error(func, err);
    }
}

void HttpCzarIngestModuleBase::verifyUserTableName(string const& func, string const& tableName) {
    string const qservTableNamesPrefix = "qserv_";
    if (tableName.empty()) throw http::Error(func, "table name is empty");
    if (boost::iequals(tableName.substr(0, qservTableNamesPrefix.size()), qservTableNamesPrefix)) {
        auto err = "table name starts with the reserved prefix: " + qservTableNamesPrefix;
        throw http::Error(func, err);
    }
}

void HttpCzarIngestModuleBase::deleteDatabase(string const& databaseName) {
    json data = json::object();
    _requestController(http::Method::DELETE, "/ingest/database/" + databaseName, data);
}

void HttpCzarIngestModuleBase::deleteTable(string const& databaseName, string const& tableName) {
    json data = json::object();
    _requestController(http::Method::DELETE, "/ingest/table/" + databaseName + "/" + tableName, data);
}

vector<string> HttpCzarIngestModuleBase::getWorkerIds() {
    vector<string> workerIds;
    json const response = _requestController(http::Method::GET, "/replication/config");
    for (auto const& worker : response.at("config").at("workers")) {
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

map<int32_t, vector<string>> HttpCzarIngestModuleBase::_allocateChunks(set<int32_t> const& chunkIds,
                                                                       string const& databaseName) {
    json data = json::object({{"database", databaseName}, {"chunks", json::array()}});
    for (auto const& chunkId : chunkIds) {
        data.at("chunks").push_back(chunkId);
    }
    json const response = _requestController(http::Method::POST, "/ingest/chunks-multi", data);
    map<int32_t, vector<string>> result;
    for (auto const& entry : response.at("locations")) {
        result[entry.at("chunk").get<int32_t>()].push_back(entry.at("worker").get<string>());
    }
    return result;
}

void HttpCzarIngestModuleBase::_unpublishOrCreateDatabase(const string& databaseName,
                                                          bool createDefaultDirectorTable) {
    json const config = _requestController(http::Method::GET, "/replication/config").at("config");
    for (const auto& database : config.at("databases")) {
        if (boost::iequals(database.at("database").get<string>(), databaseName)) {
            if (database.at("is_published").get<int>() != 0) _unpublishDatabase(databaseName);
            if (::countDirectors(database) == 0 && createDefaultDirectorTable) {
                _createDefaultDirectorTable(databaseName);
            }
            return;
        }
    }
    _createDatabase(databaseName);
    if (createDefaultDirectorTable) {
        _createDefaultDirectorTable(databaseName);
    }
}

void HttpCzarIngestModuleBase::_createDatabase(string const& databaseName) {
    json data = json::object({{"database", databaseName},
                              {"num_stripes", HttpCzarIngestModuleBase::defaultNumStripes},
                              {"num_sub_stripes", HttpCzarIngestModuleBase::defaultNumSubStripes},
                              {"overlap", HttpCzarIngestModuleBase::defaultOverlap}});
    _requestController(http::Method::POST, "/ingest/database", data);
}

void HttpCzarIngestModuleBase::_unpublishDatabase(string const& databaseName) {
    json data = json::object({{"publish", 0}});
    _requestController(http::Method::PUT, "/replication/config/database/" + databaseName, data);
}

void HttpCzarIngestModuleBase::_publishDatabase(string const& databaseName) {
    json data = json::object();
    _requestController(http::Method::PUT, "/ingest/database/" + databaseName, data);
}

void HttpCzarIngestModuleBase::_createTable(string const& databaseName, string const& tableName,
                                            bool isPartitioned, string directorIdColName,
                                            string directorLongitudeColName, string directorLatitudeColName,
                                            string const& charsetName, string const& collationName,
                                            json const& schema) {
    json data = json::object({{"database", databaseName},
                              {"table", tableName},
                              {"is_partitioned", isPartitioned ? 1 : 0},
                              {"charset_name", charsetName},
                              {"collation_name", collationName},
                              {"schema", schema}});
    if (isPartitioned) {
        data["director_key"] = directorIdColName;
        data["longitude_key"] = directorLongitudeColName;
        data["latitude_key"] = directorLatitudeColName;
    }
    _requestController(http::Method::POST, "/ingest/table/", data);
}

void HttpCzarIngestModuleBase::_createDefaultDirectorTable(string const& databaseName) {
    json const schema = json::array({{{"name", "objectId"}, {"type", "BIGINT"}},
                                     {{"name", "ra"}, {"type", "DOUBLE"}},
                                     {{"name", "dec"}, {"type", "DOUBLE"}},
                                     {{"name", "chunkId"}, {"type", "INT UNSIGNED NOT NULL"}},
                                     {{"name", "subChunkId"}, {"type", "INT UNSIGNED NOT NULL"}}});
    json data = json::object({{"database", databaseName},
                              {"table", ::defaultDirectorTableName},
                              {"is_partitioned", 1},
                              {"director_key", "objectId"},
                              {"longitude_key", "ra"},
                              {"latitude_key", "dec"},
                              {"schema", schema}});
    // After registering the new table in Qserv, at least one chunk of the director table must
    // be allocated to ensure the table is properly initialized. No data will be ingested into
    // the table.
    _requestController(http::Method::POST, "/ingest/table/", data);
    _allocateChunk(databaseName, ::defaultChunkId);
}

uint32_t HttpCzarIngestModuleBase::_startTransaction(string const& databaseName) {
    json data = json::object({{"database", databaseName}});
    auto const response = _requestController(http::Method::POST, "/ingest/trans", data);
    return response.at("databases").at(databaseName).at("transactions")[0].at("id").get<uint32_t>();
}

void HttpCzarIngestModuleBase::_abortOrCommitTransaction(uint32_t id, bool abort) {
    json data = json::object();
    auto const service = "/ingest/trans/" + to_string(id) + "?abort=" + (abort ? "1" : "0");
    _requestController(http::Method::PUT, service, data);
}

std::string HttpCzarIngestModuleBase::_allocateChunk(std::string const& databaseName, int32_t chunkId) {
    json data = json::object({{"database", databaseName}, {"chunk", chunkId}});
    json const response = _requestController(http::Method::POST, "/ingest/chunk", data);
    return response.at("location").at("worker").get<std::string>();
}

void HttpCzarIngestModuleBase::_createIndexes(string const& func, string const& databaseName,
                                              string const& tableName, json const& indexes,
                                              list<pair<string, string>>& warnings) {
    for (auto const& indexDef : indexes) {
        if (!indexDef.is_object()) throw http::Error(func, "index definition is not a JSON object");
        try {
            json data = indexDef;
            data["database"] = databaseName;
            data["table"] = tableName;
            data["overlap"] = 0;
            _requestController(http::Method::POST, "/replication/sql/index", data);
        } catch (exception const& ex) {
            warnings.emplace_back(func, "index creation failed: " + string(ex.what()));
        }
    }
}

void HttpCzarIngestModuleBase::_countRows(string const& func, string const& databaseName,
                                          string const& tableName, list<pair<string, string>>& warnings) {
    json data = json::object({{"database", databaseName},
                              {"table", tableName},
                              {"row_counters_state_update_policy", "ENABLED"},
                              {"row_counters_deploy_at_qserv", 1}});
    try {
        _requestController(http::Method::POST, "/ingest/table-stats", data);
    } catch (exception const& ex) {
        warnings.emplace_back(func, "row count failed: " + string(ex.what()));
    }
}

string HttpCzarIngestModuleBase::_controller() {
    lock_guard<mutex> lock(_mtx);
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

string HttpCzarIngestModuleBase::_worker(string const& workerId) {
    lock_guard<mutex> lock(_mtx);
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

json HttpCzarIngestModuleBase::_request(http::Method method, string const& url, json& data) {
    json const errorExt = json::object(
            {{"method", http::method2string(method)}, {"url", url}, {"timeout_sec", _timeoutSec}});
    auto const request = _asyncRequest(method, url, data);
    request->start();
    request->wait();
    if (request->state() == http::AsyncReq::State::FINISHED) {
        if (request->responseCode() != qhttp::STATUS_OK) {
            throw http::Error(__func__,
                              "request state: " + http::AsyncReq::state2str(request->state()) +
                                      ", error: " + request->errorMessage() +
                                      ", http_code: " + to_string(request->responseCode()),
                              errorExt);
        }
    } else {
        throw http::Error(__func__,
                          "request state: " + http::AsyncReq::state2str(request->state()) +
                                  ", error: " + request->errorMessage(),
                          errorExt);
    }

    json response;
    try {
        response = json::parse(request->responseBody());
    } catch (exception const& ex) {
        throw http::Error(__func__, "ex: " + string(ex.what()), errorExt);
    }
    if (response.at("success").get<int>() == 0) {
        throw http::Error(__func__, "error: " + response.at("error").get<string>(), errorExt);
    }
    return response;
}

shared_ptr<http::AsyncReq> HttpCzarIngestModuleBase::_asyncRequest(http::Method method, string const& url,
                                                                   json& data) {
    shared_ptr<http::AsyncReq> request;
    if (method == http::Method::GET) {
        string const url_ = url + "?version=" + to_string(http::MetaModule::version) +
                            "&instance_id=" + cconfig::CzarConfig::instance()->replicationInstanceId();
        request = http::AsyncReq::create(_io_service, nullptr, method, url_);
    } else {
        setProtocolFields(data);
        unordered_map<string, string> const headers({{"Content-Type", "application/json"}});
        request = http::AsyncReq::create(_io_service, nullptr, method, url, data.dump(), headers);
    }
    request->setExpirationIval(_timeoutSec);
    return request;
}

shared_ptr<http::AsyncReq> HttpCzarIngestModuleBase::_asyncPostRequest(string const& url,
                                                                       string const& data) {
    unordered_map<string, string> const headers({{"Content-Type", "application/json"}});
    auto const request = http::AsyncReq::create(_io_service, nullptr, http::Method::POST, url, data, headers);
    request->setExpirationIval(_timeoutSec);
    return request;
}

shared_ptr<http::Client> HttpCzarIngestModuleBase::_syncMimePostRequest(
        string const& url, list<http::ClientMimeEntry> const& mimeData,
        shared_ptr<http::ClientConnPool> const& connPool) {
    vector<string> const headers;
    http::ClientConfig clientConfig;
    clientConfig.connectTimeout = _timeoutSec;
    clientConfig.timeout = _timeoutSec;
    return make_shared<http::Client>(url, mimeData, headers, clientConfig, connPool);
}

void HttpCzarIngestModuleBase::setProtocolFields(json& data) const {
    data["version"] = http::MetaModule::version;
    data["instance_id"] = cconfig::CzarConfig::instance()->replicationInstanceId();
    data["auth_key"] = cconfig::CzarConfig::instance()->replicationAuthKey();
    data["admin_auth_key"] = cconfig::CzarConfig::instance()->replicationAdminAuthKey();
}

void HttpCzarIngestModuleBase::setProtocolFields(list<http::ClientMimeEntry>& mimeData) const {
    // IMPORTANT: The order of the fields is important in the MIMEPOST request. Non-file
    // fields should be placed before the file field. The collection that is being ammeded
    // by this method may already contain some fields, including the file fields.
    mimeData.push_front({"version", to_string(http::MetaModule::version), "", ""});
    mimeData.push_front({"instance_id", cconfig::CzarConfig::instance()->replicationInstanceId(), "", ""});
    mimeData.push_front({"auth_key", cconfig::CzarConfig::instance()->replicationAuthKey(), "", ""});
    mimeData.push_front(
            {"admin_auth_key", cconfig::CzarConfig::instance()->replicationAdminAuthKey(), "", ""});
}

}  // namespace lsst::qserv::czar
