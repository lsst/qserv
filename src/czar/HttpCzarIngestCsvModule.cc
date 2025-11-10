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
#include "czar/HttpCzarIngestCsvModule.h"

// System headers
#include <algorithm>
#include <set>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "czar/WorkerIngestProcessor.h"
#include "global/stringUtil.h"
#include "http/AsyncReq.h"
#include "http/Auth.h"
#include "http/Client.h"
#include "http/Exceptions.h"
#include "http/RequestBodyJSON.h"
#include "partition/ChunkIndex.h"
#include "partition/ConfigStore.h"
#include "partition/Exceptions.h"
#include "partition/PartitionTool.h"
#include "qmeta/UserTables.h"
#include "qhttp/Status.h"

using namespace std;
namespace asio = boost::asio;
namespace cconfig = lsst::qserv::cconfig;
namespace fs = boost::filesystem;
namespace http = lsst::qserv::http;
using json = nlohmann::json;

namespace {

/// @brief Assert that a column exists in the schema.
/// @param func The name of the function performing the check.
/// @param schema The JSON schema to check against.
/// @param colName The name of the column to check for.
/// @param columnRole A description of the column's role (e.g., "id", "latitude", "longitude").
void assertColumnExists(string const& func, json const& schema, string const& colName,
                        string const& columnRole) {
    if (colName.empty()) {
        throw http::Error(func, "the " + columnRole + " column name is required for partitioned tables");
    }
    bool const found = find_if(schema.begin(), schema.end(), [&](json const& col) {
                           return col.at("name").get<string>() == colName;
                       }) != schema.end();
    if (!found) {
        throw http::Error(func, "the " + columnRole + " column '" + colName + "' is not found in the schema");
    }
}

/**
 * Translate a string to a single character. The string is expected to be either a single character or an
 * escaped character (e.g., "\t", "\n", "\r", "\\", "\0"). The translation is needed to pass the single
 * character string to the partitioning tool.
 *
 * @param str The string to translate.
 * @return The translated single character.
 * @throw http::Error If the string is not a single character or a valid escaped character.
 */
string translateToSingleChar(string const& str) {
    if (str.size() == 1) {
        return str;
    }
    if (str.size() == 2 && str[0] == '\\') {
        switch (str[1]) {
            case 't':
                return "\t";
            case 'n':
                return "\n";
            case 'r':
                return "\r";
            case '\\':
                return "\\";
            case '0':
                return "\0";
            default:
                throw http::Error(__func__, "unsupported escape sequence: '" + str + "'");
        }
    }
    throw http::Error(__func__,
                      "invalid value: '" + str + "'. Expected a single character or an escaped character.");
}

}  // namespace

namespace lsst::qserv::czar {

void HttpCzarIngestCsvModule::process(asio::io_service& io_service, string const& context,
                                      string const& tmpDir, httplib::Request const& req,
                                      httplib::Response& resp, httplib::ContentReader const& contentReader,
                                      shared_ptr<http::ClientConnPool> const& clientConnPool,
                                      shared_ptr<ingest::Processor> const& workerIngestProcessor,
                                      http::AuthType const authType) {
    HttpCzarIngestCsvModule module(io_service, context, tmpDir, req, resp, contentReader, clientConnPool,
                                   workerIngestProcessor);
    string const subModuleName;
    module.execute(subModuleName, authType);
}

HttpCzarIngestCsvModule::HttpCzarIngestCsvModule(asio::io_service& io_service, string const& context,
                                                 string const& tmpDir, httplib::Request const& req,
                                                 httplib::Response& resp,
                                                 httplib::ContentReader const& contentReader,
                                                 shared_ptr<http::ClientConnPool> const& clientConnPool,
                                                 shared_ptr<ingest::Processor> const& workerIngestProcessor)
        : http::FileUploadModule(cconfig::CzarConfig::instance()->httpAuthContext(), req, resp,
                                 contentReader),
          HttpCzarIngestModuleBase(io_service),
          _context(context),
          _tmpDir(tmpDir),
          _clientConnPool(clientConnPool),
          _workerIngestProcessor(workerIngestProcessor) {}

HttpCzarIngestCsvModule::~HttpCzarIngestCsvModule() {
    if (!_csvFileName.empty()) {
        boost::system::error_code ec;
        fs::remove(_csvFileName, ec);
        if (ec.value() != 0) {
            warn(__func__, "failed to delete the data file " + _csvFileName + ", error: " + ec.message());
        }
    }

    // DEBUG: Don't delete the chunks before it's clear that the algorithm works as expected.
#if 0
    if (!_chunksDirName.empty()) {
        boost::system::error_code ec;
        fs::remove_all(_chunksDirName, ec);
        if (ec.value() != 0) {
            warn(__func__, "failed to delete the chunks directory " + _chunksDirName + ", error: " + ec.message());
        }
    }
#endif
}

string HttpCzarIngestCsvModule::context() const { return _context; }

void HttpCzarIngestCsvModule::onStartOfFile(string const& name, string const& fileName,
                                            string const& contentType) {
    debug(__func__, "name: '" + name + "', fileName: '" + fileName + "', contentType: '" + contentType + "'");
    if (name == "rows") {
        if (!_csvFileName.empty()) {
            throw http::Error(__func__, "the data file is already uploaded");
        }
        boost::system::error_code ec;
        fs::path const uniqueFileName = fs::unique_path("http-ingest-%%%%-%%%%-%%%%-%%%%.csv", ec);
        if (ec.value() != 0) {
            throw http::Error(__func__, "failed to generate a unique file name, error: " + ec.message());
        }
        _csvFileName = _tmpDir + "/" + uniqueFileName.string();
        _csvFile.open(_csvFileName, ios::binary);
        if (!_csvFile.is_open()) {
            throw http::Error(__func__, "failed to open the data file " + _csvFileName + " for writing");
        }
    } else if (name == "schema") {
        if (!_schema.empty()) {
            throw http::Error(__func__, "the schema file is already uploaded");
        }
    } else if (name == "indexes") {
        if (!_indexes.empty()) {
            throw http::Error(__func__, "the indexes file is already uploaded");
        }
    } else {
        throw http::Error(__func__, "unexpected file name: " + name);
    }
    _name = name;
}

void HttpCzarIngestCsvModule::onFileData(char const* data, size_t length) {
    debug(__func__, "name: '" + _name + "', length: " + to_string(length));
    if (_name == "rows") {
        _csvFile.write(data, length);
    } else if (_name == "schema") {
        _schema.append(data, length);
    } else if (_name == "indexes") {
        _indexes.append(data, length);
    } else {
        throw http::Error(__func__, "unexpected file name: " + _name);
    }
}

void HttpCzarIngestCsvModule::onEndOfFile() {
    debug(__func__);
    if (_name == "rows") {
        _csvFile.close();
    } else if (_name == "schema") {
        try {
            body().objJson[_name] = json::parse(_schema);
        } catch (exception const& ex) {
            throw http::Error(__func__, "failed to parse the schema file: " + string(ex.what()));
        }
    } else if (_name == "indexes") {
        try {
            body().objJson[_name] = json::parse(_indexes);
        } catch (exception const& ex) {
            throw http::Error(__func__, "failed to parse the indexes file: " + string(ex.what()));
        }
    } else {
        throw http::Error(__func__, "unexpected file name: " + _name);
    }
}

json HttpCzarIngestCsvModule::onEndOfBody() {
    debug(__func__);
    checkApiVersion(__func__, 52);

    _databaseName = body().required<string>("database");
    _tableName = body().required<string>("table");
    _charsetName = body().optional<string>("charset_name", "latin1");
    _collationName = body().optional<string>("collation_name", "latin1_swedish_ci");
    _fieldsTerminatedBy = body().optional<string>("fields_terminated_by", R"(\t)");
    _fieldsEnclosedBy = body().optional<string>("fields_enclosed_by", R"(\0)");
    _fieldsEscapedBy = body().optional<string>("fields_escaped_by", R"(\\)");
    _linesTerminatedBy = body().optional<string>("lines_terminated_by", R"(\n)");
    _isPartitioned = body().optional<string>("is_partitioned", "0") != "0";
    if (_isPartitioned) {
        _isDirector = body().required<string>("is_director") != "0";
        if (!_isDirector) {
            throw http::Error(__func__, "only director tables are supported for partitioned tables");
        }
        _idColName = body().required<string>("id_col_name");
        _latitudeColName = body().required<string>("latitude_col_name");
        _longitudeColName = body().required<string>("longitude_col_name");
    }
    string const timeoutSecStr = body().optional<string>("timeout", string());
    if (timeoutSecStr.empty()) {
        setTimeoutSec(max(1U, timeoutSec()));
    } else {
        try {
            setTimeoutSec(max(1U, qserv::stoui(timeoutSecStr)));
        } catch (exception const& ex) {
            throw http::Error(__func__, "failed to parse the timeout value: " + string(ex.what()));
        }
    }

    debug(__func__, "database: '" + _databaseName + "'");
    debug(__func__, "table: '" + _tableName + "'");
    debug(__func__, "charsetName: '" + _charsetName + "'");
    debug(__func__, "collationName: '" + _collationName + "'");
    debug(__func__, "fields_terminated_by: '" + _fieldsTerminatedBy + "'");
    debug(__func__, "fields_enclosed_by: '" + _fieldsEnclosedBy + "'");
    debug(__func__, "fields_escaped_by: '" + _fieldsEscapedBy + "'");
    debug(__func__, "lines_terminated_by: '" + _linesTerminatedBy + "'");
    debug(__func__, "is_partitioned: " + string(_isPartitioned ? "1" : "0"));
    debug(__func__, "is_director: " + string(_isDirector ? "1" : "0"));
    debug(__func__, "id_col_name: '" + _idColName + "'");
    debug(__func__, "latitude_col_name: '" + _latitudeColName + "'");
    debug(__func__, "longitude_col_name: '" + _longitudeColName + "'");
    debug(__func__, "timeout: " + to_string(timeoutSec()));
    debug(__func__, "data file name: '" + _csvFileName + "'");

    verifyUserDatabaseName(__func__, _databaseName);
    verifyUserTableName(__func__, _tableName);

    // TODO: check if the required data file (CSV) was uploaded an saved to disk
    if (_csvFileName.empty()) {
        throw http::Error(__func__, "data file is missing in the request");
    }

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

    // The index definitions are optional and are expected to be an array of index descriptors.
    json indexes = json::array();
    if (body().has("indexes")) {
        indexes = body().objJson.at("indexes");
        if (!indexes.is_array()) {
            throw http::Error(__func__, "index definitions found in the request is not the JSON array");
        }
    }

    // Register the request in the QMeta database.
    _userTables = make_shared<qmeta::UserTables>(cconfig::CzarConfig::instance()->getMySqlQmetaConfig());
    _request.database = _databaseName;
    _request.table = _tableName;
    _request.dataFormat = qmeta::UserTableIngestRequest::DataFormat::CSV;
    _request.schema = schema;
    _request.indexes = indexes;
    _request.extended["charset"] = _charsetName;
    _request.extended["collation"] = _collationName;
    _request.extended["fields_terminated_by"] = _fieldsTerminatedBy;
    _request.extended["fields_enclosed_by"] = _fieldsEnclosedBy;
    _request.extended["fields_escaped_by"] = _fieldsEscapedBy;
    _request.extended["lines_terminated_by"] = _linesTerminatedBy;

    if (_isPartitioned) {
        // The current implementation supports only director tables for partitioned tables.
        _request.isTemporary = false;
        _request.tableType = qmeta::UserTableIngestRequest::TableType::DIRECTOR;
        _request.extended["is_director"] = "1";
        _request.extended["id_col_name"] = _idColName;
        _request.extended["latitude_col_name"] = _latitudeColName;
        _request.extended["longitude_col_name"] = _longitudeColName;

        // Extend the schema with the additonal columns that will be added to the partitioned
        // CVS files by the partitioning tool. This change neededs to be done here before
        // registering the new table to make sure the table schema in the Qserv prototype
        // database and in the Replication system's database matched the schema of the chunk
        // contributions to be generated by the partitioning tool.
        _request.schema.push_back(json::object({{"name", "chunkId"}, {"type", "INT UNSIGNED NOT NULL"}}));
        _request.schema.push_back(json::object({{"name", "subChunkId"}, {"type", "INT UNSIGNED NOT NULL"}}));
    } else {
        _request.isTemporary = true;
        _request.tableType = qmeta::UserTableIngestRequest::TableType::FULLY_REPLICATED;
    }
    _request = _userTables->registerRequest(_request);

    debug(__func__, "registered a new ingest request, id: " + to_string(_request.id));

    // Update the ingest statistics
    _getFileSize();

    // The rest of the ingest workflow depends on the table type
    if (_isPartitioned) {
        return _ingestDirectorTable();
    } else {
        return _ingestFullyReplicatedTable();
    }
}

json HttpCzarIngestCsvModule::_ingestDirectorTable() {
    debug(__func__);

    ::assertColumnExists(__func__, _request.schema, _idColName, "id_col_name");
    ::assertColumnExists(__func__, _request.schema, _latitudeColName, "latitude_col_name");
    ::assertColumnExists(__func__, _request.schema, _longitudeColName, "longitude_col_name");

    _createChunksDir();
    _partitionTableData();

    // Push the data to all workers and monitor the progress.
    try {
        bool const isPartitioned = true;
        list<pair<string, string>> const warnings = ingestData(
                _databaseName, _tableName, isPartitioned, _idColName, _latitudeColName, _longitudeColName,
                _charsetName, _collationName, _request.schema, _request.indexes, _chunkIds,
                [&](uint32_t transactionId,
                    map<int32_t, string> const& chunk2workerId) -> map<string, string> {
                    return _pushChunksToWorkers(transactionId, chunk2workerId);
                });

        // Make sure any warnings reported during the ingest are returned to the caller.
        for (auto const& warning : warnings) {
            warn(warning.first, warning.second);
        }
        _reportCompletedRequest(__func__);
    } catch (http::Error const& ex) {
        _reportFailedRequest(__func__, "push chunks to workers", ex.what(), ex.errorExt());
        throw;
    } catch (exception const& ex) {
        _reportFailedRequest(__func__, "push chunks to workers", ex.what());
        throw;
    }
    return json();
}

void HttpCzarIngestCsvModule::_createChunksDir() {
    boost::system::error_code ec;
    _chunksDirName = _tmpDir + "/" + "http-ingest-chunks-" + to_string(_request.id);
    fs::create_directories(_chunksDirName, ec);
    if (ec.value() != 0) {
        json const errorExt = _reportFailedRequest(
                __func__, "create chunks dir", "failed to create a temporary directory for chunk files",
                {{"path", _chunksDirName}, {"error_code", ec.value()}, {"error_message", ec.message()}});
        throw http::Error(__func__, "ingest request failed", errorExt);
    }
}

void HttpCzarIngestCsvModule::_getFileSize() {
    // Get the size of the input data file.
    boost::system::error_code ec;
    _numBytes = fs::file_size(_csvFileName, ec);
    if (ec.value() != 0) {
        json const errorExt = _reportFailedRequest(
                __func__, "get file size", "failed to get the size of the data file",
                {{"file", _csvFileName}, {"error_code", ec.value()}, {"error_message", ec.message()}});
        throw http::Error(__func__, "ingest request failed", errorExt);
    }
    debug(__func__, "data file size: " + to_string(_numBytes) + " bytes");
}

void HttpCzarIngestCsvModule::_partitionTableData() {
    json config = json::object(
            {{"dirTable", _tableName},
             {"dirColName", _idColName},
             {"id", _idColName},
             {"pos", json::array({_latitudeColName + ", " + _longitudeColName})},
             {"part", json::object({{"pos", _latitudeColName + ", " + _longitudeColName},
                                    {"num-stripes", HttpCzarIngestModuleBase::defaultNumStripes},
                                    {"num-sub-stripes", HttpCzarIngestModuleBase::defaultNumSubStripes},
                                    {"chunk", "chunkId"},
                                    {"sub-chunk", "subChunkId"},
                                    {"overlap", HttpCzarIngestModuleBase::defaultOverlap}})},
             {"in",
              json::object({{"path", json::array({_csvFileName})},
                            {"csv", json::object({{"null", "\\N"},
                                                  {"delimiter", ::translateToSingleChar(_fieldsTerminatedBy)},
                                                  {"enclose", ::translateToSingleChar(_fieldsEnclosedBy)},
                                                  {"escape", ::translateToSingleChar(_fieldsEscapedBy)},
                                                  {"field", json::array()}})}})},
             {"out",
              json::object({{"dir", _chunksDirName},
                            {"csv", json::object({{"null", "\\N"},
                                                  {"delimiter", ::translateToSingleChar(_fieldsTerminatedBy)},
                                                  {"enclose", ::translateToSingleChar(_fieldsEnclosedBy)},
                                                  {"escape", ::translateToSingleChar(_fieldsEscapedBy)},
                                                  {"no-quote", true}})}})},
             {"mr", json::object({{"num-workers", 1}})}});
    for (auto const& col : _request.schema) {
        string const colName = col.at("name").get<string>();
        if (colName == "chunkId" || colName == "subChunkId") {
            // These special columns are injected by the partitioning tool into the (yet to be) generated
            // chunk contributions and should not be present in the input collection of columns.
            continue;
        }
        config["in"]["csv"]["field"].push_back(colName);
    }
    int const argc = 1;
    char const* argv[] = {"czar-http"};
    try {
        // Instantiate and run the partitioning tool.
        partition::PartitionTool partitioner(config, argc, argv);

        // Get a collection of unique chunks, chunk tables and overlap tables from the index
        for (auto const& [chunkId, entry] : partitioner.chunkIndex->getChunks()) {
            _chunkIds.insert(chunkId);
            if (entry.numRecords > 0) {
                _chunkTables[chunkId] = _chunksDirName + "/chunk_" + to_string(chunkId) + ".txt";
                _numRows += entry.numRecords;
            }
            if (entry.numOverlapRecords > 0) {
                _overlapTables[chunkId] = _chunksDirName + "/chunk_" + to_string(chunkId) + "_overlap.txt";
            }
        }
        debug(__func__, "partitioned the data into " + to_string(_chunkIds.size()) + " unique chunks");
        debug(__func__, "chunk tables: " + to_string(_chunkTables.size()));
        debug(__func__, "overlap tables: " + to_string(_overlapTables.size()));
        if (_chunkIds.empty()) {
            throw http::Error(__func__, "no chunks were created during partitioning");
        }
        _numChunks = _chunkIds.size();
    } catch (partition::ExitOnHelp const& ex) {
        _reportFailedRequest(__func__, "partition data", "incorrect implementation of the application");
        throw;
    } catch (exception const& ex) {
        _reportFailedRequest(__func__, "partition data", ex.what());
        throw;
    }
}

map<string, string> HttpCzarIngestCsvModule::_pushChunksToWorkers(
        uint32_t transactionId, map<int32_t, string> const& chunk2workerId) {
    _transactionId = transactionId;

    auto const makeWorkerRequestKey = [&](int32_t chunkId, bool overlap) -> string {
        return chunk2workerId.at(chunkId) + ":" + to_string(chunkId) + (overlap ? ":overlap" : "");
    };
    auto const resultQueue = ingest::ResultQueue::create();
    for (auto const& [chunkId, file] : _chunkTables) {
        ingest::Request request(
                [&]() -> ingest::Result {
                    ingest::Result result{makeWorkerRequestKey(chunkId, false), ""};
                    try {
                        auto const mimeData = _makeMimeData(chunkId, false);
                        auto const workerId = chunk2workerId.at(chunkId);
                        auto const req = syncCsvRequestWorker(workerId, mimeData, _clientConnPool);
                        auto const resp = req->readAsJson();
                        if (resp.at("success").get<int>() == 0) {
                            result.error = "error: " + resp.at("error").get<string>();
                        }
                    } catch (exception const& ex) {
                        result.error = "ex: " + string(ex.what());
                    }
                    return result;
                },
                resultQueue);
        _workerIngestProcessor->push(request);
    }
    for (auto const& [chunkId, file] : _overlapTables) {
        ingest::Request request(
                [&]() -> ingest::Result {
                    ingest::Result result{makeWorkerRequestKey(chunkId, true), ""};
                    try {
                        auto const mimeData = _makeMimeData(chunkId, true);
                        auto const workerId = chunk2workerId.at(chunkId);
                        auto const req = syncCsvRequestWorker(workerId, mimeData, _clientConnPool);
                        auto const resp = req->readAsJson();
                        if (resp.at("success").get<int>() == 0) {
                            result.error = "error: " + resp.at("error").get<string>();
                        }
                    } catch (exception const& ex) {
                        result.error = "ex: " + string(ex.what());
                    }
                    return result;
                },
                resultQueue);
        _workerIngestProcessor->push(request);
    }

    // Wait for responses and analyze completion status of each worker request.
    // The loop will block until all workers have completed their tasks and results are collected
    // or the timeout is reached.
    map<string, string> errors;
    for (auto const& [chunkId, file] : _chunkTables) {
        auto const result = resultQueue->pop();
        if (!result.error.empty()) errors[makeWorkerRequestKey(chunkId, false)] = result.error;
    }
    for (auto const& [chunkId, file] : _overlapTables) {
        auto const result = resultQueue->pop();
        if (!result.error.empty()) errors[makeWorkerRequestKey(chunkId, true)] = result.error;
    }
    return errors;
}

list<http::ClientMimeEntry> HttpCzarIngestCsvModule::_makeMimeData(int32_t chunkId, bool overlap) const {
    list<http::ClientMimeEntry> mimeData = {
            {"transaction_id", to_string(_transactionId), "", ""},
            {"table", _tableName, "", ""},
            {"chunk", to_string(chunkId), "", ""},
            {"overlap", overlap ? "1" : "0", "", ""},
            {"charset_name", _charsetName, "", ""},
            {"fields_terminated_by", _fieldsTerminatedBy, "", ""},
            {"fields_enclosed_by", _fieldsEnclosedBy, "", ""},
            {"fields_escaped_by", _fieldsEscapedBy, "", ""},
            {"lines_terminated_by", _linesTerminatedBy, "", ""},
            {"rows", "", overlap ? _overlapTables.at(chunkId) : _chunkTables.at(chunkId), "text/csv"}};
    setProtocolFields(mimeData);
    return mimeData;
}

json HttpCzarIngestCsvModule::_ingestFullyReplicatedTable() {
    debug(__func__);

    // Push the data to all workers and monitor the progress.
    try {
        bool const isPartitioned = false;
        string const emptyDirectorIdColName;
        string const emptyDirectorLongitudeColName;
        string const emptyDirectorLatitudeColName;
        set<int32_t> chunkIds;  // no chunks for the fully replicated tables
        list<pair<string, string>> const warnings =
                ingestData(_databaseName, _tableName, isPartitioned, emptyDirectorIdColName,
                           emptyDirectorLongitudeColName, emptyDirectorLatitudeColName, _charsetName,
                           _collationName, _request.schema, _request.indexes, chunkIds,
                           [&](uint32_t transactionId, map<int32_t, string> const&) -> map<string, string> {
                               return _pushDataToWorkers(transactionId);
                           });

        // Make sure any warnings reported during the ingest are returned to the caller.
        for (auto const& warning : warnings) {
            warn(warning.first, warning.second);
        }
        _reportCompletedRequest(__func__);
    } catch (http::Error const& ex) {
        _reportFailedRequest(__func__, "push data to workers", ex.what(), ex.errorExt());
        throw;
    } catch (exception const& ex) {
        _reportFailedRequest(__func__, "push data to workers", ex.what());
        throw;
    }
    return json();
}

void HttpCzarIngestCsvModule::_reportCompletedRequest(string const& func) {
    _request = _userTables->ingestFinished(_request.id, qmeta::UserTableIngestRequest::Status::COMPLETED,
                                           string(), _transactionId, _numChunks, _numRows, _numBytes);
    debug(func, "ingest request completed, id: " + to_string(_request.id));
}

json HttpCzarIngestCsvModule::_reportFailedRequest(string const& func, string const& operation,
                                                   string const& errorMessage, json const& errorExt) {
    json errorJson = {{"id", _request.id}, {"operation", operation}, {"error", errorMessage}};
    if (!errorExt.empty()) {
        errorJson["errorExt"] = errorExt;
    }
    string const errorJsonStr = errorJson.dump();
    _request = _userTables->ingestFinished(_request.id, qmeta::UserTableIngestRequest::Status::FAILED,
                                           errorJsonStr, _transactionId, _numChunks, _numRows, _numBytes);
    error(func, "ingest request failed: " + errorJsonStr);
    return errorJson;
}

map<string, string> HttpCzarIngestCsvModule::_pushDataToWorkers(uint32_t transactionId) {
    _transactionId = transactionId;

    auto const resultQueue = ingest::ResultQueue::create();
    auto const workers = getWorkerIds();
    for (auto const& workerId : workers) {
        ingest::Request request(
                [&]() -> ingest::Result {
                    ingest::Result result{workerId, ""};
                    try {
                        auto const mimeData = _makeMimeData();
                        auto const req = syncCsvRequestWorker(workerId, mimeData, _clientConnPool);
                        auto const resp = req->readAsJson();
                        if (resp.at("success").get<int>() == 0) {
                            result.error = "error: " + resp.at("error").get<string>();
                        } else {
                            // All workers should return the same number of rows for the fully replicated
                            // tables.
                            _numRows = resp.at("contrib").at("num_rows").get<std::uint64_t>();
                        }
                    } catch (exception const& ex) {
                        result.error = "ex: " + string(ex.what());
                    }
                    return result;
                },
                resultQueue);
        _workerIngestProcessor->push(request);
    }

    // Wait for responses and analyze completion status of each worker request.
    // The loop will block until all workers have completed their tasks and results are collected
    // or the timeout is reached.
    map<string, string> errors;
    for (auto const& workerId : workers) {
        auto const result = resultQueue->pop();
        if (!result.error.empty()) errors[workerId] = result.error;
    }
    return errors;
}

list<http::ClientMimeEntry> HttpCzarIngestCsvModule::_makeMimeData() const {
    list<http::ClientMimeEntry> mimeData = {{"transaction_id", to_string(_transactionId), "", ""},
                                            {"table", _tableName, "", ""},
                                            {"chunk", "0", "", ""},
                                            {"overlap", "0", "", ""},
                                            {"charset_name", _charsetName, "", ""},
                                            {"fields_terminated_by", _fieldsTerminatedBy, "", ""},
                                            {"fields_enclosed_by", _fieldsEnclosedBy, "", ""},
                                            {"fields_escaped_by", _fieldsEscapedBy, "", ""},
                                            {"lines_terminated_by", _linesTerminatedBy, "", ""},
                                            {"rows", "", _csvFileName, "text/csv"}};
    setProtocolFields(mimeData);
    return mimeData;
}

}  // namespace lsst::qserv::czar
