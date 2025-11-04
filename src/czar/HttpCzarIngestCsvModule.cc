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
#include "qmeta/UserTables.h"
#include "qmeta/UserTableIngestRequest.h"
#include "qhttp/Status.h"

using namespace std;
namespace asio = boost::asio;
namespace cconfig = lsst::qserv::cconfig;
namespace fs = boost::filesystem;
namespace http = lsst::qserv::http;
using json = nlohmann::json;

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
    checkApiVersion(__func__, 49);

    _databaseName = body().required<string>("database");
    _tableName = body().required<string>("table");
    _charsetName = body().optional<string>("charset_name", "latin1");
    _collationName = body().optional<string>("collation_name", "latin1_swedish_ci");
    _fieldsTerminatedBy = body().optional<string>("fields_terminated_by", R"(\t)");
    _fieldsEnclosedBy = body().optional<string>("fields_enclosed_by", R"(\0)");
    _fieldsEscapedBy = body().optional<string>("fields_escaped_by", R"(\\)");
    _linesTerminatedBy = body().optional<string>("lines_terminated_by", R"(\n)");

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
    qmeta::UserTables userTables(cconfig::CzarConfig::instance()->getMySqlQmetaConfig());
    qmeta::UserTableIngestRequest request;
    request.database = _databaseName;
    request.table = _tableName;
    request.tableType = qmeta::UserTableIngestRequest::TableType::FULLY_REPLICATED;
    request.isTemporary = true;
    request.dataFormat = qmeta::UserTableIngestRequest::DataFormat::CSV;
    request.schema = schema;
    request.indexes = indexes;
    request.extended["charset"] = _charsetName;
    request.extended["collation"] = _collationName;
    request.extended["fields_terminated_by"] = _fieldsTerminatedBy;
    request.extended["fields_enclosed_by"] = _fieldsEnclosedBy;
    request.extended["fields_escaped_by"] = _fieldsEscapedBy;
    request.extended["lines_terminated_by"] = _linesTerminatedBy;
    request = userTables.registerRequest(request);
    debug(__func__, "registered a new ingest request, id: " + to_string(request.id));

    // Push the data to all workers and monitor the progress.
    try {
        list<pair<string, string>> const warnings =
                ingestData(_databaseName, _tableName, _charsetName, _collationName, schema, indexes,
                           [&](uint32_t transactionId) -> map<string, string> {
                               return _pushDataToWorkers(transactionId);
                           });

        // Make sure any warnings reported during the ingest are returned to the caller.
        for (auto const& warning : warnings) {
            warn(warning.first, warning.second);
        }
        request = userTables.ingestFinished(request.id, qmeta::UserTableIngestRequest::Status::COMPLETED,
                                            string(), _transactionId, _numChunks, _numRows, _numBytes);
        debug(__func__, "ingest request completed, id: " + to_string(request.id));
    } catch (http::Error const& ex) {
        json const errorJson = {
                {"id", request.id}, {"error", string(ex.what())}, {"errorExt", ex.errorExt()}};
        string const errorJsonStr = errorJson.dump();
        request = userTables.ingestFinished(request.id, qmeta::UserTableIngestRequest::Status::FAILED,
                                            errorJsonStr, _transactionId, _numChunks, _numRows, _numBytes);
        error(__func__, "ingest request failed: " + errorJsonStr);
        throw;
    } catch (exception const& ex) {
        json const errorDetails = {{"id", request.id}, {"error", string(ex.what())}};
        string const errorDetailsStr = errorDetails.dump();
        request = userTables.ingestFinished(request.id, qmeta::UserTableIngestRequest::Status::FAILED,
                                            errorDetailsStr, _transactionId, _numChunks, _numRows, _numBytes);
        error(__func__, "ingest request failed: " + errorDetailsStr);
        throw;
    }
    return json();
}

map<string, string> HttpCzarIngestCsvModule::_pushDataToWorkers(uint32_t transactionId) {
    list<http::ClientMimeEntry> mimeData = {{"transaction_id", to_string(transactionId), "", ""},
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

    debug(__func__, "pushing data to workers, transactionId: " + to_string(transactionId) + ", table: '" +
                            _tableName + "', data file: '" + _csvFileName + "'");

    _transactionId = transactionId;

    // Send table data to all eligible workers.
    auto const resultQueue = ingest::ResultQueue::create();
    auto const workers = getWorkerIds();
    for (auto const& workerId : workers) {
        ingest::Request request(
                [&]() -> ingest::Result {
                    ingest::Result result{workerId, ""};
                    try {
                        auto const req = syncCsvRequestWorker(workerId, mimeData, _clientConnPool);
                        auto const resp = req->readAsJson();
                        if (resp.at("success").get<int>() == 0) {
                            result.error = "error: " + resp.at("error").get<string>();
                        } else {
                            // Update ingest statistics. Values of the counters reported by workers
                            // are expected to be the same for the fully replicated tables.
                            // Though the last statement is not checked or enforced by the current
                            // implementation updating ingest statistics for each worker allows to get the
                            // values even if only one worker completes the request successfully.
                            json const& contrib = resp.at("contrib");
                            _numRows = contrib.at("num_rows").get<std::uint64_t>();
                            _numBytes = contrib.at("num_bytes").get<std::uint64_t>();
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

}  // namespace lsst::qserv::czar
