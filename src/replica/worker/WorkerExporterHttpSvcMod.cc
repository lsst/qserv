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
#include "replica/worker/WorkerExporterHttpSvcMod.h"

// System headers
#include <fstream>
#include <stdexcept>
#include <set>
#include <vector>

// Third-party headers
#include "boost/filesystem.hpp"
#include <httplib.h>

// Qserv header
#include "global/stringUtil.h"
#include "http/Auth.h"
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLExceptions.h"
#include "replica/mysql/DatabaseMySQLGenerator.h"
#include "replica/mysql/DatabaseMySQLTypes.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ChunkedTable.h"
#include "replica/util/ChunkNumber.h"
#include "replica/util/FileUtils.h"
#include "util/String.h"

using namespace std;
using json = nlohmann::json;
namespace fs = boost::filesystem;

namespace {
bool const sendCustomResponse = true;
size_t const maxRecLen = 1024 * 1024;  // 1 MB
}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

void WorkerExporterHttpSvcMod::process(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName,
        shared_ptr<database::mysql::ConnectionPool> const& databaseConnectionPool,
        httplib::Request const& req, httplib::Response& resp, string const& subModuleName,
        http::AuthType const authType) {
    WorkerExporterHttpSvcMod module(serviceProvider, workerName, databaseConnectionPool, req, resp);
    module.execute(subModuleName, authType);
}

WorkerExporterHttpSvcMod::WorkerExporterHttpSvcMod(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName,
        shared_ptr<database::mysql::ConnectionPool> const& databaseConnectionPool,
        httplib::Request const& req, httplib::Response& resp)
        : http::ChttpModule(serviceProvider->httpAuthContext(), req, resp, ::sendCustomResponse),
          _serviceProvider(serviceProvider),
          _workerName(workerName),
          _databaseConnectionPool(databaseConnectionPool) {}

string WorkerExporterHttpSvcMod::context() const { return "WORKER-EXPORTER-HTTP-SVC "; }

json WorkerExporterHttpSvcMod::executeImpl(string const& subModuleName) {
    debug(__func__, "subModuleName: '" + subModuleName + "'");

    auto const sendError = [&](string const& func, string const& msg, httplib::StatusCode status) {
        string const content = "<!DOCTYPE html><html><head><title>Error</title></head><body><h1>Code: " +
                               to_string(static_cast<int>(status)) + "</h1><p>" + msg + "</p></body></html>";
        error(func, msg);
        resp().set_content(content, "text/html");
        resp().status = status;
    };

    // Notes on exception handling and reporting:
    //
    // - std::invalid_argument, database::mysql::ER_NO_SUCH_TABLE_
    //   are interpreted as resource not found errors. They are reported
    //   to the client with status code 404.
    //
    // - Other exceptions are reported as internal server error 500 w/o exposing
    //   too many details to the client.
    //
    // - The code 501 is used as an indication that the sub-module name is unknown.
    //   Normally this means that the request routing is inconsistent with
    //   the implementation of this module.
    try {
        if (subModuleName == "TABLE") {
            _table();
        } else if (subModuleName == "CHUNK") {
            _chunk();
        } else {
            sendError(__func__, "unsupported sub-module: '" + subModuleName + "'",
                      httplib::StatusCode::NotImplemented_501);
        }
    } catch (invalid_argument const& ex) {
        sendError(__func__, "std::invalid_argument: " + string(ex.what()), httplib::StatusCode::NotFound_404);
    } catch (database::mysql::ER_NO_SUCH_TABLE_ const& ex) {
        sendError(__func__, "database::mysql::ER_NO_SUCH_TABLE_: " + string(ex.what()),
                  httplib::StatusCode::NotFound_404);
    } catch (exception const& ex) {
        sendError(__func__, "std::exception: " + string(ex.what()),
                  httplib::StatusCode::InternalServerError_500);
    }

    // This module uses custom response sending mechanism. Return an empty JSON object
    // as required by the base class's interface.
    return json();
}

void WorkerExporterHttpSvcMod::_table() {
    debug(__func__);
    checkApiVersion(__func__, 53);
    _isChunk = false;
    _processRequest(__func__);
}

void WorkerExporterHttpSvcMod::_chunk() {
    debug(__func__);
    checkApiVersion(__func__, 53);
    _isChunk = true;
    _processRequest(__func__);
}

void WorkerExporterHttpSvcMod::_processRequest(string const& func) {
    _parseParameters(func);
    _createTemporaryFile(func);
    _dumpTableIntoFile(func);
    _sendFileInResponse(func);
}

void WorkerExporterHttpSvcMod::_parseParameters(string const& func) {
    // Parse and validate required parameters in the request's path
    _assertParamExists(func, "database");
    _assertParamExists(func, "table");

    _databaseName = params().at("database");
    _tableName = params().at("table");

    debug(func, "database=" + _databaseName);
    debug(func, "table=" + _tableName);

    // Database and table existence will be validated and by the corresponding calls
    // to the configuration and the database services. These methods will throw
    // std::invalid_argument if the database or the table don't exist.
    auto const config = _serviceProvider->config();
    auto const database = config->databaseInfo(_databaseName);
    auto const table = database.findTable(_tableName);

    if (_isChunk) {
        _assertParamExists(func, "chunk");
        string const chunkStr = params().at("chunk");
        debug(func, "chunk=" + chunkStr);
        try {
            _chunkNumber = qserv::stoui(chunkStr);
        } catch (exception const& ex) {
            throw http::Error(func,
                              "the 'chunk' parameter is not a valid unsigned integer: '" + chunkStr + "");
        }
        auto const family = config->databaseFamilyInfo(database.family);
        ChunkNumberQservValidator const validator(family.numStripes, family.numSubStripes);
        if (!validator.valid(_chunkNumber)) {
            throw http::Error(func, "this chunk number " + to_string(_chunkNumber) +
                                            " is not valid in the scope of database '" + database.name + "'");
        }
    }

    // Parse optional parameters in the query string
    if (_isChunk) {
        _isOverlap = query().optionalUInt("overlap", _isOverlap ? 1 : 0) != 0;
        debug(func, "overlap=" + string(_isOverlap ? "1" : "0"));
    }

    // Parse optional format-specific parameters in the query string
    _format = util::String::toUpper(query().optionalString("format", _format));
    debug(func, "format=" + _format);
    if (_format != "CSV") {
        throw http::Error(func, "the 'format' parameter has unsupported value: '" + _format +
                                        "'. The current implementation supports only 'CSV' format.");
    }
    _parseCsvColumnFilters(func);
    _parseCsvDialect(func);
}

void WorkerExporterHttpSvcMod::_assertParamExists(string const& func, string const& name) {
    if (params().count(name) == 0) {
        throw http::Error(func, "the required '" + name + "' parameter is missing");
    }
}

void WorkerExporterHttpSvcMod::_parseCsvColumnFilters(string const& func) {
    auto parseFlag = [&](string const& name, bool& value) {
        value = query().optionalUInt(name, value ? 1 : 0) != 0;
        debug(func, name + "=" + string(value ? "1" : "0"));
    };
    parseFlag("keep_trans_id", _keepTransIdColumn);
    if (_isChunk) {
        parseFlag("keep_chunk_id", _keepChunkIdColumn);
        parseFlag("keep_sub_chunk_id", _keepSubChunkIdColumn);
    }
}

void WorkerExporterHttpSvcMod::_parseCsvDialect(string const& func) {
    csv::DialectInput di;
    di.fieldsTerminatedBy = query().optionalString("fields_terminated_by", di.fieldsTerminatedBy);
    di.fieldsEnclosedBy = query().optionalString("fields_enclosed_by", di.fieldsEnclosedBy);
    di.fieldsEscapedBy = query().optionalString("fields_escaped_by", di.fieldsEscapedBy);
    di.linesTerminatedBy = query().optionalString("lines_terminated_by", di.linesTerminatedBy);
    debug(func, "fields_terminated_by='" + di.fieldsTerminatedBy + "'");
    debug(func, "fields_enclosed_by='" + di.fieldsEnclosedBy + "'");
    debug(func, "fields_escaped_by='" + di.fieldsEscapedBy + "'");
    debug(func, "lines_terminated_by='" + di.linesTerminatedBy + "'");
    _csvDialect = csv::Dialect(di);
}

void WorkerExporterHttpSvcMod::_createTemporaryFile(string const& func) {
    // This algorithm creates a temporary file name and then removes the file
    // to allow streaming the exported data into the file later during processing.
    // The file will be created later when data will be written into it.
    // Besides generating the name, the method also validates that the file
    // can be created and removed in the specified directory.
    try {
        auto const baseFileName =
                _databaseName + "-" +
                (_isChunk ? ChunkedTable(_tableName, _chunkNumber, _isOverlap).name() : _tableName);
        _filePath = FileUtils::createTemporaryFile(
                _serviceProvider->config()->get<string>("worker", "exporter-tmp-dir"), baseFileName,
                "-%%%%-%%%%-%%%%-%%%%", ".csv");
    } catch (exception const& ex) {
        throw http::Error(func, "failed to create the temporary file '" + _filePath + "', ex: " + ex.what());
    }
    boost::system::error_code ec;
    fs::remove(fs::path(_filePath), ec);
    if (ec.value() != 0) {
        throw http::Error(func, "failed to remove the temporary file '" + _filePath +
                                        "', code: " + to_string(ec.value()) + ", message: " + ec.message());
    }
}

void WorkerExporterHttpSvcMod::_dumpTableIntoFile(string const& func) {
    // Databae connection is allocated from the pool by the RAII helper to ensure
    // its proper release back into the pool.
    ConnectionHandler h(_databaseConnectionPool);
    QueryGenerator const g(h.conn);
    auto const sqlTable =
            _isChunk ? g.id(_databaseName, ChunkedTable(_tableName, _chunkNumber, _isOverlap).name())
                     : g.id(_databaseName, _tableName);
    string query;
    if (_keepAllCsvColumns()) {
        query = g.select(Sql::STAR);
    } else {
        auto const tableSchema = tableSchemaDetailed(h.conn, _databaseName, _tableName);
        set<string> columns2drop;
        if (!_keepTransIdColumn) columns2drop.insert("qserv_trans_id");
        if (_isChunk) {
            if (!_keepChunkIdColumn) columns2drop.insert("chunkId");
            if (!_keepSubChunkIdColumn) columns2drop.insert("subChunkId");
        }
        vector<string> columns2keep;
        for (auto const& columnInfo : tableSchema) {
            string const columnName = columnInfo.at("COLUMN_NAME").get<string>();
            if (columns2drop.find(columnName) == columns2drop.end()) {
                columns2keep.push_back(columnName);
            }
        }
        query = g.select(columns2keep);
    }
    query += g.from(sqlTable) + g.intoOutfile(_filePath, _csvDialect);

    // Non-existing database, table or chunk will be reported by the database layer
    // by throwing database::mysql::ER_NO_SUCH_TABLE_ exception.
    h.conn->executeInOwnTransaction([&query](decltype(h.conn) const& conn) { conn->execute(query); });
}

void WorkerExporterHttpSvcMod::_sendFileInResponse(string const& func) {
    // Get file size
    boost::system::error_code ec;
    auto const fileSize = fs::file_size(fs::path(_filePath), ec);
    if (ec.value() != 0) {
        throw http::Error(func, "failed to get the size of the temporary file '" + _filePath +
                                        "', code: " + to_string(ec.value()) + ", message: " + ec.message());
    }
    debug(func, "file size: " + to_string(fileSize) + " bytes");

    // Open the file for reading
    shared_ptr<ifstream> const filePtr = make_shared<ifstream>(_filePath, ios::in | ios::binary);
    if (!filePtr->is_open()) {
        throw http::Error(func, "failed to open the temporary file '" + _filePath + "' for reading");
    }

    // A buffer for sending data in response
    shared_ptr<string> const sendDataBuf = make_shared<string>(::maxRecLen, '\0');

    // Send the file in response using the streaming mechanism.
    //
    // IMPORTANT: This is not the blocking call. The data will be streamed
    // in chunks via the provided callback function after the current
    // method will return and after the current object will be destroyed.
    // Hence, variables filePtr and sendDataBuf captured by the callback function are
    // allocated on the heap (using shared_ptr for automatic memory management).
    // One downside of this approach is that error reporting into the application's
    // logging system is not possible. The problem may be solved later after
    // introducing the persistent backend for bookkeeping the table export operations.
    //
    // The method will close and delete the file upon completion of the data
    // transfer (successfully or not).
    resp().set_content_provider(
            // Response headers: 'Content-Length' and 'Content-Type'
            fileSize, "text/csv",
            // The callback function is called repeatedly to stream data in chunks
            [filePtr, sendDataBuf](size_t offset, size_t length, httplib::DataSink& sink) {
                size_t const recLen = std::min(length, ::maxRecLen);
                filePtr->read(&(*sendDataBuf)[0], recLen);
                sink.write(sendDataBuf->data(), recLen);
                return true;  // 'false' is reserved for canceling the operation
            },
            // The completion callback is called once when all data has been sent
            // or when an error has occurred.
            [filePtr, filePath = _filePath](bool success) {
                filePtr->close();
                boost::system::error_code ec;
                fs::remove(fs::path(filePath), ec);
            });
}

}  // namespace lsst::qserv::replica
