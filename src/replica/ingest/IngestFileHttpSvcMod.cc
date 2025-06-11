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
#include "replica/ingest/IngestFileHttpSvcMod.h"

// Qserv header
#include "http/Auth.h"
#include "http/BinaryEncoding.h"
#include "http/Exceptions.h"
#include "http/Url.h"
#include "replica/config/Configuration.h"
#include "replica/ingest/IngestUtils.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Csv.h"

// System headers
#include <errno.h>
#include <stdexcept>
#include <vector>

// Third party headers
#include "httplib.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void IngestFileHttpSvcMod::process(shared_ptr<ServiceProvider> const& serviceProvider,
                                   string const& workerName, httplib::Request const& req,
                                   httplib::Response& resp, httplib::ContentReader const& contentReader,
                                   http::AuthType const authType) {
    IngestFileHttpSvcMod module(serviceProvider, workerName, req, resp, contentReader);
    string const subModuleName;
    module.execute(subModuleName, authType);
}

IngestFileHttpSvcMod::IngestFileHttpSvcMod(shared_ptr<ServiceProvider> const& serviceProvider,
                                           string const& workerName, httplib::Request const& req,
                                           httplib::Response& resp,
                                           httplib::ContentReader const& contentReader)
        : http::FileUploadModule(serviceProvider->httpAuthContext(), req, resp, contentReader),
          IngestFileSvc(serviceProvider, workerName) {}

string IngestFileHttpSvcMod::context() const { return "INGEST-FILE-HTTP-SVC "; }

void IngestFileHttpSvcMod::onStartOfFile(string const& name, string const& fileName,
                                         string const& contentType) {
    debug(__func__);
    checkApiVersion(__func__, 38);

    auto const context_ = context() + __func__;
    auto const config = serviceProvider()->config();
    auto const databaseServices = serviceProvider()->databaseServices();

    if (isOpen()) {
        throw http::Error(context_, "a file is already opened");
    }
    if (!_contrib.tmpFile.empty()) {
        throw http::Error(context_, "the service only allows one file per request");
    }

    // Fill out parameters in the contribution descriptor. This information is needed
    // for bookeeping and monitoring purposes. The descriptor's state will be kept
    // updated in the Replication/Ingest's database as the contribution processing
    // will be happening.
    _contrib.transactionId = body().requiredUInt("transaction_id");
    _contrib.table = body().required<string>("table");
    _contrib.chunk = body().requiredUInt("chunk");
    _contrib.isOverlap = body().requiredUInt("overlap") != 0;
    _contrib.worker = workerName();

    // To indicate the file contents was streamed directly into the service
    _contrib.url = "data-csv://" + req().remote_addr + "/" + fileName;
    _contrib.charsetName =
            body().optional<string>("charset_name", config->get<string>("worker", "ingest-charset-name"));
    _contrib.dialectInput = parseDialectInput(body());

    // Retries are allowed before an attemp to load data into MySQL. When such attempt
    // is made the persistent state of the destination table is supposed to be changed.
    _contrib.retryAllowed = true;

    // This parameters sets a limit foe the number of warnings (should there be any)
    // reported by MySQL after contribution loading attempt. Warnings is an important
    // mechanism for debugging problems with the ingested data.
    _contrib.maxNumWarnings = body().optionalUInt("max_num_warnings",
                                                  config->get<unsigned int>("worker", "loader-max-warnings"));

    debug(__func__, "transaction_id: " + to_string(_contrib.transactionId));
    debug(__func__, "table: '" + _contrib.table + "'");
    debug(__func__, "chunk: " + to_string(_contrib.chunk));
    debug(__func__, "overlap: " + string(_contrib.isOverlap ? "1" : "0"));
    debug(__func__, "charset_name: '" + _contrib.charsetName + "'");
    debug(__func__, "max_num_warnings: " + to_string(_contrib.maxNumWarnings));

    // Attempts to pass invalid transaction identifiers or tables are not recorded
    // as transaction contributions in the persistent state of the Replication/Ingest
    // system since it's impossible to determine a context of these operations.
    // The following operations will throw exceptions should any problems with
    // validation a context of the request will be encountered.
    TransactionInfo const trans = databaseServices->transaction(_contrib.transactionId);
    _contrib.database = trans.database;

    DatabaseInfo const database = config->databaseInfo(_contrib.database);
    TableInfo const table = database.findTable(_contrib.table);

    // Prescreen parameters of the request to ensure they're valid in the given
    // contex. Check the state of the transaction. Refuse to proceed with the request
    // if any issues were detected.

    bool const failed = true;

    if (trans.state != TransactionInfo::State::STARTED) {
        _contrib.error = context_ + " transactionId=" + to_string(_contrib.transactionId) + " is not active";
        _contrib = databaseServices->createdTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        throw http::Error(context_, _contrib.error);
    }

    csv::Dialect dialect;
    try {
        http::Url const resource(_contrib.url);
        if (resource.scheme() != http::Url::DATA_CSV) {
            throw invalid_argument(context_ + " unsupported url '" + _contrib.url + "'");
        }
        dialect = csv::Dialect(_contrib.dialectInput);
        _parser.reset(new csv::Parser(dialect));
    } catch (exception const& ex) {
        _contrib.error = ex.what();
        _contrib = databaseServices->createdTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        throw;
    }

    // Register the contribution
    _contrib = databaseServices->createdTransactionContrib(_contrib);

    // This is where the actual processing of the request begins.
    try {
        _contrib.tmpFile = openFile(_contrib.transactionId, _contrib.table, dialect, _contrib.charsetName,
                                    _contrib.chunk, _contrib.isOverlap);
        _contrib = databaseServices->startedTransactionContrib(_contrib);
    } catch (http::Error const& ex) {
        json const errorExt = ex.errorExt();
        if (!errorExt.empty()) {
            _contrib.httpError = errorExt["http_error"];
            _contrib.systemError = errorExt["system_error"];
            _contrib.retryAllowed = errorExt["retry_allowed"].get<int>() != 0;
        }
        _contrib.error = ex.what();
        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        throw;
    } catch (exception const& ex) {
        _contrib.systemError = errno;
        _contrib.error = ex.what();
        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        throw;
    }
}

void IngestFileHttpSvcMod::onFileData(char const* data, size_t length) {
    auto const context_ = context() + __func__;
    if (!isOpen()) {
        throw http::Error(context_, "no file was opened");
    }
    _parseAndWriteData(data, length, false);
}

void IngestFileHttpSvcMod::onEndOfFile() {
    auto const context_ = context() + __func__;
    if (!isOpen()) {
        throw http::Error(context_, "no file was opened");
    }

    // Flush the parser to ensure the last row (if any) has been writen
    // into the output file.
    char const data[0] = {};
    size_t const length = 0;
    _parseAndWriteData(data, length, true);

    // Report that processing of the input data and preparing the contribution file is over.
    auto const databaseServices = serviceProvider()->databaseServices();
    _contrib = databaseServices->readTransactionContrib(_contrib);

    // Finished reading and preprocessing the input file.
    // Begin making irreversible changes to the destination table.
    _contrib.retryAllowed = false;
    try {
        loadDataIntoTable(_contrib.maxNumWarnings);
        _contrib.numWarnings = numWarnings();
        _contrib.warnings = warnings();
        _contrib.numRowsLoaded = numRowsLoaded();
        _contrib = databaseServices->loadedTransactionContrib(_contrib);
        closeFile();
    } catch (exception const& ex) {
        _contrib.error = "MySQL load failed, ex: " + string(ex.what());
        _contrib.systemError = errno;
        bool const failed = true;
        databaseServices->loadedTransactionContrib(_contrib, failed);
        _failed(context_);
        throw http::Error(context_, _contrib.error);
    }
}

json IngestFileHttpSvcMod::onEndOfBody() {
    auto const context_ = context() + __func__;
    if (_contrib.tmpFile.empty()) {
        throw http::Error(context_, "no file was sent in the request");
    }
    if (isOpen()) {
        throw http::Error(context_, "the file is still open");
    }
    return json::object({{"contrib", _contrib.toJson()}});
}

void IngestFileHttpSvcMod::_parseAndWriteData(char const* data, size_t length, bool flush) {
    _parser->parse(data, length, flush, [&](char const* buf, size_t size) {
        writeRowIntoFile(buf, size);
        _contrib.numRows++;
    });
    _contrib.numBytes += length;  // count unmodified input data
}

void IngestFileHttpSvcMod::_failed(string const& context_) {
    error(context_, _contrib.error);
    closeFile();
}

}  // namespace lsst::qserv::replica
