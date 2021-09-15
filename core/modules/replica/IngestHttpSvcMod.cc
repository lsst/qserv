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
#include "replica/IngestHttpSvcMod.h"

// System headers
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include "curl/curl.h"

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/Csv.h"
#include "replica/DatabaseServices.h"
#include "replica/FileUtils.h"
#include "replica/HttpExceptions.h"
#include "replica/HttpFileReader.h"
#include "replica/Performance.h"
#include "replica/Url.h"

using namespace std;
namespace fs = boost::filesystem;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

/**
 * Class TemporaryCertFileRAII is used for storing certificate bandles in
 * temporary files managed based on the RAII paradigm.
 */
class TemporaryCertFileRAII {
public:
    /// The default constructor won't create any file.
    TemporaryCertFileRAII() = default;

    TemporaryCertFileRAII(TemporaryCertFileRAII const&) = delete;
    TemporaryCertFileRAII& operator=(TemporaryCertFileRAII const&) = delete;

    /// The destructor will take care of deleting a file should the one be created.
    ~TemporaryCertFileRAII() {
        // Make the best effort to delete the file. Ignore any errors.
        if (!_fileName.empty()) {
            boost::system::error_code ec;
            fs::remove(fs::path(_fileName), ec);
        }
    }

    /**
     * Create a temporary file and write a certificate bundle into it.
     * @param baseDir A folder where the file is created.
     * @param database The name of a database for which the file gets created.
     * @param cert The certificate bundle to be written into the file.
     * @return A path to the file including its folder.
     * @throw HttpError If the file couldn't be open for writing.
     */
    string write(string const& baseDir, string const& database, string const& cert) {
        string const prefix = database + "-";
        string const model = "%%%%-%%%%-%%%%-%%%%";
        string const suffix = ".cert";
        unsigned int const maxRetries = 1;
        _fileName = FileUtils::createTemporaryFile(baseDir, prefix, model, suffix, maxRetries);
        ofstream fs;
        fs.open(_fileName, ios::out|ios::trunc);
        if (!fs.is_open()) {
            raiseRetryAllowedError("TemporaryCertFileRAII::" + string(__func__),
                    "failed to open/create file '" + _fileName+ "'.");
        }
        fs << cert;
        fs.flush();
        fs.close();
        return _fileName;
    }
private:
    string _fileName;
};
}

namespace lsst {
namespace qserv {
namespace replica {

void IngestHttpSvcMod::process(ServiceProvider::Ptr const& serviceProvider,
                               string const& workerName,
                               string const& authKey,
                               string const& adminAuthKey,
                               qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp,
                               string const& subModuleName,
                               HttpModuleBase::AuthType const authType) {
    IngestHttpSvcMod module(serviceProvider, workerName, authKey, adminAuthKey, req, resp);
    module.execute(subModuleName, authType);
}


IngestHttpSvcMod::IngestHttpSvcMod(ServiceProvider::Ptr const& serviceProvider,
                                   string const& workerName,
                                   string const& authKey,
                                   string const& adminAuthKey,
                                   qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp)
        :   HttpModuleBase(authKey, adminAuthKey, req, resp),
            IngestFileSvc(serviceProvider, workerName) {
}


string IngestHttpSvcMod::context() const {
    return "INGEST-HTTP-SVC ";
}


json IngestHttpSvcMod::executeImpl(string const& subModuleName) {

    string const context = "IngestHttpSvcMod::" + string(__func__) + " ";
    debug(__func__, "subModuleName: '" + subModuleName + "'");

    if (!subModuleName.empty()) {
        throw invalid_argument(string(__func__) + " unsupported sub-module '" + subModuleName + "'");
    }

    // Initialize the contribution descriptor. It will be used through the rest of
    // the loading code as a source of the parameters of the request and for reporting
    // errors.

    TransactionContribInfo contrib;
    contrib.transactionId        = body().required<uint32_t>("transaction_id");
    contrib.table                = body().required<string>("table");
    contrib.chunk                = body().required<unsigned int>("chunk");
    contrib.isOverlap            = body().required<int>("overlap") != 0;
    contrib.worker               = workerInfo().name;
    contrib.url                  = body().required<string>("url");
    contrib.expirationTimeoutSec = body().optional<unsigned int>("expiration_timeout_sec", 0);

    // Allow "column_separator" for the sake of the backward compatibility with the older
    // version of the API. The parameter "column_separator" if present will override the one
    // of "fields_terminated_by"
    contrib.fieldsTerminatedBy = body().optional<string>(
        "column_separator",
        body().optional<string>("fields_terminated_by", csv::Dialect::defaultFieldsTerminatedBy)
    );
    contrib.fieldsEnclosedBy  = body().optional<string>("fields_enclosed_by",  csv::Dialect::defaultFieldsEnclosedBy);;
    contrib.fieldsEscapedBy   = body().optional<string>("fields_escaped_by",   csv::Dialect::defaultFieldsEscapedBy);;
    contrib.linesTerminatedBy = body().optional<string>("lines_terminated_by", csv::Dialect::defaultLinesTerminatedBy);;

    string const httpMethod = body().optional<string>("http_method", "GET");
    string const httpData = body().optional<string>("http_data", string());
    vector<string> const httpHeaders = body().optionalColl<string>("http_headers", vector<string>());

    debug(__func__, "transactionId: "          + to_string(contrib.transactionId));
    debug(__func__, "table: '"                 + contrib.table + "'");
    debug(__func__, "fields_terminated_by: '"  + contrib.fieldsTerminatedBy + "'");
    debug(__func__, "fields_enclosed_by: '"    + contrib.fieldsEnclosedBy + "'");
    debug(__func__, "fields_escaped_by: '"     + contrib.fieldsEscapedBy + "'");
    debug(__func__, "lines_terminated_by: '"   + contrib.linesTerminatedBy + "'");
    debug(__func__, "chunk: "                  + to_string(contrib.chunk));
    debug(__func__, "isOverlap: "              + string(contrib.isOverlap ? "1": "0"));
    debug(__func__, "url: '"                   + contrib.url + "'");
    debug(__func__, "http_method: '"           + httpMethod + "'");
    debug(__func__, "http_data: '"             + httpData + "'");
    debug(__func__, "http_headers.size(): "    + to_string(httpHeaders.size()));
    debug(__func__, "expiration_timeout_sec: " + to_string(contrib.expirationTimeoutSec));

    // Attempts to pass invalid transaction identifiers or tables are not recorded
    // as transaction contributions since it's impossible to determine a context
    // of these operations.

    auto const config = serviceProvider()->config();
    auto const databaseServices = serviceProvider()->databaseServices();

    auto const trans = databaseServices->transaction(contrib.transactionId);
    contrib.database = trans.database;

    if (!config->databaseInfo(contrib.database).hasTable(contrib.table)) {
        throw invalid_argument(context + "no such table '" + contrib.table + "' in database '" + contrib.database + "'.");
    }

    // Prescreen parameters of the request to ensure they're valid in the given
    // contex. Locate and check the state of the transaction. Refuse to proceed
    // with the request should any issues were detected.
    bool const failed = true;

    if (trans.state != TransactionInfo::STARTED) {
        contrib.error = context + " transactionId=" + to_string(contrib.transactionId) + " is not active";
        contrib = databaseServices->createdTransactionContrib(contrib, failed);
        throw logic_error(contrib.error);
    }

    unique_ptr<Url> resource;
    csv::Dialect dialect;
    try {
        resource.reset(new Url(contrib.url));
        switch (resource->scheme()) {
            case Url::FILE:
            case Url::HTTP:
            case Url::HTTPS:
                break;
            default:
                throw invalid_argument(context + " unsupported url '" + contrib.url + "'");
        }
        dialect = csv::Dialect(
            contrib.fieldsTerminatedBy,
            contrib.fieldsEnclosedBy,
            contrib.fieldsEscapedBy,
            contrib.linesTerminatedBy
        );
    } catch (exception const& ex) {
        contrib.error = string(ex.what());
        contrib = databaseServices->createdTransactionContrib(contrib, failed);
        throw;
    }
    csv::Parser parser(dialect);

    // Register the contribution
    contrib = databaseServices->createdTransactionContrib(contrib);

    // This is where the actual processing of the request begins. 
    try {
        openFile(contrib.transactionId, contrib.table, dialect, contrib.chunk, contrib.isOverlap);
        contrib = databaseServices->startedTransactionContrib(contrib);
    } catch (HttpError const& ex) {
        json const errorExt = ex.errorExt();
        if (!errorExt.empty()) {
            contrib.httpError = errorExt["http_error"];
            contrib.systemError = errorExt["system_error"];
            contrib.retryAllowed = errorExt["retry_allowed"].get<int>() != 0;
        }
        contrib.error = ex.what();
        contrib = databaseServices->startedTransactionContrib(contrib, failed);
        throw;
    } catch (exception const& ex) {
        contrib.systemError = errno;
        contrib.error = ex.what();
        contrib = databaseServices->startedTransactionContrib(contrib, failed);
        throw;
    }

    // Performance and statistics of the ingest operations (collected for each
    // file ingested). Timestamps represent the number of milliseconds since UNIX EPOCH
    json stats = json::object();
    json perf = json::object();

    // Start reading and preprocessing the input file.
    try {
        perf["begin_file_read_ms"] = PerformanceUtils::now();
        switch(resource->scheme()) {
            case Url::FILE:
                stats = _readLocal(parser, resource->filePath());
                break;
            case Url::HTTP:
            case Url::HTTPS:
                stats = _readRemote(parser, contrib.database, httpMethod, contrib.url, httpData, httpHeaders);
                break;
            default:
                throw invalid_argument(string(__func__) + " unsupported url '" + contrib.url + "'");
        }
        perf["end_file_read_ms"] = PerformanceUtils::now();

    } catch (HttpError const& ex) {
        json const errorExt = ex.errorExt();
        if (!errorExt.empty()) {
            contrib.httpError = errorExt["http_error"];
            contrib.systemError = errorExt["system_error"];
            contrib.retryAllowed = errorExt["retry_allowed"].get<int>() != 0;
        }
        contrib.error = ex.what();
        contrib = databaseServices->readTransactionContrib(contrib, failed);
        closeFile();
        throw;
    } catch (exception const& ex) {
        contrib.systemError = errno;
        contrib.error = ex.what();
        contrib = databaseServices->readTransactionContrib(contrib, failed);
        closeFile();
        throw;
    }
    contrib = databaseServices->readTransactionContrib(contrib);

    // Load the preprocessed input file into MySQL.
    try {
        perf["begin_file_ingest_ms"] = PerformanceUtils::now();
        loadDataIntoTable();
        perf["end_file_ingest_ms"] = PerformanceUtils::now();

        // Update contribution info in the database
        contrib.numBytes = stats["num_bytes"];
        contrib.numRows = stats["num_rows"];
        contrib = databaseServices->loadedTransactionContrib(contrib);
        debug(__func__, "transaction contribution recorded: " + contrib.toJson().dump());
    } catch (exception const& ex) {
        contrib.systemError = errno;
        contrib.error = ex.what();
        contrib = databaseServices->loadedTransactionContrib(contrib, failed);
        closeFile();
        throw;
    }
    closeFile();


    json result = json::object();
    result["stats"] = stats;
    result["perf"] = perf;
    return result;
}


json IngestHttpSvcMod::_readLocal(csv::Parser& parser,
                                  string const& filename) {
    debug(__func__);

    size_t numBytes = 0;
    size_t numRows = 0;
    unique_ptr<char[]> const record(new char[defaultRecordSizeBytes]);
    ifstream infile(filename, ios::binary);
    if (!infile.is_open()) {
        raiseRetryAllowedError(__func__, "failed to open the file '" + filename
                + "', error: '" + strerror(errno) + "', errno: " + to_string(errno));
    }
    bool eof = false;
    do {
        eof = !infile.read(record.get(), defaultRecordSizeBytes);
        if (eof && !infile.eof()) {
            raiseRetryAllowedError(__func__, "failed to read the file '" + filename
                    + "', error: '" + strerror(errno) + "', errno: " + to_string(errno));
        }
        size_t const num = infile.gcount();
        numBytes += num;
        // Flush the last record if the end of the file.
        parser.parse(record.get(), num, eof, [&](char const* buf, size_t size) {
            writeRowIntoFile(buf, size);
            ++numRows;
        });
    } while (!eof);

    json result = json::object();
    result["num_bytes"] = numBytes;
    result["num_rows"] = numRows;
    return result;
}


json IngestHttpSvcMod::_readRemote(csv::Parser& parser,
                                   string const& database,
                                   string const& method,
                                   string const& url,
                                   string const& data,
                                   vector<string> const& headers) {
    debug(__func__);
    size_t numBytes = 0;
    size_t numRows = 0;
    auto const reportRow = [&](char const* buf, size_t size) {
        writeRowIntoFile(buf, size);
        ++numRows;
    };

    // The configuration may be updated later if certificate bundles were loaded
    // by a client into the config store.
    auto fileConfig = _fileConfig(database);

    // Check if values of the certificate bundles were loaded into the configuration
    // store for the catalog. If so then write the certificates into temporary files
    // at the work folder configured to support HTTP-based file ingest operations.
    // The files are managed by the RAII resources, and they will get automatically
    // removed after successfully finishing reading the remote file or in case of any
    // exceptions.

    TemporaryCertFileRAII caInfoFile;
    if (!fileConfig.caInfoVal.empty()) {
        // Use this file instead of the existing path.
        fileConfig.caInfo = caInfoFile.write(
                workerInfo().httpLoaderTmpDir, database, fileConfig.caInfoVal);
    }
    TemporaryCertFileRAII proxyCaInfoFile;
    if (!fileConfig.proxyCaInfoVal.empty()) {
        // Use this file instead of the existing path.
        fileConfig.proxyCaInfo = proxyCaInfoFile.write(
                workerInfo().httpLoaderTmpDir, database, fileConfig.proxyCaInfoVal);
    }

    // Read and parse data from the data source
    bool const flush = true;
    HttpFileReader reader(method, url, data, headers, fileConfig);
    reader.read([&](char const* record, size_t size) {
        parser.parse(record, size, !flush, reportRow);
        numBytes += size;
    });
    // Flush the last non-terminated line stored in the parser (if any).
    string const emptyRecord;
    parser.parse(emptyRecord.data(), emptyRecord.size(), flush, reportRow);

    json result = json::object();
    result["num_bytes"] = numBytes;
    result["num_rows"] = numRows;
    return result;
}


HttpFileReaderConfig IngestHttpSvcMod::_fileConfig(string const& database) const {
    auto const databaseServices = serviceProvider()->databaseServices();

    auto const getString = [&databaseServices, &database](string& val, string const& key) -> bool {
        try {
            val = databaseServices->ingestParam(database, HttpFileReaderConfig::category, key).value;
        } catch (DatabaseServicesNotFound const&) {
            return false;
        }
        return true;
    };
    auto const getBool = [&getString](bool& val, string const& key) {
        string str;
        if (getString(str, key)) val = stoi(str) != 0;
    };
    auto const getLong = [&getString](long& val, string const& key) {
        string str;
        if (getString(str, key)) val = stol(str);
    };
    HttpFileReaderConfig fileConfig;
    getBool(  fileConfig.sslVerifyHost,      HttpFileReaderConfig::sslVerifyHostKey);
    getBool(  fileConfig.sslVerifyPeer,      HttpFileReaderConfig::sslVerifyPeerKey);
    getString(fileConfig.caPath,             HttpFileReaderConfig::caPathKey);
    getString(fileConfig.caInfo,             HttpFileReaderConfig::caInfoKey);
    getString(fileConfig.caInfoVal,          HttpFileReaderConfig::caInfoValKey);
    getBool(  fileConfig.proxySslVerifyHost, HttpFileReaderConfig::proxySslVerifyHostKey);
    getBool(  fileConfig.proxySslVerifyPeer, HttpFileReaderConfig::proxySslVerifyPeerKey);
    getString(fileConfig.proxyCaPath,        HttpFileReaderConfig::proxyCaPathKey);
    getString(fileConfig.proxyCaInfo,        HttpFileReaderConfig::proxyCaInfoKey);
    getString(fileConfig.proxyCaInfoVal,     HttpFileReaderConfig::proxyCaInfoValKey);
    getLong(  fileConfig.connectTimeout,     HttpFileReaderConfig::connectTimeoutKey);
    getLong(  fileConfig.timeout,            HttpFileReaderConfig::timeoutKey);
    getLong(  fileConfig.lowSpeedLimit,      HttpFileReaderConfig::lowSpeedLimitKey);
    getLong(  fileConfig.lowSpeedTime,       HttpFileReaderConfig::lowSpeedTimeKey);
    return fileConfig;
}

}}} // namespace lsst::qserv::replica
