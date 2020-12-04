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
#include <cstring>
#include <fstream>
#include <iostream>
#include "curl/curl.h"

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/Common.h"
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
 * temporary files managed basd on the RAII paradigm.
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
            HttpError("TemporaryCertFileRAII::" + string(__func__), "failed to open/create file '"
                        + _fileName+ "'.");
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
    debug(__func__, "subModuleName: '" + subModuleName + "'");
    if (!subModuleName.empty()) {
        throw invalid_argument(string(__func__) + " unsupported sub-module '" + subModuleName + "'");
    }
    TransactionId const transactionId = body().required<uint32_t>("transaction_id");
    string const table = body().required<string>("table");
    string const columnSeparatorStr = body().required<string>("column_separator");
    if (columnSeparatorStr.empty() || columnSeparatorStr.size() != 1) {
        throw invalid_argument(string(__func__) + " column separator must be a single character string");
    }
    char const columnSeparator = columnSeparatorStr[0];
    unsigned int const chunk = body().required<unsigned int>("chunk");
    bool const isOverlap = body().required<int>("overlap") != 0;
    string const url = body().required<string>("url");
    string const httpMethod = body().optional<string>("http_method", "GET");
    string const httpData = body().optional<string>("http_data", string());
    vector<string> const httpHeaders = body().optionalColl<string>("http_headers", vector<string>());

    debug(__func__, "transactionId: " + to_string(transactionId));
    debug(__func__, "table: '" + table + "'");
    debug(__func__, "columnSeparator: '" + to_string(columnSeparator) + "'");
    debug(__func__, "chunk: " + to_string(chunk));
    debug(__func__, "isOverlap: " + string(isOverlap ? "1": "0"));
    debug(__func__, "url: '" + url + "'");
    debug(__func__, "http_method: '" + httpMethod + "'");
    debug(__func__, "http_data: '" + httpData + "'");
    debug(__func__, "http_headers.size(): " + to_string(httpHeaders.size()));

    auto const databaseServices = serviceProvider()->databaseServices();
    TransactionInfo const trans = databaseServices->transaction(transactionId);

    // Performance and statistics of the ingest operations (collected for each
    // file ingested). Timestamps represent the number of milliseconds since UNIX EPOCH
    json stats = json::object();
    json perf = json::object();
    openFile(transactionId, table, columnSeparator, chunk, isOverlap);
    try {
        Url const resource(url);

        perf["begin_file_read_ms"] = PerformanceUtils::now();
        switch(resource.scheme()) {
            case Url::FILE:
                stats = _readLocal(resource.filePath());
                break;
            case Url::HTTP:
            case Url::HTTPS:
                stats = _readRemote(trans.database, httpMethod, resource.url(), httpData, httpHeaders);
                break;
            default:
                throw invalid_argument(string(__func__) + " unsupported url '" + url + "'");
        }
        perf["end_file_read_ms"] = PerformanceUtils::now();

        perf["begin_file_ingest_ms"] = PerformanceUtils::now();
        loadDataIntoTable();
        perf["end_file_ingest_ms"] = PerformanceUtils::now();

    } catch (...) {
        closeFile();
        throw;
    }
    closeFile();
    json result = json::object();
    result["stats"] = stats;
    result["perf"] = perf;
    return result;
}


json IngestHttpSvcMod::_readLocal(string const& filename) {
    debug(__func__);
    size_t numBytes = 0;
    size_t numRows = 0;
    ifstream infile(filename);
    if (!infile.is_open()) {
        throw HttpError(
                __func__, "failed to open file '" + filename
                + "', error: '" + strerror(errno) + "', errno: " + to_string(errno));
    }
    for (string row; getline(infile, row);) {
        writeRowIntoFile(row);
        numBytes += row.size() + 1;     // counting the newline character
        ++numRows;
    }
    json result = json::object();
    result["num_bytes"] = numBytes;
    result["num_rows"] = numRows;
    return result;
}


json IngestHttpSvcMod::_readRemote(string const& database,
                                   string const& method,
                                   string const& url,
                                   string const& data,
                                   vector<string> const& headers) {
    debug(__func__);
    size_t numBytes = 0;
    size_t numRows = 0;

    // The configuration may be amended later if certificate bundles were loaded
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

    // Read the file from the data source
    HttpFileReader reader(method, url, data, headers, fileConfig);
    reader.read([this, &numBytes, &numRows](string const& row) {
        this->writeRowIntoFile(row);
        numBytes += row.size() + 1;     // counting the newline character
        ++numRows;
    });
    json result = json::object();
    result["num_bytes"] = numBytes;
    result["num_rows"] = numRows;
    return result;
}


HttpFileReaderConfig IngestHttpSvcMod::_fileConfig(string const& database) const {
    auto const databaseServices = serviceProvider()->databaseServices();
    auto const getBoolParam = [&databaseServices, &database](bool& val, string const& key) {
        try {
            val = stoi(databaseServices->ingestParam(
                    database, HttpFileReaderConfig::category, key).value) != 0;
        } catch (DatabaseServicesNotFound const&) {}
    };
    auto const getStringParam = [&databaseServices, &database](string& val, string const& key) {
        try {
            val = databaseServices->ingestParam(
                    database, HttpFileReaderConfig::category, key).value;
        } catch (DatabaseServicesNotFound const&) {}
    };
    HttpFileReaderConfig fileConfig;
    getBoolParam(fileConfig.sslVerifyHost, HttpFileReaderConfig::sslVerifyHostKey);
    getBoolParam(fileConfig.sslVerifyPeer, HttpFileReaderConfig::sslVerifyPeerKey);
    getStringParam(fileConfig.caPath, HttpFileReaderConfig::caPathKey);
    getStringParam(fileConfig.caInfo, HttpFileReaderConfig::caInfoKey);
    getStringParam(fileConfig.caInfoVal, HttpFileReaderConfig::caInfoValKey);
    getBoolParam(fileConfig.proxySslVerifyHost, HttpFileReaderConfig::proxySslVerifyHostKey);
    getBoolParam(fileConfig.proxySslVerifyPeer, HttpFileReaderConfig::proxySslVerifyPeerKey);
    getStringParam(fileConfig.proxyCaPath, HttpFileReaderConfig::proxyCaPathKey);
    getStringParam(fileConfig.proxyCaInfo, HttpFileReaderConfig::proxyCaInfoKey);
    getStringParam(fileConfig.proxyCaInfoVal, HttpFileReaderConfig::proxyCaInfoValKey);
    return fileConfig;
}

}}} // namespace lsst::qserv::replica
