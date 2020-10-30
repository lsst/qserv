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
#include "curl/curl.h"

// Qserv headers
#include "replica/Common.h"
#include "replica/HttpExceptions.h"
#include "replica/HttpFileReader.h"
#include "replica/Performance.h"
#include "replica/Url.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

void IngestHttpSvcMod::process(ServiceProvider::Ptr const& serviceProvider,
                               string const& workerName,
                               string const& authKey,
                               qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp,
                               string const& subModuleName,
                               HttpModuleBase::AuthType const authType) {
    IngestHttpSvcMod module(serviceProvider, workerName, authKey, req, resp);
    module.execute(subModuleName, authType);
}


IngestHttpSvcMod::IngestHttpSvcMod(ServiceProvider::Ptr const& serviceProvider,
                                   string const& workerName,
                                   string const& authKey,
                                   qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp)
        :   HttpModuleBase(authKey, req, resp),
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
                stats = _readRemote(httpMethod, resource.url(), httpData, httpHeaders);
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
        throw HttpError(__func__, "failed to open file '" + filename+ "'.");
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


json IngestHttpSvcMod::_readRemote(string const& method,
                                   string const& url,
                                   string const& data,
                                   vector<string> const& headers) {
    debug(__func__);
    size_t numBytes = 0;
    size_t numRows = 0;
    HttpFileReader reader(method, url, data, headers);
    reader.read([this,&numBytes,&numRows](string const& row) {
        this->writeRowIntoFile(row);
        numBytes += row.size() + 1;     // counting the newline character
        ++numRows;
    });
    json result = json::object();
    result["num_bytes"] = numBytes;
    result["num_rows"] = numRows;
    return result;
}

}}} // namespace lsst::qserv::replica
