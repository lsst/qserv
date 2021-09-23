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
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

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
            _serviceProvider(serviceProvider),
            _workerName(workerName) {
}


string IngestHttpSvcMod::context() const {
    return "INGEST-HTTP-SVC ";
}


json IngestHttpSvcMod::executeImpl(string const& subModuleName) {
    debug(__func__, "subModuleName: '" + subModuleName + "'");
    if (subModuleName == "SYNC-PROCESS") return _syncProcessRequest();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json IngestHttpSvcMod::_syncProcessRequest() const {
    bool const async = false;
    auto const request = _createRequst(async);
    request->process();
    auto const contrib = request->transactionContribInfo();

    // Performance and statistics of the ingest operations (collected for each
    // file ingested). Timestamps represent the number of milliseconds since UNIX EPOCH.
    return json::object({
        {"stats", {
            {"num_bytes", contrib.numBytes},
            {"num_rows",  contrib.numRows}
        }},
        {"perf", {
            {"begin_file_read_ms",   contrib.startTime},
            {"end_file_read_ms",     contrib.readTime},
            {"begin_file_ingest_ms", contrib.readTime},
            {"end_file_ingest_ms",   contrib.loadTime}
        }}
    });
}


IngestRequest::Ptr IngestHttpSvcMod::_createRequst(bool async) const {
    TransactionId const transactionId = body().required<uint32_t>("transaction_id");
    string const table = body().required<string>("table");
    unsigned int const chunk = body().required<unsigned int>("chunk");
    bool const isOverlap = body().required<int>("overlap") != 0;
    string const url = body().required<string>("url");

    // Allow "column_separator" for the sake of the backward compatibility with the older
    // version of the API. The parameter "column_separator" if present will override the one
    // of "fields_terminated_by"
    string const fieldsTerminatedBy = body().optional<string>(
        "column_separator",
        body().optional<string>("fields_terminated_by", csv::Dialect::defaultFieldsTerminatedBy)
    );
    string const fieldsEnclosedBy  = body().optional<string>("fields_enclosed_by",  csv::Dialect::defaultFieldsEnclosedBy);
    string const fieldsEscapedBy   = body().optional<string>("fields_escaped_by",   csv::Dialect::defaultFieldsEscapedBy);
    string const linesTerminatedBy = body().optional<string>("lines_terminated_by", csv::Dialect::defaultLinesTerminatedBy);

    string const httpMethod = body().optional<string>("http_method", "GET");
    string const httpData = body().optional<string>("http_data", string());
    vector<string> const httpHeaders = body().optionalColl<string>("http_headers", vector<string>());

    debug(__func__, "transactionId: "          + to_string(transactionId));
    debug(__func__, "table: '"                 + table + "'");
    debug(__func__, "fields_terminated_by: '"  + fieldsTerminatedBy + "'");
    debug(__func__, "fields_enclosed_by: '"    + fieldsEnclosedBy + "'");
    debug(__func__, "fields_escaped_by: '"     + fieldsEscapedBy + "'");
    debug(__func__, "lines_terminated_by: '"   + linesTerminatedBy + "'");
    debug(__func__, "chunk: "                  + to_string(chunk));
    debug(__func__, "isOverlap: "              + string(isOverlap ? "1": "0"));
    debug(__func__, "url: '"                   + url + "'");
    debug(__func__, "http_method: '"           + httpMethod + "'");
    debug(__func__, "http_data: '"             + httpData + "'");
    debug(__func__, "http_headers.size(): "    + to_string(httpHeaders.size()));

    IngestRequest::Ptr const request = IngestRequest::create(
        _serviceProvider,
        _workerName,
        transactionId,
        table,
        chunk,
        isOverlap,
        url,
        async,
        fieldsTerminatedBy,
        fieldsEnclosedBy,
        fieldsEscapedBy,
        linesTerminatedBy,
        httpMethod,
        httpData,
        httpHeaders
    );
    return request;
}

}}} // namespace lsst::qserv::replica
