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
#include "czar/HttpCzarWorkerModule.h"

// System headers
#include <set>
#include <stdexcept>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "czar/Czar.h"
#include "qdisp/Executive.h"
#include "qdisp/UberJob.h"
#include "global/intTypes.h"
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "util/String.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.HttpCzarWorkerModule");
}

namespace lsst::qserv::czar {

void HttpCzarWorkerModule::process(string const& context, shared_ptr<qhttp::Request> const& req,
                                   shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                   http::AuthType const authType) {
    HttpCzarWorkerModule module(context, req, resp);
    module.execute(subModuleName, authType);
}

HttpCzarWorkerModule::HttpCzarWorkerModule(string const& context, shared_ptr<qhttp::Request> const& req,
                                           shared_ptr<qhttp::Response> const& resp)
        : HttpModule(context, req, resp) {}

json HttpCzarWorkerModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    enforceCzarName(func);
    if (subModuleName == "QUERYJOB-ERROR")
        return _queryJobError();
    else if (subModuleName == "QUERYJOB-READY")
        return _queryJobReady();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpCzarWorkerModule::_queryJobError() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " queryJobError json=" << body().objJson);
    auto ret = _handleJobError(__func__);
    return json::object();
}

json HttpCzarWorkerModule::_queryJobReady() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " queryJobReady json=" << body().objJson);
    auto ret = _handleJobReady(__func__);
    return ret;
}

json HttpCzarWorkerModule::_handleJobError(string const& func) {
    // Metadata-only responses for the file-based protocol should not have any data

    // Parse and verify the json message and then kill the UberJob.
    json jsRet = {{"success", 0}, {"errortype", "unknown"}, {"note", "initialized"}};
    try {
        // See qdisp::UberJob::runUberJob() for json message construction.
        string const targetWorkerId = body().required<string>("workerid");
        string const czarName = body().required<string>("czar");
        qmeta::CzarId const czarId = body().required<qmeta::CzarId>("czarid");
        QueryId const queryId = body().required<QueryId>("queryid");
        UberJobId const uberJobId = body().required<UberJobId>("uberjobid");
        int const errorCode = body().required<int>("errorCode");
        string const errorMsg = body().required<string>("errorMsg");

        // Find UberJob
        qdisp::Executive::Ptr exec = czar::Czar::getCzar()->getExecutiveFromMap(queryId);
        if (exec == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobError No executive for qid=") +
                                   to_string(queryId) + " czar=" + to_string(czarId));
        }
        qdisp::UberJob::Ptr uj = exec->findUberJob(uberJobId);
        if (uj == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobError No UberJob for qid=") +
                                   to_string(queryId) + " ujId=" + to_string(uberJobId) +
                                   " czar=" + to_string(czarId));
        }

        auto importRes = uj->workerError(errorCode, errorMsg);
        jsRet = importRes;

    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleJobError received " << iaEx.what() << " js=" << body().objJson);
        jsRet = {{"success", 0}, {"errortype", "parse"}, {"note", iaEx.what()}};
    }
    return jsRet;
}

json HttpCzarWorkerModule::_handleJobReady(string const& func) {
    // Metadata-only responses for the file-based protocol should not have any data

    // Parse and verify the json message and then have the uberjob import the file.
    json jsRet = {{"success", 1}, {"errortype", "unknown"}, {"note", "initialized"}};
    try {
        // See qdisp::UberJob::runUberJob() for json message construction.
        string const targetWorkerId = body().required<string>("workerid");
        string const czarName = body().required<string>("czar");
        qmeta::CzarId const czarId = body().required<qmeta::CzarId>("czarid");
        QueryId const queryId = body().required<QueryId>("queryid");
        UberJobId const uberJobId = body().required<UberJobId>("uberjobid");
        string const fileUrl = body().required<string>("fileUrl");
        uint64_t const rowCount = body().required<uint64_t>("rowCount");
        uint64_t const fileSize = body().required<uint64_t>("fileSize");

        // Find UberJob
        qdisp::Executive::Ptr exec = czar::Czar::getCzar()->getExecutiveFromMap(queryId);
        if (exec == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobReady No executive for qid=") +
                                   to_string(queryId) + " czar=" + to_string(czarId));
        }
        qdisp::UberJob::Ptr uj = exec->findUberJob(uberJobId);
        if (uj == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobReady No UberJob for qid=") +
                                   to_string(queryId) + " ujId=" + to_string(uberJobId) +
                                   " czar=" + to_string(czarId));
        }

        auto importRes = uj->importResultFile(fileUrl, rowCount, fileSize);
        jsRet = importRes;

    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleJobReady received " << iaEx.what() << " js=" << body().objJson);
        jsRet = {{"success", 0}, {"errortype", "parse"}, {"note", iaEx.what()}};
    }
    return jsRet;
}

}  // namespace lsst::qserv::czar
