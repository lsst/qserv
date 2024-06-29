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
    LOGS(_log, LOG_LVL_WARN, "&&&uj &&&UJR HttpCzarWorkerModule::executeImpl " << func);
    debug(func);
    //&&&uj this seems irrelevant for a worker enforceInstanceId(func,
    // cconfig::CzarConfig::instance()->replicationInstanceId());
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
    LOGS(_log, LOG_LVL_INFO, __func__ << "&&&uj queryJobError json=" << body().objJson);  //&&&
    //&&&uj NEED CODE for this
    auto ret = _handleJobError(__func__);
    return json::object();
}

json HttpCzarWorkerModule::_queryJobReady() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    LOGS(_log, LOG_LVL_INFO, __func__ << "&&&uj queryJobReady json=" << body().objJson);  //&&&
    auto ret = _handleJobReady(__func__);
    return ret;

    //&&& json ret = {{"success", 1}};
    //&&&return json::object();
}

json HttpCzarWorkerModule::_handleJobError(string const& func) {
    // Metadata-only responses for the file-based protocol should not have any data

    // Parse and verify the json message and then kill the UberJob.
    json jsRet = {{"success", 0}, {"errortype", "unknown"}, {"note", "initialized"}};
    try {
        // See qdisp::UberJob::runUberJob() for json message construction.
        auto const& js = body().objJson;
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR js=" << js);
        string const targetWorkerId = body().required<string>("workerid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR targetWorkerId=" << targetWorkerId);
        string const czarName = body().required<string>("czar");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR czarName=" << czarName);
        qmeta::CzarId const czarId = body().required<qmeta::CzarId>("czarid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR czarId=" << czarId);
        QueryId const queryId = body().required<QueryId>("queryid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR queryId=" << queryId);
        UberJobId const uberJobId = body().required<UberJobId>("uberjobid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR uberJobId=" << uberJobId);
        int const errorCode = body().required<int>("errorCode");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR errorCode=" << errorCode);
        string const errorMsg = body().required<string>("errorMsg");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR errorMsg=" << errorMsg);

        // Find UberJob
        qdisp::Executive::Ptr exec = czar::Czar::getCzar()->getExecutiveFromMap(queryId);
        if (exec == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobError No executive for qid=") +
                                   to_string(queryId));
        }
        qdisp::UberJob::Ptr uj = exec->findUberJob(uberJobId);
        if (uj == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobError No UberJob for qid=") +
                                   to_string(queryId) + " ujId=" + to_string(uberJobId));
        }
        // &&&uj NEED CODE to verify incoming values to those in the UberJob

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
        auto const& js = body().objJson;
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR js=" << js);
        string const targetWorkerId = body().required<string>("workerid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR targetWorkerId=" << targetWorkerId);
        string const czarName = body().required<string>("czar");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR czarName=" << czarName);
        qmeta::CzarId const czarId = body().required<qmeta::CzarId>("czarid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR czarId=" << czarId);
        QueryId const queryId = body().required<QueryId>("queryid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR queryId=" << queryId);
        UberJobId const uberJobId = body().required<UberJobId>("uberjobid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR uberJobId=" << uberJobId);
        string const fileUrl = body().required<string>("fileUrl");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR fileUrl=" << fileUrl);
        uint64_t const rowCount = body().required<uint64_t>("rowCount");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR rowCount=" << rowCount);
        uint64_t const fileSize = body().required<uint64_t>("fileSize");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&UJR fileSize=" << fileSize);

        // Find UberJob
        qdisp::Executive::Ptr exec = czar::Czar::getCzar()->getExecutiveFromMap(queryId);
        if (exec == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobReady No executive for qid=") +
                                   to_string(queryId));
        }
        qdisp::UberJob::Ptr uj = exec->findUberJob(uberJobId);
        if (uj == nullptr) {
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobReady No UberJob for qid=") +
                                   to_string(queryId) + " ujId=" + to_string(uberJobId));
        }
        // &&&uj NEED CODE to verify incoming values to those in the UberJob

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
