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
#include "protojson/UberJobErrorMsg.h"
#include "protojson/UberJobReadyMsg.h"
#include "protojson/WorkerCzarComIssue.h"
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
        : QhttpModule(context, req, resp) {}

json HttpCzarWorkerModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    cconfig::CzarConfig::instance()->replicationInstanceId();
    enforceCzarName(func);
    if (subModuleName == "QUERYJOB-ERROR")
        return _queryJobError();
    else if (subModuleName == "QUERYJOB-READY")
        return _queryJobReady();
    else if (subModuleName == "WORKERCZARCOMISSUE")
        return _workerCzarComIssue();
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

json HttpCzarWorkerModule::_workerCzarComIssue() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    LOGS(_log, LOG_LVL_DEBUG, __func__ << " workerczarcomissue json=" << body().objJson);
    auto ret = _handleWorkerCzarComIssue(__func__);
    return ret;
}

json HttpCzarWorkerModule::_handleJobError(string const& func) {
    LOGS(_log, LOG_LVL_DEBUG, "HttpCzarWorkerModule::_handleJobError start " << body().objJson);

    // Metadata-only responses for the file-based protocol should not have any data

    // Parse and verify the json message and then kill the UberJob.
    json jsRet = {{"success", 0}, {"errortype", "unknown"}, {"note", "initialized"}};
    try {
        string const& repliInstanceId = cconfig::CzarConfig::instance()->replicationInstanceId();
        string const& repliAuthKey = cconfig::CzarConfig::instance()->replicationAuthKey();
        auto const& jsReq = body().objJson;
        auto jrMsg = protojson::UberJobErrorMsg::createFromJson(jsReq, repliInstanceId, repliAuthKey);

        auto const queryId = jrMsg->getQueryId();
        auto const czarId = jrMsg->getCzarId();
        auto const uberJobId = jrMsg->getUberJobId();

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

        auto importRes = uj->workerError(jrMsg->getErrorCode(), jrMsg->getErrorMsg());
        jsRet = importRes;
    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleJobError received " << iaEx.what() << " js=" << body().objJson);
        jsRet = {{"success", 0}, {"errortype", "parse"}, {"note", iaEx.what()}};
    }
    LOGS(_log, LOG_LVL_DEBUG, "HttpCzarWorkerModule::_handleJobError end");
    return jsRet;
}

json HttpCzarWorkerModule::_handleJobReady(string const& func) {
    string const fName = "HttpCzarWorkerModule::_handleJobReady";
    LOGS(_log, LOG_LVL_DEBUG, fName << " start");
    // Metadata-only responses for the file-based protocol should not have any data

    // Parse and verify the json message and then have the uberjob import the file.
    json jsRet = {{"success", 1}, {"errortype", "unknown"}, {"note", "initialized"}};
    try {
        auto const& jsReq = body().objJson;
        auto jrMsg = protojson::UberJobReadyMsg::createFromJson(jsReq);

        // Find UberJob
        auto queryId = jrMsg->getQueryId();
        auto czarId = jrMsg->getCzarId();
        auto uberJobId = jrMsg->getUberJobId();
        qdisp::Executive::Ptr exec = czar::Czar::getCzar()->getExecutiveFromMap(queryId);
        if (exec == nullptr) {
            LOGS(_log, LOG_LVL_WARN,
                 fName << " null exec QID:" << queryId << " ujId=" << uberJobId << " cz=" << czarId);
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobReady No executive for qid=") +
                                   to_string(queryId) + " czar=" + to_string(czarId));
        }

        qdisp::UberJob::Ptr uj = exec->findUberJob(uberJobId);
        if (uj == nullptr) {
            LOGS(_log, LOG_LVL_WARN,
                 fName << " null uj QID:" << queryId << " ujId=" << uberJobId << " cz=" << czarId);
            throw invalid_argument(string("HttpCzarWorkerModule::_handleJobReady No UberJob for qid=") +
                                   to_string(queryId) + " ujId=" + to_string(uberJobId) +
                                   " czar=" + to_string(czarId));
        }

        uj->setResultFileSize(jrMsg->getFileSize());
        exec->checkResultFileSize(jrMsg->getFileSize());

        auto importRes =
                uj->importResultFile(jrMsg->getFileUrl(), jrMsg->getRowCount(), jrMsg->getFileSize());
        jsRet = importRes;

    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleJobReady received " << iaEx.what() << " js=" << body().objJson);
        jsRet = {{"success", 0}, {"errortype", "parse"}, {"note", iaEx.what()}};
    }
    LOGS(_log, LOG_LVL_DEBUG, "HttpCzarWorkerModule::_handleJobReady end");
    return jsRet;
}

json HttpCzarWorkerModule::_handleWorkerCzarComIssue(string const& func) {
    LOGS(_log, LOG_LVL_DEBUG, "HttpCzarWorkerModule::_handleWorkerCzarComIssue start");
    // Parse and verify the json message and then deal with the problems.
    json jsRet = {{"success", 0}, {"errortype", "unknown"}, {"note", "initialized"}};
    try {
        string const replicationInstanceId = cconfig::CzarConfig::instance()->replicationInstanceId();
        string const replicationAuthKey = cconfig::CzarConfig::instance()->replicationAuthKey();
        auto const& jsReq = body().objJson;
        auto wccIssue = protojson::WorkerCzarComIssue::createFromJson(jsReq, replicationInstanceId,
                                                                      replicationAuthKey);

        auto wId = wccIssue->getWorkerInfo()->wId;
        if (wccIssue->getThoughtCzarWasDead()) {
            LOGS(_log, LOG_LVL_WARN,
                 "HttpCzarWorkerModule::_handleWorkerCzarComIssue worker="
                         << wId << " thought czar was dead and killed related uberjobs.");

            // Find all incomplete UberJobs with this workerId and re-assign them.
            // Use a copy to avoid mutex issues.
            auto execMap = czar::Czar::getCzar()->getExecMapCopy();
            for (auto const& [exKey, execWeak] : execMap) {
                auto execPtr = execWeak.lock();
                if (execPtr == nullptr) continue;
                execPtr->killIncompleteUberJobsOnWorker(wId);
            }
        }
        jsRet = wccIssue->responseToJson();
        LOGS(_log, LOG_LVL_TRACE, "HttpCzarWorkerModule::_handleWorkerCzarComIssue jsRet=" << jsRet.dump());

    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleWorkerCzarComIssue received " << iaEx.what()
                                                                         << " js=" << body().objJson);
        jsRet = {{"success", 0}, {"errortype", "parse"}, {"note", iaEx.what()}};
    }
    return jsRet;
}

}  // namespace lsst::qserv::czar
