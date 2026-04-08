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
#include "protojson/PwHideJson.h"
#include "protojson/ResponseMsg.h"
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
    else if (subModuleName == "QUERYJOB-READY") {
        return _queryJobReady();
    } else if (subModuleName == "WORKERCZARCOMISSUE")
        return _workerCzarComIssue();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpCzarWorkerModule::_queryJobError() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    auto ret = _handleJobError(__func__);
    return json::object();
}

json HttpCzarWorkerModule::_queryJobReady() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    auto ret = _handleJobReady(__func__);
    return ret;
}

json HttpCzarWorkerModule::_workerCzarComIssue() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    auto ret = _handleWorkerCzarComIssue(__func__);
    return ret;
}

json HttpCzarWorkerModule::_handleJobError(string const& func) {
    string const fName("HttpCzarWorkerModule::_handleJobError");
    LOGS(_log, LOG_LVL_DEBUG, fName << " start");
    // Metadata-only responses for the file-based protocol should not have any data

    // Parse and verify the json message and then kill the UberJob.
    try {
        auto const& jsReq = body().objJson;
        auto jrMsg = protojson::UberJobErrorMsg::createFromJson(jsReq);
        auto importRes = czar::Czar::getCzar()->handleUberJobErrorMsg(jrMsg, fName);
        return importRes->toJson();
    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleJobError received "
                     << iaEx.what() << " js=" << protojson::pwHide(body().objJson));
        protojson::ExecutiveRespMsg respMsg(false, false, 0, 0, 0, "parse", iaEx.what());
        return respMsg.toJson();
    }
}

json HttpCzarWorkerModule::_handleJobReady(string const& func) {
    string const fName = "HttpCzarWorkerModule::_handleJobReady";
    LOGS(_log, LOG_LVL_DEBUG, fName << " start");
    // Metadata-only responses for the file-based protocol should not have any data

    // Parse and verify the json message and then have the uberjob import the file.
    try {
        auto const& jsReq = body().objJson;
        auto jrMsg = protojson::UberJobReadyMsg::createFromJson(jsReq);
        auto importRes = czar::Czar::getCzar()->handleUberJobReadyMsg(jrMsg, fName);
        return importRes->toJson();
    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleJobReady received "
                     << iaEx.what() << " js=" << protojson::pwHide(body().objJson));
        protojson::ExecutiveRespMsg respMsg(false, false, 0, 0, 0, "parse", iaEx.what());
        return respMsg.toJson();
    }
}

json HttpCzarWorkerModule::_handleWorkerCzarComIssue(string const& func) {
    string const fName("HttpCzarWorkerModule::_handleWorkerCzarComIssue");
    LOGS(_log, LOG_LVL_DEBUG, fName << " start");
    // Parse and verify the json message and then deal with the problems.
    string wId = "unknown";
    try {
        protojson::AuthContext const authC(cconfig::CzarConfig::instance()->replicationInstanceId(),
                                           cconfig::CzarConfig::instance()->replicationAuthKey());
        auto const& jsReq = body().objJson;
        auto wccIssue = protojson::WorkerCzarComIssue::createFromJson(jsReq, authC);

        wId = wccIssue->getWorkerInfo()->wId;
        if (wccIssue->getThoughtCzarWasDeadTime() > 0) {
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

        // Responses are sent for all `failedTransmits` in the message. If
        // something couldn't be parsed, the response indicates that and
        // the UberJob will be abandoned by the worker. If the query
        // could finish without the results of that uberjob, it indicates
        // that the result file is obsolete. If the this was successful,
        // the worker just waits for the czar to collect the file as usual.
        // In all cases, the worker will remove the item from its
        // `failedTransmits` list so it won't be tried again.
        vector<protojson::ExecutiveRespMsg::Ptr> execRespMsgs;
        auto failedTransmits = wccIssue->takeFailedTransmitsMap();
        for (auto& [key, elem] : *failedTransmits) {
            protojson::UberJobStatusMsg::Ptr& statusMsg = elem;
            auto rdyMsg = dynamic_pointer_cast<protojson::UberJobReadyMsg>(statusMsg);
            if (rdyMsg != nullptr) {
                // Put the file on a queue to be collected later.
                auto exRespMsg = czar::Czar::getCzar()->handleUberJobReadyMsgNoThrow(rdyMsg, fName);
                execRespMsgs.push_back(exRespMsg);
            } else {
                auto errMsg = dynamic_pointer_cast<protojson::UberJobErrorMsg>(statusMsg);
                // Kill the UberJob or user query depending on the error. (Doesn't throw)
                auto exRespMsg = czar::Czar::getCzar()->handleUberJobErrorMsg(errMsg, fName);
                execRespMsgs.push_back(exRespMsg);
            }
        }
        auto jsRet = wccIssue->responseToJson(wccIssue->getThoughtCzarWasDeadTime(), execRespMsgs);
        LOGS(_log, LOG_LVL_TRACE,
             "HttpCzarWorkerModule::_handleWorkerCzarComIssue jsRet=" << protojson::pwHide(jsRet));
        return jsRet;
    } catch (std::invalid_argument const& iaEx) {
        LOGS(_log, LOG_LVL_ERROR,
             "HttpCzarWorkerModule::_handleWorkerCzarComIssue received "
                     << iaEx.what() << " js=" << protojson::pwHide(body().objJson));
        // This is very bad as there's no way to know what is going wrong. Just one of these is surviveable,
        // but if it keeps happening, the system is unstable.
        Czar::getCzar()->incrCommErrCount("WorkerCzarComIssue", wId, iaEx.what());
        protojson::ResponseMsg respMsg(false, "parse", iaEx.what());
        return respMsg.toJson();
    }
}

}  // namespace lsst::qserv::czar
