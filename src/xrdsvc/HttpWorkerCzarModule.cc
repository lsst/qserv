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
#include "xrdsvc/HttpWorkerCzarModule.h"

// System headers
#include <sstream>
#include <stdexcept>
#include <vector>

// Third party headers
#include "lsst/log/Log.h"
#include "XrdSsi/XrdSsiCluster.hh"

// Qserv headers
#include "http/Client.h"  // &&&uj will probably need to be removed
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/RequestBody.h"
#include "http/RequestQuery.h"
#include "mysql/MySqlUtils.h"
#include "qmeta/types.h"  // &&&uj
#include "util/String.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/Foreman.h"
#include "wcontrol/ResourceMonitor.h"
#include "wpublish/ChunkInventory.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

//&&&extern XrdSsiProvider* XrdSsiProviderLookup;

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.HttpReplicaMgt");
}

namespace {
// These markers if reported in the extended error response object of the failed
// requests could be used by a caller for refining the completion status
// of the corresponding Controller-side operation.
// &&& These errors seem useful enought to be centralized ???
json const extErrorInvalidParam = json::object({{"invalid_param", 1}});
json const extErrorReplicaInUse = json::object({{"in_use", 1}});

}  // namespace

namespace lsst::qserv::xrdsvc {

void HttpWorkerCzarModule::process(string const& context, shared_ptr<wcontrol::Foreman> const& foreman,
                                   shared_ptr<qhttp::Request> const& req,
                                   shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                   http::AuthType const authType) {
    HttpWorkerCzarModule module(context, foreman, req, resp);
    module.execute(subModuleName, authType);
}

HttpWorkerCzarModule::HttpWorkerCzarModule(string const& context,
                                           shared_ptr<wcontrol::Foreman> const& foreman,
                                           shared_ptr<qhttp::Request> const& req,
                                           shared_ptr<qhttp::Response> const& resp)
        : HttpModule(context, foreman, req, resp) {}

json HttpWorkerCzarModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func + "&&&uj xrdsvc");
    enforceInstanceId(func, wconfig::WorkerConfig::instance()->replicationInstanceId());
    enforceWorkerId(func);
    if (subModuleName == "QUERYJOB") return _queryJob();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpWorkerCzarModule::_queryJob() {  // &&&uj
    //&&&debug(__func__);
    debug(string(__func__) + " &&&uj _queryJob()");
    checkApiVersion(__func__, 34);
    // At this point, API version, correct worker, and auth have been checked.
    json jsRet = _handleQueryJob(__func__);
    return jsRet;
}

json HttpWorkerCzarModule::_handleQueryJob(string const& func) {
    // See qdisp::UberJob::runUberJob() for json message construction.
    LOGS(_log, LOG_LVL_ERROR, __func__ << "&&&SUBC NEEDS CODE");
    auto const& jsReq = body().objJson;
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC jsReq=" << jsReq);
    string const targetWorkerId = body().required<string>("worker");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC targetWorkerId=" << targetWorkerId);

    // &&& ??? Maybe add RequestBody(json const& js) constructor to leverage functions for nested items like
    // "czar".
    //&&&auto const& jsCzar = jsReq["czar"];
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC a");
    http::RequestBody rbCzar(body().required<json>("czar"));
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC b");
    auto czarName = rbCzar.required<string>("name");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC c");
    auto czarId = rbCzar.required<qmeta::CzarId>("id");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC d");
    auto czarPort = rbCzar.required<int>("management-port");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC e");
    auto czarHostName = rbCzar.required<string>("management-host-name");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC f");
    LOGS(_log, LOG_LVL_WARN,
         __func__ << "&&&SUBC czar n=" << czarName << " id=" << czarId << " p=" << czarPort
                  << " h=" << czarHostName);

    http::RequestBody rbUberJob(body().required<json>("uberjob"));
    auto ujQueryId = rbUberJob.required<uint64_t>("queryid");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC g");
    auto ujId = rbUberJob.required<int>("uberjobid");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC h");
    auto ujCzarId = rbUberJob.required<int>("czarid");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC i");
    auto ujJobs = rbUberJob.required<json>("jobs");
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC j");
    LOGS(_log, LOG_LVL_WARN,
         __func__ << "&&&SUBC uj qid=" << ujQueryId << " ujid=" << ujId << " czid=" << ujCzarId);

    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k");
    for (auto const& job : ujJobs) {
        json const& jsJobDesc = job["jobdesc"];
        http::RequestBody rbJobDesc(jsJobDesc);
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC jobdesc " << jsJobDesc);
        // See qproc::TaskMsgFactory::makeMsgJson for message construction.
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k1");
        auto const jdCzarId = rbJobDesc.required<qmeta::CzarId>("czarId");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k2");
        auto const jdQueryId = rbJobDesc.required<int>("queryId");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k3");
        auto const jdJobId = rbJobDesc.required<int>("jobId");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k4");
        auto const jdAttemptCount = rbJobDesc.required<int>("attemptCount");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k5");
        auto const jdQuerySpecDb = rbJobDesc.required<string>("querySpecDb");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k6");
        auto const jdScanPriority = rbJobDesc.required<int>("scanPriority");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k7");
        auto const jdScanInteractive = rbJobDesc.required<bool>("scanInteractive");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k8");
        auto const jdMaxTableSize = rbJobDesc.required<int>("maxTableSize");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k9");
        auto const jdChunkId = rbJobDesc.required<int>("chunkId");
        LOGS(_log, LOG_LVL_WARN,
             __func__ << "&&&SUBC jd cid=" << jdCzarId << " jdQId=" << jdQueryId << " jdJobId=" << jdJobId
                      << " jdAtt=" << jdAttemptCount << " jdQDb=" << jdQuerySpecDb
                      << " jdScanPri=" << jdScanPriority << " interactive=" << jdScanInteractive
                      << " maxTblSz=" << jdMaxTableSize << " chunkId=" << jdChunkId);

        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10");
        auto const jdQueryFragments = rbJobDesc.required<json>("queryFragments");
        for (auto const& frag : jdQueryFragments) {
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10a");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC frag=" << frag);
            http::RequestBody rbFrag(frag);
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10b");
            auto const& jsQueries = rbFrag.required<json>("queries");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10c");
            for (auto const& subQ :
                 jsQueries) {  // &&&uj move to uberjob, these should be the same for all jobs
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10c1");
                http::RequestBody rbSubQ(subQ);
                auto const subQuery = rbSubQ.required<string>("subQuery");
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10c2");
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC subQuery=" << subQuery);
            }
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10d1");
            auto const& resultTable = rbFrag.required<string>("resultTable");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10d2");
            auto const& jsSubIds = rbFrag.required<json>("subchunkIds");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC scId jsSubIds=" << jsSubIds);
            for (auto const& scId : jsSubIds) {
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10e1");
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC scId=" << scId);
            }
            auto const& jsSubTables = rbFrag.required<json>("subchunkTables");
            for (string scTable : jsSubTables) {  // &&&uj are these the same for all jobs?
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10f1");
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC scTable=" << scTable);
            }
        }

        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k11");
        auto const jdChunkScanTables = rbJobDesc.required<json>("chunkScanTables");
        for (auto const& tbl : jdChunkScanTables) {
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k11a1");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC tbl=" << tbl);
            http::RequestBody rbTbl(tbl);
            auto const& chunkScanDb = rbTbl.required<string>("db");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k11a2");
            auto const& lockInMemory = rbTbl.required<bool>("lockInMemory");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k11a3");
            auto const& chunkScanTable = rbTbl.required<string>("table");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k11a4");
            auto const& tblScanRating = rbTbl.required<int>("tblScanRating");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k11a5");
            LOGS(_log, LOG_LVL_WARN,
                 __func__ << "&&&SUBC chunkSDb=" << chunkScanDb << " lockinmem=" << lockInMemory
                          << " csTble=" << chunkScanTable << " tblScanRating=" << tblScanRating);
        }
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k12");
    }

    // &&&uj temporary, send response back to czar saying file is ready. The file is not ready, but this
    //       an initial comms test
    _temporaryRespFunc(targetWorkerId, czarName, czarId, czarHostName, czarPort, ujQueryId, ujId);

    json jsRet = {{"success", 1}, {"errortype", "none"}, {"note", "none"}};
    return jsRet;
}

void HttpWorkerCzarModule::_temporaryRespFunc(string const& targetWorkerId, string const& czarName,
                                              qmeta::CzarId czarId, string const& czarHostName, int czarPort,
                                              uint64_t queryId, uint64_t uberJobId) {
    json request = {{"version", http::MetaModule::version},
                    {"workerid", foreman()->chunkInventory()->id()},
                    {"auth_key", authKey()},
                    {"czar", czarName},
                    {"czarid", czarId},
                    {"queryid", queryId},
                    {"uberjobid", uberJobId}};

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    string const url = "http://" + czarHostName + ":" + to_string(czarPort) + "/queryjob-ready";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    http::Client client(method, url, request.dump(), headers);
    bool transmitSuccess = false;
    try {
        json const response = client.readAsJson();
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj response=" << response);
        if (0 != response.at("success").get<int>()) {
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj success");
            transmitSuccess = true;
        } else {
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj NEED CODE success=0");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, requestContext + " &&&uj failed, ex: " + ex.what());
    }
    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR,
             __func__ << "&&&uj NEED CODE try again??? Let czar find out through polling worker status???");
    } else {
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj NEED CODE do nothing, czar should collect file");
    }
}

}  // namespace lsst::qserv::xrdsvc
