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

// Qserv headers
#include "http/Client.h"  // &&&uj will probably need to be removed
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/RequestBody.h"
#include "http/RequestQuery.h"
#include "mysql/MySqlUtils.h"
#include "qmeta/types.h"  // &&&uj
#include "util/String.h"
#include "util/Timer.h"
#include "wbase/FileChannelShared.h"
#include "wbase/Task.h"
#include "wbase/UberJobData.h"
#include "wbase/UserQueryInfo.h"
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
    json jsRet;
    vector<wbase::Task::Ptr> ujTasks;
    try {
        // See qdisp::UberJob::runUberJob() for json message construction.
        LOGS(_log, LOG_LVL_ERROR, __func__ << "&&&SUBC NEEDS CODE");
        auto const& jsReq = body().objJson;
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC jsReq=" << jsReq);
        string const targetWorkerId = body().required<string>("worker");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC targetWorkerId=" << targetWorkerId);

        // &&& ??? Maybe add RequestBody(json const& js) constructor to leverage functions for nested items
        // like "czar".
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
        auto ujQueryId = rbUberJob.required<QueryId>("queryid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC g");
        auto ujId = rbUberJob.required<UberJobId>("uberjobid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC h");
        auto ujCzarId = rbUberJob.required<int>("czarid");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC i");
        auto ujJobs = rbUberJob.required<json>("jobs");
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC j");
        LOGS(_log, LOG_LVL_WARN,
             __func__ << "&&&SUBC uj qid=" << ujQueryId << " ujid=" << ujId << " czid=" << ujCzarId);

        //&&&uj make UberJobData, UseQueryInfo entry, FileChannelShared, and Tasks.
        auto ujData =
                wbase::UberJobData::create(ujId, czarName, czarId, czarHostName, czarPort, ujQueryId, targetWorkerId, foreman(), authKey());
        LOGS(_log, LOG_LVL_WARN, "&&&uj (ujData != nullptr) = " << (ujData != nullptr));

        // Find the entry for this queryId, creat a new one if needed.
        wbase::UserQueryInfo::Ptr userQueryInfo = wbase::UserQueryInfo::uqMapInsert(ujQueryId);
        userQueryInfo->addUberJob(ujData);

        auto channelShared =
                wbase::FileChannelShared::create(ujData, czarId, czarHostName, czarPort, targetWorkerId);
        ujData->setFileChannelShared(channelShared);


        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k");
        for (auto const& job : ujJobs) {
            json const& jsJobDesc = job["jobdesc"];
            http::RequestBody rbJobDesc(jsJobDesc);
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC jobdesc " << jsJobDesc);
            // See qproc::TaskMsgFactory::makeMsgJson for message construction.
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k1");
            auto const jdCzarId = rbJobDesc.required<qmeta::CzarId>("czarId");
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k2");
            auto const jdQueryId = rbJobDesc.required<QueryId>("queryId");
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

            //&&&uj need scan table info befor making tasks
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k11");
            //&&&proto::ScanTableInfo::ListOf scanTables;
            proto::ScanInfo scanInfo;
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
                scanInfo.infoTables.emplace_back(chunkScanDb, chunkScanTable, lockInMemory, tblScanRating);
            }
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k12");
            scanInfo.scanRating = jdScanPriority;

            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC k10");

            // create tasks and add them to ujData
            auto chunkTasks = wbase::Task::createTasksForChunk(
                    ujData, ujJobs, channelShared, scanInfo, jdScanInteractive, jdMaxTableSize,
                    foreman()->chunkResourceMgr(), foreman()->mySqlConfig(), foreman()->sqlConnMgr(),
                    foreman()->queriesAndChunks(), foreman()->httpPort());
            ujTasks.insert(ujTasks.end(), chunkTasks.begin(), chunkTasks.end());
        }
        channelShared->setTaskCount(ujTasks.size());
        ujData->addTasks(ujTasks);

        util::Timer timer;
        timer.start();
        foreman()->processTasks(ujTasks);  // Queues tasks to be run later.
        timer.stop();
        LOGS(_log, LOG_LVL_WARN, __func__  << "&&& Enqueued UberJob time=" << timer.getElapsed() << " " << jsReq);

#if 0   /// &&&&&&&&
        // Now that the request is decoded (successfully or not), release the
        // xrootd request buffer. To avoid data races, this must happen before
        // the task is handed off to another thread for processing, as there is a
        // reference to this SsiRequest inside the reply channel for the task,
        // and after the call to BindRequest.
            ReleaseRequestBuffer();
            t.start();
            _foreman->processTasks(tasks);  // Queues tasks to be run later. //&&&uj next
            t.stop();
            LOGS(_log, LOG_LVL_DEBUG,
                 "Enqueued TaskMsg for " << ru << " in " << t.getElapsed() << " seconds");
            break;
        }
#endif  /// &&&&&&&&

        // &&&uj temporary, send response back to czar saying file is ready. The file is not ready, but this
        //       is just an initial comms test
        //&&&_temporaryRespFunc(targetWorkerId, czarName, czarId, czarHostName, czarPort, ujQueryId, ujId);

        string note = string("qId=") + to_string(ujQueryId) + " ujId=" + to_string(ujId) +
                      " tasks in uberJob=" + to_string(channelShared->getTaskCount());
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC note=" << note);
        jsRet = {{"success", 1}, {"errortype", "none"}, {"note", note}};

    } catch (wbase::TaskException const& texp) {
        LOGS(_log, LOG_LVL_ERROR, "wbase::TaskException received " << texp.what());
        jsRet = {{"success", 0}, {"errortype", "parse"}, {"note", texp.what()}};
    }
    return jsRet;
}

// &&&uj delete
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
    string const url = "http://" + czarHostName + ":" + to_string(czarPort) + "/queryjob-error";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_WARN, "czarName=" << czarName << " czarHostName=" << czarHostName << " &&&uj HttpWorkerCzarModule::_temporaryRespFunc url=" << url << " request=" << request.dump());
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
