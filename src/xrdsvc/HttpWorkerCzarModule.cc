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
#include "http/Client.h"  // TODO:UJ will probably need to be removed
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/RequestBodyJSON.h"
#include "http/RequestQuery.h"
#include "http/WorkerQueryStatusData.h"
#include "mysql/MySqlUtils.h"
#include "qmeta/types.h"
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
#include "wpublish/QueriesAndChunks.h"
#include "wpublish/QueryStatistics.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.HttpReplicaMgt");
}

namespace {
// These markers if reported in the extended error response object of the failed
// requests could be used by a caller for refining the completion status
// of the corresponding Controller-side operation.
// TODO:UJ Are these errors seem useful enought to be centralized ???
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
    enforceInstanceId(func, wconfig::WorkerConfig::instance()->replicationInstanceId());
    enforceWorkerId(func);
    if (subModuleName == "/queryjob") return _queryJob();
    if (subModuleName == "/querystatus") return _queryStatus();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpWorkerCzarModule::_queryJob() {
    debug(__func__);
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
        auto const& jsReq = body().objJson;
        string const targetWorkerId = body().required<string>("worker");

        http::RequestBodyJSON rbCzar(body().required<json>("czar"));
        auto czarName = rbCzar.required<string>("name");
        auto czarId = rbCzar.required<qmeta::CzarId>("id");
        auto czarPort = rbCzar.required<int>("management-port");
        auto czarHostName = rbCzar.required<string>("management-host-name");
        LOGS(_log, LOG_LVL_TRACE,
             __func__ << " czar n=" << czarName << " id=" << czarId << " p=" << czarPort
                      << " h=" << czarHostName);

        http::RequestBodyJSON rbUberJob(body().required<json>("uberjob"));
        auto ujQueryId = rbUberJob.required<QueryId>("queryid");
        auto ujId = rbUberJob.required<UberJobId>("uberjobid");
        auto ujCzarId = rbUberJob.required<int>("czarid");
        auto ujJobs = rbUberJob.required<json>("jobs");
        LOGS(_log, LOG_LVL_TRACE,
             __func__ << " uj qid=" << ujQueryId << " ujid=" << ujId << " czid=" << ujCzarId);

        // Get or create QueryStatistics and UserQueryInfo instances.
        auto queryStats = foreman()->addQueryId(ujQueryId);
        auto userQueryInfo = queryStats->getUserQueryInfo();

        if (userQueryInfo->getCancelledByCzar()) {
            throw wbase::TaskException(
                    ERR_LOC, string("Already cancelled by czar. ujQueryId=") + to_string(ujQueryId));
        }
        if (userQueryInfo->isUberJobDead(ujId)) {
            throw wbase::TaskException(ERR_LOC, string("UberJob already dead. ujQueryId=") +
                                                        to_string(ujQueryId) + " ujId=" + to_string(ujId));
        }

        auto ujData = wbase::UberJobData::create(ujId, czarName, czarId, czarHostName, czarPort, ujQueryId,
                                                 targetWorkerId, foreman(), authKey());

        // Find the entry for this queryId, creat a new one if needed.
        userQueryInfo->addUberJob(ujData);
        auto channelShared =
                wbase::FileChannelShared::create(ujData, czarId, czarHostName, czarPort, targetWorkerId);
        ujData->setFileChannelShared(channelShared);

        // TODO:UJ These items should be stored higher in the message structure as they get
        //   duplicated and should always be the same within an UberJob.
        QueryId jdQueryId = 0;
        proto::ScanInfo scanInfo;
        bool scanInfoSet = false;
        bool jdScanInteractive = false;
        int jdMaxTableSize = 0;

        for (auto const& job : ujJobs) {
            json const& jsJobDesc = job["jobdesc"];
            http::RequestBodyJSON rbJobDesc(jsJobDesc);
            // See qproc::TaskMsgFactory::makeMsgJson for message construction.
            auto const jdCzarId = rbJobDesc.required<qmeta::CzarId>("czarId");
            jdQueryId = rbJobDesc.required<QueryId>("queryId");
            auto const jdJobId = rbJobDesc.required<int>("jobId");
            auto const jdAttemptCount = rbJobDesc.required<int>("attemptCount");
            auto const jdQuerySpecDb = rbJobDesc.required<string>("querySpecDb");
            auto const jdScanPriority = rbJobDesc.required<int>("scanPriority");
            jdScanInteractive = rbJobDesc.required<bool>("scanInteractive");
            jdMaxTableSize = rbJobDesc.required<int>("maxTableSize");
            auto const jdChunkId = rbJobDesc.required<int>("chunkId");
            LOGS(_log, LOG_LVL_TRACE,
                 __func__ << " jd cid=" << jdCzarId << " jdQId=" << jdQueryId << " jdJobId=" << jdJobId
                          << " jdAtt=" << jdAttemptCount << " jdQDb=" << jdQuerySpecDb
                          << " jdScanPri=" << jdScanPriority << " interactive=" << jdScanInteractive
                          << " maxTblSz=" << jdMaxTableSize << " chunkId=" << jdChunkId);

            auto const jdChunkScanTables = rbJobDesc.required<json>("chunkScanTables");
            if (!scanInfoSet) {
                for (auto const& tbl : jdChunkScanTables) {
                    http::RequestBodyJSON rbTbl(tbl);
                    auto const& chunkScanDb = rbTbl.required<string>("db");
                    auto lockInMemory = rbTbl.required<bool>("lockInMemory");
                    auto const& chunkScanTable = rbTbl.required<string>("table");
                    auto tblScanRating = rbTbl.required<int>("tblScanRating");
                    LOGS(_log, LOG_LVL_TRACE,
                         __func__ << " chunkSDb=" << chunkScanDb << " lockinmem=" << lockInMemory
                                  << " csTble=" << chunkScanTable << " tblScanRating=" << tblScanRating);
                    scanInfo.infoTables.emplace_back(chunkScanDb, chunkScanTable, lockInMemory,
                                                     tblScanRating);
                    scanInfoSet = true;
                }
            }
            scanInfo.scanRating = jdScanPriority;
        }

        // create tasks and add them to ujData
        auto chunkTasks = wbase::Task::createTasksForChunk(
                ujData, ujJobs, channelShared, scanInfo, jdScanInteractive, jdMaxTableSize,
                foreman()->chunkResourceMgr(), foreman()->mySqlConfig(), foreman()->sqlConnMgr(),
                foreman()->queriesAndChunks(), foreman()->httpPort());
        ujTasks.insert(ujTasks.end(), chunkTasks.begin(), chunkTasks.end());

        channelShared->setTaskCount(ujTasks.size());
        ujData->addTasks(ujTasks);

        util::Timer timer;
        timer.start();
        foreman()->processTasks(ujTasks);  // Queues tasks to be run later.
        timer.stop();
        LOGS(_log, LOG_LVL_DEBUG,
             __func__ << " Enqueued UberJob time=" << timer.getElapsed() << " " << jsReq);

        string note = string("qId=") + to_string(ujQueryId) + " ujId=" + to_string(ujId) +
                      " tasks in uberJob=" + to_string(channelShared->getTaskCount());
        jsRet = {{"success", 1}, {"errortype", "none"}, {"note", note}};

    } catch (wbase::TaskException const& texp) {
        LOGS(_log, LOG_LVL_ERROR, "wbase::TaskException received " << texp.what());
        jsRet = {{"success", 0}, {"errortype", "parse"}, {"note", texp.what()}};
    }
    return jsRet;
}

json HttpWorkerCzarModule::_queryStatus() {
    debug(__func__);
    checkApiVersion(__func__, 34);
    // At this point, API version, correct worker, and auth have been checked.
    json jsRet = _handleQueryStatus(__func__);
    return jsRet;
}

json HttpWorkerCzarModule::_handleQueryStatus(std::string const& func) {
    LOGS(_log, LOG_LVL_ERROR, "&&& HttpWorkerCzarModule::_handleQueryStatus");

    json jsRet;
    auto now = CLOCK::now();
    auto const workerConfig = wconfig::WorkerConfig::instance();
    auto const replicationInstanceId = workerConfig->replicationInstanceId();
    auto const replicationAuthKey = workerConfig->replicationAuthKey();

    auto const& jsReq = body().objJson;
    auto wqsData =
            http::WorkerQueryStatusData::createJson(jsReq, replicationInstanceId, replicationAuthKey, now);

    // For all queryId and czarId items, if the item can't be found, it is simply ignored. Anything that
    // is missed will eventually be picked up by other mechanisms, such as results being rejected
    // by the czar.

    // If a czar was restarted, cancel and/or delete the abandoned items.
    if (wqsData->isCzarRestart()) {
        auto restartCzarId = wqsData->getCzarRestartCzarId();
        auto restartQId = wqsData->getCzarRestartQueryId();
        if (restartCzarId > 0 && restartQId > 0) {
            wbase::FileChannelShared::cleanUpResultsOnCzarRestart(wqsData->getCzarRestartCzarId(),
                                                                  wqsData->getCzarRestartQueryId());
        }
    }

    // Take the values from the lists in the message to cancel the
    // appropriate queries and tasks as needed.
    auto const queriesAndChunks = foreman()->queriesAndChunks();
    vector<wbase::UserQueryInfo::Ptr> cancelledList;
    // Cancelled queries where we want to keep the files
    lock_guard<mutex> mapLg(wqsData->_mapMtx);
    for (auto const& [dkQid, dkTm] : wqsData->_qIdDoneKeepFiles) {
        auto qStats = queriesAndChunks->addQueryId(dkQid);
        if (qStats != nullptr) {
            auto uqInfo = qStats->getUserQueryInfo();
            if (uqInfo != nullptr) {
                if (!uqInfo->getCancelledByCzar()) {
                    cancelledList.push_back(uqInfo);
                }
            }
        }
    }

    vector<wbase::UserQueryInfo::Ptr> deleteFilesList;
    for (auto const& [dkQid, dkTm] : wqsData->_qIdDoneDeleteFiles) {
        auto qStats = queriesAndChunks->addQueryId(dkQid);
        if (qStats != nullptr) {
            auto uqInfo = qStats->getUserQueryInfo();
            if (uqInfo != nullptr) {
                if (!uqInfo->getCancelledByCzar()) {
                    cancelledList.push_back(uqInfo);
                }
                deleteFilesList.push_back(uqInfo);
            }
        }
    }

    // Cancel everything in the cancelled list.
    for (auto const& canUqInfo : cancelledList) {
        canUqInfo->cancelFromCzar();
    }

    // For dead UberJobs, add them to a list of dead uberjobs within UserQueryInfo.
    // UserQueryInfo will cancel the tasks in the uberjobs if they exist.
    // New UberJob Id's will be checked against the list, and immediately be
    // killed if they are on it. (see HttpWorkerCzarModule::_handleQueryJob)
    for (auto const& [ujQid, ujIdMap] : wqsData->_qIdDeadUberJobs) {
        auto qStats = queriesAndChunks->addQueryId(ujQid);
        if (qStats != nullptr) {
            auto uqInfo = qStats->getUserQueryInfo();
            if (uqInfo != nullptr) {
                if (!uqInfo->getCancelledByCzar()) {
                    for (auto const& [ujId, tm] : ujIdMap) {
                        uqInfo->cancelUberJob(ujId);
                    }
                }
            }
        }
    }

    // Delete files that should be deleted
    CzarIdType czarId = wqsData->_czInfo->czId;
    for (wbase::UserQueryInfo::Ptr uqiPtr : deleteFilesList) {
        if (uqiPtr == nullptr) continue;
        QueryId qId = uqiPtr->getQueryId();
        wbase::FileChannelShared::cleanUpResults(czarId, qId);
    }

    // Syntax errors in the message would throw invalid_argument, which is handled elsewhere.

    // Return a message containing lists of the queries that were cancelled.
    jsRet = wqsData->serializeResponseJson();
    return jsRet;
}

}  // namespace lsst::qserv::xrdsvc
