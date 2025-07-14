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
 * MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "qdisp/UberJob.h"

// System headers
#include <stdexcept>

// Third-party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "czar/Czar.h"
#include "cconfig/CzarConfig.h"
#include "global/LogContext.h"
#include "http/Client.h"
#include "http/MetaModule.h"
#include "protojson/UberJobMsg.h"
#include "qdisp/CzarStats.h"
#include "qdisp/JobQuery.h"
#include "qmeta/JobStatus.h"
#include "qproc/ChunkQuerySpec.h"
#include "util/Bug.h"
#include "util/common.h"
#include "util/QdispPool.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.UberJob");
}

namespace lsst::qserv::qdisp {

UberJob::Ptr UberJob::create(Executive::Ptr const& executive,
                             std::shared_ptr<ResponseHandler> const& respHandler, int queryId, int uberJobId,
                             CzarId czarId) {
    UberJob::Ptr uJob(
            new UberJob(executive, respHandler, queryId, uberJobId, czarId, executive->getUjRowLimit()));
    uJob->_setup();
    return uJob;
}

UberJob::UberJob(Executive::Ptr const& executive, std::shared_ptr<ResponseHandler> const& respHandler,
                 int queryId, int uberJobId, CzarId czarId, int rowLimit)
        : _executive(executive),
          _respHandler(respHandler),
          _queryId(queryId),
          _uberJobId(uberJobId),
          _czarId(czarId),
          _rowLimit(rowLimit),
          _idStr("QID=" + to_string(_queryId) + "_ujId=" + to_string(uberJobId)) {
    LOGS(_log, LOG_LVL_TRACE, _idStr << " created");
}

UberJob::~UberJob() {
    // UberJobs are not deleted until the executive has been deleted, which means
    // the query is done before this is called.
    getRespHandler()->cancelFileMerge();
}

void UberJob::_setup() {
    UberJob::Ptr ujPtr = shared_from_this();
    _respHandler->setUberJob(ujPtr);
}

bool UberJob::addJob(JobQuery::Ptr const& job) {
    bool success = false;
    if (job->setUberJobId(getUjId())) {
        lock_guard<mutex> lck(_jobsMtx);
        _jobs.push_back(job);
        success = true;
    }
    if (!success) {
        LOGS(_log, LOG_LVL_ERROR,
             cName(__func__) << " job already in UberJob job=" << job->dump() << " uberJob=" + dump());
    }
    return success;
}

void UberJob::runUberJob() {
    LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " start");
    LOGS(_log, LOG_LVL_ERROR, cName(__func__) << "&&&uj runuj start");
    // Build the uberjob payload for each job.
    nlohmann::json uj;
    unique_lock<mutex> jobsLock(_jobsMtx);
    auto exec = _executive.lock();
    if (exec == nullptr || exec->getCancelled()) {
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " executive shutdown");
        return;
    }

    // Send the uberjob to the worker
    auto const method = http::Method::POST;
    auto [ciwId, ciwHost, ciwManagment, ciwPort] = _wContactInfo->getAll();
    string const url = "http://" + ciwHost + ":" + to_string(ciwPort) + "/queryjob";
    vector<string> const headers = {"Content-Type: application/json"};
    auto const& czarConfig = cconfig::CzarConfig::instance();

    uint64_t maxTableSizeMB = czarConfig->getMaxTableSizeMB();
    auto czInfo = protojson::CzarContactInfo::create(
            czarConfig->name(), czarConfig->id(), czarConfig->replicationHttpPort(),
            czar::Czar::getCzar()->getFqdn(), czar::Czar::czarStartupTime);
    auto scanInfoPtr = exec->getScanInfo();
    bool scanInteractive = exec->getScanInteractive();

    auto uberJobMsg = protojson::UberJobMsg::create(
            http::MetaModule::version, czarConfig->replicationInstanceId(), czarConfig->replicationAuthKey(),
            czInfo, _wContactInfo, _queryId, _uberJobId, _rowLimit, maxTableSizeMB, scanInfoPtr,
            scanInteractive, _jobs);

    json request = uberJobMsg->toJson();

    jobsLock.unlock();  // unlock so other _jobsMtx threads can advance while this waits for transmit
    LOGS(_log, LOG_LVL_ERROR, cName(__func__) << "&&&uj runuj c");
    /* &&&
    {  // &&& testing only, delete
        auto parsedReq = protojson::UberJobMsg::createFromJson(request);
        json jsParsedReq = parsedReq->serializeJson();
        if (request == jsParsedReq) {
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " &&&uj YAY!!! ");
        } else {
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " &&&uj noYAY request != jsParsedReq");
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " &&&uj request=" << request);
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " &&&uj jsParsedReq=" << jsParsedReq);
        }
    }
    */

    LOGS(_log, LOG_LVL_TRACE, cName(__func__) << " REQ " << request);
    string const requestContext = "Czar: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_TRACE,
         cName(__func__) << " czarPost url=" << url << " request=" << request.dump()
                         << " headers=" << headers[0]);

    auto commandHttpPool = czar::Czar::getCzar()->getCommandHttpPool();
    http::ClientConfig clientConfig;
    clientConfig.httpVersion = CURL_HTTP_VERSION_1_1;  // same as in qhttp
    clientConfig.bufferSize = CURL_MAX_READ_SIZE;      // 10 MB in the current version of libcurl
    clientConfig.tcpKeepAlive = true;
    clientConfig.tcpKeepIdle = 30;  // the default is 60 sec
    clientConfig.tcpKeepIntvl = 5;  // the default is 60 sec
    http::Client client(method, url, request.dump(), headers, clientConfig, commandHttpPool);
    bool transmitSuccess = false;
    string exceptionWhat;
    try {
        LOGS(_log, LOG_LVL_TRACE, cName(__func__) << " sending");
        json const response = client.readAsJson();
        LOGS(_log, LOG_LVL_TRACE, cName(__func__) << " worker recv");

        if (0 != response.at("success").get<int>()) {
            transmitSuccess = true;
        } else {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " ujresponse success=0");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, requestContext + " ujresponse failed, ex: " + ex.what());
        exceptionWhat = ex.what();
    }
    auto endclient = CLOCK::now();                                       //&&&
    std::chrono::duration<double> secsclient = endclient - startclient;  // &&&
    histoRunUberJob.addEntry(endclient, secsclient.count());             //&&&
    LOGS(_log, LOG_LVL_INFO, "&&&uj histo " << histoRunUberJob.getString(""));
    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " transmit failure, try to send jobs elsewhere");
        _unassignJobs();  // locks _jobsMtx
        setStatusIfOk(qmeta::JobStatus::RESPONSE_ERROR,
                      cName(__func__) + " not transmitSuccess " + exceptionWhat);
    } else {
        setStatusIfOk(qmeta::JobStatus::REQUEST, cName(__func__) + " transmitSuccess");  // locks _jobsMtx
    }

    return;
}

void UberJob::_unassignJobs() {
    LOGS(_log, LOG_LVL_INFO, cName(__func__));
    lock_guard<mutex> lck(_jobsMtx);
    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " exec is null");
        return;
    }
    for (auto&& job : _jobs) {
        string jid = job->getIdStr();
        if (!job->unassignFromUberJob(getUjId())) {
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " could not unassign job=" << jid << " cancelling");
            exec->addMultiError(qmeta::JobStatus::RETRY_ERROR, "unable to re-assign " + jid,
                                util::ErrorCode::INTERNAL);
            exec->squash("_unassignJobs failure");
            return;
        }
        LOGS(_log, LOG_LVL_DEBUG,
             cName(__func__) << " job=" << jid << " attempts=" << job->getAttemptCount());
    }
    _jobs.clear();
}

bool UberJob::_setStatusIfOk(qmeta::JobStatus::State newState, string const& msg) {
    // must be locked _jobsMtx
    auto currentState = _jobStatus->getState();
    // Setting the same state twice indicates that the system is trying to do something it
    // has already done, so doing it a second time would be an error.
    if (newState <= currentState) {
        LOGS(_log, LOG_LVL_WARN,
             cName(__func__) << " could not change from state=" << _jobStatus->stateStr(currentState)
                             << " to " << _jobStatus->stateStr(newState));
        return false;
    }

    // Overwriting errors is probably not a good idea.
    if (currentState >= qmeta::JobStatus::CANCEL && currentState < qmeta::JobStatus::COMPLETE) {
        LOGS(_log, LOG_LVL_WARN,
             cName(__func__) << " already error current=" << _jobStatus->stateStr(currentState)
                             << " new=" << _jobStatus->stateStr(newState));
        return false;
    }

    _jobStatus->updateInfo(getIdStr(), newState, msg);
    for (auto&& jq : _jobs) {
        jq->getStatus()->updateInfo(jq->getIdStr(), newState, msg);
    }
    return true;
}

void UberJob::callMarkCompleteFunc(bool success) {
    LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " success=" << success);

    lock_guard<mutex> lck(_jobsMtx);
    // Need to set this uberJob's status, however exec->markCompleted will set
    // the status for each job when it is called.
    string source = string("UberJob_") + (success ? "SUCCESS" : "FAILED");
    _jobStatus->updateInfo(getIdStr(), qmeta::JobStatus::COMPLETE, source);
    for (auto&& job : _jobs) {
        string idStr = job->getIdStr();
        if (success) {
            job->getStatus()->updateInfo(idStr, qmeta::JobStatus::COMPLETE, source);
        } else {
            job->getStatus()->updateInfoNoErrorOverwrite(idStr, qmeta::JobStatus::RESULT_ERROR, source,
                                                         util::ErrorCode::INTERNAL, "UberJob_failure");
        }
        auto exec = _executive.lock();
        exec->markCompleted(job->getJobId(), success);
    }

    // No longer need these here. Executive should still have copies.
    _jobs.clear();
}

json UberJob::importResultFile(string const& fileUrl, uint64_t rowCount, uint64_t fileSize) {
    LOGS(_log, LOG_LVL_DEBUG,
         cName(__func__) << " fileUrl=" << fileUrl << " rowCount=" << rowCount << " fileSize=" << fileSize);

    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) + " no executive");
        return importResultError(true, "cancelled", "Query cancelled - no executive");
    }

    if (exec->getCancelled()) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " import job was cancelled.");
        return importResultError(true, "cancelled", "Query cancelled");
    }

    if (exec->isRowLimitComplete()) {
        int dataIgnored = exec->incrDataIgnoredCount();
        if ((dataIgnored - 1) % 1000 == 0) {
            LOGS(_log, LOG_LVL_INFO,
                 "UberJob ignoring, enough rows already " << "dataIgnored=" << dataIgnored);
        }
        return importResultError(false, "rowLimited", "Enough rows already");
    }

    LOGS(_log, LOG_LVL_TRACE, cName(__func__) << " fileSize=" << fileSize);
    bool const statusSet = setStatusIfOk(qmeta::JobStatus::RESPONSE_READY, getIdStr() + " " + fileUrl);
    if (!statusSet) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " setStatusFail could not set status to RESPONSE_READY");
        return importResultError(false, "setStatusFail", "could not set status to RESPONSE_READY");
    }

    weak_ptr<UberJob> ujThis = weak_from_this();

    // fileCollectFunc will be put on the queue to run later.
    string const idStr = _idStr;
    auto fileCollectFunc = [ujThis, fileUrl, fileSize, rowCount, idStr](util::CmdData*) {
        auto ujPtr = ujThis.lock();
        if (ujPtr == nullptr) {
            LOGS(_log, LOG_LVL_DEBUG,
                 "UberJob::fileCollectFunction uberjob ptr is null " << idStr << " " << fileUrl);
            return;
        }
        auto exec = ujPtr->getExecutive();
        if (exec == nullptr) {
            LOGS(_log, LOG_LVL_DEBUG,
                 "UberJob::fileCollectFunction exec ptr is null " << idStr << " " << fileUrl);
            return;
        }

        exec->collectFile(ujPtr, fileUrl, fileSize, rowCount, idStr);
    };

    auto cmd = util::PriorityCommand::Ptr(new util::PriorityCommand(fileCollectFunc));
    exec->queueFileCollect(cmd);

    // The file collection has been queued for later, let the worker know that it's okay so far.
    protojson::ResponseMsg respMsg(true, "", "queued for collection");
    return respMsg.toJson();
}

json UberJob::workerError(int errorCode, string const& errorMsg) {
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " errcode=" << errorCode << " errmsg=" << errorMsg);

    bool const deleteData = true;
    bool const keepData = !deleteData;
    auto exec = _executive.lock();
    if (exec == nullptr || exec->getCancelled()) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " no executive or cancelled " << errorMsg);
        return _workerErrorFinish(deleteData, "cancelled");
    }

    if (exec->isRowLimitComplete()) {
        int dataIgnored = exec->incrDataIgnoredCount();
        if ((dataIgnored - 1) % 1000 == 0) {
            LOGS(_log, LOG_LVL_INFO,
                 cName(__func__) << " ignoring, enough rows already "
                                 << "dataIgnored=" << dataIgnored);
        }
        return _workerErrorFinish(keepData, "none", "rowLimitComplete");
    }

    string const eMsg = "host:" + _wContactInfo->getWHost() + " " + errorMsg;
    exec->addMultiError(errorCode, eMsg, util::ErrorCode::WORKER_ERROR);

    // Currently there are no detectable recoverable errors from workers. The only
    // error that a worker could send back that may possibly be recoverable would
    // be a missing table error, which is not trivial to detect. A worker local
    // database error may also qualify.
    // TODO:DM-53238 If recoverable errors can be detected on the workers, or
    //   maybe allow a single retry before sending the error back to the user?
    bool recoverableError = false;

    if (recoverableError) {
        // The czar should have new maps before the the new UberJob(s) for
        // these Jobs are created. (see Czar::_monitor)
        _unassignJobs();
    } else {
        // Get the error message to the user and kill the user query.
        int errState = util::ErrorCode::MYSQLEXEC;
        getRespHandler()->flushHttpError(errorCode, errorMsg, errState);
        exec->addMultiError(errorCode, errorMsg, errState);
        exec->squash(string("UberJob::workerError ") + errorMsg);
    }

    string errType = to_string(errorCode) + ":" + errorMsg;
    return _workerErrorFinish(deleteData, errType, "");
}

json UberJob::importResultError(bool shouldCancel, string const& errorType, string const& note) {
    protojson::ResponseMsg respMsg(false, errorType, note);
    // In all cases, the worker should delete the file as this czar will not ask for it.

    auto exec = _executive.lock();
    if (exec != nullptr) {
        LOGS(_log, LOG_LVL_ERROR,
             cName(__func__) << " shouldCancel=" << shouldCancel << " errorType=" << errorType << " "
                             << note);
        if (shouldCancel) {
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " failing jobs");
            callMarkCompleteFunc(false);  // all jobs failed, no retry
            exec->squash(string("_importResultError shouldCancel"));
        } else {
            /// - each JobQuery in _jobs needs to be flagged as needing to be
            ///   put in an UberJob and it's attempt count increased and checked
            ///   against the attempt limit.
            /// - executive needs to be told to make new UberJobs until all
            ///   JobQueries are being handled by an UberJob.
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " reassigning jobs");
            _unassignJobs();
            exec->assignJobsToUberJobs();
        }
    } else {
        LOGS(_log, LOG_LVL_INFO,
             cName(__func__) << " already cancelled shouldCancel=" << shouldCancel
                             << " errorType=" << errorType << " " << note);
    }
    return respMsg.toJson();
}

bool UberJob::importResultFinish() {
    LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " start");

    /// If this is called, the file has been collected and the worker should delete it
    ///
    /// This function should call markComplete for all jobs in the uberjob
    /// and return a "success:1" json message to be sent to the worker.
    bool const statusSet =
            setStatusIfOk(qmeta::JobStatus::RESPONSE_DONE, getIdStr() + " _importResultFinish");
    if (statusSet) {
        bool const success = true;
        callMarkCompleteFunc(success);  // sets status to COMPLETE
    }
    return statusSet;
}

nlohmann::json UberJob::_workerErrorFinish(bool deleteData, std::string const& errorType,
                                           std::string const& note) {
    // If this is called, the file has been collected and the worker should delete it
    //
    // Should this call markComplete for all jobs in the uberjob???
    // TODO:DM-53238 Only recoverable errors would be: communication failure, or missing table ???
    // Return a "success:1" json message to be sent to the worker.
    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " executive is null");
        protojson::ResponseMsg respMsg(false, "cancelled", "executive is null");
        return respMsg.toJson();
    }

    protojson::ResponseMsg respMsg(true);
    json jsRet = respMsg.toJson();
    jsRet["deletedata"] = deleteData;
    return jsRet;
}

bool UberJob::killUberJob() {
    // Usually called when a worker has effectively died.
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " stopping this UberJob and re-assigning jobs.");

    auto exec = _executive.lock();
    if (exec == nullptr || exec->getCancelled()) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " no executive or cancelled");
        return true;
    }

    if (exec->isRowLimitComplete()) {
        int dataIgnored = exec->incrDataIgnoredCount();
        if ((dataIgnored - 1) % 1000 == 0) {
            LOGS(_log, LOG_LVL_INFO, cName(__func__) << " ignoring, enough rows already.");
        }
        return true;
    }

    // At this point the user query appears to be alive and needs more data.
    // The reason to call this is that it is likely that a worker died and the
    // jobs need to go to different workers to be run.

    // Put this UberJob on the list of UberJobs that the worker should drop.
    auto activeWorkerMap = czar::Czar::getCzar()->getActiveWorkerMap();
    auto activeWorker = activeWorkerMap->getActiveWorker(_wContactInfo->wId);
    if (activeWorker != nullptr) {
        activeWorker->addDeadUberJob(_queryId, _uberJobId);
    }

    // If there are any ongoing file merges, either the czar has managed to collect
    // the result file from the worker and it will be merged or the czar will not be
    // able to retrieve said file and this UberJob will be killed.

    // This will only return false if merging the result file with the table has
    // happened or is happening now (all required data for this UberJob is already on
    // this czar.)
    bool cancelledMerge = getRespHandler()->cancelFileMerge();
    if (cancelledMerge) {
        _unassignJobs();
    }
    // Let Czar::_monitor reassign jobs - other UberJobs are probably being killed
    // so waiting probably gets a better distribution.
    return cancelledMerge;
}

std::ostream& UberJob::dumpOS(std::ostream& os) const {
    os << "(jobs sz=" << _jobs.size() << "(";
    lock_guard<mutex> lockJobsMtx(_jobsMtx);
    for (auto const& job : _jobs) {
        JobDescription::Ptr desc = job->getDescription();
        ResourceUnit ru = desc->resource();
        os << ru.db() << ":" << ru.chunk() << ",";
    }
    os << "))";
    return os;
}

std::string UberJob::dump() const {
    std::ostringstream os;
    dumpOS(os);
    return os.str();
}

std::ostream& operator<<(std::ostream& os, UberJob const& uj) { return uj.dumpOS(os); }

}  // namespace lsst::qserv::qdisp
