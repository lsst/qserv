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
#include <google/protobuf/arena.h>
#include "nlohmann/json.hpp"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "global/LogContext.h"
#include "http/Client.h"
#include "http/MetaModule.h"
#include "proto/ProtoImporter.h"
#include "proto/worker.pb.h"
#include "qdisp/JobQuery.h"
#include "util/Bug.h"
#include "util/common.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.UberJob");
}

namespace lsst { namespace qserv { namespace qdisp {

UberJob::Ptr UberJob::create(Executive::Ptr const& executive,
                             std::shared_ptr<ResponseHandler> const& respHandler, int queryId, int uberJobId,
                             qmeta::CzarId czarId,
                             czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData) {
    UberJob::Ptr uJob(new UberJob(executive, respHandler, queryId, uberJobId, czarId, workerData));
    uJob->_setup();
    return uJob;
}

UberJob::UberJob(Executive::Ptr const& executive, std::shared_ptr<ResponseHandler> const& respHandler,
                 int queryId, int uberJobId, qmeta::CzarId czarId,
                 czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData)
        : JobBase(),
          _executive(executive),
          _respHandler(respHandler),
          _queryId(queryId),
          _uberJobId(uberJobId),
          _czarId(czarId),
          _idStr("QID=" + to_string(_queryId) + ":uber=" + to_string(uberJobId)),
          _workerData(workerData) {
    _qdispPool = executive->getQdispPool();
    _jobStatus = make_shared<JobStatus>();
}

void UberJob::_setup() {
    JobBase::Ptr jbPtr = shared_from_this();
    _respHandler->setJobQuery(jbPtr);
}

bool UberJob::addJob(JobQuery::Ptr const& job) {
    bool success = false;
    if (job->inUberJob()) {
        throw util::Bug(ERR_LOC, string("job already in UberJob job=") + job->dump() + " uberJob=" + dump());
    }
    lock_guard<mutex> lck(_jobsMtx);
    _jobs.push_back(job);
    job->setInUberJob(true);
    success = true;
    return success;
}

bool UberJob::runUberJob() {
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() start");
    // &&&uj most, if not all, of this should be done in a command in the QDispPool.
    // &&&uk especially the communication parts.
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() a");
    // Build the uberjob payload for each job.
    nlohmann::json uj;
    unique_lock<mutex> jobsLock(_jobsMtx);
    for (auto const& jqPtr : _jobs) {
        LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() a1");
        jqPtr->getDescription()->incrAttemptCountScrubResultsJson();
    }

    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() b");
    // Send the uberjob to the worker
    auto const method = http::Method::POST;
    string const url = "http://" + _wContactInfo->wHost + ":" + to_string(_wContactInfo->wPort) + "/queryjob";
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() c " << url);
    vector<string> const headers = {"Content-Type: application/json"};
    auto const& czarConfig = cconfig::CzarConfig::instance();
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() c");
    // See xrdsvc::httpWorkerCzarModule::_handleQueryJob for json message parsing.
    json request = {{"version", http::MetaModule::version},
                    {"instance_id", czarConfig->replicationInstanceId()},
                    {"auth_key", czarConfig->replicationAuthKey()},
                    {"worker", _wContactInfo->wId},
                    {"czar",
                     {{"name", czarConfig->name()},
                      {"id", czarConfig->id()},
                      {"management-port", czarConfig->replicationHttpPort()},
                      {"management-host-name", util::get_current_host_fqdn()}}},
                    {"uberjob",
                     {{"queryid", _queryId},
                      {"uberjobid", _uberJobId},
                      {"czarid", _czarId},
                      {"jobs", json::array()}}}};

    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() d " << request);
    auto& jsUberJob = request["uberjob"];
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() e " << jsUberJob);
    auto& jsJobs = jsUberJob["jobs"];
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() f " << jsJobs);
    for (auto const& jbPtr : _jobs) {
        LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() f1");
        json jsJob = {{"jobdesc", *(jbPtr->getDescription()->getJsForWorker())}};
        jsJobs.push_back(jsJob);
        jbPtr->getDescription()->resetJsForWorker();  // no longer needed.
    }
    jobsLock.unlock();

    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() g");
    LOGS(_log, LOG_LVL_WARN, __func__ << " &&&REQ " << request);
    string const requestContext = "Czar: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_TRACE,
         __func__ << " czarPost url=" << url << " request=" << request.dump() << " headers=" << headers[0]);
    http::Client client(method, url, request.dump(), headers);
    bool transmitSuccess = false;
    try {
        json const response = client.readAsJson();
        LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::runUberJob() response=" << response);
        if (0 != response.at("success").get<int>()) {
            LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::runUberJob() success");
            transmitSuccess = true;
        } else {
            LOGS(_log, LOG_LVL_WARN, "&&&uj NEED CODE UberJob::runUberJob() success=0");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, requestContext + " &&&uj failed, ex: " + ex.what());
    }
    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR, "&&&uj NEED CODE UberJob::runUberJob() need to try to send jobs elsewhere");
    } else {
        LOGS(_log, LOG_LVL_WARN,
             "&&&uj NEED CODE UberJob::runUberJob() need to register all jobs as transmitted to worker");
    }

#if 0   // &&&uj Everything in this block needs to happen in some manner. Where is the question
    // For simplicity in the first pass, just make a TaskMsg for each Job and append it to the
    // UberJobMsg.
    //         This is terribly inefficient and should be replaced by using a template and list of chunks that
    //         the worker fills in, much like subchunks are done now.
    {
        google::protobuf::Arena arena;
        proto::UberJobMsg* ujMsg = google::protobuf::Arena::CreateMessage<proto::UberJobMsg>(&arena);
        ujMsg->set_queryid(getQueryId());
        ujMsg->set_czarid(_czarId);
        ujMsg->set_uberjobid(_uberJobId);
        ujMsg->set_magicnumber(UberJob::getMagicNumber());
        LOGS(_log, LOG_LVL_INFO, "&&& runUberJob sz=" << _jobs.size());
        for (auto&& job : _jobs) {
            proto::TaskMsg* tMsg = ujMsg->add_taskmsgs();
            job->getDescription()->fillTaskMsg(tMsg);
        }
        ujMsg->SerializeToString(&_payload);
    }

    auto executive = _executive.lock();
    if (executive == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "runUberJob failed executive==nullptr");
        return false;
    }
    bool cancelled = executive->getCancelled();
    bool handlerReset = _respHandler->reset();
    bool started = _started.exchange(true);
    if (!cancelled && handlerReset && !started) {
        auto criticalErr = [this, &executive](std::string const& msg) {
            LOGS(_log, LOG_LVL_ERROR, msg << " " << *this << " Canceling user query!");
            executive->squash();  // This should kill all jobs in this user query.
        };

        LOGS(_log, LOG_LVL_DEBUG, "runUberJob verifying payloads");
        if (!verifyPayload()) {
            criticalErr("bad payload");
            return false;
        }

        // At this point we are all set to actually run the queries. We create a
        // a shared pointer to this object to prevent it from escaping while we
        // are trying to start this whole process. We also make sure we record
        // whether or not we are in SSI as cancellation handling differs.
        //
        LOGS(_log, LOG_LVL_TRACE, "runUberJob calls StartQuery()");
        std::shared_ptr<UberJob> uJob(dynamic_pointer_cast<UberJob>(shared_from_this()));
        _inSsi = true;
        if (executive->startUberJob(uJob)) {
            //&&&_jobStatus->updateInfo(_idStr, JobStatus::REQUEST);
            _jobStatus->updateInfo(_idStr, JobStatus::REQUEST, "EXEC");
            return true;
        }
        _inSsi = false;
    }
    LOGS(_log, LOG_LVL_WARN,
         "runUberJob failed. cancelled=" << cancelled << " reset=" << handlerReset << " started=" << started);
#endif  // &&&
    return false;
}

void UberJob::prepScrubResults() {
    // &&&uj There's a good chance this will not be needed as incomplete files will not be merged
    //       so you don't have to worry about removing rows from incomplete jobs or uberjobs
    //       from the result table.
    throw util::Bug(ERR_LOC,
                    "&&&uj If needed, should call prepScrubResults for all JobQueries in the UberJob ");
}

bool UberJob::isQueryCancelled() {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "_executive == nullptr");
        return true;  // Safer to assume the worst.
    }
    return exec->getCancelled();
}

bool UberJob::verifyPayload() const {
    proto::ProtoImporter<proto::UberJobMsg> pi;
    if (!pi.messageAcceptable(_payload)) {
        LOGS(_log, LOG_LVL_DEBUG, _idStr << " Error serializing UberJobMsg.");
        return false;
    }
    return true;
}

void UberJob::callMarkCompleteFunc(bool success) { // &&&uj make private
    LOGS(_log, LOG_LVL_DEBUG, "UberJob::callMarkCompleteFunc success=" << success);
    if (!success) {
        // &&&uj this function should probably only be called for successful completion.
        throw util::Bug(ERR_LOC, "&&&NEED_CODE may need code to properly handle failed uberjob");
    }

    lock_guard<mutex> lck(_jobsMtx);
    for (auto&& job : _jobs) {
        string idStr = job->getIdStr();
        job->getStatus()->updateInfo(idStr, JobStatus::COMPLETE, "COMPLETE");
        job->callMarkCompleteFunc(success);
    }

    // No longer need these here. Executive should still have copies.
    _jobs.clear();

    //&&&uj NEED CODE _resultStatus = MERGESUCCESS;
    //&&&uj NEED CODE _status = COMPLETE;
}


/// Retrieve and process a result file using the file-based protocol
/// Uses a copy of JobQuery::Ptr instead of _jobQuery as a call to cancel() would reset _jobQuery.
//&&&bool QueryRequest::_importResultFile(JobBase::Ptr const& job) {
nlohmann::json UberJob::importResultFile(string const& fileUrl, uint64_t rowCount, uint64_t fileSize) {
    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile a");
    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile fileUrl=" << fileUrl << " rowCount=" << rowCount << " fileSize=" << fileSize);

    //&&&uj NEED CODE update status for each job in this uberjob
    //      jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_READY, "SSI");

    // It's possible jq and _jobQuery differ, so need to use jq.
    if (isQueryCancelled()) {
        LOGS(_log, LOG_LVL_WARN, "UberJob::importResultFile import job was cancelled.");
        return _errorFinish(true);
    }
    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile b");
    /* &&&
    auto jq = std::dynamic_pointer_cast<JobQuery>(job);
    if (jq == nullptr) {
        throw util::Bug(ERR_LOC, string(__func__) + " unexpected pointer type for job");
    }
    */
    auto exec = _executive.lock();
    if (exec == nullptr || exec->getCancelled()) {
        LOGS(_log, LOG_LVL_WARN, "UberJob::importResultFile no executive or cancelled");
        return _errorFinish(true);
    }
    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile c");

    if (exec->isLimitRowComplete()) {
        int dataIgnored = exec->incrDataIgnoredCount();
        if ((dataIgnored - 1) % 1000 == 0) {
            LOGS(_log, LOG_LVL_INFO,
                    "UberJob ignoring, enough rows already "
                    << "dataIgnored=" << dataIgnored);
        }
        return _errorFinish(false);
    }

    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile d");
    //&&& int messageSize = 0;
    //&&& const char* message = GetMetadata(messageSize);

    LOGS(_log, LOG_LVL_DEBUG, __func__ << " _jobIdStr=" << getIdStr() << ", fileSize=" << fileSize);

    JobBase::Ptr jBaseThis = shared_from_this();
    weak_ptr<UberJob> ujThis = std::dynamic_pointer_cast<UberJob>(jBaseThis);

    /// &&&&&&&&&&&&&&&&&&&&&&uj This NEEDS CODE Command class item instead of lambda and queue that to qdisppool &&&&&&&&&&&&&&&&&
    /// &&&&&&&&&uj Also, HttpCzarWorkerModule::_handleJobReady isn't getting message from the worker UberJobData::fileReadyResponse &&&&&&&&&
    auto fileCollectFunc = [ujThis, fileUrl, rowCount](util::CmdData*) {
        LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile::fileCollectFunc a");
        /* &&&
        // &&&uj this version of flush is going to have issues.
        // &&&uj the reading of the file needs to happen elsewhere.
        uint32_t resultRows = 0;
        if (!jq->getDescription()->respHandler()->flush(responseSummary, resultRows)) {
            LOGS(_log, LOG_LVL_ERROR, __func__ << " not flushOk");
            _flushError(jq);
            return false;
        }
        //&&&_totalRows += resultRows;
         *
         */
        auto ujPtr = ujThis.lock();
        if (ujPtr == nullptr) {
            LOGS(_log, LOG_LVL_DEBUG, "UberJob::importResultFile::fileCollectFunction uberjob ptr is null " << fileUrl);
            return;
        }
        uint64_t resultRows = 0;
        auto [flushSuccess, flushShouldCancel] = ujPtr->getRespHandler()->flushHttp(fileUrl, rowCount, resultRows);
        LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile::fileCollectFunc b");
        if (!flushSuccess) {
            // This would probably indicate malformed file+rowCount or
            // writing the result table failed.
            ujPtr->_errorFinish(flushShouldCancel);
        }


        // At this point all data for this job have been read, there's no point in
        // having XrdSsi wait for anything.
        //&&&jq->getStatus()->updateInfo(_jobIdStr, JobStatus::COMPLETE, "COMPLETE");
        LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile::fileCollectFunc c");
        ujPtr->_finish(resultRows);  //&&&uj flush and finish need to happen elsewhere, put it in qdisppool.


        LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::importResultFile::fileCollectFunc end");
    };

    //&&&fileCollectFunc();

    //&&&auto cmd = std::make_shared<qdisp::PriorityCommand>(fileCollectFunc);
    auto cmd = qdisp::PriorityCommand::Ptr(new qdisp::PriorityCommand(fileCollectFunc));
    exec->queueFileCollect(cmd);


    /* &&&uj no need for this
    proto::ResponseSummary responseSummary;
    if (!(responseSummary.ParseFromArray(message, messageSize) && responseSummary.IsInitialized())) {
        string const err = "failed to parse the response summary, messageSize=" + to_string(messageSize);
        LOGS(_log, LOG_LVL_ERROR, __func__ << " " << err);
        throw util::Bug(ERR_LOC, err);
    }
    */


    // If the query meets the limit row complete complete criteria, it will start
    // squashing superfluous results so the answer can be returned quickly.

    json jsRet = {{"success", 1}, {"errortype", ""}, {"note", "queued for collection"}};
    return jsRet;

#if 0 //&&&
    // It's possible jq and _jobQuery differ, so need to use jq.
    if (job->isQueryCancelled()) {
        LOGS(_log, LOG_LVL_WARN, "QueryRequest::_processData job was cancelled.");
        _errorFinish(true);
        return false;
    }
    auto jq = std::dynamic_pointer_cast<JobQuery>(job);
    if (jq == nullptr) {
        throw util::Bug(ERR_LOC, string(__func__) + " unexpected pointer type for job");
    }
    auto executive = jq->getExecutive();
    if (executive == nullptr || executive->getCancelled() || executive->isLimitRowComplete()) {
        if (executive == nullptr || executive->getCancelled()) {
            LOGS(_log, LOG_LVL_WARN, "QueryRequest::_processData job was cancelled.");
        } else {
            int dataIgnored = (executive->incrDataIgnoredCount());
            if ((dataIgnored - 1) % 1000 == 0) {
                LOGS(_log, LOG_LVL_INFO,
                     "QueryRequest::_processData ignoring, enough rows already "
                             << "dataIgnored=" << dataIgnored);
            }
        }
        _errorFinish(true);
        return false;
    }

    int messageSize = 0;
    const char* message = GetMetadata(messageSize);

    LOGS(_log, LOG_LVL_DEBUG, __func__ << " _jobIdStr=" << _jobIdStr << ", messageSize=" << messageSize);

    proto::ResponseSummary responseSummary;
    if (!(responseSummary.ParseFromArray(message, messageSize) && responseSummary.IsInitialized())) {
        string const err = "failed to parse the response summary, messageSize=" + to_string(messageSize);
        LOGS(_log, LOG_LVL_ERROR, __func__ << " " << err);
        throw util::Bug(ERR_LOC, err);
    }
    uint32_t resultRows = 0;
    if (!jq->getDescription()->respHandler()->flush(responseSummary, resultRows)) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " not flushOk");
        _flushError(jq);
        return false;
    }
    _totalRows += resultRows;

    // At this point all data for this job have been read, there's no point in
    // having XrdSsi wait for anything.
    jq->getStatus()->updateInfo(_jobIdStr, JobStatus::COMPLETE, "COMPLETE");
    _finish();

    // If the query meets the limit row complete complete criteria, it will start
    // squashing superfluous results so the answer can be returned quickly.
    executive->addResultRows(_totalRows);
    executive->checkLimitRowComplete();

    return true;
#endif // &&&
}

json UberJob::_errorFinish(bool shouldCancel) {
    json jsRet = {{"success", 0}, {"errortype", "dataproblem"}, {"note", ""}};
    /// &&&uj NEED CODE
    ///          - each JobQuery in _jobs needs to be flagged as needing to be
    ///            put in an UberJob and it's attempt count increased and checked
    ///            against the attempt limit.
    ///          - executive needs to be told to make new UberJobs until all
    ///              JobQueries are being handled by an UberJob.
    /// &&&uj see QueryRequest for some details
    ///       If shouldCancel is false, it may be possible to recover, so all
    ///       jobs that were in this query should marked NEED_RETRY so they
    ///       will be retried.
    ///       If shouldCancel is true, this function should call markComplete
    ///       for all jobs in the uberjob, with all jobs failed.
    ///
    ///       In both case, the worker should delete the file as
    ///       this czar will not ask for it, so return a "success:0" json
    ///       message to the worker.
    if (shouldCancel) {
        jsRet = {{"success", 0}, {"errortype", "cancelling"}, {"note", ""}};
    } else {
        ;
    }
    return jsRet;
}

nlohmann::json UberJob::_finish(uint64_t resultRows) {
    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::_finish a");
    /// &&&uj NEED CODE
    /// &&&uj see QueryRequest for some details
    ///       If this is called, the file has been collected and the worker should delete it
    ///
    ///       This function should call markComplete for all jobs in the uberjob
    ///       and return a "success:1" json message to be sent to the worker.
    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_DEBUG, "UberJob::_finish executive is null qId=" << getQueryId() << " ujId=" << getJobId());
        return {{"success", 0}, {"errortype", "cancelled"}, {"note", "executive is null"}};
    }

    bool const success = true;
    callMarkCompleteFunc(success);
    exec->addResultRows(resultRows);
    exec->checkLimitRowComplete();



    json jsRet = {{"success", 1}, {"errortype", ""}, {"note", ""}};
    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::_finish end");
    return jsRet;
}


std::ostream& UberJob::dumpOS(std::ostream& os) const {
    os << "(jobs sz=" << _jobs.size() << "(";
    for (auto const& job : _jobs) {
        JobDescription::Ptr desc = job->getDescription();
        ResourceUnit ru = desc->resource();
        os << ru.db() << ":" << ru.chunk() << ",";
    }
    os << "))";
    return os;
}

}}}  // namespace lsst::qserv::qdisp
