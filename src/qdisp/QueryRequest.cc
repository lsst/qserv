// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

/**
 * @file
 *
 * @brief QueryRequest. XrdSsiRequest impl for czar query dispatch
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "qdisp/QdispPool.h"
#include "qdisp/QueryRequest.h"

// System headers
#include <cstddef>
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "czar/Czar.h"
#include "qdisp/CzarStats.h"
#include "qdisp/UberJob.h"
#include "global/LogContext.h"
#include "proto/worker.pb.h"
#include "qdisp/JobStatus.h"
#include "qdisp/ResponseHandler.h"
#include "util/Bug.h"
#include "util/common.h"
#include "util/InstanceCount.h"
#include "util/Timer.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.QueryRequest");
}

namespace lsst::qserv::qdisp {

QueryRequest::QueryRequest(JobBase::Ptr const& job)
        : _job(job),
          _qid(job->getQueryId()),
          _jobid(job->getJobId()),
          _jobIdStr(job->getIdStr()),
          _qdispPool(_job->getQdispPool()) {
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    LOGS(_log, LOG_LVL_TRACE, "New QueryRequest");
}

QueryRequest::~QueryRequest() {
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    LOGS(_log, LOG_LVL_TRACE, __func__);
    if (!_finishedCalled) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " cleaning up calling Finished");
        bool ok = Finished();
        if (!ok) {
            LOGS(_log, LOG_LVL_ERROR, __func__ << " Finished NOT ok");
        }
    }
}

// content of request data
char* QueryRequest::GetRequest(int& requestLength) {
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    lock_guard<mutex> lock(_finishStatusMutex);
    auto jq = _job;
    if (_finishStatus != ACTIVE || jq == nullptr) {
        LOGS(_log, LOG_LVL_DEBUG, __func__ << " called after job finished (cancelled?)");
        requestLength = 0;
        return const_cast<char*>("");
    }
    requestLength = jq->getPayload().size();
    LOGS(_log, LOG_LVL_DEBUG, "Requesting, payload size: " << requestLength);
    // Andy promises that his code won't corrupt it.
    return const_cast<char*>(jq->getPayload().data());
}

// Must not throw exceptions: calling thread cannot trap them.
// Callback function for XrdSsiRequest.
//
bool QueryRequest::ProcessResponse(XrdSsiErrInfo const& eInfo, XrdSsiRespInfo const& rInfo) {
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    LOGS(_log, LOG_LVL_DEBUG, "workerName=" << GetEndPoint() << " " << __func__);
    string errorDesc = _jobIdStr + " ";
    if (isQueryCancelled()) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " job already cancelled");
        cancel();  // calls _errorFinish()
        return true;
    }

    // Make a copy of the _jobQuery shared_ptr in case _jobQuery gets reset by a call to  cancel()
    auto jq = _job;
    {
        lock_guard<mutex> lock(_finishStatusMutex);
        if ((_finishStatus != ACTIVE) || (jq == nullptr)) {
            LOGS(_log, LOG_LVL_WARN, __func__ << " called after job finished (cancelled?)");
            return true;
        }
    }
    if (eInfo.hasError()) {
        ostringstream os;
        os << _jobIdStr << __func__ << " request failed " << getSsiErr(eInfo, nullptr) << " "
           << GetEndPoint();
        jq->getRespHandler()->errorFlush(os.str(), -1);
        jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_ERROR, "SSI");
        _errorFinish();
        return true;
    }

    string responseTypeName;  // for error reporting
    switch (rInfo.rType) {
        case XrdSsiRespInfo::isNone:
            responseTypeName = "isNone";
            break;
        case XrdSsiRespInfo::isData:
            if (string(rInfo.buff, rInfo.blen) == "MockResponse") {
                jq->getStatus()->updateInfo(_jobIdStr, JobStatus::COMPLETE, "MOCK");
                _finish();
                return true;
            } else if (rInfo.blen == 0) {
                // Metadata-only responses for the file-based protocol should not have any data
                jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_READY, "SSI");
                return _importResultFile(jq);
            }
            responseTypeName = "isData";
            break;
        case XrdSsiRespInfo::isError:
            jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_ERROR, "SSI", rInfo.eNum,
                                        string(rInfo.eMsg));
            return _importError(string(rInfo.eMsg), rInfo.eNum);
        case XrdSsiRespInfo::isFile:
            responseTypeName = "isFile";
            break;
        case XrdSsiRespInfo::isStream:
            responseTypeName = "isStream";
            break;
        default:
            responseTypeName = "<unknown>";
    }
    return _importError("Unexpected XrdSsiRespInfo.rType == " + responseTypeName, -1);
}

/// Retrieve and process a result file using the file-based protocol
/// Uses a copy of JobQuery::Ptr instead of _jobQuery as a call to cancel() would reset _jobQuery.
bool QueryRequest::_importResultFile(JobBase::Ptr const& job) {
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
                     "QueryRequest::_processData ignoring, enough rows already " << "dataIgnored="
                                                                                 << dataIgnored);
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
}

/// Process an incoming error.
bool QueryRequest::_importError(string const& msg, int code) {
    auto jq = _job;
    {
        lock_guard<mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE || jq == nullptr) {
            LOGS(_log, LOG_LVL_WARN,
                 "QueryRequest::_importError code=" << code << " msg=" << msg << " not passed");
            return false;
        }
        jq->getRespHandler()->errorFlush(msg, code);
    }
    _errorFinish();
    return true;
}

void QueryRequest::ProcessResponseData(XrdSsiErrInfo const& eInfo, char* buff, int blen, bool last) {
    string const err = "the method has no use in this implementation of Qserv";
    LOGS(_log, LOG_LVL_ERROR, __func__ << " " << err);
    throw util::Bug(ERR_LOC, err);
}

void QueryRequest::_flushError(JobBase::Ptr const& jq) {
    ResponseHandler::Error err = jq->getRespHandler()->getError();
    jq->getStatus()->updateInfo(_jobIdStr, JobStatus::MERGE_ERROR, "MERGE", err.getCode(), err.getMsg(),
                                MSG_ERROR);
    _errorFinish(true);
}

/// @return true if QueryRequest cancelled successfully.
bool QueryRequest::cancel() {
    LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::cancel");
    {
        lock_guard<mutex> lock(_finishStatusMutex);
        if (_cancelled) {
            LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::cancel already cancelled, ignoring");
            return false;  // Don't do anything if already cancelled.
        }
        _cancelled = true;
        _retried = true;  // Prevent retries.
        // Only call the following if the job is NOT already done.
        if (_finishStatus == ACTIVE) {
            auto jq = _job;
            if (jq != nullptr) jq->getStatus()->updateInfo(_jobIdStr, JobStatus::CANCEL, "CANCEL");
        }
    }
    return _errorFinish(true);  // return true if errorFinish cancelled
}

/// @return true if this object's JobQuery, or its Executive has been cancelled.
/// It takes time for the Executive to flag all jobs as being cancelled
bool QueryRequest::isQueryCancelled() {
    auto jq = _job;
    if (jq == nullptr) {
        // Need to check if _jobQuery is null due to cancellation.
        return isQueryRequestCancelled();
    }
    return jq->isQueryCancelled();
}

/// @return true if QueryRequest::cancel() has been called.
/// QueryRequest::isQueryCancelled() is a much better indicator of user query cancellation.
bool QueryRequest::isQueryRequestCancelled() {
    lock_guard<mutex> lock(_finishStatusMutex);
    return _cancelled;
}

/// Cleanup pointers so this class can be deleted.
/// This should only be called by _finish or _errorFinish.
void QueryRequest::cleanup() {
    LOGS(_log, LOG_LVL_TRACE, "QueryRequest::cleanup()");
    {
        lock_guard<mutex> lock(_finishStatusMutex);
        if (_finishStatus == ACTIVE) {
            LOGS(_log, LOG_LVL_ERROR, "QueryRequest::cleanup called before _finish or _errorFinish");
            return;
        }
    }

    // These need to be outside the mutex lock, or you could delete
    // _finishStatusMutex before it is unlocked.
    // This should reset _jobquery and _keepAlive without risk of either being deleted
    // before being reset.
    shared_ptr<JobBase> jq(move(_job));
    shared_ptr<QueryRequest> keep(move(_keepAlive));
}

/// Finalize under error conditions and retry or report completion
/// THIS FUNCTION WILL RESULT IN THIS OBJECT BEING DESTROYED, UNLESS there is
/// a local shared pointer for this QueryRequest and/or its owner JobQuery.
/// See QueryRequest::cleanup()
/// @return true if this QueryRequest object had the authority to make changes.
bool QueryRequest::_errorFinish(bool shouldCancel) {
    LOGS(_log, LOG_LVL_DEBUG, "_errorFinish() shouldCancel=" << shouldCancel);

    auto jbase = _job;
    JobQuery::Ptr jq = dynamic_pointer_cast<JobQuery>(jbase);
    if (jq == nullptr) {
        //&&&uj IMPORTANT UberJob failures are different than JobQuery failures.
        UberJob::Ptr uberJob = dynamic_pointer_cast<UberJob>(jbase);
        if (uberJob != nullptr) {
            throw util::Bug(ERR_LOC, "&&&NEED_CODE for _errorFinish to work correctly with UberJob");
            // UberJobs breakup into their JobQueries when they fail and run the jobs directly.
        }
        return false;
    }

    // Normal JobQuery error handling.
    {
        // Running _errorFinish more than once could cause errors.
        lock_guard<mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE || jq == nullptr) {
            // Either _finish or _errorFinish has already been called.
            LOGS_DEBUG("_errorFinish() job no longer ACTIVE, ignoring "
                       << " _finishStatus=" << _finishStatus << " ACTIVE=" << ACTIVE << " jq=" << jq);
            return false;
        }
        _finishStatus = ERROR;
    }

    // Make the calls outside of the mutex lock.
    LOGS(_log, LOG_LVL_DEBUG, "calling Finished(shouldCancel=" << shouldCancel << ")");
    bool ok = Finished(shouldCancel);
    _finishedCalled = true;
    if (!ok) {
        LOGS(_log, LOG_LVL_ERROR, "QueryRequest::_errorFinish !ok ");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::_errorFinish ok");
    }

    if (!_retried.exchange(true) && !shouldCancel) {
        // There's a slight race condition here. _jobQuery::runJob() creates a
        // new QueryRequest object which will replace this one in _jobQuery.
        // The replacement could show up before this one's cleanup() is called,
        // so this will keep this alive until cleanup() is done.
        LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::_errorFinish retrying");
        _keepAlive = jq->getQueryRequest();  // shared pointer to this
        if (!jq->runJob()) {
            // Retry failed, nothing left to try.
            LOGS(_log, LOG_LVL_DEBUG, "errorFinish retry failed");
            _callMarkComplete(false);
        }
    } else {
        _callMarkComplete(false);
    }
    cleanup();  // Reset smart pointers so this object can be deleted.
    return true;
}

/// Finalize under success conditions and report completion.
/// THIS FUNCTION WILL RESULT IN THIS OBJECT BEING DESTROYED, UNLESS there is
/// a local shared pointer for this QueryRequest and/or its owner JobQuery.
/// See QueryRequest::cleanup()
void QueryRequest::_finish() {
    LOGS(_log, LOG_LVL_TRACE, "QueryRequest::_finish");
    {
        // Running _finish more than once would cause errors.
        lock_guard<mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            // Either _finish or _errorFinish has already been called.
            LOGS(_log, LOG_LVL_WARN, "QueryRequest::_finish called when not ACTIVE, ignoring");
            return;
        }
        _finishStatus = FINISHED;
    }

    bool ok = Finished();
    _finishedCalled = true;
    if (!ok) {
        LOGS(_log, LOG_LVL_ERROR, "QueryRequest::finish Finished() !ok ");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::finish Finished() ok.");
    }
    _callMarkComplete(true);
    cleanup();
}

/* &&&
/// Inform the Executive that this query completed, and
// Call MarkCompleteFunc only once, it should only be called from _finish() or _errorFinish.
void QueryRequest::_callMarkComplete(bool success) {
    if (!_calledMarkComplete.exchange(true)) {
        auto jq = _jobQuery;
        if (jq != nullptr) jq->getMarkCompleteFunc()->operator()(success);
    }
}
*/

void QueryRequest::_callMarkComplete(bool success) {
    if (!_calledMarkComplete.exchange(true)) {
        auto jq = _job;
        if (jq != nullptr) {
            jq->callMarkCompleteFunc(success);
        }
    }
}

ostream& operator<<(ostream& os, QueryRequest const& qr) {
    os << "QueryRequest " << qr._jobIdStr;
    return os;
}

/// @return The error text and code that SSI set.
/// if eCode != nullptr, it is set to the error code set by SSI.
string QueryRequest::getSsiErr(XrdSsiErrInfo const& eInfo, int* eCode) {
    int errNum;
    string errText = eInfo.Get(errNum);
    if (eCode != nullptr) {
        *eCode = errNum;
    }
    ostringstream os;
    os << "SSI_Error(" << errNum << ":" << errText << ")";
    return os.str();
}

}  // namespace lsst::qserv::qdisp
