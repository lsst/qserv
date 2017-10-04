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
#include "qdisp/QueryRequest.h"

// System headers
#include <cstddef>
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "czar/Czar.h"
#include "qdisp/JobStatus.h"
#include "qdisp/LargeResultMgr.h"
#include "qdisp/ResponseHandler.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.QueryRequest");
}

namespace lsst {
namespace qserv {
namespace qdisp {

////////////////////////////////////////////////////////////////////////
// QueryRequest
////////////////////////////////////////////////////////////////////////
QueryRequest::QueryRequest(JobQuery::Ptr const& jobQuery) :
  _jobQuery(jobQuery),
  _jobIdStr(jobQuery->getIdStr()),
  _largeResultSafety(_jobQuery->getLargeResultMgr(), _jobIdStr){
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr <<" New QueryRequest");
}

QueryRequest::~QueryRequest() {
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " ~QueryRequest");
}

// content of request data
char* QueryRequest::GetRequest(int& requestLength) {
    std::lock_guard<std::mutex> lock(_finishStatusMutex);
    auto jq = _jobQuery;
    if (_finishStatus != ACTIVE || jq == nullptr) {
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " QueryRequest::GetRequest called after job finished (cancelled?)");
        requestLength = 0;
        return const_cast<char*>("");
    }
    requestLength = jq->getDescription()->payload().size();
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " Requesting, payload size: " << requestLength);
    // Andy promises that his code won't corrupt it.
    return const_cast<char*>(jq->getDescription()->payload().data());
}

// precondition: rInfo.rType != isNone
// Must not throw exceptions: calling thread cannot trap them.
// Callback function for XrdSsiRequest.
//
bool QueryRequest::ProcessResponse(XrdSsiErrInfo  const& eInfo,
                                   XrdSsiRespInfo const& rInfo) {
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " ProcessResponse");
    std::string errorDesc = _jobIdStr + " ";
    if (isQueryCancelled()) {
        LOGS(_log, LOG_LVL_WARN, _jobIdStr << " QueryRequest::ProcessResponse job already cancelled");
        cancel(); // calls _errorFinish()
        return true;
    }

    // Make a copy of the _jobQuery shared_ptr in case _jobQuery gets reset by a call to  cancel()
    auto jq = _jobQuery;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            LOGS(_log, LOG_LVL_WARN,
                 _jobIdStr << " QueryRequest::GetRequest called after job finished (cancelled?)");
            return true;
        }
    }
    if (eInfo.hasError()) {
        std::ostringstream os;
        os << _jobIdStr << "ProcessResponse request failed "
           << getSsiErr(eInfo, nullptr);
        jq->getDescription()->respHandler()->errorFlush(os.str(), -1);
        jq->getStatus()->updateInfo(JobStatus::RESPONSE_ERROR);
        _errorFinish();
        return true;
    }
    switch(rInfo.rType) {
    case XrdSsiRespInfo::isNone: // All responses are non-null right now
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isNone";
        break;
    case XrdSsiRespInfo::isData: // Local-only for Mock tests!
        if (!strncmp(rInfo.buff, "MockResponse", rInfo.blen)) {
           jq->getStatus()->updateInfo(JobStatus::COMPLETE);
           _finish();
           return true;
        }
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isData";
        break;
    case XrdSsiRespInfo::isError:
        jq->getStatus()->updateInfo(JobStatus::RESPONSE_ERROR, rInfo.eNum, std::string(rInfo.eMsg));
        return _importError(std::string(rInfo.eMsg), rInfo.eNum);
    case XrdSsiRespInfo::isFile: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isFile";
        break;
    case XrdSsiRespInfo::isStream: // All remote requests
        jq->getStatus()->updateInfo(JobStatus::RESPONSE_READY);
        return _importStream(jq);
    default:
        errorDesc += "Out of range XrdSsiRespInfo.rType";
    }
    return _importError(errorDesc, -1);
}

/// Retrieve and process results in using the XrdSsi stream mechanism
/// Uses a copy of JobQuery::Ptr instead of _jobQuery as a call to cancel() would reset _jobQuery.
bool QueryRequest::_importStream(JobQuery::Ptr const& jq) {

    // Pass ResponseHandler's buffer directly.
    std::vector<char>& buffer = jq->getDescription()->respHandler()->nextBuffer();
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " _importStream buffer.size=" << buffer.size());
    const void* pbuf = (void*)(&buffer[0]);
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " _importStream->GetResponseData size="
         << buffer.size() << " " << pbuf << " " << util::prettyCharList(buffer, 5));
    GetResponseData(&buffer[0], buffer.size());
    return true;
}

/// Process an incoming error.
bool QueryRequest::_importError(std::string const& msg, int code) {
    auto jq = _jobQuery;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE || jq == nullptr) {
            LOGS_WARN(_jobIdStr << " QueryRequest::_importError code=" << code
                      << " msg=" << msg << " not passed");
            return false;
        }
        jq->getDescription()->respHandler()->errorFlush(msg, code);
    }
    _errorFinish();
    return true;
}


void QueryRequest::_setHoldState(HoldState state) {
    if (state != _holdState) {
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " holdState changed from " << _holdState << " to " << state);
    }
    _holdState = state;
}


XrdSsiRequest::PRD_Xeq QueryRequest::ProcessResponseData(XrdSsiErrInfo const& eInfo,
                                     char *buff, int blen, bool last) { // Step 7
    util::InstanceCount instC("instC QueryRequest::ProcessResponseData");
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " ProcessResponseData with buflen=" << blen
         << " " << (last ? "(last)" : "(more)"));

    // Class to make certain that the LargeResultMgr semaphore gets decremented.
    class CallOnFuncExit {
    public:
        CallOnFuncExit(bool callLargeResult, std::function<void()> resetHState)
          : _callLargeResult(callLargeResult), _resetHState(resetHState) {}

        ~CallOnFuncExit() { callNow(); }
        void callNow() {
            if (_callLargeResult) _resetHState();
            _callLargeResult = false; // only reset _holdState once
        }
        void setCallLargeResult() { _callLargeResult = true; }
    private:
        bool _callLargeResult{false};
        std::function<void()> _resetHState;
    };

    // If the _holdState is MERGE2, _largeResultMgr->finishBlock must be called at the end of this function.
    std::function<void()> resetHoldState = [this]() {
        _setHoldState(NO_HOLD0);
        _largeResultSafety.finishBlock();
    };
    CallOnFuncExit callOnExit(_holdState == MERGE2, resetHoldState);

    // Work with a copy of _jobQuery so it doesn't get reset underneath us by a call to cancel().
    JobQuery::Ptr jq = _jobQuery;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE || jq == nullptr || jq->isQueryCancelled()) {
            LOGS(_log, LOG_LVL_INFO, _jobIdStr << "ProcessResponseData job is inactive.");
            // Something must have killed this job.
            if (_holdState != NO_HOLD0) {
                LOGS(_log, LOG_LVL_INFO, _jobIdStr << "ProcessResponseData clearing heldData");
                // Must call largeResultMgr->finishBlock to free the semaphore.
                callOnExit.setCallLargeResult();
            }
            return XrdSsiRequest::PRD_Normal;
        }
    }

    auto getResponseDataFunc = [this, &jq]() {
        std::vector<char>& buffer = jq->getDescription()->respHandler()->nextBuffer();
        const void* pbuf = (void*)(&buffer[0]);
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " holdState=" << _holdState
                << " DataFunc->GetResponseData size=" << buffer.size() << " "
                << pbuf << " " << util::prettyCharList(buffer, 5));
        GetResponseData(&buffer[0], buffer.size());
    };

    // Get data for large response and prepare for merge on next ProcessResponseData call.
    if (_holdState == GET_DATA1) {
        _setHoldState(MERGE2);
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " holdState=" << _holdState << " held getResponseDataFunc()");
        getResponseDataFunc();
        return XrdSsiRequest::PRD_Normal;
    }

    if (blen < 0) { // error, check errinfo object.
        int eCode;
        auto reason = getSsiErr(eInfo, &eCode);
        jq->getStatus()->updateInfo(JobStatus::RESPONSE_DATA_NACK, eCode, reason);
        LOGS(_log, LOG_LVL_ERROR, _jobIdStr << " ProcessResponse[data] error(" << eCode
             << " " << reason << ")");
        jq->getDescription()->respHandler()->errorFlush(
            "Couldn't retrieve response data:" + reason + " " + _jobIdStr, eCode);
        _errorFinish();
        // An error occurred, let processing continue so it can be cleaned up soon.
        return XrdSsiRequest::PRD_Normal;
    }
    jq->getStatus()->updateInfo(JobStatus::RESPONSE_DATA);
    bool largeResult = false;
    bool flushOk = jq->getDescription()->respHandler()->flush(blen, last, largeResult);
    if (largeResult) {
        if (!_largeResult) LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " holdState largeResult set to true");
        _largeResult = true; // Once the worker indicates it's a large result, it stays that way.
    }

    // If _holdState was MERGE2 when callOnExit was created, the data should have been merged with the result
    // table in the above flush() call. The merge is the expensive part of handling large results, and since
    // it is finished, the LargeResultMgr semaphore needs to freed now so the next header can be read below.
    callOnExit.callNow();

    if (flushOk) {
        if (last) {
            auto sz = jq->getDescription()->respHandler()->nextBufferSize();
            if (last && sz != 0) {
                LOGS(_log, LOG_LVL_WARN,
                     _jobIdStr << " Connection closed when more information expected sz=" << sz);
            }
            jq->getStatus()->updateInfo(JobStatus::COMPLETE);
            _finish();
            // At this point all blocks for this job have been read, there's no point in
            // having XrdSsi wait for anything.
            return XrdSsiRequest::PRD_Normal;
        } else {
            if (_largeResult) {
                LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " being held");
                if (_holdState == NO_HOLD0) {
                    _setHoldState(GET_DATA1);
                    _largeResultSafety.startBlock();
                    return XrdSsiRequest::PRD_Hold; // semaphore locked.
                }
                return XrdSsiRequest::PRD_Normal;
            } else {
                LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " holdState=" << _holdState
                     << " not held getResponseDataFunc()");
                getResponseDataFunc();
            }
        }
    } else {
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " ProcessResponse data flush failed");
        ResponseHandler::Error err = jq->getDescription()->respHandler()->getError();
        jq->getStatus()->updateInfo(JobStatus::MERGE_ERROR, err.getCode(), err.getMsg());
        // @todo DM-2378 Take a closer look at what causes this error and take
        // appropriate action. There could be cases where this is recoverable.
        _retried.store(true); // Do not retry
        _errorFinish(true);
        if (_holdState != NO_HOLD0) {
            // Must call largeResultMgr->finishBlock to free the semaphore since it was locked.
            callOnExit.setCallLargeResult();
        }
        return XrdSsiRequest::PRD_Normal;
    }
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " QueryRequest::ProcessResponseData " << instC.getCount());
    return XrdSsiRequest::PRD_Normal;
}


/// @return true if QueryRequest cancelled successfully.
bool QueryRequest::cancel() {
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " QueryRequest::cancel");
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_cancelled) {
            LOGS(_log, LOG_LVL_DEBUG, _jobIdStr <<" QueryRequest::cancel already cancelled, ignoring");
            return false; // Don't do anything if already cancelled.
        }
        _cancelled = true;
        _retried.store(true); // Prevent retries.
        // Only call the following if the job is NOT already done.
        if (_finishStatus == ACTIVE) {
            auto jq = _jobQuery;
            if (jq != nullptr) jq->getStatus()->updateInfo(JobStatus::CANCEL);
        }
    }
    return _errorFinish(true); // return true if errorFinish cancelled
}


/// @return true if this object's JobQuery, or its Executive has been cancelled.
/// It takes time for the Executive to flag all jobs as being cancelled
bool QueryRequest::isQueryCancelled() {
    auto jq = _jobQuery;
    if (jq == nullptr) {
        // Need to check if _jobQuery is null due to cancellation.
        return isQueryRequestCancelled();
    }
    return jq->isQueryCancelled();
}


/// @return true if QueryRequest::cancel() has been called.
/// QueryRequest::isCancelled() is a much better indicator of user query cancellation.
bool QueryRequest::isQueryRequestCancelled() {
    std::lock_guard<std::mutex> lock(_finishStatusMutex);
    return _cancelled;
}


/// Cleanup pointers so class can be deleted. This should only be called by _finish or _errorFinish.
void QueryRequest::cleanup() {
    LOGS_DEBUG(_jobIdStr << " QueryRequest::cleanup()");
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus == ACTIVE) {
            LOGS_ERROR(_jobIdStr << " QueryRequest::cleanup called before _finish or _errorFinish");
            return;
        }
    }

    // Release this in the off chance it has not been released already.
    if (_largeResultSafety.finishBlock()) {
        LOGS_ERROR(_jobIdStr << " QueryRequest::cleanup had to call finishBlock()");
    }

    // These need to be outside the mutex lock, or you could delete
    // _finishStatusMutex before it is unlocked.
    // This should reset _jobquery and _keepAlive without risk of either being deleted
    // before being reset.
    std::shared_ptr<JobQuery> jq(std::move(_jobQuery));
    std::shared_ptr<QueryRequest> keep(std::move(_keepAlive));
}


/// Finalize under error conditions and retry or report completion
/// This function will destroy this object.
/// @return true if this QueryRequest object had the authority to make changes.
bool QueryRequest::_errorFinish(bool shouldCancel) {
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " _errorFinish() shouldCancel=" << shouldCancel);
    auto jq = _jobQuery;
    {
        // Running _errorFinish more than once could cause errors.
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE || jq == nullptr) {
            // Either _finish or _errorFinish has already been called.
            LOGS_DEBUG(_jobIdStr << " _errorFinish() job no longer ACTIVE, ignoring "
                       << " _finishStatus=" << _finishStatus
                       << " ACTIVE=" << ACTIVE << " jq=" << jq);
            return false;
        }
        _finishStatus = ERROR;
    }

    // Make the calls outside of the mutex lock.
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " calling Finished(shouldCancel=" << shouldCancel << ")");
    bool ok = Finished(shouldCancel);
    if (!ok) {
        LOGS(_log, LOG_LVL_ERROR, _jobIdStr << " QueryRequest::_errorFinish !ok ");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " QueryRequest::_errorFinish ok");
    }

    if (!_retried.exchange(true) && !shouldCancel) {
        // There's a slight race condition here. _jobQuery::runJob() creates a
        // new QueryRequest object which will replace this one in _jobQuery.
        // The replacement could show up before this one's cleanup() is called,
        // so this will keep this alive.
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " QueryRequest::_errorFinish retrying");
        _keepAlive = jq->getQueryRequest(); // shared pointer to this
        if (!jq->runJob()) {
            // Retry failed, nothing left to try.
            LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << "errorFinish retry failed");
            _callMarkComplete(false);
        }
    } else {
        _callMarkComplete(false);
    }
    cleanup(); // Reset smart pointers so this object can be deleted.
    return true;
}

/// Finalize under success conditions and report completion.
void QueryRequest::_finish() {
    LOGS_DEBUG(_jobIdStr << " QueryRequest::_finish");
    {
        // Running _finish more than once would cause errors.
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            // Either _finish or _errorFinish has already been called.
            LOGS_WARN(_jobIdStr << " QueryRequest::_finish called when not ACTIVE, ignoring");
            return;
        }
        _finishStatus = FINISHED;
    }
    bool ok = Finished();
    if (!ok) {
        LOGS(_log, LOG_LVL_ERROR, _jobIdStr << " QueryRequest::finish Finished() !ok ");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " QueryRequest::finish Finished() ok.");
    }

    _callMarkComplete(true);
    cleanup();
}

/// Inform the Executive that this query completed, and
// Call MarkCompleteFunc only once, it should only be called from _finish() or _errorFinish.
void QueryRequest::_callMarkComplete(bool success) {
    if (!_calledMarkComplete.exchange(true)) {
        auto jq = _jobQuery;
        if (jq != nullptr) jq->getMarkCompleteFunc()->operator()(success);
    }
}

std::ostream& operator<<(std::ostream& os, QueryRequest const& qr) {
    os << "QueryRequest " << qr._jobIdStr;
    return os;
}


/// @return The error text and code that SSI set.
/// if eCode != nullptr, it is set to the error code set by SSI.
std::string QueryRequest::getSsiErr(XrdSsiErrInfo const& eInfo, int* eCode) {
    int errNum;
    std::string errText = eInfo.Get(errNum);
    if (eCode != nullptr) {
        *eCode = errNum;
    }
    std::ostringstream os;
    os << "SSI_Error(" << errNum << ":" << errText << ")";
    return os.str();
}


LargeResultSafety::~LargeResultSafety() {
    if (finishBlock()) {
        LOGS(_log, LOG_LVL_INFO, _jobIdStr << " ~LargeResultSafety had to call finishBlock");
    }
}

void LargeResultSafety::startBlock() {
    std::lock_guard<std::mutex> lg(_blockMtx);
    _startBlockCalled = true;
    _largeResultMgr->startBlock(_jobIdStr);
}


/// @return true if finishBlock() was called.
bool LargeResultSafety::finishBlock() {
    std::lock_guard<std::mutex> lg(_blockMtx);
    if (_startBlockCalled) {
        _startBlockCalled = false;
        _largeResultMgr->finishBlock(_jobIdStr);
        return true;
    }
    return false;
}

}}} // lsst::qserv::qdisp
