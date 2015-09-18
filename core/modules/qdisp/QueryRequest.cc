// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qdisp/JobStatus.h"
#include "qdisp/ResponseRequester.h"
#include "util/common.h"

namespace lsst {
namespace qserv {
namespace qdisp {

////////////////////////////////////////////////////////////////////////
// QueryRequest
////////////////////////////////////////////////////////////////////////
QueryRequest::QueryRequest( XrdSsiSession* session, std::shared_ptr<JobQuery> const& jobQuery) :
    _session(session), _jobQuery(jobQuery), _jobDesc(_jobQuery->getDescription()) {
    LOGF_INFO("New QueryRequest with payload(%1%)" % _jobDesc.payload().size());
}

QueryRequest::~QueryRequest() {
    LOGF_DEBUG("~QueryRequest");
    if(_session) {
          if(_session->Unprovision()) {
              LOGF_DEBUG("Unprovision ok.");
          } else {
              LOGF_ERROR("Error unprovisioning");
          }
      }
}

// content of request data
char* QueryRequest::GetRequest(int& requestLength) {
    requestLength = _jobDesc.payload().size();
    LOGF_DEBUG("Requesting, payload size: [%1%]" % requestLength);
    // Andy promises that his code won't corrupt it.
    return const_cast<char*>(_jobDesc.payload().data());
}

// Deleting the buffer (payload) would cause us problems, as this class is not the owner.
void QueryRequest::RelRequestBuffer() {
    LOGF_DEBUG("RelRequestBuffer");
}
// precondition: rInfo.rType != isNone
// Must not throw exceptions: calling thread cannot trap them.
// Callback function for XrdSsiRequest.
// See QueryResource::ProvisionDone which invokes ProcessRequest(QueryRequest*))
bool QueryRequest::ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk) {
    std::string errorDesc;
    if(isCancelled()) {
        cancel(); // calls _errorFinish()
        return true;
    }
    if(!isOk) {
        _jobDesc.respHandler()->errorFlush(std::string("Request failed"), -1);
        _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_ERROR);
        _errorFinish();
        return true;
    }
    switch(rInfo.rType) {
    case XrdSsiRespInfo::isNone: // All responses are non-null right now
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isNone";
        break;
    case XrdSsiRespInfo::isData: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isData";
        break;
    case XrdSsiRespInfo::isError: // isOk == true
        _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_ERROR, rInfo.eNum, std::string(rInfo.eMsg));
        return _importError(std::string(rInfo.eMsg), rInfo.eNum);
    case XrdSsiRespInfo::isFile: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isFile";
        break;
    case XrdSsiRespInfo::isStream: // All remote requests
        _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_READY);
        return _importStream();
    default:
        errorDesc += "Out of range XrdSsiRespInfo.rType";
    }
    return _importError(errorDesc, -1);
}

/// Retrieve and process results in using the XrdSsi stream mechanism
bool QueryRequest::_importStream() {
    bool success = false;
    // Pass ResponseHandler's buffer directly.
    std::vector<char>& buffer = _jobDesc.respHandler()->nextBuffer();
    LOGF_DEBUG("QueryRequest::_importStream buffer.size=%1%" % buffer.size());
    const void* pbuf = (void*)(&buffer[0]);
    LOGF_DEBUG("_importStream->GetResponseData size=%1% %2% %3%" %
              buffer.size() % pbuf % util::prettyCharList(buffer, 5));
    success = GetResponseData(&buffer[0], buffer.size());
    LOGF_DEBUG("Initiated request %1%" % (success ? "ok" : "err"));
    if(!success) {
        _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_DATA_ERROR);
        if (Finished()) {
            _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_DATA_ERROR_OK);
        } else {
            _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_DATA_ERROR_CORRUPT);
        }
        _errorFinish();
        return false;
    } else {
        return true;
    }
}

/// Process an incoming error.
bool QueryRequest::_importError(std::string const& msg, int code) {
    _jobDesc.respHandler()->errorFlush(msg, code);
    _errorFinish();
    return true;
}

void QueryRequest::ProcessResponseData(char *buff, int blen, bool last) { // Step 7
    LOGF_INFO("ProcessResponseData with buflen=%1% %2%" % blen % (last ? "(last)" : "(more)"));
    if(blen < 0) { // error, check errinfo object.
        int eCode;
        const char* chs = eInfo.Get(eCode);
        std::string reason = (chs == nullptr) ? "nullptr" : chs;
        _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_DATA_NACK, eCode, reason);
        LOGF_ERROR("ProcessResponse[data] error(%1%,\"%2%\")" % eCode % reason);
        _jobDesc.respHandler()->errorFlush("Couldn't retrieve response data:" + reason, eCode);
        _errorFinish();
        return;
    }
    _jobQuery->getStatus()->updateInfo(JobStatus::RESPONSE_DATA);
    bool flushOk = _jobDesc.respHandler()->flush(blen, last);
    if(flushOk) {
        if (last) {
            auto sz = _jobDesc.respHandler()->nextBuffer().size();
            if (last && sz != 0) {
                LOGF_WARN("Connection closed when more information expected sz=%1%" % sz);
            }
            _jobQuery->getStatus()->updateInfo(JobStatus::COMPLETE);
            _finish();
        } else {
            std::vector<char>& buffer = _jobDesc.respHandler()->nextBuffer();
            const void* pbuf = (void*)(&buffer[0]);
            LOGF_INFO("_importStream->GetResponseData size=%1% %2% %3%" %
                      buffer.size() % pbuf % util::prettyCharList(buffer, 5));
            if(!GetResponseData(&buffer[0], buffer.size())) {
                _errorFinish();
                return;
            }
        }
    } else {
        LOGF_INFO("ProcessResponse data flush failed");
        ResponseHandler::Error err = _jobDesc.respHandler()->getError();
        _jobQuery->getStatus()->updateInfo(JobStatus::MERGE_ERROR, err.getCode(), err.getMsg());
        // @todo DM-2378 Take a closer look at what causes this error and take
        // appropriate action. There could be cases where this is recoverable.
        _retried.store(true); // Do not retry
        _errorFinish();
    }
}

void QueryRequest::cancel() {
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if(_cancelled) {
            return; // Don't do anything if already cancelled.
        }
        _cancelled = true;
        _retried.store(true); // Prevent retries.
    }
    _jobQuery->getStatus()->updateInfo(JobStatus::CANCEL);
    _errorFinish(true);
}

bool QueryRequest::isCancelled() {
    std::lock_guard<std::mutex> lock(_finishStatusMutex);
    return _cancelled;
}

void QueryRequest::cleanup() {
    _jobQuery.reset();
    _keepAlive.reset();
}

/// Finalize under error conditions and retry or report completion
/// This function will destroy this object.
void QueryRequest::_errorFinish(bool shouldCancel) {
    LOGF_DEBUG("Error finish");
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            return;
        }
        _finishStatus = ERROR;
        bool ok = Finished(shouldCancel);
        if(!ok) {
            LOGF_ERROR("Error cleaning up QueryRequest");
        } else {
            LOGF_INFO("Request::Finished() with error (clean).");
        }
    }
    // Make the calls outside of the mutex lock.
    if (!_retried.exchange(true)) {
        // There's a slight race condition here. _jobQuery::runJob() creates a
        // new QueryResource object which is used to create a new QueryRequest object
        // which will replace this one in _jobQuery. The replacement could show up
        // before this one's cleanup is called, so this will keep this alive.
        _keepAlive = _jobQuery->getQueryRequest(); // shared pointer to this
        if (_jobQuery->runJob()) {
            // Retry failed, nothing left to try.
            _callMarkComplete(false);
        }
    } else {
        _callMarkComplete(false);
    }
    cleanup(); // Reset smart pointers so this object can be deleted.
}

/// Finalize under success conditions and report completion.
void QueryRequest::_finish() {
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            return;
        }
        _finishStatus = FINISHED;
        bool ok = Finished();
        if(!ok) {
            LOGF_ERROR("Error with Finished()");
        } else {
            LOGF_INFO("Finished() ok.");
        }
    }

    _callMarkComplete(true);
    cleanup();
}

/// Inform the Executive that this query completed, and
// Call MarkCompleteFunc only once.
void QueryRequest::_callMarkComplete(bool success) {
    if (!_calledMarkComplete.exchange(true)) {
        _jobQuery->getMarkCompleteFunc()->operator ()(success);
    }
}

std::ostream& operator<<(std::ostream& os, QueryRequest const& r) {
    return os;
}

}}} // lsst::qserv::qdisp
