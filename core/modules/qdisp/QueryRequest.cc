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

inline void unprovisionSession(XrdSsiSession* session) {
    if(session) {
        bool ok = session->Unprovision();
        if(!ok) {
            LOGF_ERROR("Error unprovisioning");
        } else {
            LOGF_DEBUG("Unprovision ok.");
        }
    }
}

////////////////////////////////////////////////////////////////////////
// QueryRequest::Canceller
////////////////////////////////////////////////////////////////////////
class QueryRequest::Canceller : public util::VoidCallable<void> {
public:
    Canceller(QueryRequest* qr) : _queryRequest(qr) {}
    virtual ~Canceller() { delete _queryRequest; }
    virtual void operator()() {
        if (_queryRequest != NULL) {
            _queryRequest->cancel();
        }
    }
private:
    QueryRequest* _queryRequest;
};
////////////////////////////////////////////////////////////////////////
// QueryRequest
////////////////////////////////////////////////////////////////////////
QueryRequest::QueryRequest(
    XrdSsiSession* session,
    std::string const& payload,
    std::shared_ptr<ResponseRequester> const requester,
    std::shared_ptr<util::UnaryCallable<void, bool> > const finishFunc,
    std::shared_ptr<util::VoidCallable<void> > const retryFunc,
    JobStatus& status)
    : _session(session),
      _payload(payload),
      _requester(requester),
      _finishFunc(finishFunc),
      _retryFunc(retryFunc),
      _status(status),
      _finishStatus(QueryRequest::ACTIVE) {
    LOGF_INFO("New QueryRequest with payload(%1%)" % payload.size());
    _registerSelfDestruct();
}

QueryRequest::~QueryRequest() {
    unprovisionSession(_session);
}

// content of request data
char* QueryRequest::GetRequest(int& requestLength) {
    requestLength = _payload.size();
    LOGF_DEBUG("Requesting, payload size: [%1%]" % requestLength);
    // Andy promises that his code won't corrupt it.
    return const_cast<char*>(_payload.data());
}

void QueryRequest::RelRequestBuffer() {
    LOGF_DEBUG("Early release of request buffer");
    _payload.clear();
}
// precondition: rInfo.rType != isNone
// Must not throw exceptions: calling thread cannot trap them.
// Callback function for XrdSsiRequest.
bool QueryRequest::ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk) {
    std::string errorDesc;
    bool shouldStop = cancelled();
    if(shouldStop) {
        cancel(); // calls _errorFinish() which deletes this
        return true;
    }
    if(!isOk) {
        _requester->errorFlush(std::string("Request failed"), -1);
        _status.updateInfo(JobStatus::RESPONSE_ERROR);
        _errorFinish(); // deletes this
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
        _status.updateInfo(JobStatus::RESPONSE_ERROR, rInfo.eNum,
                       std::string(rInfo.eMsg));
        return _importError(std::string(rInfo.eMsg), rInfo.eNum);
    case XrdSsiRespInfo::isFile: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isFile";
        break;
    case XrdSsiRespInfo::isStream: // All remote requests
        _status.updateInfo(JobStatus::RESPONSE_READY);
        return _importStream();
    default:
        errorDesc += "Out of range XrdSsiRespInfo.rType";
    }
    return _importError(errorDesc, -1);
}

/// Retrieve and process results in using the XrdSsi stream mechanism
bool QueryRequest::_importStream() {
    bool retrieveInitiated = false;
    // Pass ResponseRequester's buffer directly.
    std::vector<char>& buffer = _requester->nextBuffer();
    LOGF_DEBUG("QueryRequest::_importStream buffer.size=%1%" % buffer.size());
    for(auto iter=buffer.begin(); iter != buffer.end(); iter++) *iter=8;
    std::ostringstream dbg;
    const void* pbuf = (void*)(&buffer[0]);
    dbg << "_importStream->GetResponseData size=" << buffer.size();
    dbg << ' ' << pbuf << ' ' <<  util::prettyCharList(buffer, 25);
    LOGF_INFO(dbg.str().c_str());
    retrieveInitiated = GetResponseData(&buffer[0], buffer.size());
    LOGF_INFO("Initiated request %1%" % (retrieveInitiated ? "ok" : "err"));
    if(!retrieveInitiated) {
        _status.updateInfo(JobStatus::RESPONSE_DATA_ERROR);
        if (Finished()) {
            _status.updateInfo(JobStatus::RESPONSE_DATA_ERROR_OK);
        } else {
            _status.updateInfo(JobStatus::RESPONSE_DATA_ERROR_CORRUPT);
        }
        if(_retryFunc) { // Retry.
            (*_retryFunc)();
        }
        _errorFinish();
        return false;
    } else {
        return true;
    }
}

/// Process an incoming error.
bool QueryRequest::_importError(std::string const& msg, int code) {
    _requester->errorFlush(msg, code);
    _errorFinish();
    return true;
}

void QueryRequest::ProcessResponseData(char *buff, int blen, bool last) { // Step 7
    LOGF_INFO("ProcessResponseData with buflen=%1% %2%" % blen % (last ? "(last)" : "(more)"));
    if(blen < 0) { // error, check errinfo object.
        int eCode;
        const char* chs = eInfo.Get(eCode);
        std::string reason = (chs == NULL) ? "Null" : chs;
        _status.updateInfo(JobStatus::RESPONSE_DATA_NACK, eCode, reason);
        LOGF_ERROR("ProcessResponse[data] error(%1%,\"%2%\")" % eCode % reason);
        _requester->errorFlush("Couldn't retrieve response data:" + reason, eCode);
        _errorFinish();
        return;
    }
    _status.updateInfo(JobStatus::RESPONSE_DATA);
    bool flushOk = _requester->flush(blen, last);
    if(flushOk) {
        if (last) {
            auto sz = _requester->nextBuffer().size();
            if (last && sz != 0) {
                LOGF_WARN("Connection closed when more information expected sz=%1%" % sz);
            }
            _status.updateInfo(JobStatus::COMPLETE);
            _finish();
        } else {
            std::vector<char>& buffer = _requester->nextBuffer();
            for(auto iter=buffer.begin(); iter != buffer.end(); iter++) *iter=9;
            std::ostringstream dbg;
            const void* pbuf = (void*)(&buffer[0]);
            dbg << "ProcessResponseData->GetResponseData size=" << buffer.size();
            dbg << ' '<< pbuf << ' ' <<  util::prettyCharList(buffer, 25);
            LOGF_INFO(dbg.str().c_str());
            if(!GetResponseData(&buffer[0], buffer.size())) {
                _errorFinish();
                return;
            }
        }
    } else {
        LOGF_INFO("ProcessResponse data flush failed");
        ResponseRequester::Error err = _requester->getError();
        _status.updateInfo(JobStatus::MERGE_ERROR, err.getCode(), err.getMsg());
        // @todo DM-2378 Take a closer look at what causes this error and take
        // appropriate action. There could be cases where this is recoverable.
        _retryFunc.reset();
        _errorFinish();
    }
}

void QueryRequest::cancel() {
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if(_finishStatus == CANCELLED) {
            return; // Don't do anything if already cancelled.
        }
        _finishStatus = CANCELLED;
        _retryFunc.reset(); // Prevent retries.
    }
    _status.updateInfo(JobStatus::CANCEL);
    _errorFinish(true);
}

bool QueryRequest::cancelled() {
    std::lock_guard<std::mutex> lock(_finishStatusMutex);
    return _finishStatus == CANCELLED;
}

void QueryRequest::cleanup() {
    _retryFunc.reset();
    _requester.reset();
}

/// Finalize under error conditions and retry or report completion
/// This function will destroy this object.
void QueryRequest::_errorFinish(bool shouldCancel) {
    LOGF_DEBUG("Error finish");
    std::shared_ptr<util::UnaryCallable<void, bool>> finish;
    std::shared_ptr<util::VoidCallable<void>> retry;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            return;
        }
        bool ok = Finished(shouldCancel);
        if(!ok) {
            LOGF_ERROR("Error cleaning up QueryRequest");
        } else {
            LOGF_INFO("Request::Finished() with error (clean).");
        }
        if(_retryFunc) { // Protect against multiple calls of retry or finish.
            retry.swap(_retryFunc);
        } else if(_finishFunc) {
            finish.swap(_finishFunc);
        }
        _finishStatus = ERROR;
    }
    if(retry) {
        (*retry)();
    } else if(finish) {
        (*finish)(false);
    }
    // canceller is responsible for deleting upon destruction
    cleanup(); // This causes the canceller to delete this.
}

/// Finalize under success conditions and report completion.
void QueryRequest::_finish() {
    std::shared_ptr<util::UnaryCallable<void, bool>> finish;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            return;
        }
        bool ok = Finished();
        if(!ok) {
            LOGF_ERROR("Error with Finished()");
        } else {
            LOGF_INFO("Finished() ok.");
        }
        finish.swap(_finishFunc);
        _finishStatus = FINISHED;
    }
    if(finish) {
        (*finish)(true);
    }
    // canceller is responsible for deleting upon destruction
    cleanup(); // This causes the canceller to delete this.
}

/// Register a cancellation function with the query receiver in order to receive
/// notifications upon errors detected in the receiver.
void QueryRequest::_registerSelfDestruct() {
    std::shared_ptr<Canceller> canceller(new Canceller(this));
    _requester->registerCancel(canceller);
}

std::ostream& operator<<(std::ostream& os, QueryRequest const& r) {
    return os;
}

}}} // lsst::qserv::qdisp
