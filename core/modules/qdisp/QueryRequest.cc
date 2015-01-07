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
#include "qdisp/ExecStatus.h"
#include "qdisp/ResponseRequester.h"

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
    Canceller(QueryRequest& qr) : _queryRequest(qr) {}
    virtual void operator()() {
        _queryRequest.Finished(true); // Abort using XrdSsiRequest interface
    }
private:
    QueryRequest& _queryRequest;
};
////////////////////////////////////////////////////////////////////////
// QueryRequest
////////////////////////////////////////////////////////////////////////
QueryRequest::QueryRequest(
    XrdSsiSession* session,
    std::string const& payload,
    boost::shared_ptr<ResponseRequester> const requester,
    boost::shared_ptr<util::UnaryCallable<void, bool> > const finishFunc,
    boost::shared_ptr<util::VoidCallable<void> > const retryFunc,
    ExecStatus& status)
    : _session(session),
      _payload(payload),
      _requester(requester),
      _finishFunc(finishFunc),
      _retryFunc(retryFunc),
      _status(status) {
    _registerSelfDestruct();
    LOGF_INFO("New QueryRequest with payload(%1%)" % payload.size());
}

QueryRequest::~QueryRequest() {
    unprovisionSession(_session);
}

// content of request data
char* QueryRequest::GetRequest(int& requestLength) {
    requestLength = _payload.size();
    LOGF_DEBUG("Requesting [%1%] %2%" % requestLength % _payload);
    // Andy promises that his code won't corrupt it.
    return const_cast<char*>(_payload.data());
}

void QueryRequest::RelRequestBuffer() {
    LOGF_DEBUG("Early release of request buffer");
    _payload.clear();
}
// precondition: rInfo.rType != isNone
// Must not throw exceptions: calling thread cannot trap them.
bool QueryRequest::ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk) {
    std::string errorDesc;
    if(!isOk) {
        _requester->errorFlush(std::string("Request failed"), -1);
        _errorFinish();
        _status.report(ExecStatus::RESPONSE_ERROR);
        return true;
    }
    //LOGF_DEBUG("Response type is %1%" % rInfo.State());
    switch(rInfo.rType) {
    case XrdSsiRespInfo::isNone: // All responses are non-null right now
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isNone";
        break;
    case XrdSsiRespInfo::isData: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isData";
        break;
    case XrdSsiRespInfo::isError: // isOk == true
        //errorDesc += "isOk == true, but XrdSsiRespInfo.rType == isError";
        _status.report(ExecStatus::RESPONSE_ERROR, rInfo.eNum,
                       std::string(rInfo.eMsg));
        return _importError(std::string(rInfo.eMsg), rInfo.eNum);
    case XrdSsiRespInfo::isFile: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isFile";
        break;
    case XrdSsiRespInfo::isStream: // All remote requests
        _status.report(ExecStatus::RESPONSE_READY);
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
    LOGF_INFO("GetResponseData with buffer of %1%" % _bufferRemain);
    retrieveInitiated = GetResponseData(&buffer[0], buffer.size());
    LOGF_INFO("Initiated request %1%" % (retrieveInitiated ? "ok" : "err"));
    if(!retrieveInitiated) {
        _status.report(ExecStatus::RESPONSE_DATA_ERROR);
        bool ok = Finished();
        if(_retryFunc) { // Retry.
            (*_retryFunc)();
        }
        // delete this; // Don't delete! need to stay alive for error.
        // Not sure when to delete.
        if(ok) {
            _errorDesc += "Couldn't initiate result retr (clean)";
        } else {
            _errorDesc += "Couldn't initiate result retr (UNCLEAN)";
        }
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
    LOGF_INFO("ProcessResponse[data] with buflen=%1% %2%" %
              blen % (last ? "(last)" : "(more)"));
    if(blen < 0) { // error, check errinfo object.
        int eCode;
        std::string reason(eInfo.Get(eCode));
        _status.report(ExecStatus::RESPONSE_DATA_NACK, eCode, reason);
        LOGF_ERROR("ProcessResponse[data] error(%1%,\"%2%\")" % eCode % reason);
        _requester->errorFlush("Couldn't retrieve response data:" + reason, eCode);
        _errorFinish();
        return;
    }
    _status.report(ExecStatus::RESPONSE_DATA);
    bool flushOk = _requester->flush(blen, last);
    if(flushOk) {
        if(last) {
            _status.report(ExecStatus::COMPLETE);
            _finish();
        } else {
            std::vector<char>& buffer = _requester->nextBuffer();
            if(!GetResponseData(&buffer[0], buffer.size())) {
                _errorFinish();
            }
        }
    } else {
        ResponseRequester::Error err = _requester->getError();
        _status.report(ExecStatus::MERGE_ERROR, err.code, err.msg);
    }
}

/// Finalize under error conditions and retry or report completion
void QueryRequest::_errorFinish() {
    LOGF_DEBUG("Error finish");
    bool ok = Finished();
    if(!ok) {
        LOGF_ERROR("Error cleaning up QueryRequest");
    } else {
        LOGF_INFO("Request::Finished() with error (clean).");
    }
    if(_retryFunc) {
        (*_retryFunc)();
    } else if(_finishFunc) {
        (*_finishFunc)(false);
    }

    delete this; // Self-cleanup is expected.
}

/// Finalize under success conditions and report completion.
void QueryRequest::_finish() {
    bool ok = Finished();
    if(!ok) {
        LOGF_ERROR("Error with Finished()");
    } else {
        LOGF_INFO("Finished() ok.");
    }
    if(_finishFunc) {
        (*_finishFunc)(true);
    }
    delete this; // Self-cleanup is expected.
}

/// Register a cancellation function with the query receiver in order to receive
/// notifications upon errors detected in the receiver.
void QueryRequest::_registerSelfDestruct() {
    boost::shared_ptr<Canceller> canceller(new Canceller(*this));
    _requester->registerCancel(canceller);
}

std::ostream& operator<<(std::ostream& os, QueryRequest const& r) {
    return os;
}

}}} // lsst::qserv::qdisp
