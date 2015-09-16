// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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
#ifndef LSST_QSERV_QDISP_QUERYREQUEST_H
#define LSST_QSERV_QDISP_QUERYREQUEST_H

// System headers
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>


// Third-party headers
#include "XrdSsi/XrdSsiRequest.hh"

// Local headers
#include "qdisp/Executive.h"
#include "qdisp/JobQuery.h"
#include "util/Callable.h"
#include "util/threadSafe.h"

namespace lsst {
namespace qserv {
namespace qdisp {

/// Bad response received from xrootd API
class BadResponseError : public std::exception {
public:
    BadResponseError(std::string const& s_)
        : std::exception(),
          s("BadResponseError:" + s_) {}
    virtual ~BadResponseError() throw() {}
    virtual const char* what() throw() {
        return s.c_str();
    }
    std::string s;
};

/// Error in QueryRequest
class RequestError : public std::exception {
public:
    RequestError(std::string const& s_)
        : std::exception(),
          s("QueryRequest error:" + s_) {}
    virtual ~RequestError() throw() {}
    virtual char const* what() throw() {
        return s.c_str();
    }
    std::string s;
};

/// A client implementation of an XrdSsiRequest that adapts qserv's executing
/// queries to the XrdSsi API.
///
/// Memory allocation notes:
/// In the XrdSsi API, raw pointers are passed around for XrdSsiRequest objects,
/// and care needs to be taken to avoid deleting the request objects before
/// Finished() is called. Typically, an XrdSsiRequest subclass is allocated with
/// operator new, and passed into XrdSsi. At certain points in the transaction,
/// XrdSsi will call methods in the request object or hand back the request
/// object pointer. XrdSsi ceases interest in the object once the
/// XrdSsiRequest::Finished() completes. Generally, this would mean the
/// QueryRequest should clean itself up after calling Finished(). This requires
/// special care, because there is a cancellation function in the wild that may
/// call into QueryRequest after Finished() has been called. The cancellation
/// code is
/// designed to allow the client requester (elsewhere in qserv) to request
/// cancellation without knowledge of XrdSsi, so the QueryRequest registers a
/// cancellation function with its client that maintains a pointer to the
/// QueryRequest. After Finished(), the cancellation function must be prevented
/// from accessing the QueryRequest instance.
class QueryRequest : public XrdSsiRequest {
public:
    typedef std::shared_ptr<QueryRequest> Ptr;
    QueryRequest(XrdSsiSession* session, std::shared_ptr<JobQuery> const& jobQuery);

    virtual ~QueryRequest();

    /// Called by xrootd to get the request payload
    /// @return content of request data
    virtual char* GetRequest(int& requestLength);

    /// Called by xrootd to release the allocated request payload
    virtual void RelRequestBuffer();

    /// Called by xrootd when a response is ready
    /// precondition: rInfo.rType != isNone
    virtual bool ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk);

    /// Called by xrootd when new data is available.
    virtual void ProcessResponseData(char *buff, int blen, bool last);

    void cancel();
    bool getCancelled();

    void cleanup(); ///< Must be called when this object is no longer needed.

private:
    void _callMarkComplete(bool success);
    bool _importStream();
    bool _importError(std::string const& msg, int code);
    void _errorFinish(bool shouldCancel=false);
    void _finish();

    XrdSsiSession* _session;

    std::shared_ptr<JobQuery> _jobQuery; ///< Job information.
    JobDescription& _jobDesc; ///< Convenience reference to JobDescription inside _jobQuery.

    util::Flag<bool> _retried {false}; ///< Protect against multiple retries of _jobQuery from a single QueryRequest.
    util::Flag<bool> _calledMarkComplete {false}; ///< Protect against multiple calls to MarkCompleteFunc
                                          /// from a single QueryRequest.

    std::mutex _finishStatusMutex;
    enum FinishStatus { ACTIVE, FINISHED, ERROR } _finishStatus {ACTIVE}; // _finishStatusMutex
    bool _cancelled {false}; ///< true if cancelled, protected by _finishStatusMutex.

    std::shared_ptr<QueryRequest> _keepAlive; ///< Used to keep this object alive during race condition.
};

std::ostream& operator<<(std::ostream& os, QueryRequest const& r);

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_QUERYREQUEST_H
