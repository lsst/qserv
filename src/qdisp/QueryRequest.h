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
#include "czar/Czar.h"
#include "qdisp/JobQuery.h"
#include "qdisp/QdispPool.h"

namespace lsst::qserv::qdisp {

/// Bad response received from SSI API
class BadResponseError : public std::exception {
public:
    BadResponseError(std::string const& s_) : std::exception(), s("BadResponseError:" + s_) {}
    virtual ~BadResponseError() throw() {}
    virtual const char* what() const throw() { return s.c_str(); }
    std::string s;
};

/// Error in QueryRequest
class RequestError : public std::exception {
public:
    RequestError(std::string const& s_) : std::exception(), s("QueryRequest error:" + s_) {}
    virtual ~RequestError() throw() {}
    virtual const char* what() const throw() { return s.c_str(); }
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
class QueryRequest : public XrdSsiRequest, public std::enable_shared_from_this<QueryRequest> {
public:
    typedef std::shared_ptr<QueryRequest> Ptr;

    /* &&&
    static Ptr create(std::shared_ptr<JobQuery> const& jobQuery) {
        Ptr newQueryRequest(new QueryRequest(jobQuery));
        newQueryRequest->_keepAlive = newQueryRequest;
        return newQueryRequest;
    }
    */
    static Ptr create(std::shared_ptr<JobBase> const& jobBase) {
        Ptr newQueryRequest(new QueryRequest(jobBase));
        return newQueryRequest;
    }

    virtual ~QueryRequest();

    /// Called by SSI to get the request payload
    /// @return content of request data
    char* GetRequest(int& requestLength) override;

    /// Called by SSI to release the allocated request payload. As we don't
    /// own the buffer, so we can't release it. Therefore, we accept the
    /// default implementation that does nothing.
    /// void RelRequestBuffer() override;

    /// Called by SSI when a response is ready
    /// precondition: rInfo.rType != isNone
    bool ProcessResponse(XrdSsiErrInfo const& eInfo, XrdSsiRespInfo const& rInfo) override;

    /// Called by SSI when new data is available.
    void ProcessResponseData(XrdSsiErrInfo const& eInfo, char* buff, int blen, bool last) override;

    bool cancel();
    bool isQueryCancelled();
    bool isQueryRequestCancelled();
    void doNotRetry() { _retried.store(true); }
    std::string getSsiErr(XrdSsiErrInfo const& eInfo, int* eCode);
    void cleanup();  ///< Must be called when this object is no longer needed.

    friend std::ostream& operator<<(std::ostream& os, QueryRequest const& r);

private:
    // Private constructor to safeguard enable_shared_from_this construction.
    //&&&QueryRequest(std::shared_ptr<JobQuery> const& jobQuery);
    QueryRequest(JobBase::Ptr const& job);

    /// Inform the Executive that this query completed, and call MarkCompleteFunc only once.
    /// This should only be called from _finish() or _errorFinish.
    void _callMarkComplete(bool success);
    //&&&bool _importResultFile(JobQuery::Ptr const& jq);
    bool _importResultFile(JobBase::Ptr const& jq);
    bool _importError(std::string const& msg, int code);
    bool _errorFinish(bool stopTrying = false);
    void _finish();
    //&&&void _flushError(JobQuery::Ptr const& jq);
    void _flushError(JobBase::Ptr const& jq);

    /* &&&
    /// Job information. Not using a weak_ptr as Executive could drop its JobQuery::Ptr before we're done with
    /// it. A call to cancel() could reset _jobQuery early, so copy or protect _jobQuery with
    /// _finishStatusMutex as needed. If (_finishStatus == ACTIVE) _jobQuery should be good.
    std::shared_ptr<JobQuery> _jobQuery;
    */
    /// Job information. Not using a weak_ptr as Executive could drop its JobBase::Ptr before we're done with
    /// it. A call to cancel() could reset _job early, so copy or protect _job with _finishStatusMutex as
    /// needed. If (_finishStatus == ACTIVE) _job should be good.
    std::shared_ptr<JobBase> _job;

    std::atomic<bool> _retried{false};             ///< Protect against multiple retries of _jobQuery from a
                                                   /// single QueryRequest.
    std::atomic<bool> _calledMarkComplete{false};  ///< Protect against multiple calls to MarkCompleteFunc
                                                   /// from a single QueryRequest.

    std::mutex _finishStatusMutex;  ///< used to protect _cancelled, _finishStatus, and _jobQuery.
    enum FinishStatus { ACTIVE, FINISHED, ERROR } _finishStatus{ACTIVE};  // _finishStatusMutex
    bool _cancelled{false};  ///< true if cancelled, protected by _finishStatusMutex.

    std::shared_ptr<QueryRequest> _keepAlive;  ///< Used to keep this object alive during race condition.
    QueryId _qid = 0;                          // for logging
    int _jobid = -1;                           // for logging
    std::string _jobIdStr{QueryIdHelper::makeIdStr(0, 0, true)};  ///< for debugging only.

    std::atomic<bool> _finishedCalled{false};

    QdispPool::Ptr _qdispPool;

    int64_t _totalRows = 0;  ///< number of rows in query added to the result table.

    std::atomic<int> _rowsIgnored{0};  ///< Limit log messages about rows being ignored.
    std::atomic<uint> _respCount{0};   ///< number of responses created
};

std::ostream& operator<<(std::ostream& os, QueryRequest const& r);

}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_QDISP_QUERYREQUEST_H
