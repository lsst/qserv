// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

// Third-party headers
#include <exception>
#include <string>
#include "boost/shared_ptr.hpp"
#include "boost/make_shared.hpp"
#include "XrdSsi/XrdSsiRequest.hh"

// Local headers
#include "util/Callable.h"

namespace lsst {
namespace qserv {
namespace qdisp {
class ExecStatus;
class QueryReceiver;

/// Bad response received from xrootd API
class BadResponseError : public std::exception {
public:
    BadResponseError(std::string const& s_)
        : std::exception(),
          s("BadResponseError:" + s_) {}
    ~BadResponseError() throw() {}
    virtual char const* what() throw() {
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

const int QueryRequest_receiveBufferSize = 1024*1024; // 1MB receive buffer

/// A client implementation of an XrdSsiRequest that adapts qserv's executing
/// queries to the XrdSsi API.
class QueryRequest : public XrdSsiRequest {
public:
    QueryRequest(XrdSsiSession* session,
                 std::string const& payload,
                 boost::shared_ptr<QueryReceiver> receiver,
                 boost::shared_ptr<util::UnaryCallable<void, bool> > finishFunc,
                 boost::shared_ptr<util::VoidCallable<void> > retryFunc,
                 ExecStatus& status);

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

private:
    bool _importStream();
    bool _importError(std::string const& msg, int code);
    void _errorFinish();
    void _finish();
    void _registerSelfDestruct();
    void _resetBuffer();

    XrdSsiSession* _session;

    char* _buffer; ///< Response buffer
    char* _cursor; ///< Response buffer cursor
    int _bufferSize; ///< Response buffer size
    int _bufferRemain; ///< Remaining size (_cursor to end)
    std::string _payload; ///< Request buffer
    boost::shared_ptr<QueryReceiver> _receiver; ///< Response receiver

    /// To be called when the request completes
    boost::shared_ptr<util::UnaryCallable<void, bool> > _finishFunc;
    /// To be called to retry a failed request
    boost::shared_ptr<util::VoidCallable<void> > _retryFunc;
    /// Reference to an updatable Status
    ExecStatus& _status;
    std::string _errorDesc; ///< Error description
    class Canceller;
    friend class Canceller;
}; // class QueryRequest

std::ostream& operator<<(std::ostream& os, QueryRequest const& r);

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_QUERYREQUEST_H
