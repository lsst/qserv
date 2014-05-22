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
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include "XrdSsi/XrdSsiRequest.hh"

// Local headers
//#include "ccontrol/transaction.h"
#include "util/Callable.h"
//#include "xrdc/xrdfile.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace ccontrol {
//    class AsyncQueryManager;
}
namespace xrdc {
//    class PacketIter;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qdisp {
class ExecStatus;
class QueryReceiver;

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

class QueryRequest : public XrdSsiRequest {
public:
    QueryRequest(XrdSsiSession* session,
                 std::string const& payload,
                 boost::shared_ptr<QueryReceiver> receiver,
                 boost::shared_ptr<util::VoidCallable<void> > retryFunc,
                 ExecStatus& status);

    virtual ~QueryRequest();

    // content of request data
    virtual char* GetRequest(int& requestLength);

    virtual void RelRequestBuffer();

    // precondition: rInfo.rType != isNone
    virtual bool ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk);

    virtual void ProcessResponseData(char *buff, int blen, bool last);

private:
    bool _importStream();
    bool _importError(std::string const& msg, int code);
    void _errorFinish();
    void _finish();
    void _registerSelfDestruct();
    void _resetBuffer();

    XrdSsiSession* _session;

    char* _buffer;
    char* _cursor;
    int _bufferSize;
    int _bufferRemain;
    std::string _payload;
    boost::shared_ptr<QueryReceiver> _receiver;
    boost::shared_ptr<util::VoidCallable<void> > _retryFunc;
    ExecStatus& _status;
    std::string _errorDesc;
    class Canceller;
    friend class Canceller;
}; // class QueryRequest

std::ostream& operator<<(std::ostream& os, QueryRequest const& r);

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_QUERYREQUEST_H
