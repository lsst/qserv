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
#ifndef LSST_QSERV_XRDSVC_SSISESSION_REPLYCHANNEL_H
#define LSST_QSERV_XRDSVC_SSISESSION_REPLYCHANNEL_H

// System headers
//#include <deque>

// Third-party headers
//#include <boost/thread/condition_variable.hpp>
//#include <boost/thread/mutex.hpp>
//#include "XrdSsi/XrdSsiErrInfo.hh" // required by XrdSsiStream 
//#include "XrdSsi/XrdSsiStream.hh"
#include "XrdSsi/XrdSsiResponder.hh"

// Qserv headers
//#include "wlog/WLogger.h"
#include "wbase/SendChannel.h"
#include "xrdsvc/SsiSession.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

class ChannelStream; // Forward declaration

/// ReplyChannel is a SendChannel implementation that adapts XrdSsiSession
/// objects as backend data acceptors. ReplyChannel channel instances are
/// tightly coupled to SsiSession instances, and make use of protected fields in
/// XrdSsiResponder (which SsiSession inherits from).
class SsiSession::ReplyChannel : public wbase::SendChannel {
public:
    typedef boost::shared_ptr<ReplyChannel> Ptr;

    ReplyChannel(SsiSession& s)
        : _ssiSession(s), _stream(0) {}

    virtual bool send(char const* buf, int bufLen);
    virtual bool sendError(std::string const& msg, int code);
    virtual bool sendFile(int fd, Size fSize);
    virtual bool sendStream(char const* buf, int bufLen, bool last);

private:    
    void _initStream();

    SsiSession& _ssiSession;
    ChannelStream* _stream;
};

}}} // namespace lsst::qserv::xrdsvc

#endif // LSST_QSERV_XRDSVC_SSISESSION_REPLYCHANNEL_H
