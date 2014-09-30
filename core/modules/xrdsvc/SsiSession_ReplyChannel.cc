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
#include "xrdsvc/SsiSession_ReplyChannel.h"

// Qserv headers
#include "util/Timer.h"
#include "wlog/WLogger.h"
#include "xrdsvc/ChannelStream.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

bool
SsiSession::ReplyChannel::send(char const* buf, int bufLen) {
    Status s = _ssiSession.SetResponse(buf, bufLen);
    if(s != XrdSsiResponder::wasPosted) {
        std::ostringstream os;
        os << "DANGER: Couldn't post response of length="
           << bufLen << std::endl;
        _ssiSession._log->error(os.str());
        return false;
    }
    return true;
}

bool
SsiSession::ReplyChannel::sendError(std::string const& msg, int code) {
    Status s = _ssiSession.SetErrResponse(msg.c_str(), code);
    if(s != XrdSsiResponder::wasPosted) {
        std::ostringstream os;
        os << "DANGER: Couldn't post error response " << msg
           << std::endl;
        _ssiSession._log->error(os.str());
        return false;
    }
    return true;
}

bool
SsiSession::ReplyChannel::sendFile(int fd, Size fSize) {
    util::Timer t;
    t.start();
    Status s = _ssiSession.SetResponse(fSize, fd);
    std::ostringstream os;
    if(s == XrdSsiResponder::wasPosted) {
        os << "file posted ok";
    } else {
        if(s == XrdSsiResponder::notActive) {
            os << "DANGER: Couldn't post response file of length="
               << fSize << " responder not active.\n";
        } else {
            os << "DANGER: Couldn't post response file of length="
               << fSize << std::endl;
        }
        release();
        sendError("Internal error posting response file", 1);
        return false; // sendError handles everything else.
    }
    _ssiSession._log->error(os.str());
    t.stop();
    os.str("");
    os << "sendFile took " << t.getElapsed() << " seconds";
    _ssiSession._log->info(os.str());
    return true;
}

bool
SsiSession::ReplyChannel::sendStream(char const* buf, int bufLen, bool last) {
    // Initialize streaming object if not initialized.
    std::ostringstream os;
    os << "sendStream, checking stream " << (void*) _stream << ")";
    _ssiSession._log->info(os.str());
    if(!_stream) {
        _initStream();
    } else if(_stream->closed()) {
        return false;
    }
    _stream->append(buf, bufLen, last);
    return true;
}

void
SsiSession::ReplyChannel::_initStream() {
    //_stream.reset(new Stream);
    _stream = new ChannelStream(_ssiSession._log);
    _ssiSession.SetResponse(_stream);
}

}}} // lsst::qserv::xrdsvc
