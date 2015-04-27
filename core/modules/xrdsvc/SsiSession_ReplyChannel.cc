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

// Third-party headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/Timer.h"
#include "xrdsvc/ChannelStream.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

bool
SsiSession::ReplyChannel::send(char const* buf, int bufLen) {
    Status s = _ssiSession.SetResponse(buf, bufLen);
    if(s != XrdSsiResponder::wasPosted) {
        LOGF_ERROR("DANGER: Couldn't post response of length=%1%" % bufLen);
        return false;
    }
    return true;
}

bool
SsiSession::ReplyChannel::sendError(std::string const& msg, int code) {
    Status s = _ssiSession.SetErrResponse(msg.c_str(), code);
    if(s != XrdSsiResponder::wasPosted) {
        LOGF_ERROR("DANGER: Couldn't post error response %1%" % msg);
        return false;
    }
    return true;
}

bool
SsiSession::ReplyChannel::sendFile(int fd, Size fSize) {
    util::Timer t;
    t.start();
    Status s = _ssiSession.SetResponse(fSize, fd);
    if(s == XrdSsiResponder::wasPosted) {
        LOG_INFO("file posted ok");
    } else {
        if(s == XrdSsiResponder::notActive) {
            LOGF_ERROR("DANGER: Couldn't post response file of length=%1%"
               " responder not active." % fSize);
        } else {
            LOGF_ERROR("DANGER: Couldn't post response file of length=%1%" % fSize);
        }
        release();
        sendError("Internal error posting response file", 1);
        return false; // sendError handles everything else.
    }
    t.stop();
    LOGF_INFO("sendFile took %1% seconds" % t.getElapsed());
    return true;
}

bool
SsiSession::ReplyChannel::sendStream(char const* buf, int bufLen, bool last) {
    // Initialize streaming object if not initialized.
    LOGF_INFO("sendStream, checking stream %1% len=%2% last=%3%" %
            (void*) _stream % bufLen % last);
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
    _stream = new ChannelStream();
    _ssiSession.SetResponse(_stream);
}

}}} // lsst::qserv::xrdsvc
