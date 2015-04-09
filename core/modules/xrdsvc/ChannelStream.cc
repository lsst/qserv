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
#include "xrdsvc/ChannelStream.h"

// Third-party headers
#include "boost/thread/locks.hpp"
#include "boost/utility.hpp"
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
#include "global/debugUtil.h"
#include "util/common.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

/// SimpleBuffer is a really simple buffer for transferring data packets to
/// XrdSsi
class SimpleBuffer : public XrdSsiStream::Buffer, boost::noncopyable {
public:
    SimpleBuffer(std::string const& input) {
        data = new char[input.size()];
        memcpy(data, input.data(), input.size());
        next = 0;
    }

    //!> Call to recycle the buffer when finished
    virtual void Recycle() {
        delete this; // Self-destruct. FIXME: Not sure this is right.
    }

    // Inherited from XrdSsiStream:
    // char  *data; //!> -> Buffer containing the data
    // Buffer *next; //!> For chaining by buffer receiver

    virtual ~SimpleBuffer() {
        delete[] data;
    }
};

////////////////////////////////////////////////////////////////////////
// ChannelStream implementation
////////////////////////////////////////////////////////////////////////

/// Constructor
ChannelStream::ChannelStream()
    : XrdSsiStream(isActive),
      _closed(false) {}

/// Destructor
ChannelStream::~ChannelStream() {
#if 0 // Enable to debug ChannelStream lifetime
    try {
        LOGF_INFO("Stream (%1%) deleted" % (void*)this);
    } catch (...) {} // Destructors have nowhere to throw exceptions
#endif
}

/// Push in a data packet
void
ChannelStream::append(char const* buf, int bufLen, bool last) {
    if(_closed) {
        throw Bug("ChannelStream::append: Stream closed, append(...,last=true) already received");
    }
    //LOGF_INFO("last=%1% %2%" % last % makeByteStreamAnnotated("StreamMsg", buf, bufLen));
    LOGF_INFO("last=%1% %2%" % last % util::prettyCharBuf(buf, bufLen, 10));
    {
        boost::unique_lock<boost::mutex> lock(_mutex);
        LOG_INFO(" trying to append message (flowing)");

        _msgs.push_back(std::string(buf, bufLen));
        _closed = last; // if last is true, then we are closed.
        _hasDataCondition.notify_one();
    }
}

/// Pull out a data packet as a Buffer object (called by XrdSsi code)
XrdSsiStream::Buffer*
ChannelStream::GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) {
    boost::unique_lock<boost::mutex> lock(_mutex);
    while(_msgs.empty() && !_closed) { // No msgs, but we aren't done
        // wait.
        LOG_INFO("Waiting, no data ready");
        _hasDataCondition.wait(lock);
    }
    if(_msgs.empty() && _closed) { // We are closed and no more
        // msgs are available.
        LOG_INFO("Not waiting, but closed");
        dlen = 0;
        eInfo.Set("Not an active stream", EOPNOTSUPP);
        return 0;
    }
    SimpleBuffer* sb = new SimpleBuffer(_msgs.front());
    dlen = _msgs.front().size();
    _msgs.pop_front();
    last = _closed && _msgs.empty();
    LOGF_INFO("returning buffer (%1%, %2%)" % dlen % (last ? "(last)" : "(more)"));
    return sb;
}

}}} // lsst::qserv::xrdsvc
