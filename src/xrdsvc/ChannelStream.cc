// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 LSST Corporation.
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

// Class header
#include "xrdsvc/ChannelStream.h"

// Third-party headers
#include "boost/utility.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/debugUtil.h"
#include "util/Bug.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.ChannelStream");
}

using namespace std;

namespace lsst::qserv::xrdsvc {

/// Provide each Channel stream with a unique identifier.
atomic<uint64_t> ChannelStream::_sequenceSource{0};

/// Constructor
ChannelStream::ChannelStream() : XrdSsiStream(isActive), _closed(false), _seq(_sequenceSource++) {}

/// Destructor
ChannelStream::~ChannelStream() { clearMsgs(); }

/// Push in a data packet
void ChannelStream::append(StreamBuffer::Ptr const &streamBuffer, bool last, int scsSeq) {
    if (_closed) {
        throw util::Bug(ERR_LOC,
                        "ChannelStream::append: Stream closed, append(...,last=true) already received");
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "seq=" << _seq << " scsseq=" << scsSeq << " ChannelStream::append last=" << last << " "
                << util::prettyCharBuf(streamBuffer->data, streamBuffer->getSize(), 5));
    {
        unique_lock<mutex> lock(_mutex);
        ++_appendCount;
        LOGS(_log, LOG_LVL_DEBUG,
             "seq=" << to_string(_seq) << " scsseq=" << scsSeq << " Trying to append message (flowing) appC="
                    << _appendCount << " getBC=" << _getBufCount);
        _msgs.push_back(streamBuffer);
        _closed = last;  // if last is true, then we are closed.
    }
    _hasDataCondition.notify_one();
}

/// Pull out a data packet as a Buffer object (called by XrdSsi code)
XrdSsiStream::Buffer *ChannelStream::GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) {
    ++_getBufCount;
    // This InstanceCount should be fairly quiet as there should only be one at a time.
    util::InstanceCount inst("GetBuf seq=" + to_string(_seq));
    unique_lock<mutex> lock(_mutex);
    while (_msgs.empty() && !_closed) {  // No msgs, but we aren't done
        // wait.
        LOGS(_log, LOG_LVL_INFO, "seq=" << _seq << " Waiting, no data ready ");
        _hasDataCondition.wait(lock);
    }
    if (_msgs.empty() && _closed) {
        // It's closed and no more msgs are available.
        LOGS(_log, LOG_LVL_INFO, "seq=" << _seq << " Not waiting, but closed");
        dlen = 0;
        eInfo.Set("Not an active stream", EOPNOTSUPP);
        return 0;
    }

    StreamBuffer::Ptr sb = _msgs.front();
    dlen = sb->getSize();
    _msgs.pop_front();
    last = _closed && _msgs.empty();
    LOGS(_log, LOG_LVL_INFO,
         "seq=" << to_string(_seq) << " returning buffer (" << dlen << ", " << (last ? "(last)" : "(more)")
                << ")"
                << " getBufCount=" << _getBufCount);
    return sb.get();
}

void ChannelStream::clearMsgs() {
    LOGS(_log, LOG_LVL_DEBUG, "seq=" << to_string(_seq) << " ChannelStream::clearMsgs()");
    unique_lock<mutex> lock(_mutex);
    while (!_msgs.empty()) {
        _msgs.front()->Recycle();
        _msgs.pop_front();
    }
}

}  // namespace lsst::qserv::xrdsvc
