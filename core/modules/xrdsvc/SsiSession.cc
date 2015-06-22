// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#include "xrdsvc/SsiSession.h"

// System headers
#include <cctype>
#include <cstddef>
#include <iostream>
#include <string>

// Third-party headers
#include "XrdSsi/XrdSsiRequest.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/worker.pb.h"
#include "util/Timer.h"
#include "wbase/MsgProcessor.h"
#include "wbase/SendChannel.h"
#include "xrdsvc/SsiSession_ReplyChannel.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

// Step 4
/// Called by XrdSsi to actually process a request.
void
SsiSession::ProcessRequest(XrdSsiRequest* req, unsigned short timeout) {
    util::Timer t;

    LOGF_INFO("ProcessRequest, service=%1%" % sessName);

    t.start();
    BindRequest(req, this); // Step 5
    t.stop();
    LOGF_INFO("BindRequest took %1% seconds" % t.getElapsed());

    char *reqData = nullptr;
    int reqSize;
    t.start();
    reqData = req->GetRequest(reqSize);
    t.stop();
    LOGF_INFO("GetRequest took %1% seconds" % t.getElapsed());

    auto replyChannel = std::make_shared<ReplyChannel>(*this);

    ResourceUnit ru(sessName);
    if (ru.unitType() != ResourceUnit::DBCHUNK) {
        std::ostringstream os;
        os << "Unexpected unit type in query db=" << ru.db() << " unitType=" << ru.unitType();
        LOGF_ERROR(os.str());
        replyChannel->sendError(os.str().c_str(), EINVAL);
        ReleaseRequestBuffer();
        return;
    }

    if(!(*_validator)(ru)) {
        LOGF_WARN("WARNING: unowned chunk query detected: %1%" % ru.path());
        ReleaseRequestBuffer();
        return;
    }

    // reqData has the entire request, so we can unpack it without waiting for
    // more data.
    LOGF_INFO("Decoding TaskMsg of size %1%" % reqSize);
    auto task = std::make_shared<proto::TaskMsg>();
    bool ok = task->ParseFromArray(reqData, reqSize) && task->IsInitialized();

    // Now that the request is decoded (successfully or not), release the
    // xrootd request buffer. To avoid data races, this must happen before
    // the task is handed off to another thread for processing, as there is a
    // reference to this SsiSession inside the reply channel for the task.
    ReleaseRequestBuffer();

    if (!ok) {
        std::ostringstream os;
        os << "Failed to decode TaskMsg on resource db=" << ru.db() << " chunkId=" << ru.chunk();
        LOGF_ERROR(os.str());
        replyChannel->sendError(os.str().c_str(), EINVAL);
        return;
    }

    if(!task->has_db() || !task->has_chunkid() || (ru.db() != task->db()) || (ru.chunk() != task->chunkid())) {
        std::ostringstream os;
        os << "Mismatched db/chunk in TaskMsg on resource db=" << ru.db() << " chunkId=" << ru.chunk();
        LOGF_ERROR(os.str());
        replyChannel->sendError(os.str().c_str(), EINVAL);
        return;
    }

    t.start();
    SsiSession::CancelFuncPtr p = (*_processor)(task, replyChannel);
    _addCanceller(p);
    t.stop();
    LOGF_INFO("Enqueued TaskMsg for %1% in %2% seconds" % ru % t.getElapsed());
}

/// Called by XrdSsi to free resources.
void
SsiSession::RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                            bool cancel) { // Step 8
    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)
    {
        std::lock_guard<std::mutex> lock(_cancelMutex);
        if(!_cancelled && cancel) { // Cancel if not already cancelled
            _cancelled = true;
            typedef std::vector<CancelFuncPtr>::iterator Iter;
            for(Iter i=_cancellers.begin(), e=_cancellers.end(); i != e; ++i) {
                assert(*i);
                (**i)();
            }
        }
    }
    // No buffers allocated, so don't need to free.
    // We can release/unlink the file now
    const char* type = "";
    switch(rinfo.rType) {
    case XrdSsiRespInfo::isNone: type = "type=isNone"; break;
    case XrdSsiRespInfo::isData: type = "type=isData"; break;
    case XrdSsiRespInfo::isError: type = "type=isError"; break;
    case XrdSsiRespInfo::isFile: type = "type=isFile"; break;
    case XrdSsiRespInfo::isStream: type = "type=isStream"; break;
    }
    // We can't do much other than close the file.
    // It should work (on linux) to unlink the file after we open it, though.
    LOGF_INFO("RequestFinished %1%" % type);
}

bool
SsiSession::Unprovision(bool forced) {
    // all requests guaranteed to be finished or cancelled.
    delete this;
    return true; // false if we can't unprovision now.
}

void SsiSession::_addCanceller(CancelFuncPtr p) {
    bool shouldCall = false;
    {
        std::lock_guard<std::mutex> lock(_cancelMutex);
        if(_cancelled) {
            // Don't add the canceller, just call it.
            shouldCall = true;
        } else {
            _cancellers.push_back(p);
        }
    }
    if (shouldCall) {
        (*p)(); // call outside of the lock
    }
}

}}} // lsst::qserv::xrdsvc
