// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
#include "wbase/SendChannel.h"
#include "xrdsvc/SsiSession_ReplyChannel.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.SsiSession");
}

namespace lsst {
namespace qserv {
namespace xrdsvc {

SsiSession::~SsiSession() {
    // XrdSsiSession::sessName is unmanaged, need to free()
    LOGS(_log, LOG_LVL_DEBUG, "~SsiSession()");
    if (sessName) { ::free(sessName); sessName = 0; }
}

// Step 4
/// Called by XrdSsi to actually process a request.
void SsiSession::ProcessRequest(XrdSsiRequest* req, unsigned short timeout) {
    util::Timer t;

    LOGS(_log, LOG_LVL_DEBUG, "ProcessRequest, service=" << sessName);

    char *reqData = nullptr;
    int reqSize;
    t.start();
    reqData = req->GetRequest(reqSize);
    t.stop();
    LOGS(_log, LOG_LVL_DEBUG, "GetRequest took " << t.getElapsed() << " seconds");

    auto replyChannel = std::make_shared<ReplyChannel>(*this);

    auto errorFunc = [this, &req, &replyChannel](std::string const& errStr) {
        replyChannel->sendError(errStr, EINVAL);
        BindRequest(req, this);
        ReleaseRequestBuffer();
    };

    ResourceUnit ru(sessName);
    if (ru.unitType() != ResourceUnit::DBCHUNK) {
        std::ostringstream os;
        os << "Unexpected unit type in query db=" << ru.db() << " unitType=" << ru.unitType();
        LOGS(_log, LOG_LVL_ERROR, os.str());
        errorFunc(os.str());
        return;
    }

    if (!(*_validator)(ru)) {
        std::ostringstream os;
        os << "WARNING: unowned chunk query detected:" << ru.path();
        LOGS(_log, LOG_LVL_WARN, os.str());
        errorFunc(os.str());
        return;
    }

    // reqData has the entire request, so we can unpack it without waiting for
    // more data.
    LOGS(_log, LOG_LVL_DEBUG, "Decoding TaskMsg of size " << reqSize);
    auto taskMsg = std::make_shared<proto::TaskMsg>();
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 1");
    bool ok = taskMsg->ParseFromArray(reqData, reqSize) && taskMsg->IsInitialized();
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 2");
    if (!ok) {
        std::ostringstream os;
        os << "Failed to decode TaskMsg on resource db=" << ru.db() << " chunkId=" << ru.chunk();
        LOGS(_log, LOG_LVL_ERROR, os.str());
        errorFunc(os.str());
        return;
    }

    if (!taskMsg->has_db() || !taskMsg->has_chunkid()
        || (ru.db() != taskMsg->db()) || (ru.chunk() != taskMsg->chunkid())) {
        std::ostringstream os;
        os << "Mismatched db/chunk in TaskMsg on resource db=" << ru.db() << " chunkId=" << ru.chunk();
        LOGS(_log, LOG_LVL_ERROR, os.str());
        errorFunc(os.str());
        return;
    }

    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 3");
    // Once BindRequest has been called, we don't want to send errors back to xrootd
    // if the task has been cancelled. Also, task needs to exist before binding
    // to avoid any chance of missing the cancel call.
    auto task = std::make_shared<wbase::Task>(taskMsg, replyChannel);
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 4");
    _addTask(task);
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 5");
    t.start();
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 6");
    BindRequest(req, this); // Step 5
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 7");
    t.stop();
    // Now that the request is decoded (successfully or not), release the
    // xrootd request buffer. To avoid data races, this must happen before
    // the task is handed off to another thread for processing, as there is a
    // reference to this SsiSession inside the reply channel for the task,
    // and after the call to BindRequest.
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 8");
    ReleaseRequestBuffer();
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 9");
    t.start();
    _processor->processTask(task); // Queues task to be run later.
    LOGS(_log, LOG_LVL_DEBUG, "&&& ProcessRequest 10");
    t.stop();
    LOGS(_log, LOG_LVL_DEBUG, "BindRequest took " << t.getElapsed() << " seconds");
    LOGS(_log, LOG_LVL_DEBUG, "Enqueued TaskMsg for " << ru << " in " << t.getElapsed() << " seconds");
}

/// Called by XrdSsi to free resources.
void SsiSession::RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo, bool cancel) { // Step 8
    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)
    {
        std::lock_guard<std::mutex> lock(_tasksMutex);
        if (cancel && !_cancelled.exchange(true)) { // Cancel if not already cancelled
            for (auto task: _tasks) {
                task->cancel();
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
    LOGS(_log, LOG_LVL_DEBUG, "RequestFinished " << type);
}

bool SsiSession::Unprovision(bool forced) {
    // All requests guaranteed to be finished or cancelled.
    delete this;
    return true; // false if we can't unprovision now.
}

void SsiSession::_addTask(wbase::Task::Ptr const& task) {
    {
        std::lock_guard<std::mutex> lock(_tasksMutex);
        _tasks.push_back(task);

    }
    if (_cancelled) {
        // Calling Task::cancel multiple times should be harmless.
        task->cancel();
    }
}

}}} // namespace
