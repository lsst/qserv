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
#include <xrdsvc/SsiRequest.h>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string>

// Third-party headers
#include "XrdSsi/XrdSsiRequest.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/intTypes.h"
#include "global/LogContext.h"
#include "global/ResourceUnit.h"
#include "proto/FrameBuffer.h"
#include "proto/worker.pb.h"
#include "util/InstanceCount.h"
#include "util/HoldTrack.h"
#include "util/Timer.h"
#include "wbase/FileChannelShared.h"
#include "wbase/TaskState.h"
#include "wbase/Task.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/Foreman.h"
#include "wcontrol/ResourceMonitor.h"
#include "wpublish/ChunkInventory.h"
#include "xrdsvc/ChannelStream.h"

namespace proto = lsst::qserv::proto;
namespace wbase = lsst::qserv::wbase;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.SsiRequest");

}  // namespace

namespace lsst::qserv::xrdsvc {

SsiRequest::Ptr SsiRequest::newSsiRequest(std::string const& rname,
                                          std::shared_ptr<wcontrol::Foreman> const& foreman) {
    auto req = SsiRequest::Ptr(new SsiRequest(rname, foreman));
    req->_selfKeepAlive = req;
    return req;
}

SsiRequest::SsiRequest(std::string const& rname, std::shared_ptr<wcontrol::Foreman> const& foreman)
        : _validator(foreman->chunkInventory()->newValidator()), _foreman(foreman), _resourceName(rname) {}

SsiRequest::~SsiRequest() {
    LOGS(_log, LOG_LVL_DEBUG, "~SsiRequest()");
    UnBindRequest();
}

void SsiRequest::reportError(std::string const& errStr) {
    LOGS(_log, LOG_LVL_WARN, errStr);
    replyError(errStr, EINVAL);
    ReleaseRequestBuffer();
}

uint64_t countLimiter = 0;  // LockupDB

// Step 4
/// Called by XrdSsi to actually process a request.
void SsiRequest::execute(XrdSsiRequest& req) {
    util::Timer t;
    LOGS(_log, LOG_LVL_DEBUG, "Execute request, resource=" << _resourceName);

    char* reqData = nullptr;
    int reqSize;
    t.start();
    reqData = req.GetRequest(reqSize);
    t.stop();
    LOGS(_log, LOG_LVL_DEBUG, "GetRequest took " << t.getElapsed() << " seconds");

    // We bind this object to the request now. This allows us to respond at any
    // time (much simpler). Though the manual forgot to say that all pending
    // events will be reflected on a different thread the moment we bind the
    // request; the fact allows us to use a mutex to serialize the order of
    // initialization and possible early cancellation. We protect this code
    // with a mutex gaurd which will be released upon exit.
    //
    std::lock_guard<std::mutex> lock(_finMutex);
    BindRequest(req);

    ResourceUnit ru(_resourceName);

    // Make sure the requested resource belongs to this worker
    if (!(*_validator)(ru)) {
        reportError("WARNING: request to the unowned resource detected:" + _resourceName);
        return;
    }

    auto const sendChannel = std::make_shared<wbase::SendChannel>(shared_from_this());

    // Process the request
    switch (ru.unitType()) {
        case ResourceUnit::DBCHUNK: {
            // Increment the counter of the database/chunk resources in use
            _foreman->resourceMonitor()->increment(_resourceName);

            // reqData has the entire request, so we can unpack it without waiting for
            // more data.
            LOGS(_log, LOG_LVL_DEBUG, "Decoding TaskMsg of size " << reqSize);
            auto taskMsg = std::make_shared<proto::TaskMsg>();
            if (!taskMsg->ParseFromArray(reqData, reqSize) || !taskMsg->IsInitialized()) {
                reportError("Failed to decode TaskMsg on resource db=" + ru.db() +
                            " chunkId=" + std::to_string(ru.chunk()));
                return;
            }

            QSERV_LOGCONTEXT_QUERY_JOB(taskMsg->queryid(), taskMsg->jobid());

            if (!taskMsg->has_db() || !taskMsg->has_chunkid() || (ru.db() != taskMsg->db()) ||
                (ru.chunk() != taskMsg->chunkid())) {
                reportError("Mismatched db/chunk in TaskMsg on resource db=" + ru.db() +
                            " chunkId=" + std::to_string(ru.chunk()));
                return;
            }

            if (not(taskMsg->has_queryid() && taskMsg->has_jobid() && taskMsg->has_scaninteractive() &&
                    taskMsg->has_attemptcount() && taskMsg->has_czarid())) {
                reportError(std::string("taskMsg missing required field ") +
                            " queryid:" + std::to_string(taskMsg->has_queryid()) +
                            " jobid:" + std::to_string(taskMsg->has_jobid()) +
                            " scaninteractive:" + std::to_string(taskMsg->has_scaninteractive()) +
                            " attemptcount:" + std::to_string(taskMsg->has_attemptcount()) +
                            " czarid:" + std::to_string(taskMsg->has_czarid()));
                return;
            }
            std::shared_ptr<wbase::FileChannelShared> channelShared;
            switch (wconfig::WorkerConfig::instance()->resultDeliveryProtocol()) {
                case wconfig::WorkerConfig::ResultDeliveryProtocol::XROOT:
                case wconfig::WorkerConfig::ResultDeliveryProtocol::HTTP:
                    channelShared = wbase::FileChannelShared::create(sendChannel, taskMsg->czarid(),
                                                                     _foreman->chunkInventory()->id());
                    break;
                default:
                    throw std::runtime_error("SsiRequest::" + std::string(__func__) +
                                             " unsupported result delivery protocol");
            }
            auto const tasks = wbase::Task::createTasks(taskMsg, channelShared, _foreman->chunkResourceMgr(),
                                                        _foreman->mySqlConfig(), _foreman->sqlConnMgr(),
                                                        _foreman->queriesAndChunks(), _foreman->httpPort());
            for (auto const& task : tasks) {
                _tasks.push_back(task);
            }

            // Now that the request is decoded (successfully or not), release the
            // xrootd request buffer. To avoid data races, this must happen before
            // the task is handed off to another thread for processing, as there is a
            // reference to this SsiRequest inside the reply channel for the task,
            // and after the call to BindRequest.
            ReleaseRequestBuffer();
            t.start();
            _foreman->processTasks(tasks);  // Queues tasks to be run later.
            t.stop();
            LOGS(_log, LOG_LVL_DEBUG,
                 "Enqueued TaskMsg for " << ru << " in " << t.getElapsed() << " seconds");
            break;
        }
        case ResourceUnit::QUERY: {
            LOGS(_log, LOG_LVL_DEBUG, "Parsing request details for resource=" << _resourceName);
            proto::QueryManagement request;
            try {
                // reqData has the entire request, so we can unpack it without waiting for
                // more data.
                proto::FrameBufferView view(reqData, reqSize);
                view.parse(request);
                ReleaseRequestBuffer();
            } catch (proto::FrameBufferError const& ex) {
                reportError("Failed to decode a query completion/cancellation command, error: " +
                            std::string(ex.what()));
                break;
            }
            LOGS(_log, LOG_LVL_DEBUG,
                 "QueryManagement: op=" << proto::QueryManagement_Operation_Name(request.op())
                                        << " query_id=" << request.query_id());

            switch (wconfig::WorkerConfig::instance()->resultDeliveryProtocol()) {
                case wconfig::WorkerConfig::ResultDeliveryProtocol::XROOT:
                case wconfig::WorkerConfig::ResultDeliveryProtocol::HTTP:
                    switch (request.op()) {
                        case proto::QueryManagement::CANCEL_AFTER_RESTART:
                            // TODO: locate and cancel the coresponding tasks, remove the tasks
                            //       from the scheduler queues.
                            wbase::FileChannelShared::cleanUpResultsOnCzarRestart(request.czar_id(),
                                                                                  request.query_id());
                            break;
                        case proto::QueryManagement::CANCEL:
                            // TODO: locate and cancel the coresponding tasks, remove the tasks
                            //       from the scheduler queues.
                            wbase::FileChannelShared::cleanUpResults(request.czar_id(), request.query_id());
                            break;
                        case proto::QueryManagement::COMPLETE:
                            wbase::FileChannelShared::cleanUpResults(request.czar_id(), request.query_id());
                            break;
                        default:
                            reportError("QueryManagement: op=" +
                                        proto::QueryManagement_Operation_Name(request.op()) +
                                        " is not supported by the current implementation.");
                            return;
                    }
                    break;
                default:
                    throw std::runtime_error("SsiRequest::" + std::string(__func__) +
                                             " unsupported result delivery protocol");
            }

            // Send back the empty response since no info is expected by a caller
            // for this type of requests beyond the usual error notifications (if any).
            this->reply((char const*)0, 0);
            break;
        }
        default:
            reportError("Unexpected unit type '" + std::to_string(ru.unitType()) +
                        "', resource name: " + _resourceName);
            break;
    }

    // Note that upon exit the _finMutex will be unlocked allowing Finished()
    // to actually do something once everything is actually setup.
}

/// Called by SSI to free resources.
void SsiRequest::Finished(XrdSsiRequest& req, XrdSsiRespInfo const& rinfo, bool cancel) {  // Step 8
    util::HoldTrack::Mark markA(ERR_LOC, "SsiRequest::Finished start");
    if (cancel) {
        // Either the czar of xrootd has decided to cancel the Job.
        // Try to cancel all of the tasks, if there are any.
        for (auto&& wTask : _tasks) {
            auto task = wTask.lock();
            if (task != nullptr) {
                task->cancel();
            }
        }
    }

    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)
    // But first we must make sure that request setup completed (i.e execute()) by
    // locking _finMutex.
    {
        std::lock_guard<std::mutex> finLock(_finMutex);
        // Clean up _stream if it exists and don't add anything new to it either.
        _reqFinished = true;
        if (_stream != nullptr) {
            _stream->clearMsgs();
        }
    }

    auto keepAlive = freeSelfKeepAlive();

    // No buffers allocated, so don't need to free.
    // We can release/unlink the file now
    const char* type = "";
    switch (rinfo.rType) {
        case XrdSsiRespInfo::isNone:
            type = "type=isNone";
            break;
        case XrdSsiRespInfo::isData:
            type = "type=isData";
            break;
        case XrdSsiRespInfo::isError:
            type = "type=isError";
            break;
        case XrdSsiRespInfo::isFile:
            type = "type=isFile";
            break;
        case XrdSsiRespInfo::isStream:
            type = "type=isStream";
            break;
        case XrdSsiRespInfo::isHandle:
            type = "type=isHandle";
            break;
    }

    // Decrement the counter of the database/chunk resources in use
    ResourceUnit ru(_resourceName);
    if (ru.unitType() == ResourceUnit::DBCHUNK) {
        _foreman->resourceMonitor()->decrement(_resourceName);
    }

    // We can't do much other than close the file.
    // It should work (on linux) to unlink the file after we open it, though.
    // With the optimizer on '-Og', there was a double free for a SsiRequest.
    // The likely cause could be keepAlive being optimized out for being unused.
    // The problem has not reoccurred since adding keepAlive to the following
    // comment, but having code depend on a comment line is ugly in its own way.
    LOGS(_log, LOG_LVL_DEBUG, "RequestFinished " << type << " " << keepAlive.use_count());
}

bool SsiRequest::reply(char const* buf, int bufLen) {
    Status s = SetResponse(buf, bufLen);
    if (s != XrdSsiResponder::wasPosted) {
        LOGS(_log, LOG_LVL_ERROR, "DANGER: Couldn't post response of length=" << bufLen);
        return false;
    }
    return true;
}

bool SsiRequest::replyError(std::string const& msg, int code) {
    Status s = SetErrResponse(msg.c_str(), code);
    if (s != XrdSsiResponder::wasPosted) {
        LOGS(_log, LOG_LVL_ERROR, "DANGER: Couldn't post error response " << msg);
        return false;
    }
    return true;
}

bool SsiRequest::replyStream(StreamBuffer::Ptr const& sBuf, bool last) {
    LOGS(_log, LOG_LVL_DEBUG, "replyStream, checking stream size=" << sBuf->getSize() << " last=" << last);

    // Normally, XrdSsi would call Recycle() when it is done with sBuf, but if this function
    // returns false, then it must call Recycle(). Otherwise, the scheduler will likely
    // wedge waiting for the buffer to be released.
    std::lock_guard<std::mutex> finLock(_finMutex);
    if (_reqFinished) {
        // Finished() was called, give up.
        LOGS(_log, LOG_LVL_ERROR, "replyStream called after reqFinished.");
        sBuf->Recycle();
        return false;
    }
    // Create a stream if needed.
    if (!_stream) {
        _stream = std::make_shared<ChannelStream>();
        if (SetResponse(_stream.get()) != XrdSsiResponder::Status::wasPosted) {
            LOGS(_log, LOG_LVL_WARN, "SetResponse stream failed, calling Recycle for sBuf");
            // SetResponse return value indicates XrdSsi wont call Recycle().
            sBuf->Recycle();
            return false;
        }
    } else if (_stream->closed()) {
        // XrdSsi isn't going to call Recycle if we wind up here.
        LOGS(_log, LOG_LVL_ERROR, "Logic error SsiRequest::replyStream called with stream closed.");
        sBuf->Recycle();
        return false;
    }
    // XrdSsi or Finished() will call Recycle().
    LOGS(_log, LOG_LVL_INFO, "SsiRequest::replyStream seq=" << getSeq());
    _stream->append(sBuf, last);
    return true;
}

bool SsiRequest::sendMetadata(const char* buf, int blen) {
    Status stat = SetMetadata(buf, blen);
    switch (stat) {
        case XrdSsiResponder::wasPosted:
            return true;
        case XrdSsiResponder::notActive:
            LOGS(_log, LOG_LVL_ERROR, "failed to " << __func__ << " notActive");
            break;
        case XrdSsiResponder::notPosted:
            LOGS(_log, LOG_LVL_ERROR, "failed to " << __func__ << " notPosted blen=" << blen);
            break;
        default:
            LOGS(_log, LOG_LVL_ERROR, "failed to " << __func__ << " unkown state blen=" << blen);
    }
    return false;
}

SsiRequest::Ptr SsiRequest::freeSelfKeepAlive() {
    Ptr keepAlive = std::move(_selfKeepAlive);
    return keepAlive;
}

uint64_t SsiRequest::getSeq() const {
    if (_stream == nullptr) return 0;
    return _stream->getSeq();
}

}  // namespace lsst::qserv::xrdsvc
