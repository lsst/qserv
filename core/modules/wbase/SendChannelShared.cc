/*
 * LSST Data Management System
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
#include "wbase/SendChannelShared.h"

// System headers

// Qserv headers
#include "global/LogContext.h"
#include "util/Timer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.SendChannelShared");
}

namespace lsst {
namespace qserv {
namespace wbase {

SendChannelShared::Ptr SendChannelShared::create(SendChannel::Ptr const& sendChannel,
                                                 wcontrol::TransmitMgr::Ptr const& transmitMgr,
                                                 bool joinable)  {
    auto scs = shared_ptr<SendChannelShared>(new SendChannelShared(sendChannel, transmitMgr, joinable));
    scs->_keepAlive = scs;
    //scs->_run();
    return scs;
}


SendChannelShared::~SendChannelShared() {
    if (!_sendChannel->isDead()) {
        LOGS(_log, LOG_LVL_ERROR, "SendChannelShared destructor called while _sendChannel still alive.");
        _sendChannel->kill();
    }
    if (_threadStarted == true) {
        _lastRecvd = true;
        _qFrontCv.notify_all();
        _qBackCv.notify_all();
    }
}


void SendChannelShared::setTaskCount(int taskCount) {
    _taskCount = taskCount;
}


bool SendChannelShared::transmitTaskLast(StreamGuard sLock, bool inLast) {
    /// _caller must have locked _streamMutex before calling this.
    if (not inLast) return false; // This wasn't the last message buffer for this task, so it doesn't matter.
    ++_lastCount;
    bool lastTaskDone = _lastCount >= _taskCount;
    return lastTaskDone;
}


bool SendChannelShared::kill(StreamGuard sLock) {
    LOGS(_log, LOG_LVL_DEBUG, "SendChannelShared::kill() called");
    bool ret = _sendChannel->kill();
    _lastRecvd = true;
    _qFrontCv.notify_all();
    _qBackCv.notify_all();
    return ret;
}

string SendChannelShared::makeIdStr(int qId, int jId) {
    string str("QID" + (qId == 0 ? "" : to_string(qId) + "#" + to_string(jId)));
    return str;
}

bool SendChannelShared::addTransmit(bool cancelled, bool erred, bool last, bool largeResult,
                                    TransmitData::Ptr const& tData, int qId, int jId) {
    QSERV_LOGCONTEXT_QUERY_JOB(qId, jId);
    assert(tData != nullptr);
    _run(); // Only starts the thread if it isn't already running.

    /// Wait if there are enough TransmitData objects already in the queue.
    bool reallyLast = _lastRecvd;

    string idStr(makeIdStr(qId, jId));

    if (reallyLast || isDead()) {
        // If there's been some kind of error, make sure that nothing hangs waiting
        // for this.
        LOGS(_log, LOG_LVL_WARN, "addTransmit getting messages after isDead or reallyLast " << idStr);
        _lastRecvd = true;
        _qFrontCv.notify_all();
        _qBackCv.notify_all();
        return false;
    }
    {
        lock_guard<mutex> streamLock(streamMutex);
        reallyLast = transmitTaskLast(streamLock, last);
    }

    // Can this addTransmit wait?
    // If erred or cancelled, this should be handled asap.
    std::unique_lock<std::mutex> qLock(_queueMtx);
    if (!erred && !cancelled) {
        _qBackCv.wait(qLock, [this](){ return _transmitQueue.size() < 2; });
    }
    _transmitQueue.push(tData);
    if (reallyLast || erred || cancelled) {
        _lastRecvd = true;
    }

    qLock.unlock();
    _qFrontCv.notify_one();
    return true;
}


void SendChannelShared::_run() {
    if (_threadStarted.exchange(true)) {
        return;
    }
    std::thread thrd(&SendChannelShared::_transmitLoop, this);
    _thread = std::move(thrd);
    if (!_joinable) {
        // There's no good option for joining later on the worker.
        _thread.detach();
    }
    _threadStarted = true;
}

void SendChannelShared::join(bool onlyJoinIfRunning) {
    if (!_joinable) {
        throw Bug("SendChannelShared::join called on a detached thread.");
    }
    // This is normally only used in unit tests to prevent segv.
    if (onlyJoinIfRunning && !_threadStarted) {
        return;
    }
    _thread.join();
}


void SendChannelShared::_transmitLoop() {
    LOGS(_log, LOG_LVL_DEBUG, "SendChannelShared _transmitLoop start");
    // Allow this thread and 'this' instance to die only
    // after this function exits.
    Ptr keepAlive = std::move(_keepAlive);

    bool loop = true;
    string idStr = "QID?";
    while (loop) {
        std::unique_lock<std::mutex> qLock(_queueMtx);
        // Conditions:
        //  - There are at least 2 messages in queue, which is the minimum to send a message
        //  - Or _lastRecvd is true. This indicates no more messages will be added to this queue
        //    either because the last TransmitData object for this SendChannel is already
        //    in the queue, or the queue is empty due to cancellation or an error.
        _qFrontCv.wait(qLock, [this](){ return (_transmitQueue.size() >= 2 || _lastRecvd); });

        if (_transmitQueue.size() == 0) {
            /// This may happen when a query is cancelled.
            LOGS(_log, LOG_LVL_WARN, "_lastRecvd and nothing to transmit.");
            return;
        }
        TransmitData::Ptr thisTransmit = _transmitQueue.front();
        _transmitQueue.pop();
        // If this function exits before _qBackCv.notify_one() is called, deadlock.
        // throw Bug doesn't count as it should never happen and will crash the program anyway.
        if (thisTransmit == nullptr) {
            throw Bug("_transmitLoop() _transmitQueue had nullptr!");
        } else if (thisTransmit->result == nullptr) {
            throw Bug("_transmitLoop() had nullptr result!");
        }
        idStr = makeIdStr(thisTransmit->result->queryid(), thisTransmit->result->jobid());

        // Append the header for the next message to  thisTransmit->dataMsg.
        auto sz = _transmitQueue.size();
        // Is this really the last message for this SharedSendChannel.
        bool reallyLast = (_lastRecvd && sz == 0);

        proto::ProtoHeader* nextPHdr;
        if (sz == 0) {
            if (!_lastRecvd) {
                throw Bug("SendChannelShared::_transmit() size=" + to_string(sz) + " but not last.");
            }
            // Create a header for an empty result using the protobuf arena from thisTransmit.
            // This is the signal to the czar that this SharedSendChannel is finished.
            nextPHdr = thisTransmit->createHeader();
        } else {
            auto nextT = _transmitQueue.front();
            nextPHdr = nextT->header;
        }
        nextPHdr->set_endnodata(reallyLast);
        string nextHeaderString;
        nextPHdr->SerializeToString(&nextHeaderString);
        thisTransmit->dataMsg += proto::ProtoHeaderWrap::wrap(nextHeaderString);
        if (reallyLast) loop = false;
        // Done with the queue for a while and this function may need to wait below.
        qLock.unlock();
        _qBackCv.notify_one();

        // Limit the number of concurrent transmits.
        bool scanInteractive = thisTransmit->scanInteractive;
        // If there was an error, give this high priority.
        if (thisTransmit->erred) {
            scanInteractive = true;
        }
        bool largeResult = thisTransmit->largeResult;
        int czarId = thisTransmit->czarId;
        // Do NOT create transmitLock this with a mutex locked. High risk of deadlock.
        wcontrol::TransmitLock transmitLock(*_transmitMgr, scanInteractive, largeResult, czarId);

        // The first message needs to put its header data in metadata as there's
        // no previous message it could attach its header to.
        if (_firstTransmit.exchange(false)) {
            // Put the header for the first message in metadata
            // _metaDataBuf must remain valid until Finished() is called.
            proto::ProtoHeader* thisPHdr = thisTransmit->header;
            string thisHeaderString;
            thisPHdr->SerializeToString(&thisHeaderString);
            _metadataBuf = proto::ProtoHeaderWrap::wrap(thisHeaderString);
            lock_guard<mutex> streamLock(streamMutex);
            bool metaSet = _sendChannel->setMetadata(_metadataBuf.data(), _metadataBuf.size());
            if (!metaSet) {
                LOGS(_log, LOG_LVL_ERROR, "Failed to setMeta " << idStr);
                kill(streamLock);
                return;
            }
        }

        // Put the data for the transmit in a StreamBuffer and send it.
        auto streamBuf = xrdsvc::StreamBuffer::createWithMove(thisTransmit->dataMsg);
        {
            lock_guard<mutex> streamLock(streamMutex);
            bool sent = _sendBuf(streamLock, streamBuf, reallyLast, "transmitLoop " + idStr);
            if (!sent) {
                LOGS(_log, LOG_LVL_ERROR, "Failed to send " << idStr);
                kill(streamLock);
                return;
            }
        }
    }
    {
        lock_guard<mutex> streamLock(streamMutex);
        kill(streamLock);
    }
    LOGS(_log, LOG_LVL_DEBUG, "transmitLoop end for " << idStr);
}


util::TimerHistogram transmitHisto("transmit Hist", {0.1, 1, 5, 10, 20, 40});


bool SendChannelShared::_sendBuf(lock_guard<mutex> const& streamLock,
                                 xrdsvc::StreamBuffer::Ptr& streamBuf, bool last,
                                 string const& note) {
    bool sent = _sendChannel->sendStream(streamBuf, last);
    if (!sent) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to transmit " << note << "!");
        return false;
    } else {
        util::Timer t;
        t.start();
        LOGS(_log, LOG_LVL_DEBUG, "_sendbuf wait start");
        streamBuf->waitForDoneWithThis(); // Block until this buffer has been sent.
        t.stop();
        auto logMsg = transmitHisto.addTime(t.getElapsed(), note);
        LOGS(_log, LOG_LVL_DEBUG, logMsg);
    }
    return sent;
}


}}} // namespace lsst::qserv::wbase
