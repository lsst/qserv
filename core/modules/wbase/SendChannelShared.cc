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
                                                 wcontrol::TransmitMgr::Ptr const& transmitMgr)  {
    auto scs = shared_ptr<SendChannelShared>(new SendChannelShared(sendChannel, transmitMgr));
    return scs;
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

    // This lock may be held for a very long time.
    std::unique_lock<std::mutex> qLock(_queueMtx);
    _transmitQueue.push(tData);

    // If _lastRecvd is true, the last message has already been transmitted and
    // this SendChannel is effectively dead.
    bool reallyLast = _lastRecvd;
    string idStr(makeIdStr(qId, jId));

    // If something bad already happened, just give up.
    if (reallyLast || isDead()) {
        // If there's been some kind of error, make sure that nothing hangs waiting
        // for this.
        LOGS(_log, LOG_LVL_WARN, "addTransmit getting messages after isDead or reallyLast " << idStr);
        _lastRecvd = true;
        return false;
    }
    {
        lock_guard<mutex> streamLock(streamMutex);
        reallyLast = transmitTaskLast(streamLock, last);
    }

    if (reallyLast || erred || cancelled) {
        _lastRecvd = true;
    }

    // If this is reallyLast or at least 2 items are in the queue, the transmit can happen
    if (reallyLast || _transmitQueue.size() >= 2) {
        bool scanInteractive = tData->scanInteractive;
        // If there was an error, give this high priority.
        if (erred) scanInteractive = true;
        int czarId = tData->czarId;
        // Don't wait if cancelled or error.
        if (erred || cancelled) {
            return _transmit(erred, largeResult, czarId);
        }
        // Limit the number of concurrent transmits.
        wcontrol::TransmitLock transmitLock(*_transmitMgr, scanInteractive, largeResult, czarId);
        return _transmit(erred, largeResult, czarId);
    } else {
        // Not enough information to transmit. Maybe there will be with the next call
        // to addTransmit.
    }
    return true;
}


bool SendChannelShared::_transmit(bool erred, bool largeResult, qmeta::CzarId czarId) {
    string idStr = "QID?";

    // keep looping until nothing more can be transmitted.
    while(_transmitQueue.size() >= 2 || _lastRecvd) {
        TransmitData::Ptr thisTransmit = _transmitQueue.front();
        _transmitQueue.pop();
        if (thisTransmit == nullptr) {
            throw Bug("_transmitLoop() _transmitQueue had nullptr!");
        } else if (thisTransmit->result == nullptr) {
            throw Bug("_transmitLoop() had nullptr result!");
        }

        idStr = makeIdStr(thisTransmit->result->queryid(), thisTransmit->result->jobid());
        auto sz = _transmitQueue.size();
        // Is this really the last message for this SharedSendChannel?
        bool reallyLast = (_lastRecvd && sz == 0);

        // Append the header for the next message to  thisTransmit->dataMsg.
        proto::ProtoHeader* nextPHdr;
        if (sz == 0) {
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
                return false;
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
                return false;
            }
        }

        // If that was the last message, break the loop.
        if (reallyLast) return true;
    }
    return true;
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
