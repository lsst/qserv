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
#include "proto/ProtoHeaderWrap.h"
#include "util/Timer.h"
#include "wcontrol/TransmitMgr.h"

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


SendChannelShared::~SendChannelShared() {
    if (_sendChannel != nullptr) {
        _sendChannel->setDestroying();
        if (!_sendChannel->isDead()) {
            _sendChannel->kill("~SendChannelShared()");
        }
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


bool SendChannelShared::kill(StreamGuard sLock, std::string const& note) {
    LOGS(_log, LOG_LVL_DEBUG, "SendChannelShared::kill() called " << note);
    bool ret = _sendChannel->kill(note);
    _lastRecvd = true;
    return ret;
}

string SendChannelShared::makeIdStr(int qId, int jId) {
    string str("QID" + (qId == 0 ? "" : to_string(qId) + "#" + to_string(jId)));
    return str;
}


void SendChannelShared::waitTransmitLock(wcontrol::TransmitMgr& transmitMgr, bool interactive, QueryId const& qId) {
    if (_transmitLock != nullptr) {
        return;
    }

    {
        unique_lock<mutex> uLock(_transmitLockMtx);
        bool first = _firstTransmitLock.exchange(false);
        if (first) {
            // This will wait until TransmitMgr has resources available.
            _transmitLock.reset(new wcontrol::TransmitLock(transmitMgr, interactive, qId));
        } else {
            _transmitLockCv.wait(uLock, [this](){ return _transmitLock != nullptr; });
        }
    }
    _transmitLockCv.notify_one();
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
        LOGS(_log, LOG_LVL_DEBUG, "addTransmit lastRecvd=" << _lastRecvd << " really=" << reallyLast
                                  << " erred=" << erred << " cancelled=" << cancelled);
    }

    // If this is reallyLast or at least 2 items are in the queue, the transmit can happen
    if (_lastRecvd || _transmitQueue.size() >= 2) {
        bool scanInteractive = tData->scanInteractive;
        // If there was an error, give this high priority.
        if (erred || cancelled) scanInteractive = true;
        int czarId = tData->czarId;
        return _transmit(erred, scanInteractive, largeResult, czarId);
    } else {
        // Not enough information to transmit. Maybe there will be with the next call
        // to addTransmit.
    }
    return true;
}


util::TimerHistogram scsTransmitSend("scsTransmitSend", {0.01, 0.1, 1.0, 2.0, 5.0, 10.0, 20.0});

bool SendChannelShared::_transmit(bool erred, bool scanInteractive, QueryId const qid, qmeta::CzarId czarId) {
    string idStr = "QID?";

    // Result data is transmitted in messages containing data and headers.
    // data - is the result data
    // header - contains information about the next chunk of result data,
    //          most importantly the size of the next data message.
    //          The header has a fixed size (about 255 bytes)
    // header_END - indicates there will be no more msg.
    // msg - contains data and header.
    // metadata - special xrootd buffer that can only be set once per SendChannelShared
    //            instance. It is used to send the first header.
    // A complete set of results to the czar looks like
    //    metadata[header_A] -> msg_A[data_A, header_END]
    // or
    //    metadata[header_A] -> msg_A[data_A, header_B]
    //          -> msg_B[data_B, header_C] -> ... -> msg_X[data_x, header_END]
    //
    // Since you can't send msg_A until you know the size of data_B, you can't
    // transmit until there are at least 2 msg in the queue, or you know
    // that msg_A is the last msg in the queue.
    // Note that the order of result rows does not matter, but data_B must come after header_B.
    // Keep looping until nothing more can be transmitted.
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
        uint32_t seq = _sendChannel->getSeq();
        int scsSeq = ++_scsSeq;
        string seqStr = string("seq=" + to_string(seq) + " scsseq=" + to_string(scsSeq));
        nextPHdr->set_endnodata(reallyLast);
        nextPHdr->set_seq(seq);
        nextPHdr->set_scsseq(scsSeq);
        string nextHeaderString;
        nextPHdr->SerializeToString(&nextHeaderString);
        thisTransmit->dataMsg += proto::ProtoHeaderWrap::wrap(nextHeaderString);

        // The first message needs to put its header data in metadata as there's
        // no previous message it could attach its header to.
        {
            lock_guard<mutex> streamLock(streamMutex); // Must keep meta and buffer together.
            if (_firstTransmit.exchange(false)) {
                // Put the header for the first message in metadata
                // _metaDataBuf must remain valid until Finished() is called.
                proto::ProtoHeader* thisPHdr = thisTransmit->header;
                thisPHdr->set_seq(seq);
                thisPHdr->set_scsseq(scsSeq - 1); // should always be 0
                string thisHeaderString;
                thisPHdr->SerializeToString(&thisHeaderString);
                _metadataBuf = proto::ProtoHeaderWrap::wrap(thisHeaderString);
                bool metaSet = _sendChannel->setMetadata(_metadataBuf.data(), _metadataBuf.size());
                if (!metaSet) {
                    LOGS(_log, LOG_LVL_ERROR, "Failed to setMeta " << idStr);
                    kill(streamLock, "metadata");
                    return false;
                }
            }

            // Put the data for the transmit in a StreamBuffer and send it.
            auto streamBuf = xrdsvc::StreamBuffer::createWithMove(thisTransmit->dataMsg);
            {
                util::Timer sendTimer;
                sendTimer.start();
                bool sent = _sendBuf(streamLock, streamBuf, reallyLast, "transmitLoop " + idStr + " " + seqStr, scsSeq);
                sendTimer.stop();
                auto logMsgSend = scsTransmitSend.addTime(sendTimer.getElapsed(), idStr);
                LOGS(_log, LOG_LVL_INFO, logMsgSend);
                if (!sent) {
                    LOGS(_log, LOG_LVL_ERROR, "Failed to send " << idStr);
                    kill(streamLock, "SendChannelShared::_transmit b");
                    return false;
                }
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
                                 string const& note, int scsSeq) {
    bool sent = _sendChannel->sendStream(streamBuf, last, scsSeq);
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
