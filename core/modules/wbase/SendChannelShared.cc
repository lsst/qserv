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
#include "util/InstanceCount.h" //&&&
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


uint64_t SendChannelShared::_idSequence = 0;


SendChannelShared::Ptr SendChannelShared::create(SendChannel::Ptr const& sendChannel,
                                                 wcontrol::TransmitMgr::Ptr const& transmitMgr)  {
    auto scs = shared_ptr<SendChannelShared>(
            new SendChannelShared(SendChannelShared::_idSequence++, sendChannel, transmitMgr));
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


bool SendChannelShared::transmitTaskLast(bool inLast) {
    if (not inLast) return false; // This can't be the really last task.
    lock_guard<mutex> lg(_lastCMtx);
    ++_lastCount;
    bool lastTaskDone = _lastCount >= _taskCount;
    LOGS(_log, LOG_LVL_INFO, "&&& transmitTaskLast A channel=" << _id << " seq=" << getSeq()
                             << " inLast=" << inLast << " lastC="
                             << _lastCount << " taskC=" << _taskCount << " lastTaskDone=" << lastTaskDone);
    return lastTaskDone;
}


bool SendChannelShared::kill(StreamGuard sLock, std::string const& note) {
    LOGS(_log, LOG_LVL_DEBUG, "channel=" << _id << " SendChannelShared::kill() called " << note);
    bool ret = _sendChannel->kill(note);
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

    // Update the queue, if the "reallyLast" item has been added to the queue, set _lastRecvd true.
    {
        util::InstanceCount icA("SCS::addTransmitA&&&");
        std::unique_lock<std::mutex> qLock(_queueMtx);
        util::InstanceCount icB("SCS::addTransmitB&&&");
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
        reallyLast = transmitTaskLast(last);

        // If there's been an error or the query was cancelled, no following items matter
        // so _lastRecvd is true.
        if (reallyLast || erred || cancelled) {
            _lastRecvd = true;
        }
    }

    /* &&&
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
    */

    util::InstanceCount icAT("SCS::addTransmit_tr&&& ch=" + to_string(_id) + " seq=" + to_string(getSeq()));
    bool scanInteractive = tData->scanInteractive;
    if (erred || cancelled) scanInteractive = true;
    int czarId = tData->czarId;
    return _transmit(erred, scanInteractive, largeResult, czarId);
}


bool SendChannelShared::_transmit(bool erred, bool scanInteractive, bool largeResult, qmeta::CzarId czarId) {
    string idStr = "QID?";

    util::InstanceCount icA("SCS::_Transmit A&&&");
    std::unique_lock<std::mutex> qLock(_queueMtx);
    util::InstanceCount icB("SCS::_Transmit B&&&");

    // keep looping until nothing more can be transmitted.
    while(_transmitQueue.size() >= 2 || _lastRecvd) {
        if (_transmitQueue.size() == 0) {
            LOGS(_log, LOG_LVL_INFO, "messages returned by another _transmit");
            return true;
        }
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
        LOGS(_log, LOG_LVL_INFO, "&&& reallyLast=" << reallyLast << " _lastRecvd=" << _lastRecvd << " sz=" << sz);

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
        /* &&&
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
                kill(streamLock, "metadata");
                return false;
            }
        }

        // Put the data for the transmit in a StreamBuffer and send it.
        auto streamBuf = xrdsvc::StreamBuffer::createWithMove(thisTransmit->dataMsg);
        */

        {
            // Need to lock streamLock now so some other message (with a different size)
            // doesn't get placed on the stream before thisTransmit.
            util::InstanceCount icSA("SCS::_Transmit streamLock A&&&");
            lock_guard<mutex> streamLock(streamMutex);
            util::InstanceCount icSB("SCS::_Transmit streamLock B&&&");
            if (_firstTransmit.exchange(false)) {
                // Put the header for the first message in metadata
                // _metaDataBuf must remain valid until Finished() is called.
                proto::ProtoHeader* thisPHdr = thisTransmit->header;
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
            qLock.unlock(); // Unlock so new messages can be added to the queue

            // Put the data for the transmit in a StreamBuffer and send it.
            auto streamBuf = xrdsvc::StreamBuffer::createWithMove(thisTransmit->dataMsg);
            {
                // Limit the number of concurrent transmits. See xrdssu.cnf [transmits] maxtransmits.
                //wcontrol::TransmitLock transmitLock(*_transmitMgr, scanInteractive, largeResult, czarId); // &&& re-enable
                util::InstanceCount icSBA("SCS::_Transmit sendBuf A&&&");
                bool sent = _sendBuf(streamLock, streamBuf, reallyLast, "transmitLoop " + idStr);
                util::InstanceCount icSBB("SCS::_Transmit sendBuf B&&&");
                if (!sent) {
                    LOGS(_log, LOG_LVL_ERROR, "Failed to send " << idStr);
                    kill(streamLock, "SendChannelShared::_transmit b");
                    return false;
                }
            }
        } // streamlock must be unlocked before locking qLock.

        // If that was the last message, break the loop.
        if (reallyLast) return true;
        util::InstanceCount icQA("SCS::_Transmit qlock end A&&&");
        qLock.lock(); // relock after releasing streamLock, but before while() test.
        util::InstanceCount icQB("SCS::_Transmit qlock end B&&&");
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
        LOGS(_log, LOG_LVL_DEBUG, "_sendbuf wait start " << note);
        util::InstanceCount icId("SCS:_sendBuff0&&&ch=" + to_string(_id) + " seq=" + to_string(getSeq()));
        util::InstanceCount icA("SCS:_sendBuffA&&&");
        streamBuf->waitForDoneWithThis(); // Block until this buffer has been sent.
        util::InstanceCount icB("SCS:_sendBuffB&&&");
        t.stop();
        auto logMsg = transmitHisto.addTime(t.getElapsed(), note);
        LOGS(_log, LOG_LVL_DEBUG, logMsg);
    }
    return sent;
}


}}} // namespace lsst::qserv::wbase
