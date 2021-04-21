// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2018 LSST Corporation.
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
#include "wbase/SendChannel.h"

// System headers
#include <functional>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <vector>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/ProtoHeaderWrap.h"
#include "global/LogContext.h"
#include "util/common.h"
#include "util/Timer.h"
#include "xrdsvc/SsiRequest.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.SendChannel");
}

using namespace std;

namespace lsst {
namespace qserv {
namespace wbase {

/// NopChannel is a NOP implementation of SendChannel for development and
/// debugging code without an XrdSsi channel.
class NopChannel : public SendChannel {
public:
    NopChannel() {}

    bool send(char const* buf, int bufLen) override {
        cout << "NopChannel send(" << (void*) buf
             << ", " << bufLen << ");\n";
        return !isDead();
    }

    bool sendError(string const& msg, int code) override {
        if (kill()) return false;
        cout << "NopChannel sendError(\"" << msg
             << "\", " << code << ");\n";
        return true;
    }
    bool sendFile(int fd, Size fSize) override {
        cout << "NopChannel sendFile(" << fd
             << ", " << fSize << ");\n";
        return !isDead();
    }
    bool sendStream(xrdsvc::StreamBuffer::Ptr const& sBuf, bool last) override {
        cout << "NopChannel sendStream(" << (void*) sBuf.get()
             << ", " << (last ? "true" : "false") << ");\n";
        return !isDead();
    }
};


SendChannel::Ptr SendChannel::newNopChannel() {
    return make_shared<NopChannel>();
}


/// StringChannel is an almost-trivial implementation of a SendChannel that
/// remembers what it has received.
class StringChannel : public SendChannel {
public:
    StringChannel(string& dest) : _dest(dest) {}

    bool send(char const* buf, int bufLen) override {
        if (isDead()) return false;
        _dest.append(buf, bufLen);
        return true;
    }

    bool sendError(string const& msg, int code) override {
        if (kill()) return false;
        ostringstream os;
        os << "(" << code << "," << msg << ")";
        _dest.append(os.str());
        return true;
    }

    bool sendFile(int fd, Size fSize) override {
        if (isDead()) return false;
        vector<char> buf(fSize);
        Size remain = fSize;
        while(remain > 0) {
            Size frag = ::read(fd, buf.data(), remain);
            if (frag < 0) {
                cout << "ERROR reading from fd during "
                     << "StringChannel::sendFile(" << "," << fSize << ")";
                return false;
            } else if (frag == 0) {
                cout << "ERROR unexpected 0==read() during "
                     << "StringChannel::sendFile(" << "," << fSize << ")";
                return false;
            }
            _dest.append(buf.data(), frag);
            remain -= frag;
        }
        release();
        return true;
    }

    bool sendStream(xrdsvc::StreamBuffer::Ptr const& sBuf, bool last) override {
        if (isDead()) return false;
        char const* buf = sBuf->data;
        size_t bufLen = sBuf->getSize();
        _dest.append(buf, bufLen);
        cout << "StringChannel sendStream(" << (void*) buf
             << ", " << bufLen << ", "
             << (last ? "true" : "false") << ");\n";
        return true;
    }

private:
    string& _dest;
};


SendChannel::Ptr SendChannel::newStringChannel(string& d) {
    return make_shared<StringChannel>(d);

}


/// This is the standard definition of SendChannel which actually does something!
/// We vector responses posted to SendChannel via the tightly bound SsiRequest
/// object as this object knows how to effect Ssi responses.
///
bool SendChannel::send(char const* buf, int bufLen) {
    if (isDead()) return false;
    if (_ssiRequest->reply(buf, bufLen)) return true;
    kill();
    return false;
}


bool SendChannel::sendError(string const& msg, int code) {
    // Kill this send channel. If it wasn't already dead, send the error.
    if (kill()) return false;
    if (_ssiRequest->replyError(msg.c_str(), code)) return true;
    return false;
}


bool SendChannel::sendFile(int fd, Size fSize) {
    if (!isDead()) {
        if (_ssiRequest->replyFile(fSize, fd)) return true;
    }
    kill();
    release();
    return false;
}


bool SendChannel::kill() {
    return _dead.exchange(true);
}


bool SendChannel::isDead() {
    if (_dead) return true;
    if (_ssiRequest == nullptr) return true;
    if (_ssiRequest->isFinished()) kill();
    return _dead;
}


bool SendChannel::sendStream(xrdsvc::StreamBuffer::Ptr const& sBuf, bool last) {
    if (isDead()) return false;
    if (_ssiRequest->replyStream(sBuf, last)) return true;
    kill();
    return false;
}


/* &&&
bool SendChannel::setMetadata(const char *buf, int blen) {
    if (isDead()) return false;
    if (_ssiRequest->sendMetadata(buf, blen)) return true;
    return false;
}


}}} // namespace lsst::qserv::wbase

SendChannelShared::Ptr SendChannelShared::create(SendChannel::Ptr const& sendChannel, wcontrol::TransmitMgr::Ptr const& transmitMgr) {
    auto scs = shared_ptr<SendChannelShared>(new SendChannelShared(sendChannel, transmitMgr));
    scs->_keepAlive = scs;
    scs->_run();
    return scs;
}


SendChannelShared::~SendChannelShared() {
    if (!_sendChannel->isDead()) {
        LOGS(_log, LOG_LVL_ERROR, "SendChannelShared destructor called while _sendChannel still alive.");
        _sendChannel->kill();
    }
    if (_threadStarted == true) {
        _lastRecvd = true;
        _queueCv.notify_all();
    }
}

*/

/* &&&
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
    _queueCv.notify_all();
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

    /// Wait if there are enough TransmitData objects already in the queue.
    bool reallyLast = _lastRecvd;

    string idStr(makeIdStr(qId, jId));

    if (reallyLast || isDead()) {
        LOGS(_log, LOG_LVL_WARN, "addTransmit getting messages after isDead or reallyLast " << idStr);
        _lastRecvd = true;
        _queueCv.notify_all();
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
        _queueCv.wait(qLock, [this](){ return _transmitQueue.size() < 2; });
    }
    _transmitQueue.push(tData);
    if (reallyLast || erred || cancelled) {
        _lastRecvd = true;
    }

    qLock.unlock();
    _queueCv.notify_all();
    return true;
}


void SendChannelShared::_run() {
    if (_threadStarted.exchange(true)) {
        throw Bug("SendChannelShared::run was already called");
    }
    std::thread thrd(&SendChannelShared::_transmitLoop, this);
    _thread = std::move(thrd);
    _thread.detach(); // There's no good option for joining later.
    _threadStarted = true;
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
        _queueCv.wait(qLock, [this](){ return (_transmitQueue.size() >= 2 || _lastRecvd); });

        if (_transmitQueue.size() == 0) {
            /// This may happen when a query is cancelled.
            LOGS(_log, LOG_LVL_WARN, "_lastRecvd and nothing to transmit.");
            return;
        }
        TransmitData::Ptr thisTransmit = _transmitQueue.front();
        _transmitQueue.pop();
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
                LOGS(_log, LOG_LVL_ERROR, "Shared Trying _transmit first size=" << sz << " but not last");
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
        _queueCv.notify_all();

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
        LOGS(_log, LOG_LVL_DEBUG, "_sendbuf wait end");
        t.stop();
        auto logMsg = transmitHisto.addTime(t.getElapsed(), note);
        LOGS(_log, LOG_LVL_DEBUG, logMsg);
    }
    return sent;
}
*/

}}} // namespace
