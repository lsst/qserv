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
#include "wbase/ChannelShared.h"

// Qserv headers
#include "global/LogContext.h"
#include "proto/ProtoHeaderWrap.h"
#include "qmeta/types.h"
#include "util/Bug.h"
#include "util/Error.h"
#include "wcontrol/TransmitMgr.h"
#include "wbase/Task.h"
#include "wpublish/QueriesAndChunks.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.ChannelShared");
}

namespace lsst::qserv::wbase {

atomic<uint64_t> ChannelShared::scsSeqId{0};

ChannelShared::ChannelShared(shared_ptr<wbase::SendChannel> const& sendChannel,
                             shared_ptr<wcontrol::TransmitMgr> const& transmitMgr, qmeta::CzarId czarId)
        : _sendChannel(sendChannel), _transmitMgr(transmitMgr), _czarId(czarId), _scsId(scsSeqId++) {
    if (_sendChannel == nullptr) {
        throw util::Bug(ERR_LOC, "ChannelShared constructor given nullptr");
    }
}

ChannelShared::~ChannelShared() {
    if (_sendChannel != nullptr) {
        _sendChannel->setDestroying();
        if (!_sendChannel->isDead()) {
            _sendChannel->kill("~ChannelShared()");
        }
    }
}

bool ChannelShared::send(char const* buf, int bufLen) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->send(buf, bufLen);
}

bool ChannelShared::sendError(string const& msg, int code) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->sendError(msg, code);
}

bool ChannelShared::sendFile(int fd, wbase::SendChannel::Size fSize) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->sendFile(fd, fSize);
}

bool ChannelShared::sendStream(xrdsvc::StreamBuffer::Ptr const& sBuf, bool last, int scsSeq) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->sendStream(sBuf, last, scsSeq);
}

bool ChannelShared::kill(string const& note) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _kill(streamMutexLock, note);
}

bool ChannelShared::isDead() {
    if (_sendChannel == nullptr) return true;
    return _sendChannel->isDead();
}

void ChannelShared::setTaskCount(int taskCount) { _taskCount = taskCount; }

bool ChannelShared::transmitTaskLast() {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    ++_lastCount;
    bool lastTaskDone = _lastCount >= _taskCount;
    return lastTaskDone;
}

bool ChannelShared::_kill(lock_guard<mutex> const& streamMutexLock, string const& note) {
    LOGS(_log, LOG_LVL_DEBUG, "ChannelShared::kill() called " << note);
    bool ret = _sendChannel->kill(note);
    _lastRecvd = true;
    return ret;
}

string ChannelShared::makeIdStr(int qId, int jId) {
    string str("QID" + (qId == 0 ? "" : to_string(qId) + "#" + to_string(jId)));
    return str;
}

uint64_t ChannelShared::getSeq() const { return _sendChannel->getSeq(); }

string ChannelShared::dumpTransmit() const {
    lock_guard<mutex> const tMtxLock(tMtx);
    return dumpTransmit(tMtxLock);
}

bool ChannelShared::buildAndTransmitError(util::MultiError& multiErr, Task::Ptr const& task, bool cancelled) {
    auto qId = task->getQueryId();
    bool scanInteractive = true;
    waitTransmitLock(scanInteractive, qId);
    lock_guard<mutex> const tMtxLock(tMtx);
    // Ignore the existing transmitData object as it is irrelevant now
    // that there's an error. Create a new one to send the error.
    TransmitData::Ptr tData = createTransmit(tMtxLock, *task);
    transmitData = tData;
    transmitData->buildDataMsg(*task, multiErr);
    LOGS(_log, LOG_LVL_DEBUG, "ChannelShared::buildAndTransmitError " << dumpTransmit(tMtxLock));
    bool lastIn = true;
    return prepTransmit(tMtxLock, task, cancelled, lastIn);
}

string ChannelShared::dumpTransmit(lock_guard<mutex> const& lock) const {
    return string("ChannelShared::dumpTransmit ") +
           (transmitData == nullptr ? "nullptr" : transmitData->dump());
}

void ChannelShared::waitTransmitLock(bool interactive, QueryId const& qId) {
    if (_transmitLock != nullptr) return;
    {
        unique_lock<mutex> uLock(_transmitLockMtx);
        if (_firstTransmitLock.exchange(false)) {
            // This will wait until TransmitMgr has resources available.
            _transmitLock.reset(new wcontrol::TransmitLock(*_transmitMgr, interactive, qId));
        } else {
            _transmitLockCv.wait(uLock, [this]() { return _transmitLock != nullptr; });
        }
    }
    _transmitLockCv.notify_one();
}

void ChannelShared::initTransmit(lock_guard<mutex> const& tMtxLock, Task& task) {
    LOGS(_log, LOG_LVL_TRACE, "initTransmit " << task.getIdStr() << " seq=" << task.getTSeq());
    if (transmitData == nullptr) {
        transmitData = createTransmit(tMtxLock, task);
    }
}

TransmitData::Ptr ChannelShared::createTransmit(lock_guard<mutex> const& tMtxLock, Task& task) {
    LOGS(_log, LOG_LVL_TRACE, "createTransmit " << task.getIdStr() << " seq=" << task.getTSeq());
    auto tData = wbase::TransmitData::createTransmitData(_czarId, task.getIdStr());
    tData->initResult(task);
    return tData;
}

bool ChannelShared::prepTransmit(lock_guard<mutex> const& tMtxLock, Task::Ptr const& task, bool cancelled,
                                 bool lastIn) {
    auto qId = task->getQueryId();
    int jId = task->getJobId();

    QSERV_LOGCONTEXT_QUERY_JOB(qId, jId);
    LOGS(_log, LOG_LVL_DEBUG, "_transmit lastIn=" << lastIn);
    if (isDead()) {
        LOGS(_log, LOG_LVL_INFO, "aborting transmit since sendChannel is dead.");
        return false;
    }

    // Have all rows already been read, or an error?
    bool erred = transmitData->hasErrormsg();

    bool success = addTransmit(tMtxLock, task, cancelled, erred, lastIn, transmitData, qId, jId);

    // Now that transmitData is on the queue, reset and initialize a new one.
    transmitData.reset();
    initTransmit(tMtxLock, *task);  // reset transmitData

    return success;
}

bool ChannelShared::addTransmit(lock_guard<mutex> const& tMtxLock, Task::Ptr const& task, bool cancelled,
                                bool erred, bool lastIn, TransmitData::Ptr const& tData, int qId, int jId) {
    QSERV_LOGCONTEXT_QUERY_JOB(qId, jId);
    assert(tData != nullptr);

    // This lock may be held for a very long time.
    lock_guard<mutex> const queueMtxLock(_queueMtx);
    _transmitQueue.push(tData);

    // If _lastRecvd is true, the last message has already been transmitted and
    // this SendChannel is effectively dead.
    bool reallyLast = _lastRecvd;
    string idStr(makeIdStr(qId, jId));
    if (_icPtr == nullptr) {
        _icPtr = make_shared<util::InstanceCount>(to_string(qId) + "_SCS_LDB");
    }

    // If something bad already happened, just give up.
    if (reallyLast || isDead()) {
        // If there's been some kind of error, make sure that nothing hangs waiting
        // for this.
        LOGS(_log, LOG_LVL_WARN, "addTransmit getting messages after isDead or reallyLast " << idStr);
        _lastRecvd = true;
        return false;
    }

    // If lastIn is true, all tasks for this job have run to completion and
    // finished building their transmit messages.
    if (lastIn) {
        reallyLast = true;
    }
    if (reallyLast || erred || cancelled) {
        _lastRecvd = true;
        LOGS(_log, LOG_LVL_DEBUG,
             "addTransmit lastRecvd=" << _lastRecvd << " really=" << reallyLast << " erred=" << erred
                                      << " cancelled=" << cancelled);
    }

    return _transmit(tMtxLock, queueMtxLock, erred, task);
}

bool ChannelShared::_transmit(lock_guard<mutex> const& tMtxLock, lock_guard<mutex> const& queueMtxLock,
                              bool erred, Task::Ptr const& task) {
    string idStr = "QID?";

    // Result data is transmitted in messages containing data and headers.
    // data - is the result data
    // header - contains information about the next chunk of result data,
    //          most importantly the size of the next data message.
    //          The header has a fixed size (about 255 bytes)
    // header_END - indicates there will be no more msg.
    // msg - contains data and header.
    // metadata - special xrootd buffer that can only be set once per ChannelShared
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
    while (_transmitQueue.size() >= 2 || _lastRecvd) {
        TransmitData::Ptr thisTransmit = _transmitQueue.front();
        _transmitQueue.pop();
        if (thisTransmit == nullptr) {
            throw util::Bug(ERR_LOC, "_transmitLoop() _transmitQueue had nullptr!");
        }

        auto sz = _transmitQueue.size();
        // Is this really the last message for this SharedSendChannel?
        bool reallyLast = (_lastRecvd && sz == 0);

        TransmitData::Ptr nextTr;
        if (sz != 0) {
            nextTr = _transmitQueue.front();
            if (nextTr->getResultSize() == 0) {
                LOGS(_log, LOG_LVL_ERROR,
                     "RESULT SIZE IS 0, this should not happen thisTr=" << thisTransmit->dump()
                                                                        << " nextTr=" << nextTr->dump());
            }
        }
        uint32_t seq = _sendChannel->getSeq();
        int scsSeq = ++_scsSeq;
        string seqStr = string("seq=" + to_string(seq) + " scsseq=" + to_string(scsSeq) +
                               " scsId=" + to_string(_scsId));
        thisTransmit->attachNextHeader(nextTr, reallyLast, seq, scsSeq);

        // The first message needs to put its header data in metadata as there's
        // no previous message it could attach its header to.
        {
            lock_guard<mutex> const streamMutexLock(_streamMutex);  // Must keep meta and buffer together.
            if (_firstTransmit.exchange(false)) {
                // Put the header for the first message in metadata
                // _metaDataBuf must remain valid until Finished() is called.
                string thisHeaderString = thisTransmit->getHeaderString(seq, scsSeq - 1);
                _metadataBuf = proto::ProtoHeaderWrap::wrap(thisHeaderString);
                bool metaSet = _sendChannel->setMetadata(_metadataBuf.data(), _metadataBuf.size());
                if (!metaSet) {
                    LOGS(_log, LOG_LVL_ERROR, "Failed to setMeta " << idStr);
                    _kill(streamMutexLock, "metadata");
                    return false;
                }
            }

            // Put the data for the transmit in a StreamBuffer and send it.
            // Since the StreamBuffer's lifetime is beyond our control, it needs
            // its own Task pointer.
            auto streamBuf = thisTransmit->getStreamBuffer(task);
            streamBuf->startTimer();
            bool sent = _sendBuf(tMtxLock, queueMtxLock, streamMutexLock, streamBuf, reallyLast,
                                 "transmitLoop " + idStr + " " + seqStr, scsSeq);

            if (!sent) {
                LOGS(_log, LOG_LVL_ERROR, "Failed to send " << idStr);
                _kill(streamMutexLock, "ChannelShared::_transmit b");
                return false;
            }
        }
        // If that was the last message, break the loop.
        if (reallyLast) return true;
    }
    return true;
}

bool ChannelShared::_sendBuf(lock_guard<mutex> const& tMtxLock, lock_guard<mutex> const& queueMtxLock,
                             lock_guard<mutex> const& streamMutexLock, xrdsvc::StreamBuffer::Ptr& streamBuf,
                             bool last, string const& note, int scsSeq) {
    bool sent = _sendChannel->sendStream(streamBuf, last, scsSeq);
    if (!sent) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to transmit " << note << "!");
        return false;
    } else {
        LOGS(_log, LOG_LVL_INFO, "_sendbuf wait start " << note);
        streamBuf->waitForDoneWithThis();  // Block until this buffer has been sent.
    }
    return sent;
}

}  // namespace lsst::qserv::wbase
