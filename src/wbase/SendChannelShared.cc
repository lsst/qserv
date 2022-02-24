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
#include "wbase/Task.h"
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

atomic<uint64_t> SendChannelShared::scsSeqId{0};


SendChannelShared::Ptr SendChannelShared::create(SendChannel::Ptr const& sendChannel,
                                                 wcontrol::TransmitMgr::Ptr const& transmitMgr)  {
    auto scs = shared_ptr<SendChannelShared>(new SendChannelShared(sendChannel, transmitMgr));
    return scs;
}


SendChannelShared::SendChannelShared(SendChannel::Ptr const& sendChannel,
                   std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr)
         : _sendChannel(sendChannel), _transmitMgr(transmitMgr), _scsId(scsSeqId++) {
     if (_sendChannel == nullptr) {
         throw Bug("SendChannelShared constructor given nullptr");
     }
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


bool SendChannelShared::_kill(StreamGuard sLock, std::string const& note) {
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


bool SendChannelShared::_addTransmit(bool cancelled, bool erred, bool last, bool largeResult,
                                    TransmitData::Ptr const& tData, int qId, int jId) {
    QSERV_LOGCONTEXT_QUERY_JOB(qId, jId);
    LOGS(_log, LOG_LVL_WARN, "&&& SendChannelShared::_addTransmit a");
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
        lock_guard<mutex> streamLock(_streamMutex);
        reallyLast = transmitTaskLast(streamLock, last);
    }

    if (reallyLast || erred || cancelled) {
        _lastRecvd = true;
        LOGS(_log, LOG_LVL_DEBUG, "addTransmit lastRecvd=" << _lastRecvd << " really=" << reallyLast
                                  << " erred=" << erred << " cancelled=" << cancelled);
    }

    // If this is reallyLast or at least 2 items are in the queue, the transmit can happen
    if (_lastRecvd || _transmitQueue.size() >= 2) {
        bool sendNow = tData->getScanInteractive();
        // If there was an error, give this high priority.
        if (erred || cancelled) sendNow = true;
        int czarId = tData->getCzarId();
        return _transmit(erred, sendNow, largeResult, czarId);
    } else {
        // Not enough information to transmit. Maybe there will be with the next call
        // to addTransmit.
    }
    return true;
}


util::TimerHistogram scsTransmitSend("scsTransmitSend", {0.01, 0.1, 1.0, 2.0, 5.0, 10.0, 20.0});

bool SendChannelShared::_transmit(bool erred, bool scanInteractive, QueryId const qid, qmeta::CzarId czarId) { //&&& rename scanInteractive to sendNow
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
        }
        //&&&if (thisTransmit->result == nullptr) {
        //&&&    throw Bug("_transmitLoop() had nullptr result!");
        //&&&}

        auto sz = _transmitQueue.size();
        // Is this really the last message for this SharedSendChannel?
        bool reallyLast = (_lastRecvd && sz == 0);


        TransmitData::Ptr nextTr;
        if (sz != 0) {
            nextTr = _transmitQueue.front();
        }
        uint32_t seq = _sendChannel->getSeq();
        int scsSeq = ++_scsSeq;
        string seqStr = string("seq=" + to_string(seq) + " scsseq=" + to_string(scsSeq)
                             + " scsId=" + to_string(_scsId));
        thisTransmit->attachNextHeader(nextTr, reallyLast, seq, scsSeq);
        /* &&&
        idStr = makeIdStr(thisTransmit->result->queryid(), thisTransmit->result->jobid());
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
        string seqStr = string("seq=" + to_string(seq) + " scsseq=" + to_string(scsSeq)
                               + " scsId=" + to_string(_scsId));
        nextPHdr->set_endnodata(reallyLast);
        nextPHdr->set_seq(seq);
        nextPHdr->set_scsseq(scsSeq);
        string nextHeaderString;
        nextPHdr->SerializeToString(&nextHeaderString);
        thisTransmit->dataMsg += proto::ProtoHeaderWrap::wrap(nextHeaderString);
        &&& */

        // The first message needs to put its header data in metadata as there's
        // no previous message it could attach its header to.
        {
            lock_guard<mutex> streamLock(_streamMutex); // Must keep meta and buffer together.
            if (_firstTransmit.exchange(false)) {
                // Put the header for the first message in metadata
                // _metaDataBuf must remain valid until Finished() is called.
                /* &&&
                proto::ProtoHeader* thisPHdr = thisTransmit->header;
                thisPHdr->set_seq(seq);
                thisPHdr->set_scsseq(scsSeq - 1); // should always be 0
                string thisHeaderString;
                thisPHdr->SerializeToString(&thisHeaderString);
                _metadataBuf = proto::ProtoHeaderWrap::wrap(thisHeaderString);
                */
                _metadataBuf = thisTransmit->getHeaderString(seq, scsSeq - 1);
                bool metaSet = _sendChannel->setMetadata(_metadataBuf.data(), _metadataBuf.size());
                if (!metaSet) {
                    LOGS(_log, LOG_LVL_ERROR, "Failed to setMeta " << idStr);
                    _kill(streamLock, "metadata");
                    return false;
                }
            }

            // Put the data for the transmit in a StreamBuffer and send it.
            //&&& auto streamBuf = xrdsvc::StreamBuffer::createWithMove(thisTransmit->dataMsg);
            auto streamBuf = thisTransmit->getStreamBuffer();
            {
                util::Timer sendTimer;
                sendTimer.start();
                bool sent = _sendBuf(streamLock, streamBuf, reallyLast, "transmitLoop " + idStr + " " + seqStr, scsSeq);
                sendTimer.stop();
                auto logMsgSend = scsTransmitSend.addTime(sendTimer.getElapsed(), idStr);
                LOGS(_log, LOG_LVL_INFO, logMsgSend);
                if (!sent) {
                    LOGS(_log, LOG_LVL_ERROR, "Failed to send " << idStr);
                    _kill(streamLock, "SendChannelShared::_transmit b");
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
        LOGS(_log, LOG_LVL_INFO, "_sendbuf wait start " << note);
        streamBuf->waitForDoneWithThis(); // Block until this buffer has been sent.
        t.stop();
        auto logMsg = transmitHisto.addTime(t.getElapsed(), note);
        LOGS(_log, LOG_LVL_DEBUG, logMsg);
    }
    return sent;
}


TransmitData::Ptr SendChannelShared::buildError(qmeta::CzarId const& czarId, Task& task,
                                                util::MultiError& multiErr) {
    // Ignore the existing _transmitData object as it is irrelevant now
    // that there's an error. Just create a new one to send the error.
    //&&&TransmitData::Ptr tData = wbase::TransmitData::createTransmitData(czarId);
    //&&&tData->initResult(task, schemaCols); //&&&tData->_result = _initResult(); //&&&

    TransmitData::Ptr tData = _createTransmit(task, czarId);
    bool largeResult = false;
    tData->buildDataMsg(task, largeResult, multiErr);
    return tData;
    /* &&&
    bool res = _transmit(true); //&&& _transmit is QueryRunnerr::_transmit(bool)
    if (!res) {
        LOGS(_log, LOG_LVL_ERROR, "SendChannelShared::transmitError Could not report error to czar.");
    }
    return res;
    */
}


void SendChannelShared::setSchemaCols(Task& task, std::vector<SchemaCol>& schemaCols) {
    // _schemaCols should be identical for all tasks in this send channel.
    if (_schemaColsSet.exchange(true) == false) {
        _schemaCols = schemaCols;
        // If this is the first time _schemaCols has been set, it is missing
        // from the existing _transmitData object
        lock_guard<mutex> lock(_tMtx);
        if (_transmitData != nullptr) {
            _transmitData->addSchemaCols(_schemaCols);
        }
    }
}


void SendChannelShared::initTransmit(Task& task, qmeta::CzarId const& czarId) {
    //&&& czarID should be the same for all of these, we should be able to store a copy in SendChannelShared at creation.
    lock_guard<mutex> lock(_tMtx);
    _initTransmit(task, czarId);
}


void SendChannelShared::_initTransmit(Task& task, qmeta::CzarId const& czarId) {
    //&&& czarID should be the same for all of these, we should be able to store a copy in SendChannelShared at creation.
    LOGS(_log, LOG_LVL_WARN, "&&& SendChannelShared::_initTransmit a");
    if (_transmitData == nullptr) {
        _transmitData = _createTransmit(task, czarId);
    }
}


TransmitData::Ptr SendChannelShared::_createTransmit(Task& task, qmeta::CzarId const& czarId) {
    auto tData = wbase::TransmitData::createTransmitData(czarId);
    tData->initResult(task, _schemaCols);;
    return tData;
}


bool SendChannelShared::qrTransmit(Task& task, wcontrol::TransmitMgr& transmitMgr,
                                   wbase::TransmitData::Ptr const& tData,
                                   bool cancelled, bool largeResult, bool lastIn,
                                   qmeta::CzarId const& czarId) {
    LOGS(_log, LOG_LVL_WARN, "&&& SendChannelShared::qrTransmit a");
    auto qId = task.getQueryId();
    int jId = task.getJobId();
    bool scanInteractive = task.getScanInteractive();

    QSERV_LOGCONTEXT_QUERY_JOB(qId, jId);
    LOGS(_log, LOG_LVL_DEBUG, "_transmit lastIn=" << lastIn);
    if (isDead()) {
        LOGS(_log, LOG_LVL_INFO, "aborting transmit since sendChannel is dead.");
        return false;
    }

    waitTransmitLock(transmitMgr, scanInteractive, qId);
    lock_guard<mutex> lock(_tMtx);
    // Have all rows already been read, or an error?
    //&&&bool erred = tData->result->has_errormsg();
    bool erred = tData->hasErrormsg();

    //&&& tData->scanInteractive = scanInteractive;
    //&&& tData->erred = erred;
    //&&& tData->largeResult = largeResult;
    tData->setFinalValues(scanInteractive, erred, largeResult);

    //&&&bool success = _task->getSendChannel()->addTransmit(_cancelled, erred, lastIn, _largeResult, _transmitData, qId, jId);
    bool success = _addTransmit(cancelled, erred, lastIn, largeResult, _transmitData, qId, jId);

    // Now that _transmitData is on the queue, reset and initialize a new one.
    _transmitData.reset();
    _initTransmit(task, czarId); // reset _transmitData  //&&& reset initTransmit

    // Large results get priority, but new large results should not get priority until
    // after they have started transmitting.
    //&&&_largeResult = true;
    return success;
}


bool SendChannelShared::putRowsInTransmits(MYSQL_RES* mResult, int numFields, Task& task, bool largeResult,
                                           util::MultiError& multiErr, std::atomic<bool>& cancelled,bool &readRowsOk,
                                           qmeta::CzarId const& czarId) {
    LOGS(_log, LOG_LVL_WARN, "&&& SendChannelShared::putRowsInTransmits a");
    lock_guard<mutex> lock(_tMtx);
    bool erred = false;
    size_t tSize = 0;
    // If fillRows returns false, _transmitData is full and needs to be transmitted
    // fillRows returns true when there are no more rows in mResult to add.
    // tSize is set by fillRows.
    while (!_transmitData->fillRows(mResult, numFields, tSize)) {
        LOGS(_log, LOG_LVL_WARN, "&&& SendChannelShared::putRowsInTransmits b");
        if (tSize > proto::ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) {
            LOGS_ERROR("Message single row too large to send using protobuffer");
            erred = true;
            break;
        }
        LOGS(_log, LOG_LVL_TRACE, "Splitting message size=" << tSize);
        //_buildDataMsg(rowCount, tSize);
        _transmitData->buildDataMsg(task, largeResult, multiErr);
        // If readRowsOk==false, empty out the rows but don't bother trying to transmit.
        // This needs to be done or mariadb won't properly release the resources.
        //&&&if (readRowsOk && !_transmit(false)) {
        bool lastInQuery = false;
        if (readRowsOk && !qrTransmit(task, *_transmitMgr, _transmitData,
                                      cancelled, largeResult, lastInQuery, czarId)) {
            LOGS(_log, LOG_LVL_ERROR, "Could not transmit intermediate results.");
            readRowsOk = false; // Empty the fillRows data and then return false.
        }
        //&&&// Now that _transmitData is on the queue, reset and initialize a new one.
        //&&&_transmitData.reset();
        //&&&initTransmit(task, czarId); // reset _transmitData  //&&& reset initTransmit
    }
    return erred;
}


}}} // namespace lsst::qserv::wbase
