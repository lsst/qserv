// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
#include "ccontrol/MergingHandler.h"

// System headers
#include <cassert>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/msgCode.h"
#include "global/Bug.h"
#include "global/debugUtil.h"
#include "global/MsgReceiver.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/ProtoImporter.h"
#include "proto/WorkerResponse.h"
#include "qdisp/JobQuery.h"
#include "rproc/InfileMerger.h"
#include "util/common.h"
#include "util/StringHash.h"

using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::Result;
using lsst::qserv::proto::WorkerResponse;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.MergingHandler");
}


namespace lsst {
namespace qserv {
namespace ccontrol {

std::atomic<std::int64_t> MergeBuffer::_totalBytes{0};
std::atomic<int> MergeBuffer::_sequence{0};

////////////////////////////////////////////////////////////////////////
// MergingHandler public
////////////////////////////////////////////////////////////////////////
MergingHandler::MergingHandler(
    std::shared_ptr<MsgReceiver> msgReceiver,
    std::shared_ptr<rproc::InfileMerger> merger,
    std::string const& tableName)
    : _msgReceiver{msgReceiver}, _infileMerger{merger}, _tableName{tableName},
      _response{new WorkerResponse()} {
    _initState();
}

MergingHandler::~MergingHandler() {
    LOGS(_log, LOG_LVL_DEBUG, "~MergingHandler()");
}

const char* MergingHandler::getStateStr(MsgState const& state) {
    switch(state) {
    case MsgState::INVALID:          return "INVALID";
    case MsgState::HEADER_SIZE_WAIT: return "HEADER_SIZE_WAIT";
    case MsgState::RESULT_WAIT:      return "RESULT_WAIT";
    case MsgState::RESULT_RECV:      return "RESULT_RECV";
    case MsgState::RESULT_EXTRA:     return "RESULT_EXTRA";
    case MsgState::HEADER_ERR:       return "HEADER_ERR";
    case MsgState::RESULT_ERR:       return "RESULT_ERR";
    }
    return "unknown";
}

bool MergingHandler::flush(int bLen, bool& last, bool& largeResult) {
    LOGS(_log, LOG_LVL_DEBUG, "From:" << _wName << " flush state="
         << getStateStr(_state) << " blen=" << bLen << " last=" << last);
    if ((bLen < 0) || (bLen != (int)_mBuf.getSize())) {
        if (_state != MsgState::RESULT_EXTRA) {
            LOGS(_log, LOG_LVL_ERROR, "MergingRequester size mismatch: expected "
                 << _mBuf.getSize() << " got " << bLen);
            // Worker sent corrupted data, or there is some other error.
        }
    }
    switch(_state) {
    case MsgState::HEADER_SIZE_WAIT:
        _response->headerSize = static_cast<unsigned char>((_mBuf.getBuffer())[0]);
        if (!proto::ProtoHeaderWrap::unwrap(_response, _mBuf.getBuffer())) {
            std::string s = "From:" + _wName + "Error decoding proto header for " + getStateStr(_state);
            _setError(ccontrol::MSG_RESULT_DECODE, s);
            _state = MsgState::HEADER_ERR;
            return false;
        }
        if (_wName == "~") {
            _wName = _response->protoHeader.wname();
        }

        LOGS(_log, LOG_LVL_DEBUG, "HEADER_SIZE_WAIT: From:" << _wName
             << "Resizing buffer to " <<  _response->protoHeader.size());
        _mBuf.zero(); // Free memory.
        _mBuf.setTargetSize(_response->protoHeader.size());
        largeResult = _response->protoHeader.largeresult();
        _state = MsgState::RESULT_WAIT;
        return true;

    case MsgState::RESULT_WAIT:
        if (!_verifyResult()) { return false; }
        if (!_setResult()) { return false; } // set _response->result
        largeResult = _response->result.largeresult();
        LOGS(_log, LOG_LVL_DEBUG, "From:" << _wName << " _mBuf "
             << util::prettyCharList(_mBuf.getBuffer(), 5));
        _mBuf.zero(); // not needed after _response->result set.
        {
            bool msgContinues = _response->result.continues();
            _state = MsgState::RESULT_RECV;
            if (msgContinues) {
                LOGS(_log, LOG_LVL_DEBUG, "Message continues, waiting for next header.");
                _state = MsgState::RESULT_EXTRA;
                _mBuf.setTargetSize(proto::ProtoHeaderWrap::PROTO_HEADER_SIZE);
            } else {
                LOGS(_log, LOG_LVL_DEBUG, "Message ends, setting last=true");
                last = true;
            }
            LOGS(_log, LOG_LVL_DEBUG, "Flushed msgContinues=" << msgContinues
                 << " last=" << last << " for tableName=" << _tableName);

            auto success = _merge();
            if (msgContinues) {
                _response.reset(new WorkerResponse());
            }
            return success;
        }
    case MsgState::RESULT_EXTRA:
        if (!proto::ProtoHeaderWrap::unwrap(_response, _mBuf.getBuffer())) {
            _setError(ccontrol::MSG_RESULT_DECODE,
                      std::string("Error decoding proto header for ") + getStateStr(_state));
            _state = MsgState::HEADER_ERR;
            return false;
        }
        largeResult = _response->protoHeader.largeresult();
        LOGS(_log, LOG_LVL_DEBUG, "RESULT_EXTRA: Resizing buffer to "
             << _response->protoHeader.size() << " largeResult=" << largeResult);
        _mBuf.zero();
        _mBuf.setTargetSize(_response->protoHeader.size());
        _state = MsgState::RESULT_WAIT;
        return true;
    case MsgState::RESULT_RECV:
        // We shouldn't wind up here. _buffer.size(0) and last=true should end communication.
        // fall-through
    case MsgState::HEADER_ERR:
    case MsgState::RESULT_ERR:
         {
            std::ostringstream eos;
            eos << "Unexpected message From:" << _wName << " flush state="
                << getStateStr(_state) << " last=" << last;
            LOGS(_log, LOG_LVL_ERROR, eos.str());
            _setError(ccontrol::MSG_RESULT_ERROR, eos.str());
         }
        return false;
    default:
        break;
    }
    _setError(ccontrol::MSG_RESULT_ERROR, "Unexpected message (invalid)");
    return false;
}

void MergingHandler::errorFlush(std::string const& msg, int code) {
    _setError(code, msg);
    // Might want more info from result service.
    // Do something about the error. FIXME.
    LOGS(_log, LOG_LVL_ERROR, "Error receiving result.");
}

bool MergingHandler::finished() const {
    return _flushed;
}

bool MergingHandler::reset() {
    // If we've pushed any bits to the merger successfully, we have to undo them
    // to reset to a fresh state. For now, we will just fail if we've already
    // begun merging. If we implement the ability to retract a partial result
    // merge, then we can use it and do something better.
    if (_flushed) {
        return false; // Can't reset if we have already pushed state.
    }
    _initState();
    return true;
}


bool MergingHandler::prepScrubResults(int jobId, int attempt) {
    int jobIdAttempt = makeJobIdAttempt(int jobId, int attemptCount);
    return _infileMerger->prepScrub(jobIdAttempt);
}

std::ostream& MergingHandler::print(std::ostream& os) const {
    return os << "MergingRequester(" << _tableName << ", flushed="
              << (_flushed ? "true)" : "false)") ;
}
////////////////////////////////////////////////////////////////////////
// MergingRequester private
////////////////////////////////////////////////////////////////////////

void MergingHandler::_initState() {
    _mBuf.setTargetSize(proto::ProtoHeaderWrap::PROTO_HEADER_SIZE);
    _state = MsgState::HEADER_SIZE_WAIT;
    _setError(0, "");
}

bool MergingHandler::_merge() {
    if (auto job = getJobQuery().lock()) {
        if (job->isQueryCancelled()) {
            LOGS(_log, LOG_LVL_WARN, "MergingRequester::_merge(), but already cancelled");
            return false;
        }
        if (_flushed) {
            throw Bug("MergingRequester::_merge : already flushed");
        }
        bool success = _infileMerger->merge(_response);
        if (!success) {
            LOGS(_log, LOG_LVL_WARN, "_merge() failed");
            rproc::InfileMergerError const& err = _infileMerger->getError();
            _setError(ccontrol::MSG_RESULT_ERROR, err.getMsg());
            _state = MsgState::RESULT_ERR;
        }
        _response.reset();
        return success;
    }
    LOGS(_log, LOG_LVL_ERROR, "MergingHandler::_merge() failed, jobQuery was NULL");
    return false;
}

void MergingHandler::_setError(int code, std::string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, "_setErr: code: " << code << ", message: " << msg);
    std::lock_guard<std::mutex> lock(_errorMutex);
    _error = Error(code, msg);
}

bool MergingHandler::_setResult() {
    auto start = std::chrono::system_clock::now();
    auto buff = _mBuf.getBuffer();
    if (!ProtoImporter<proto::Result>::setMsgFrom(_response->result, &((buff)[0]), _mBuf.getSize())) {
        _setError(ccontrol::MSG_RESULT_DECODE, "Error decoding result msg");
        _state = MsgState::RESULT_ERR;
        return false;
    }
    auto protoEnd = std::chrono::system_clock::now();
    auto protoDur = std::chrono::duration_cast<std::chrono::milliseconds>(protoEnd - start);
    LOGS(_log, LOG_LVL_DEBUG, "protoDur=" << protoDur.count());
    return true;
}
bool MergingHandler::_verifyResult() {
    auto buff = _mBuf.getBuffer();
    if (_response->protoHeader.md5() != util::StringHash::getMd5(buff.data(), _mBuf.getSize())) {
        _setError(ccontrol::MSG_RESULT_MD5, "Result message MD5 mismatch");
        _state = MsgState::RESULT_ERR;
        return false;
    }
    return true;
}


MergeBuffer::~MergeBuffer() {
    if (_buff != nullptr && _buff->size() != 0) {
        _totalBytes -= _buff->size();
        LOGS(_log, LOG_LVL_DEBUG, _id << " ~ totalBytes=" << _totalBytes);
    }
}


MergeBuffer::bufType& MergeBuffer::getBuffer() {
    if (_buff == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, _id << " getBuffer making buffer");
        zero();
    }
    return *_buff;
}


size_t MergeBuffer::getSize() {
    if (_buff == nullptr) return 0;
    return _buff->size();
}


void MergeBuffer::setTargetSize(int sz) {
    _targetSize = sz;
    LOGS(_log, LOG_LVL_DEBUG, _id << " set targetSize=" << sz);
}


void MergeBuffer::resizeToTargetSize() {
    LOGS(_log, LOG_LVL_DEBUG, _id << " resizeToTargetSize targetSize=" << _targetSize);
    _resize(_targetSize);
}


void MergeBuffer::zero() {
    setTargetSize(0);
    if (_buff != nullptr && _buff->size() != 0) {
        _totalBytes -= _buff->size();
        LOGS(_log, LOG_LVL_DEBUG, _id << " zero totalBytes=" << _totalBytes);
    }
    // Just resizing to 0 would not guarantee freeing the memory.
    _buff.reset(new bufType(0));
 }


 void MergeBuffer::_resize(int sz) {
     if (sz != (int)_buff->size()) {
         _totalBytes += sz - _buff->size();
         _buff->resize(sz);
         LOGS(_log, LOG_LVL_DEBUG, _id << " resize totalBytes=" << _totalBytes);
     } else if (sz != 0) {
         LOGS(_log, LOG_LVL_WARN, _id << " resize called twice sz=" << sz << " totalBytes=" << _totalBytes);
     }
 }

}}} // lsst::qserv::ccontrol
