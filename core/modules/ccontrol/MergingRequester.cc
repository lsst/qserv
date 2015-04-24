// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include "ccontrol/MergingRequester.h"

// System headers
#include <cassert>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
#include "global/debugUtil.h"
#include "global/MsgReceiver.h"
#include "log/msgCode.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/ProtoImporter.h"
#include "proto/WorkerResponse.h"
#include "rproc/InfileMerger.h"
#include "util/common.h"
#include "util/StringHash.h"

using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::Result;
using lsst::qserv::proto::WorkerResponse;

namespace lsst {
namespace qserv {
namespace ccontrol {
////////////////////////////////////////////////////////////////////////
// MergingRequester public
////////////////////////////////////////////////////////////////////////
MergingRequester::MergingRequester(
    boost::shared_ptr<MsgReceiver> msgReceiver,
    boost::shared_ptr<rproc::InfileMerger> merger,
    std::string const& tableName)
    : _msgReceiver{msgReceiver},
      _infileMerger{merger},
      _tableName(tableName),
      _response{new WorkerResponse()},
      _flushed{false},
      _cancelled{false} {
    _initState();
}

const char* MergingRequester::getStateStr(MsgState const& state) {
    switch(state) {
    case MsgState::INVALID:          return "INVALID";
    case MsgState::HEADER_SIZE_WAIT: return "HEADER_SIZE_WAIT";
    case MsgState::RESULT_WAIT:      return "RESULT_WAIT";
    case MsgState::RESULT_RECV:      return "RESULT_RECV";
    case MsgState::RESULT_EXTRA:     return "RESULT_EXTRA";
    case MsgState::BUFFER_DRAIN:     return "BUFFER_DRAIN";
    case MsgState::HEADER_ERR:       return "HEADER_ERR";
    case MsgState::RESULT_ERR:       return "RESULT_ERR";
    }
    return "unknown";
}

bool MergingRequester::flush(int bLen, bool last) {
    LOGF_INFO("flush state=%1% blen=%2% last=%3%" % getStateStr(_state) % bLen % last);
    if((bLen < 0) || (bLen != (int)_buffer.size())) {
        if(_state != MsgState::RESULT_EXTRA) {
            LOGF_ERROR("MergingRequester size mismatch: expected %1%  got %2%"
                   % _buffer.size() % bLen);
            // Worker sent corrupted data, or there is some other error.
        }
    }
    switch(_state) {
    case MsgState::HEADER_SIZE_WAIT:
        _response->headerSize = static_cast<unsigned char>(_buffer[0]);
        if (!proto::ProtoHeaderWrap::unwrap(_response, _buffer)) {
            _setError(log::MSG_RESULT_DECODE,
                std::string("Error decoding proto header for ") + getStateStr(_state));
            _state = MsgState::HEADER_ERR;
            return false;
        }
        LOGF_DEBUG("HEADER_SIZE_WAIT: Resizing buffer to %1%" % _response->protoHeader.size());
        _buffer.resize(_response->protoHeader.size());
        _state = MsgState::RESULT_WAIT;
        return true;

    case MsgState::RESULT_WAIT:
        if(!_verifyResult()) { return false; }
        if(!_setResult()) { return false; }
        LOGF_INFO("_buffer %1%" % util::prettyCharList(_buffer, 5));
        {
            bool msgContinues = _response->result.continues();
            _buffer.resize(0); // Nothing further needed
            _state = MsgState::RESULT_RECV;
            if(msgContinues) {
                LOGF_INFO("Messages continues, waiting for next header.");
                _state = MsgState::RESULT_EXTRA;
                _buffer.resize(proto::ProtoHeaderWrap::PROTO_HEADER_SIZE);
            }
            LOGF_INFO("Flushed msgContinues=%1% last=%2% for tableName=%3%" %
                    msgContinues % last % _tableName);

            auto success = _merge();
            if(msgContinues) {
                _response.reset(new WorkerResponse());
            }
            return success;
        }
    case MsgState::RESULT_EXTRA:
        if (!proto::ProtoHeaderWrap::unwrap(_response, _buffer)) {
            _setError(log::MSG_RESULT_DECODE,
                    std::string("Error decoding proto header for ") + getStateStr(_state));
            _state = MsgState::HEADER_ERR;
            return false;
        }
        LOGF_INFO("RESULT_EXTRA: Resizing buffer to %1%" % _response->protoHeader.size());
        _buffer.resize(_response->protoHeader.size());
        _state = MsgState::RESULT_WAIT;
        return true;
    case MsgState::RESULT_RECV:
        LOGF_DEBUG("RESULT_RECV last = %1%" % last);
        if (!last) {
            _state = MsgState::BUFFER_DRAIN;
            _buffer.resize(1);
        }
        return true;
    case MsgState::BUFFER_DRAIN:
        // The buffer should always be empty, but last is not always set to true by xrootd
        // unless we ask xrootd to read at least one character.
        LOGF_INFO("BUFFER_DRAIN last=%1% bLen=%2% buffer=%3%" %
                last % bLen % util::prettyCharList(_buffer));
        _buffer.resize(1);
        return true;
    case MsgState::HEADER_ERR:
    case MsgState::RESULT_ERR:
        _setError(log::MSG_RESULT_ERROR, "Unexpected message");
        return false;
    default:
        break;
    }
    _setError(log::MSG_RESULT_ERROR, "Unexpected message (invalid)");
    return false;
}

void MergingRequester::errorFlush(std::string const& msg, int code) {
    _setError(code, msg);
    // Might want more info from result service.
    // Do something about the error. FIXME.
    LOGF_ERROR("Error receiving result.");
}

bool MergingRequester::finished() const {
    return _flushed;
}

bool MergingRequester::reset() {
    // If we've pushed any bits to the merger successfully, we have to undo them
    // to reset to a fresh state. For now, we will just fail if we've already
    // begun merging. If we implement the ability to retract a partial result
    // merge, then we can use it and do something better.
    if(_flushed) {
        return false; // Can't reset if we have already pushed state.
    }
    _initState();
    return true;
}

void MergingRequester::cancel() {
    {
        boost::lock_guard<boost::mutex> lock(_cancelledMutex);
        _setError(log::MSG_EXEC_SQUASHED, "Cancellation requested");
        _cancelled = true;
    }
    _callCancel(); // Pass cancellation down to worker.
}

std::ostream& MergingRequester::print(std::ostream& os) const {
    return os << "MergingRequester(" << _tableName << ", flushed="
              << (_flushed ? "true)" : "false)") ;
}
////////////////////////////////////////////////////////////////////////
// MergingRequester private
////////////////////////////////////////////////////////////////////////

void MergingRequester::_initState() {
    _buffer.resize(proto::ProtoHeaderWrap::PROTO_HEADER_SIZE);
    _state = MsgState::HEADER_SIZE_WAIT;
    _setError(0, "");
}

bool MergingRequester::_merge() {
    boost::lock_guard<boost::mutex> lock(_cancelledMutex);
    if(_cancelled) {
        LOGF_INFO("MergingRequester::_merge(), but already cancelled");
        return false;
    }
    if(_flushed) {
        throw Bug("MergingRequester::_merge : already flushed");
    }
    bool success = _infileMerger->merge(_response);
    if(!success) {
        rproc::InfileMergerError const& err = _infileMerger->getError();
        _setError(log::MSG_RESULT_ERROR, err.description);
        _state = MsgState::RESULT_ERR;
    }
    _response.reset();
    return success;
}

void MergingRequester::_setError(int code, std::string const& msg) {
    LOGF_INFO("setError");
    LOGF_INFO("setError %1% %2%" % code % msg);
    boost::lock_guard<boost::mutex> lock(_errorMutex);
    _error.code = code;
    _error.msg = msg;
}

bool MergingRequester::_setResult() {
    if(!ProtoImporter<proto::Result>::setMsgFrom(_response->result, &_buffer[0], _buffer.size())) {
        _setError(log::MSG_RESULT_DECODE, "Error decoding result msg");
        _state = MsgState::RESULT_ERR;
        return false;
    }
    return true;
}
bool MergingRequester::_verifyResult() {
    if(_response->protoHeader.md5() != util::StringHash::getMd5(_buffer.data(), _buffer.size())) {
        _setError(log::MSG_RESULT_MD5, "Result message MD5 mismatch");
        _state = MsgState::RESULT_ERR;
        return false;
    }
    return true;
}

}}} // lsst::qserv::ccontrol
