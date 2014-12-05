// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#include "ccontrol/MergingRequester.h"

// System headers
#include <cassert>
// #include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
#include "global/debugUtil.h"
#include "global/MsgReceiver.h"
#include "log/msgCode.h"
#include "proto/ProtoImporter.h"
#include "proto/WorkerResponse.h"
#include "rproc/InfileMerger.h"
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
    : _msgReceiver(msgReceiver),
      _infileMerger(merger),
      _tableName(tableName),
      _response(boost::make_shared<WorkerResponse>()),
      _flushed(false) {
    _initState();
}

bool MergingRequester::flush(int bLen, bool last) {
    if((bLen < 0) || (bLen != (int)_buffer.size())) {
        if(_state != RESULT_EXTRA) {
            LOGF_ERROR("MergingRequester size mismatch: expected %1%  got %2%"
                   % _buffer.size() % bLen);
            // Worker sent corrupted data, or there is some other error.
        }
    }
    switch(_state) {
      case HEADER_SIZE_WAIT:
        // First char: sizeof protoheader. always less than 255 char.
        _response->headerSize =
            *reinterpret_cast<unsigned char const*>(&_buffer[0]);
        _buffer.resize(_response->headerSize);
        _state = HEADER_WAIT;
        return true;
      case HEADER_WAIT:
        if(!ProtoImporter<proto::ProtoHeader>::setMsgFrom(
               _response->protoHeader,
               &_buffer[0], _buffer.size())) {
            _setError(log::MSG_RESULT_DECODE, "Error decoding proto header");
            _state = HEADER_ERR;
            return false;
        }
        _buffer.resize(_response->protoHeader.size());
        _state = RESULT_WAIT;
        return true;

      case RESULT_WAIT:
        if(!_verifyResult()) { return false; }
        if(!_setResult()) { return false; }
        _buffer.resize(0); // Nothing further needed
        _state = RESULT_RECV;
        if(!last) {
            _buffer.resize(1); // Need to make sure nothing is left
            _state = RESULT_EXTRA;
        }
        LOGF_INFO("Flushed last for tableName=%1%" % _tableName);
        return _merge();

      case RESULT_EXTRA: // future: handle multiple Result messages
        if((bLen != 0) || !last) {
            LOGF_INFO("Extra results for tableName=%1% size=%2%"
                      % _tableName % bLen);
        } else {
            _state = RESULT_LAST;
            return true;
        }
        // Fall-through
      case RESULT_RECV:
      case HEADER_ERR:
      case RESULT_ERR:
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


std::ostream& MergingRequester::print(std::ostream& os) const {
    return os << "MergingRequester(" << _tableName << ", flushed="
              << (_flushed ? "true)" : "false)") ;
}
////////////////////////////////////////////////////////////////////////
// MergingRequester private
////////////////////////////////////////////////////////////////////////

void MergingRequester::_initState() {
    _buffer.resize(1);
    // _response.reset(new WorkerResponse);// Will be overwritten
    _state = HEADER_SIZE_WAIT;
    _setError(0, "");
}

bool MergingRequester::_merge() {
    if(_flushed) {
        throw Bug("MergingRequester::_merge : already flushed");
    }
    bool success = _infileMerger->merge(_response);
    if(success) {
        _response.reset();
    } else {
        // Fetch error from merger?
        _setError(log::MSG_MERGE_ERROR, "Error merging.");
        _state = RESULT_ERR;
    }
    return success;
}

void MergingRequester::_setError(int code, std::string const& msg) {
    boost::lock_guard<boost::mutex> lock(_errorMutex);
    _error.code = code;
    _error.msg = msg;
}

bool MergingRequester::_setResult() {
    if(!ProtoImporter<proto::Result>::setMsgFrom(
           _response->result,
           &_buffer[0], _buffer.size())) {
        _setError(log::MSG_RESULT_DECODE, "Error decoding result msg");
        _state = RESULT_ERR;
        return false;
    }
    return true;
}
bool MergingRequester::_verifyResult() {
    if(_response->protoHeader.md5() !=
       util::StringHash::getMd5(_buffer.data(), _buffer.size())) {
        _setError(log::MSG_RESULT_MD5, "Result message MD5 mismatch");
        _state = RESULT_ERR;
        return false;
    }
    return true;
}

}}} // lsst::qserv::ccontrol
