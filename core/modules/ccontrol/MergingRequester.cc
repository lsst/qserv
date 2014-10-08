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
//#include "qdisp/ResponseRequ
using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::Result;
using lsst::qserv::proto::WorkerResponse;

namespace lsst {
namespace qserv {
namespace ccontrol {
#if 0
    if(resultSize != _readResult(msgs->result, cursor, resultSize)) {
        return 0;
    }
    remain -= resultSize;
    if(!_verifySession(msgs->result.session())) {
        return 0;
    }
    // Potentially, we can skip verifying the hash if the result reports
    // no rows, but if the hash disagrees, then the schema is suspect as well.
    if(!_verifyMd5(msgs->protoHeader.md5(),
                   util::StringHash::getMd5(cursor, resultSize))) {
        return -1;
    }
    if(setupResult) {
        if(!_setupTable(*msgs)) {
            return -1;
        }
    }
    // Check for the no-row condition
    if(msgs->result.row_size() == 0) {

        // Nothing further, don't bother importing
    } else {
        // Delegate merging thread mgmt to mgr, which will handle
        // LocalInfile objects
        _mgr->enqueueAction(msgs);
    }
    return length;
#endif
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
      _flushed(false),
      _response(new WorkerResponse) {
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
              _error.code = -1;
              _error.msg = "Error decoding proto header";
              _state = HEADER_ERR;
              return false;
          }
          _buffer.resize(_response->protoHeader.size());
          _state = RESULT_WAIT;
          return true;

      case RESULT_WAIT:
          if(!ProtoImporter<proto::Result>::setMsgFrom(
                 _response->result,
                 &_buffer[0], _buffer.size())) {
              _error.code = -1;
              _error.msg = "Error decoding result msg";
              _state = RESULT_ERR;
              return false;
          }
          _buffer.resize(0); // Nothing further needed
          _state = RESULT_RECV;
          if(!last) {
              _buffer.resize(1); // Need to make sure nothing is left
              _state = RESULT_EXTRA;
          }
        LOGF_INFO("Flushed last for tableName=%1%" % _tableName);
        return _merge();
      case RESULT_EXTRA:
          if((bLen != 0) || !last) {
              LOGF_INFO("Extra results for tableName=%1% size=%2%"
                        % _tableName % bLen);
          } else {
              _state = RESULT_LAST;
              return true;
          }
      case RESULT_RECV:
      case HEADER_ERR:
      case RESULT_ERR:
          _error.code = -1;
          _error.msg = "Unexpected message";
          return false;          
      default:
          break;
    }
    _error.code = -1;
    _error.msg = "Unexpected message (invalid)";
    return false;          
}

void MergingRequester::errorFlush(std::string const& msg, int code) {
    // Might want more info from result service.
    // Do something about the error. FIXME.
    _error.msg = msg;
    _error.code = code;
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
    os << "MergingRequester(" << _tableName << ", flushed="
       << (_flushed ? "true)" : "false)") ;
    return os;
}
////////////////////////////////////////////////////////////////////////
// MergingRequester private
////////////////////////////////////////////////////////////////////////

void MergingRequester::_initState() {
    _buffer.resize(1);
    // _response.reset(new WorkerResponse);// Will be overwritten
    _state = HEADER_SIZE_WAIT;
    _error.code = 0;
    _error.msg = "";
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
        _error.code = -1;
        _error.msg = "Error merging.";
        _state = RESULT_ERR;
    }
    return success;
}

}}} // lsst::qserv::ccontrol
