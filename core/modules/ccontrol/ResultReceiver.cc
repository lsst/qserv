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
#include "ccontrol/ResultReceiver.h"

// System headers
#include <cassert>
#include <cstring> // For memmove()
#include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/debugUtil.h"
#include "global/MsgReceiver.h"
#include "log/msgCode.h"
#include "rproc/InfileMerger.h"
#include "rproc/TableMerger.h"

using lsst::qserv::qdisp::QueryReceiver;

namespace lsst {
namespace qserv {
namespace ccontrol {
// Buffer needs to be big enough to hold largest (in bytes) sql statement from
// worker mysqldump. 128kB not enough. Probably want as big as
// max_allowed_packet on mysqld/mysqlclient
int const ResultReceiver_bufferSize = 2*1024*1024; // 2 megabytes.

////////////////////////////////////////////////////////////////////////
// ResultReceiver public
////////////////////////////////////////////////////////////////////////
ResultReceiver::ResultReceiver(boost::shared_ptr<MsgReceiver> msgReceiver,
                               boost::shared_ptr<rproc::InfileMerger> merger,
                               std::string const& tableName)
    : _msgReceiver(msgReceiver),
      _infileMerger(merger), _tableName(tableName),
      _actualSize(ResultReceiver_bufferSize),
      _actualBuffer(_actualSize),
      _flushed(false), _dirty(false) {
    // Consider allocating buffer lazily, at first invocation of buffer()
    _buffer = &_actualBuffer[0];
    _bufferSize = _actualSize;
}

int ResultReceiver::bufferSize() const {
    return _bufferSize;
}

char* ResultReceiver::buffer() {
    _flushed = false;
    return _buffer;
}

bool ResultReceiver::flush(int bLen, bool last) {
    // Do something with the buffer.
    LOGF_INFO("Receiver flushing %1% bytes (%2%) to table=%3%" %
              bLen % (last ? "last" : "more") % _tableName);
    LOGF_INFO("%1%" % makeByteStreamAnnotated("ResultReceiver flushbytes",
                                              _buffer, bLen));
    assert(!_tableName.empty());
    bool mergeOk = false;
    if(bLen == 0) {
        // just end it.
    } else if(bLen > 0) {
        assert(_infileMerger);
        mergeOk = _appendAndMergeBuffer(bLen);
        if(mergeOk) {
            _dirty = true;
        }
    } else {
        LOGF_ERROR("Possible error: flush with negative length");
        return false;
    }

    _flushed = true;
    if(last) {
        // Probably want to notify that we're done?
        LOGF_INFO("Flushed last for tableName=%1%" % _tableName);
    }
    return mergeOk;
}

void ResultReceiver::errorFlush(std::string const& msg, int code) {
    // Might want more info from result service.
    // Do something about the error. FIXME.
    {
        boost::lock_guard<boost::mutex> lock(_errorMutex);
        _error.msg = msg;
        _error.code = code;
    }
    LOGF_ERROR("Error receiving result.");
}

bool ResultReceiver::finished() const {
    return _flushed;
}

bool ResultReceiver::reset() {
    // If we've pushed any bits to the merger successfully, we have to undo them
    // to reset to a fresh state. For now, we will just fail if we've already
    // begun merging. If we implement the ability to retract a partial result
    // merge, then we can use it and do something better.
    if(_dirty) {
        return false; // Can't reset if we have already pushed state.
    }
    // Forget about anything we've put in the buffer so far.
    _buffer = &_actualBuffer[0];
    _bufferSize = _actualSize;
    return true;
}


std::ostream& ResultReceiver::print(std::ostream& os) const {
    os << "ResultReceiver(" << _tableName << ", flushed="
       << (_flushed ? "true)" : "false)") ;
    return os;
}

QueryReceiver::Error ResultReceiver::getError() const {
    boost::lock_guard<boost::mutex> lock(_errorMutex);
    return _error;
}

void ResultReceiver::cancel() {
    // If some error has already been recorded, leave it alone and don't worry
    // about cancelling. Otherwise, set the error and invoke cancellation.
    boost::shared_ptr<CancelFunc> f;
    {
        boost::lock_guard<boost::mutex> lock(_errorMutex);
        if(!_error.code) {
            _error.code = -1;
            _error.msg = "Squashed";
            f.swap(_cancelFunc);
        }
    }
    if(f) {
        (*f)();
    }
}
////////////////////////////////////////////////////////////////////////
// ResultReceiver private
////////////////////////////////////////////////////////////////////////

/// @return false if there was an error (invalid bytes, error in merge process)
/// If not enough bytes are available (e.g., need more bytes for a full message), this is not an error.
bool ResultReceiver::_appendAndMergeBuffer(int bLen) {
    off_t inputSize = _buffer - &_actualBuffer[0] + bLen;
    off_t mergeSize;
    assert(_infileMerger);
    try {
        mergeSize = _infileMerger->merge(&_actualBuffer[0], inputSize);
    } catch(rproc::InfileMergerError& e) {
        if(_msgReceiver) {
            (*_msgReceiver)(log::MSG_MERGE_ERROR, e.description);
        }
        return false;
    }
    if(mergeSize > 0) { // Something got merged.
        // Shift buffer contents to receive more.
        char* unMerged = &_actualBuffer[0] + mergeSize;
        off_t unMergedSize = inputSize - mergeSize;
        std::memmove(&_actualBuffer[0], unMerged, unMergedSize);
        _buffer = &_actualBuffer[0] + unMergedSize;
        _bufferSize = _actualSize - unMergedSize;
        return true;
    } else if(mergeSize == 0) {
        // Shift the cursor and wait for more bytes.
        _buffer = &_actualBuffer[0] + inputSize;
        _bufferSize = _actualSize = inputSize;

        LOGF_WARN("No merge in input. Receive buffer too small? Tried to merge %1% bytes, fresh=%2% actualsize=%3%" % inputSize % bLen % _actualSize);
        return true;
    }
    std::string msg = "Merger::merge() returned an impossible value";
    LOGF_ERROR("Die horribly %1%" % msg);

    if(_msgReceiver) {
        (*_msgReceiver)(log::MSG_MERGE_ERROR, msg);
    }
    return false;
}

}}} // lsst::qserv::ccontrol
