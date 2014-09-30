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

// Qserv headers
#include "global/debugUtil.h"
#include "global/MsgReceiver.h"
#include "log/Logger.h"
#include "log/msgCode.h"
#include "rproc/InfileMerger.h"
#include "rproc/TableMerger.h"

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
    LOGGER_INF << "Receiver flushing " << bLen << " bytes "
               << (last ? " (last)" : " (more)")
               << " to table=" << _tableName << std::endl;
    LOGGER_INF << makeByteStreamAnnotated("ResultReceiver flushbytes",
                                          _buffer, bLen) << std::endl;

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
        LOGGER_ERR << "Possible error: flush with negative length" << std::endl;
        return false;
    }

    _flushed = true;
    if(last) {
        // Probably want to notify that we're done?
        LOGGER_INF << " Flushed last for tableName="
                   << _tableName << std::endl;
    }
    return mergeOk;
}

void ResultReceiver::errorFlush(std::string const& msg, int code) {
    // Might want more info from result service.
    // Do something about the error. FIXME.
    _error.msg = msg;
    _error.code = code;
    LOGGER_ERR << "Error receiving result." << std::endl;
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

        std::ostringstream os;
        os << "No merge in input. Receive buffer too small? "
           << "Tried to merge " << inputSize
           << " bytes, fresh=" << bLen
           << " actualsize=" << _actualSize
           << std::endl;
        std::string msg = os.str();
        LOGGER_WRN << msg << std::endl;
        return true;
    }
    std::string msg = "Merger::merge() returned an impossible value";
    LOGGER_ERR << "Die horribly " << msg << std::endl;
    if(_msgReceiver) {
        (*_msgReceiver)(log::MSG_MERGE_ERROR, msg);
    }
    return false;
}

}}} // lsst::qserv::ccontrol
