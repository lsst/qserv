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
#include "log/Logger.h"
#include "rproc/TableMerger.h"

namespace lsst {
namespace qserv {
namespace ccontrol {
// Buffer needs to be big enough to hold largest (in bytes) sql statement from
// worker mysqldump. 128kB not enough. Probably want as big as
// max_allowed_packet on mysqld/mysqlclient
int const ResultReceiver_bufferSize = 2*1024*1024; // 2 megabytes.

ResultReceiver::ResultReceiver(boost::shared_ptr<rproc::TableMerger> merger,
                               std::string const& tableName)
    : _merger(merger), _tableName(tableName),
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
    assert(!_tableName.empty());
    bool mergeOk = false;
    if(bLen == 0) {
        // just end it.
    } else if(bLen > 0) {
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
        if(_finishHook) {
            (*_finishHook)(true);
        }
    }
    return mergeOk;
}

void ResultReceiver::errorFlush(std::string const& msg, int code) {
    // Might want more info from result service.
    // Do something about the error. FIXME.
    _error.msg = msg;
    _error.code = code;
    LOGGER_ERR << "Error receiving result." << std::endl;
    if(_finishHook) {
        (*_finishHook)(false);
    }
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

void ResultReceiver::addFinishHook(util::UnaryCallable<void, bool>::Ptr f) {
    assert(!_finishHook);
    _finishHook = f;
}

bool ResultReceiver::_appendAndMergeBuffer(int bLen) {
    off_t inputSize = _buffer - &_actualBuffer[0] + bLen;
    off_t mergeSize = _merger->merge(&_actualBuffer[0], inputSize,
                                     _tableName);
    if(mergeSize > 0) { // Something got merged.
        // Shift buffer contents to receive more.
        char* unMerged = &_actualBuffer[0] + mergeSize;
        off_t unMergedSize = inputSize - mergeSize;
        std::memmove(&_actualBuffer[0], unMerged, unMergedSize);
        _buffer = &_actualBuffer[0] + unMergedSize;
        _bufferSize = _actualSize - unMergedSize;
        return true;
    } else if(mergeSize == 0) {
            LOGGER_ERR << "No merge in input. Receive buffer too small? "
                       << "Tried to merge " << inputSize
                       << " bytes, fresh=" << bLen
                       << " actualsize=" << _actualSize
                       << std::endl;
        return false;
    } else {
        LOGGER_ERR << "Die horribly, for TableMerger::merge() returned an impossible value" << std::endl;
        throw std::runtime_error("Impossible return value from merge()");
    }
}

}}} // lsst::qserv::ccontrol
