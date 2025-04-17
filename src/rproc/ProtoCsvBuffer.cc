// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 LSST Corporation.
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
#include "rproc/ProtoCsvBuffer.h"

// System headers
#include <cassert>
#include <sstream>
#include <stdexcept>
#include <string.h>

// Third-party headers
#include <mysql/mysql.h>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/worker.pb.h"
#include "util/Bug.h"

////////////////////////////////////////////////////////////////////////
// Helpful constants
////////////////////////////////////////////////////////////////////////
std::string const mysqlNull("\\N");

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.rproc.ProtoCsvBuffer");

// Print the contents of a char vector, using ascii values for non-printing characters.
std::string printCharVect(std::vector<char> const& cVect) {
    std::string str;
    for (char c : cVect) {
        if (std::isprint(c)) {
            str += c;
        } else {
            str += std::string("~") + std::to_string(c) + "~";
        }
    }
    return str;
}

}  // namespace

namespace lsst::qserv::rproc {

ProtoCsvBuffer::ProtoCsvBuffer(proto::ResponseData const& res)
        : _colSep("\t"),
          _rowSep("\n"),
          _nullToken("\\N"),
          _result(res),
          _rowIdx(0),
          _rowTotal(res.row_size()),
          _currentRow(0) {
    if (_result.row_size() > 0) {
        _initCurrentRow();
    }
}

/// Fetch a up to a single row from from the Result message
unsigned ProtoCsvBuffer::fetch(char* buffer, unsigned bufLen) {
    unsigned fetched = 0;
    if (bufLen <= _currentRow.size()) {
        memcpy(buffer, &_currentRow[0], bufLen);
        _currentRow.erase(_currentRow.begin(), _currentRow.begin() + bufLen);
        fetched = bufLen;
    } else {  // Want more than we have.
        if (_currentRow.size()) {
            memcpy(buffer, &_currentRow[0], _currentRow.size());
            fetched = _currentRow.size();
            _currentRow.clear();
        }
    }
    if ((_currentRow.size() == 0) && (_rowIdx < _rowTotal)) {
        _readNextRow();
    }
    return fetched;
}

std::string ProtoCsvBuffer::dump() const {
    std::string str("ProtoCsvBuffer Row " + std::to_string(_rowIdx) + "(");
    str += printCharVect(_currentRow);
    str += ")";
    return str;
}

/// Import the next row into the buffer
void ProtoCsvBuffer::_readNextRow() {
    ++_rowIdx;
    if (_rowIdx >= _rowTotal) {
        return;
    }
    _currentRow.clear();
    // Start the new row with a row separator.
    _currentRow.insert(_currentRow.end(), _rowSep.begin(), _rowSep.end());
    _copyRowBundle(_currentRow, _result.row(_rowIdx));
    LOGS(_log, LOG_LVL_TRACE, "_currentrow=" << printCharVect(_currentRow));
}

/// Setup the row byte buffer
void ProtoCsvBuffer::_initCurrentRow() {
    // Copy row and reserve 2x size.
    int rowSize = _copyRowBundle(_currentRow, _result.row(_rowIdx));
    LOGS(_log, LOG_LVL_TRACE, "init _rowIdx=" << _rowIdx << " _currentrow=" << printCharVect(_currentRow));
    _currentRow.reserve(rowSize * 2);  // for future usage
}

}  // namespace lsst::qserv::rproc
