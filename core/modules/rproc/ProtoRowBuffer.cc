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
#include "rproc/ProtoRowBuffer.h"

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

////////////////////////////////////////////////////////////////////////
// Helpful constants
////////////////////////////////////////////////////////////////////////
std::string const mysqlNull("\\N");


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.rproc.ProtoRowBuffer");

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

} // namespace


namespace lsst {
namespace qserv {
namespace rproc {

ProtoRowBuffer::ProtoRowBuffer(proto::Result& res, int jobId, std::string const& jobIdColName,
                               std::string const& jobIdSqlType, int jobIdMysqlType)
    : _colSep("\t"),
      _rowSep("\n"),
      _nullToken("\\N"),
      _result(res),
      _rowIdx(0),
      _rowTotal(res.row_size()),
      _currentRow(0),
      _jobIdColName(jobIdColName),
      _jobIdSqlType(jobIdSqlType),
      _jobIdMysqlType(jobIdMysqlType) {
    _jobIdStr = std::string("'") + std::to_string(jobId) + "'";
    _initSchema();
    if (_result.row_size() > 0) {
        _initCurrentRow();
    }
}


/// Fetch a up to a single row from from the Result message
unsigned ProtoRowBuffer::fetch(char* buffer, unsigned bufLen) {
    unsigned fetched = 0;
    if (bufLen <= _currentRow.size()) {
        memcpy(buffer, &_currentRow[0], bufLen);
        _currentRow.erase(_currentRow.begin(), _currentRow.begin() + bufLen);
        fetched = bufLen;
    } else { // Want more than we have.
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

/// Import schema from the proto message into a Schema object
void ProtoRowBuffer::_initSchema() {
    _schema.columns.clear();

    // Set jobId and attemptCount
    sql::ColSchema jobIdCol;
    jobIdCol.name = _jobIdColName;
    jobIdCol.hasDefault = false;
    jobIdCol.colType.sqlType = _jobIdSqlType;
    jobIdCol.colType.mysqlType = _jobIdMysqlType;
    _schema.columns.push_back(jobIdCol);

    proto::RowSchema const& prs = _result.rowschema();
    for(int i=0, e=prs.columnschema_size(); i != e; ++i) {
        proto::ColumnSchema const& pcs = prs.columnschema(i);
        sql::ColSchema cs;
        if (pcs.has_name()) {
            cs.name = pcs.name();
        }
        cs.hasDefault = pcs.has_defaultvalue();
        if (cs.hasDefault) {
            cs.defaultValue = pcs.defaultvalue();
        }
        cs.colType.sqlType = pcs.sqltype();
        if (pcs.has_mysqltype()) {
            cs.colType.mysqlType = pcs.mysqltype();
        }
        _schema.columns.push_back(cs);
    }
}


std::string ProtoRowBuffer::dump() const {
    std::string str("ProtoRowBuffer schema(");
    for (auto sCol : _schema.columns) {
        str += "(Name=" + sCol.name;
        str += ",defaultValue=" + sCol.defaultValue;
        str += ",colType=" + sCol.colType.sqlType + ":" + std::to_string(sCol.colType.mysqlType) + ")";
    }
    str += ") ";
    str += "Row " + std::to_string(_rowIdx) + "(";
    str += printCharVect(_currentRow);
    str += ")";
    return str;
}


/// Import the next row into the buffer
void ProtoRowBuffer::_readNextRow() {
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
void ProtoRowBuffer::_initCurrentRow() {
    // Copy row and reserve 2x size.
    int rowSize = _copyRowBundle(_currentRow, _result.row(_rowIdx));
    LOGS(_log, LOG_LVL_TRACE, "init _rowIdx=" <<_rowIdx << " _currentrow=" << printCharVect(_currentRow));
    _currentRow.reserve(rowSize*2); // for future usage
}


}}} // lsst::qserv::mysql
