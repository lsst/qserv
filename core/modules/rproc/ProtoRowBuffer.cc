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
#include "rproc/ProtoRowBuffer.h"

// System headers
#include <cassert>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string.h>

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "proto/worker.pb.h"
#include "sql/Schema.h"

////////////////////////////////////////////////////////////////////////
// Helpful constants
////////////////////////////////////////////////////////////////////////
std::string const mysqlNull("\\N");
// should be less than 0.5 * infileBufferSize
int const largeRowThreshold = 500*1024;

namespace lsst {
namespace qserv {
namespace rproc {
////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

/// Escape a bytestring for LOAD DATA INFILE, as specified by MySQL doc:
/// https://dev.mysql.com/doc/refman/5.1/en/load-data.html
/// This is limited to:
/// Character    Escape Sequence
/// \0     An ASCII NUL (0x00) character
/// \b     A backspace character
/// \n     A newline (linefeed) character
/// \r     A carriage return character
/// \t     A tab character.
/// \Z     ASCII 26 (Control+Z)
/// \N     NULL
///
/// @return the number of bytes written to dest
template <typename Iter, typename CIter>
inline int escapeString(Iter destBegin, CIter srcBegin, CIter srcEnd) {
    //mysql_real_escape_string(_mysql, cursor, col, r.lengths[i]);
    // empty string isn't escaped
    if (srcEnd == srcBegin) return 0;
    assert(srcEnd - srcBegin > 0);
    assert(srcEnd - srcBegin < std::numeric_limits<int>::max() / 2);
    Iter destI = destBegin;
    for(CIter i = srcBegin; i != srcEnd; ++i) {
        switch(*i) {
          case '\0':   *destI++ = '\\'; *destI++ = '0'; break;
          case '\b':   *destI++ = '\\'; *destI++ = 'b'; break;
          case '\n':   *destI++ = '\\'; *destI++ = 'n'; break;
          case '\r':   *destI++ = '\\'; *destI++ = 'r'; break;
          case '\t':   *destI++ = '\\'; *destI++ = 't'; break;
          case '\032': *destI++ = '\\'; *destI++ = 'Z'; break;
          default: *destI++ = *i; break;
            // Null (\N) is not treated by escaping in this context.
        }
    }
    return destI - destBegin;
}

/// Copy a rawColumn to an STL container
template <typename T>
inline int copyColumn(T& dest, std::string const& rawColumn) {
    int existingSize = dest.size();
    dest.resize(existingSize + 2 + 2 * rawColumn.size());
    dest[existingSize] = '\'';
    int valSize = escapeString(dest.begin() + existingSize + 1,
                               rawColumn.begin(), rawColumn.end());
    dest[existingSize + 1 + valSize] = '\'';
    dest.resize(existingSize + 2 + valSize);
    return 2 + valSize;
}
////////////////////////////////////////////////////////////////////////
// ProtoRowBuffer
////////////////////////////////////////////////////////////////////////

/// ProtoRowBuffer is an implementation of RowBuffer designed to allow a
/// LocalInfile object to use a Protobufs Result message as a row source
class ProtoRowBuffer : public mysql::RowBuffer {
public:
    ProtoRowBuffer(proto::Result& res);
    virtual unsigned fetch(char* buffer, unsigned bufLen);

private:
    void _initCurrentRow();
    void _initSchema();
    void _readNextRow();
    // Copy a row bundle into a destination STL char container
    template <typename T>
    int _copyRowBundle(T& dest, proto::RowBundle const& rb) {
        int sizeBefore = dest.size();
        for(int ci=0, ce=rb.column_size(); ci != ce; ++ci) {
            if(ci != 0) {
                dest.insert(dest.end(), _colSep.begin(), _colSep.end());
            }

            if(!rb.isnull(ci)) {
                copyColumn(dest, rb.column(ci));
            } else {
                dest.insert(dest.end(), _nullToken.begin(), _nullToken.end() );
            }
        }
        return dest.size() - sizeBefore;
    }

    std::string _colSep; ///< Column separator
    std::string _rowSep; ///< Row separator
    std::string _nullToken; ///< Null indicator (e.g. \N)
    proto::Result& _result; ///< Ref to Resultmessage

    sql::Schema _schema; ///< Schema object
    int _rowIdx; ///< Row index
    int _rowTotal; ///< Total row count
    std::vector<char> _currentRow; ///< char buffer representing current row.
};

ProtoRowBuffer::ProtoRowBuffer(proto::Result& res)
    : _colSep("\t"),
      _rowSep("\n"),
      _nullToken("\\N"),
      _result(res),
      _rowIdx(0),
      _rowTotal(res.row_size()),
      _currentRow(0) {
    _initSchema();
    if(_result.row_size() > 0) {
        _initCurrentRow();
    }
}

/// Fetch a up to a single row from from the Result message
unsigned ProtoRowBuffer::fetch(char* buffer, unsigned bufLen) {
    unsigned fetched = 0;
    if(bufLen <= _currentRow.size()) {
        memcpy(buffer, &_currentRow[0], bufLen);
        _currentRow.erase(_currentRow.begin(), _currentRow.begin() + bufLen);
        fetched = bufLen;
    } else { // Want more than we have.
        if(_currentRow.size()) {
            memcpy(buffer, &_currentRow[0], _currentRow.size());
            fetched = _currentRow.size();
            _currentRow.clear();
        }
    }
    if((_currentRow.size() == 0) && (_rowIdx < _rowTotal)) {
        _readNextRow();
    }
    return fetched;
}

/// Import schema from the proto message into a Schema object
void ProtoRowBuffer::_initSchema() {
    _schema.columns.clear();
    proto::RowSchema const& prs = _result.rowschema();
    for(int i=0, e=prs.columnschema_size(); i != e; ++i) {
        proto::ColumnSchema const& pcs = prs.columnschema(i);
        sql::ColSchema cs;
        if(pcs.has_name()) {
            cs.name = pcs.name();
        }
        cs.hasDefault = pcs.has_defaultvalue();
        if(cs.hasDefault) {
            cs.defaultValue = pcs.defaultvalue();
        }
        cs.colType.sqlType = pcs.sqltype();
        if(pcs.has_mysqltype()) {
            cs.colType.mysqlType = pcs.mysqltype();
        }
        _schema.columns.push_back(cs);
    }
}
/// Import the next row into the buffer
void ProtoRowBuffer::_readNextRow() {
    ++_rowIdx;
    if(_rowIdx >= _rowTotal) {
        return;
    }
    _currentRow.clear();
    // Row separator
    _currentRow.insert(_currentRow.end(), _rowSep.begin(), _rowSep.end());
    _copyRowBundle(_currentRow, _result.row(_rowIdx));
}

/// Setup the row byte buffer
void ProtoRowBuffer::_initCurrentRow() {
    // Copy row and reserve 2x size.
    int rowSize = _copyRowBundle(_currentRow, _result.row(_rowIdx));
    _currentRow.reserve(rowSize*2); // for future usage
}

////////////////////////////////////////////////////////////////////////
// Factory function for ProtoRowBuffer
////////////////////////////////////////////////////////////////////////

mysql::RowBuffer::Ptr newProtoRowBuffer(proto::Result& res) {
    return mysql::RowBuffer::Ptr(new ProtoRowBuffer(res));
}
}}} // lsst::qserv::mysql
