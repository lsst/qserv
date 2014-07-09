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

inline int escapeString(char* dest, char const* src, int srcLength) {
    //mysql_real_escape_string(_mysql, cursor, col, r.lengths[i]);
    assert(srcLength >= 0);
    assert(srcLength < std::numeric_limits<int>::max() / 2);
    char const* end = src + srcLength;
    char const * originalSrc = src;
    while(src != end) {
        switch(*src) {
        case '\0': *dest++ = '\\'; *dest++ = '0'; break;
        case '\b': *dest++ = '\\'; *dest++ = 'b'; break;
        case '\n': *dest++ = '\\'; *dest++ = 'n'; break;
        case '\r': *dest++ = '\\'; *dest++ = 'r'; break;
        case '\t': *dest++ = '\\'; *dest++ = 't'; break;
        case '\032': *dest++ = '\\'; *dest++ = 'Z'; break;
        default: *dest++ = *src; break;
            // Null (\N) is not treated by escaping in this context.
        }
        ++src;
    }
    return src - originalSrc;
}

template <typename T>
inline int copyColumn(T& dest, std::string const& rawColumn) {
    //      std::string colValue = rb.column(ci);
    std::vector<char> colBuf(2 * rawColumn.size());
    int valSize = escapeString(&colBuf[0],
                           rawColumn.data(), rawColumn.size());
    dest.push_back('\'');
    dest.insert(dest.end(),
                colBuf.begin(), colBuf.begin() + valSize);
    dest.push_back('\'');
    return 2 + valSize;
}
////////////////////////////////////////////////////////////////////////
// ProtoRowBuffer
////////////////////////////////////////////////////////////////////////
class ProtoRowBuffer : public mysql::RowBuffer {
public:
    ProtoRowBuffer(proto::Result& res);
    unsigned fetch(char* buffer, unsigned bufLen);
private:
    void _initCurrentRow();
    void _initSchema();
    void _readNextRow();
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

    std::string _colSep;
    std::string _rowSep;
    std::string _nullToken;
    proto::Result& _result;

    sql::Schema _schema;
    int _rowIdx;
    int _rowTotal;
    std::vector<char> _currentRow;
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
        } else if(_rowIdx >= _rowTotal) {
            return 0; // Nothing to fetch, then.
        }
    }
    if(_currentRow.size() == 0) {
        _readNextRow();
    }
    return fetched;
}

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

//    for(int i=0, e=_result.row_size(); i != e; ++i);
void ProtoRowBuffer::_initCurrentRow() {
    // Copy row and reserve 2x size.
    int rowSize = _copyRowBundle(_currentRow, _result.row(_rowIdx));
    _currentRow.reserve(rowSize*2); // for future usage
}

////////////////////////////////////////////////////////////////////////
// RowBuffer Implementation
////////////////////////////////////////////////////////////////////////

mysql::RowBuffer::Ptr newProtoRowBuffer(proto::Result& res) {
    return mysql::RowBuffer::Ptr(new ProtoRowBuffer(res));
}
}}} // lsst::qserv::mysql
