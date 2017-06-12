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
#include "mysql/RowBuffer.h"

// System headers
#include <cassert>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string.h>

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "mysql/LocalInfileError.h"
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
namespace mysql {
////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////
inline unsigned updateEstRowSize(unsigned lastRowSize, Row const& r) {
    unsigned rowSize = r.minRowSize();
    if (lastRowSize < rowSize) {
        return rowSize;
    }
    return lastRowSize;
}

inline int addString(char* cursor, std::string const& s) {
    int sSize = s.size();
    memcpy(cursor, s.data(), sSize);
    return sSize;
}

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

inline int maxColFootprint(int columnLength, std::string const& sep) {
    const int overhead = 2 + sep.size(); // NULL decl + sep size
    return overhead + (2 * columnLength);
}

inline int addColumn(char* cursor, char* colData, int colSize) {
    int added = 0;
    if (colData) {
        // Sanitize field.
        // Don't need mysql_real_escape_string, because we can
        // use the simple LOAD DATA INFILE escaping rules
        added = escapeString(cursor, colData, colSize);
    } else {
        added = addString(cursor, mysqlNull);
    }
    return added;
}

class ResRowBuffer : public RowBuffer {
public:
    ResRowBuffer(MYSQL_RES* result);
    unsigned fetch(char* buffer, unsigned bufLen);
    unsigned int _addRow(Row r, char* cursor, int remaining);
    bool _fetchRow(Row& r);
    unsigned _fetchFromLargeRow(char* buffer, int bufLen);
    void _initializeLargeRow(Row const& largeRow);
    std::string dump() override;
private:
    MYSQL_RES* _result;
    bool _useLargeRow;
    int _numFields;

    // Large-row support
    Row _largeRow;
    int _fieldOffset;

    std::string _sep;
    std::string _rowSep;
};

ResRowBuffer::ResRowBuffer(MYSQL_RES* result)
    : _result(result),
      _useLargeRow(false),
      _fieldOffset(0),
      _sep("\t"), _rowSep("\n") {
    // Defer actual row fetching until fetch() is called
    assert(result);
    _numFields = mysql_num_fields(result);
    // cout << _numFields << " fields per row\n";
}


std::string ResRowBuffer::dump() {
    std::string str = std::string("&&& ResRowBuffer _numFields=") + std::to_string(_numFields);
    return str;
}


unsigned ResRowBuffer::fetch(char* buffer, unsigned bufLen) {
    unsigned fetchSize = 0;
    unsigned estRowSize = 0;
    if (bufLen <= 0) {
        throw LocalInfileError("ResRowBuffer::fetch Can't fetch non-positive bytes");
    }
    if (_useLargeRow) {
        return _fetchFromLargeRow(buffer, bufLen);
    }
    // Loop for full rows until buffer is full, or we've detected
    // a large row.
    while((2 * estRowSize) > (bufLen - fetchSize)) {
        // Try to fetch to fill the buffer.
        Row r;
        bool fetchOk = _fetchRow(r);
        if (!fetchOk) {
            return fetchSize;
        }
        estRowSize = updateEstRowSize(estRowSize, r);
        if (estRowSize > static_cast<unsigned>(largeRowThreshold)) {
            _initializeLargeRow(r);
            unsigned largeFetchSize = _fetchFromLargeRow(buffer + fetchSize,
                                                         bufLen - fetchSize);
            return fetchSize + largeFetchSize;
        } else { // Small rows, use simpler row-at-a-time logic
            unsigned rowFetch = _addRow(r,
                                        buffer + fetchSize,
                                        bufLen - fetchSize);
            if (!rowFetch) {
                break;
            }
            fetchSize += rowFetch;
            fetchSize += addString(buffer + fetchSize, _rowSep);
        }
    }
    return fetchSize;
}

/// Add a row to the buffer pointed to by cursor.
/// @return the number of bytes added.
unsigned int ResRowBuffer::_addRow(Row r, char* cursor, int remaining) {
    assert(remaining >= 0); // negative remaining is nonsensical
    char* original = cursor;
    unsigned sepSize = _sep.size();
    // 2x orig size to allow escaping + separators +
    // null-terminator for mysql_real_escape_string
    unsigned allocRowSize
        = (2*r.minRowSize()) + ((r.numFields-1) * sepSize) + 1;
    if (allocRowSize > static_cast<unsigned>(remaining)) {
        // Make buffer size in LocalInfile larger than largest
        // row.
        // largeRowThreshold should prevent this.
        throw LocalInfileError("ResRowBuffer::_addRow: Buffer too small for row");
    }
    for(int i=0; i < r.numFields; ++i) {
        if (i) {  // add separator
            cursor += addString(cursor, _sep);
        }
        cursor +=  addColumn(cursor, r.row[i], r.lengths[i]);
    }
    assert(cursor > original);
    return cursor - original;
}

/// Fetch a row from _result and fill the caller-supplied Row.
bool ResRowBuffer::_fetchRow(Row& r) {
    MYSQL_ROW mysqlRow = mysql_fetch_row(_result);
    if (!mysqlRow) {
        return false;
    }
    r.row = mysqlRow;
    r.lengths = mysql_fetch_lengths(_result);
    r.numFields = _numFields;
    assert(r.lengths);
    return true;
}

/// Attempt to fill a buffer from a large row that may not completely fit in
/// the buffer.
/// This is unfinished code, but is only triggered for rows > 500kB.  Also,
/// RowBuffer objects are used to buffer rows for LocalInfile, and because
/// ResRowBuffer is an implementation that fetches rows from a MYSQL_RES handle,
/// and Qserv will generally use rows received over-the-wire in protobufs
/// messages, ResRowBuffer objects are not planned for use in a normally
/// operating Qserv system. Still, ResRowBuffer is useful for *testing*
/// LocalInfile (e.g., loading the result of a SELECT statement using LOAD DATA
/// INFILE).
unsigned ResRowBuffer::_fetchFromLargeRow(char* buffer, int bufLen) {
    // Insert field-at-a-time,
    char* cursor = buffer;
    int remaining = bufLen;

    while(maxColFootprint(_largeRow.lengths[_fieldOffset], _sep) > remaining) {
        int addLength = addColumn(cursor,
                                  _largeRow.row[_fieldOffset],
                                  _largeRow.lengths[_fieldOffset]);
        cursor += addLength;
        remaining -= addLength;
        ++_fieldOffset;
        if (_fieldOffset >= _numFields) {
            if (!_fetchRow(_largeRow)) {
                break;
                // no more rows, return what we have
            }
            _fieldOffset = 0;
        }
        // FIXME: unfinished
    }
    if (cursor == buffer) { // Were we able to put anything in?
        throw LocalInfileError("ResRowBuffer::_fetchFromLargeRow: Buffer too small for single column!");
    }
    return bufLen - remaining;
}

/// Init structures for large rows.
void ResRowBuffer::_initializeLargeRow(Row const& largeRow) {
    _useLargeRow = true;
    _fetchRow(_largeRow);
    _fieldOffset = 0;
}

////////////////////////////////////////////////////////////////////////
// RowBuffer Implementation
////////////////////////////////////////////////////////////////////////
std::shared_ptr<RowBuffer> RowBuffer::newResRowBuffer(MYSQL_RES* result) {
    Ptr p = std::make_shared<ResRowBuffer>(result);
    return p;
}
}}} // lsst::qserv::mysql
