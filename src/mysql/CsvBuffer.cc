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
#include "mysql/CsvBuffer.h"

// System headers
#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <string.h>

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "mysql/LocalInfileError.h"
#include "mysql/MySqlUtils.h"

namespace {
std::string const mysqlNull("\\N");
int const largeRowThreshold = 500 * 1024;  // should be less than 0.5 * infileBufferSize

}  // namespace

namespace lsst::qserv::mysql {

/// Row is a mysql row abstraction that bundles field sizes and counts. Row is
/// shallow, and does not perform any memory management.
struct Row {
    Row() : row(nullptr), lengths(nullptr), numFields(-1) {}

    // Shallow copies all-around.
    Row(char** row_, unsigned long int* lengths_, int numFields_)
            : row(row_), lengths(lengths_), numFields(numFields_) {}

    unsigned int minRowSize() const {
        unsigned int sum = 0;
        for (int i = 0; i < numFields; ++i) {
            sum += lengths[i];
        }
        return sum;
    }

    char** row;
    unsigned long int* lengths;
    int numFields;
};

inline unsigned updateEstRowSize(unsigned lastRowSize, Row const& r) {
    unsigned const rowSize = r.minRowSize();
    return lastRowSize < rowSize ? rowSize : lastRowSize;
}

inline int addString(char* cursor, std::string const& s) {
    int const sSize = s.size();
    memcpy(cursor, s.data(), sSize);
    return sSize;
}

inline int maxColFootprint(int columnLength, std::string const& sep) {
    const int overhead = 2 + sep.size();  // NULL decl + sep size
    return overhead + (2 * columnLength);
}

inline int addColumn(char* cursor, char* colData, int colSize) {
    int added = 0;
    if (colData) {
        // Sanitize field.
        // Don't need mysql_real_escape_string, because we can
        // use the simple LOAD DATA INFILE escaping rules
        added = mysql::escapeString(cursor, colData, colSize);
    } else {
        added = addString(cursor, ::mysqlNull);
    }
    return added;
}

class ResCsvBuffer : public CsvBuffer {
public:
    ResCsvBuffer(MYSQL_RES* result);
    unsigned fetch(char* buffer, unsigned bufLen) override;
    unsigned int _addRow(Row r, char* cursor, int remaining);
    bool _fetchRow(Row& r);
    unsigned _fetchFromLargeRow(char* buffer, int bufLen);
    void _initializeLargeRow(Row const& largeRow);
    std::string dump() const override;

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

ResCsvBuffer::ResCsvBuffer(MYSQL_RES* result)
        : _result(result), _useLargeRow(false), _fieldOffset(0), _sep("\t"), _rowSep("\n") {
    // Defer actual row fetching until fetch() is called
    assert(result);
    _numFields = mysql_num_fields(result);
    // cout << _numFields << " fields per row\n";
}

std::string ResCsvBuffer::dump() const {
    std::string str = std::string("ResCsvBuffer _numFields=") + std::to_string(_numFields);
    return str;
}

unsigned ResCsvBuffer::fetch(char* buffer, unsigned bufLen) {
    unsigned fetchSize = 0;
    unsigned estRowSize = 0;
    if (bufLen <= 0) {
        throw LocalInfileError("ResCsvBuffer::fetch Can't fetch non-positive bytes");
    }
    if (_useLargeRow) {
        return _fetchFromLargeRow(buffer, bufLen);
    }
    // Loop for full rows until buffer is full, or we've detected
    // a large row.
    while ((2 * estRowSize) > (bufLen - fetchSize)) {
        // Try to fetch to fill the buffer.
        Row r;
        bool fetchOk = _fetchRow(r);
        if (!fetchOk) {
            return fetchSize;
        }
        estRowSize = updateEstRowSize(estRowSize, r);
        if (estRowSize > static_cast<unsigned>(::largeRowThreshold)) {
            _initializeLargeRow(r);
            unsigned largeFetchSize = _fetchFromLargeRow(buffer + fetchSize, bufLen - fetchSize);
            return fetchSize + largeFetchSize;
        } else {  // Small rows, use simpler row-at-a-time logic
            unsigned rowFetch = _addRow(r, buffer + fetchSize, bufLen - fetchSize);
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
unsigned int ResCsvBuffer::_addRow(Row r, char* cursor, int remaining) {
    assert(remaining >= 0);  // negative remaining is nonsensical
    char* original = cursor;
    unsigned sepSize = _sep.size();
    // 2x orig size to allow escaping + separators +
    // null-terminator for mysql_real_escape_string
    unsigned allocRowSize = (2 * r.minRowSize()) + ((r.numFields - 1) * sepSize) + 1;
    if (allocRowSize > static_cast<unsigned>(remaining)) {
        // Make buffer size in LocalInfile larger than largest row.
        // largeRowThreshold should prevent this.
        throw LocalInfileError("ResCsvBuffer::_addRow: Buffer too small for row");
    }
    for (int i = 0; i < r.numFields; ++i) {
        if (i) {  // add separator
            cursor += addString(cursor, _sep);
        }
        cursor += addColumn(cursor, r.row[i], r.lengths[i]);
    }
    assert(cursor > original);
    return cursor - original;
}

/// Fetch a row from _result and fill the caller-supplied Row.
bool ResCsvBuffer::_fetchRow(Row& r) {
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
/// CsvBuffer is an interface for accessing the row data for LocalInfile, and because
/// ResCsvBuffer is an implementation that fetches rows from a MYSQL_RES handle,
/// and Qserv will generally use rows received from workers as CSV-formatted
/// files, ResCsvBuffer objects are not planned for use in a normally
/// operating Qserv system. Still, ResCsvBuffer is useful for *testing*
/// LocalInfile (e.g., loading the result of a SELECT statement using LOAD DATA
/// INFILE).
unsigned ResCsvBuffer::_fetchFromLargeRow(char* buffer, int bufLen) {
    // Insert field-at-a-time,
    char* cursor = buffer;
    int remaining = bufLen;

    while (maxColFootprint(_largeRow.lengths[_fieldOffset], _sep) > remaining) {
        int addLength = addColumn(cursor, _largeRow.row[_fieldOffset], _largeRow.lengths[_fieldOffset]);
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
    if (cursor == buffer) {  // Were we able to put anything in?
        throw LocalInfileError("ResCsvBuffer::_fetchFromLargeRow: Buffer too small for single column!");
    }
    return bufLen - remaining;
}

/// Init structures for large rows.
void ResCsvBuffer::_initializeLargeRow(Row const& largeRow) {
    _useLargeRow = true;
    _fetchRow(_largeRow);
    _fieldOffset = 0;
}

std::shared_ptr<CsvBuffer> newResCsvBuffer(MYSQL_RES* result) {
    return std::make_shared<ResCsvBuffer>(result);
}

CsvStream::CsvStream(std::size_t maxRecords) : _maxRecords(maxRecords) {
    if (maxRecords == 0) {
        throw std::invalid_argument("CsvStream::CsvStream: maxRecords must be greater than 0");
    }
}

void CsvStream::push(char const* data, std::size_t size) {
    std::unique_lock<std::mutex> lock(_mtx);
    while (_records.size() >= _maxRecords) {
        _cv.wait(lock);
    }
    if (data != nullptr && size != 0) {
        _records.emplace_back(std::make_shared<std::string>(data, size));
    } else {
        // Empty string is meant to indicate the end of the stream.
        _records.emplace_back(std::make_shared<std::string>());
    }
    _cv.notify_one();
}

std::shared_ptr<std::string> CsvStream::pop() {
    std::unique_lock<std::mutex> lock(_mtx);
    while (_records.empty()) {
        _cv.wait(lock);
    }
    std::shared_ptr<std::string> front = _records.front();
    _records.pop_front();
    _cv.notify_one();
    return front;
}

bool CsvStream::empty() const {
    std::unique_lock<std::mutex> lock(_mtx);
    return _records.empty();
}

/**
 * CsvStreamBuffer is a CsvBuffer that reads from a CsvStream. It is used to read
 * data from a CsvStream in a buffered manner.
 * @note The current implementation of method fetch() could be further optimized
 * to fetch more than one record at a time. The current implementation
 * fetches one record at a time, which may be inefficient for small records.
 * Though, in practice, this is not an issue in the current design of the result
 * merging algorithm.
 */
class CsvStreamBuffer : public CsvBuffer {
public:
    explicit CsvStreamBuffer(std::shared_ptr<CsvStream> const& csvStream) : _csvStream(csvStream) {}

    unsigned fetch(char* buffer, unsigned bufLen) override {
        if (bufLen == 0) {
            throw LocalInfileError("CsvStreamBuffer::fetch Can't fetch non-positive bytes");
        }
        if (_str == nullptr) {
            _str = _csvStream->pop();
            _offset = 0;
        }
        if (_str->empty()) return 0;
        if (_offset >= _str->size()) {
            _str = _csvStream->pop();
            _offset = 0;
            if (_str->empty()) return 0;
        }
        unsigned const bytesToCopy = std::min(bufLen, static_cast<unsigned>(_str->size() - _offset));
        ::memcpy(buffer, _str->data() + _offset, bytesToCopy);
        _offset += bytesToCopy;
        return bytesToCopy;
    }

    std::string dump() const override { return "CsvStreamBuffer"; }

private:
    std::shared_ptr<CsvStream> _csvStream;
    std::shared_ptr<std::string> _str;
    std::size_t _offset = 0;
};

std::shared_ptr<CsvBuffer> newCsvStreamBuffer(std::shared_ptr<CsvStream> const& csvStream) {
    return std::make_shared<CsvStreamBuffer>(csvStream);
}

}  // namespace lsst::qserv::mysql
