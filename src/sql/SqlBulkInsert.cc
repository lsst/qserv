/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "sql/SqlBulkInsert.h"

// System headers

// LSST headers

// Qserv headers
#include "sql/SqlResults.h"

namespace lsst {
namespace qserv {
namespace sql {

// Constructors
SqlBulkInsert::SqlBulkInsert(SqlConnection* conn,
                             std::string const& table,
                             std::vector<std::string> const& columns)
    : _conn(conn), _maxSize(0), _insert(), _buffer()
{
    // Build initial INSERT
    _insert = "INSERT INTO ";
    _insert += table;
    char sep = '(';
    for (auto const& column: columns) {
        _insert += sep;
        _insert += '`';
        _insert += column;
        _insert += '`';
        sep = ',';
    }
    _insert += ") VALUES ";
}

// Insert one more row
bool
SqlBulkInsert::addRow(std::vector<std::string> const& values, SqlErrorObject& errObj) {

    if (_maxSize == 0) {
        // get max. buffer size
        std::string query = "SELECT @@session.max_allowed_packet";
        SqlResults sqlRes;
        if (!_conn->runQuery(query, sqlRes, errObj)) {
            return false;
        }
        std::string valStr;
        if (!sqlRes.extractFirstValue(valStr, errObj)) {
            return false;
        }
        try {
            _maxSize = std::stoul(valStr);
        } catch (std::exception const& exc) {
            // use defaults below
        }
        if (_maxSize != 0) {
            // save some space for packet header
            _maxSize -= 64;
        } else {
            // some safe default
            _maxSize = 16*1024;
        }
    }

    // get the size of row to be inserted
    unsigned rowSize = 0;
    for (auto&& val: values) {
        rowSize += val.size();
    }
    // plus parens and commas as in ",(val,val,val)"
    rowSize += 2 + values.size();

    if (!_buffer.empty()) {
        // check new size
        if (_buffer.size() + rowSize > _maxSize) {
            if (!flush(errObj)) {
                return false;
            }
        }
    }
    if (_buffer.empty()) {
        // first row (after flush)
        _buffer = _insert;
    } else {
        _buffer += ',';
    }

    // add values
    char sep = '(';
    for (auto&& val: values) {
        _buffer += sep;
        _buffer += val;
        sep = ',';
    }
    _buffer += ')';

    return true;
}

// flush buffer
bool SqlBulkInsert::flush(SqlErrorObject& errObj) {
    if (!_buffer.empty()) {
        if (!_conn->runQuery(_buffer, errObj)) {
            return false;
        }
        _buffer.clear();
    }
    return true;
}

}}} // namespace lsst::qserv::sql
