// -*- LSST-C++ -*-
/*
 * LSST Data Management System
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
#include "mysql/MySqlUtils.h"

// System headers
#include <string>

// Third party headers
#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
#include <mysql/errmsg.h>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"

using namespace std;
using json = nlohmann::json;

namespace {

string errInfo(lsst::qserv::mysql::MySqlConnection const& conn) {
    return "errno: " + to_string(conn.getErrno()) + ", error: " + conn.getError();
}

}  // anonymous namespace

namespace lsst::qserv::mysql {

json MySqlUtils::processList(MySqlConfig const& config, bool full) {
    string const context = "MySqlUtils::" + string(__func__);
    string const query = "SHOW" + string(full ? " FULL" : "") + " PROCESSLIST";

    MySqlConnection conn(config);
    if (!conn.connect()) {
        string const err = context + " failed to connect to the worker database, " + ::errInfo(conn);
        throw MySqlQueryError(err);
    }
    if (!conn.queryUnbuffered(query)) {
        string const err = "failed to execute the query: '" + query + "', " + ::errInfo(conn);
        throw MySqlQueryError(err);
    }
    json result;
    result["queries"] = json::object({{"columns", json::array()}, {"rows", json::array()}});
    int const numFields = conn.getResultFieldCount();
    if (numFields > 0) {
        result["queries"]["columns"] = conn.getColumnNames();
        auto& rows = result["queries"]["rows"];
        MYSQL_RES* mysqlResult = conn.getResult();
        while (true) {
            MYSQL_ROW mysqlRow = mysql_fetch_row(mysqlResult);
            if (!mysqlRow) {
                if (0 == conn.getErrno()) {
                    // End of iteration if no specific error was reported.
                    break;
                }
                string const err =
                        context + " failed to fetch next row for query: '" + query + "', " + ::errInfo(conn);
                throw MySqlQueryError(err);
            }
            size_t const* lengths = mysql_fetch_lengths(mysqlResult);
            json row = json::array();
            for (int i = 0; i < numFields; i++) {
                // Report the empty string for SQL NULL.
                auto const length = lengths[i];
                row.push_back(length == 0 ? string() : string(mysqlRow[i], length));
            }
            rows.push_back(row);
        }
    }
    return result;
}

int escapeString(char* dest, char const* src, int srcLength) {
    // mysql_real_escape_string(_mysql, cursor, col, r.lengths[i]);
    assert(srcLength >= 0);
    assert(srcLength < std::numeric_limits<int>::max() / 2);
    char const* end = src + srcLength;
    char const* originalSrc = src;
    while (src != end) {
        switch (*src) {
            case '\0':
                *dest++ = '\\';
                *dest++ = '0';
                break;
            case '\b':
                *dest++ = '\\';
                *dest++ = 'b';
                break;
            case '\n':
                *dest++ = '\\';
                *dest++ = 'n';
                break;
            case '\r':
                *dest++ = '\\';
                *dest++ = 'r';
                break;
            case '\t':
                *dest++ = '\\';
                *dest++ = 't';
                break;
            case '\032':
                *dest++ = '\\';
                *dest++ = 'Z';
                break;
            default:
                *dest++ = *src;
                break;
                // Null (\N) is not treated by escaping in this context.
        }
        ++src;
    }
    return src - originalSrc;
}

int escapeAppendString(std::string& dest, char const* srcData, size_t srcSize, bool quote, char quoteChar) {
    if (srcSize == 0) return srcSize;
    int const existingSize = dest.size();
    assert(existingSize < std::numeric_limits<int>::max() / 2);
    assert(srcSize < std::numeric_limits<int>::max() / 2);
    assert(existingSize + (quote ? 2 : 0) + 2 * srcSize < std::numeric_limits<int>::max());
    if (quote) {
        dest.resize(existingSize + 2 + 2 * srcSize);
        dest[existingSize] = quoteChar;
        int const valSize = mysql::escapeString(dest.begin() + existingSize + 1, srcData, srcData + srcSize);
        dest[existingSize + 1 + valSize] = quoteChar;
        dest.resize(existingSize + 2 + valSize);
    } else {
        dest.resize(existingSize + 2 * srcSize);
        int const valSize = mysql::escapeString(dest.begin() + existingSize, srcData, srcData + srcSize);
        dest.resize(existingSize + valSize);
    }
    return dest.size() - existingSize;
}

}  // namespace lsst::qserv::mysql
