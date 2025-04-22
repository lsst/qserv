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
#ifndef LSST_QSERV_MYSQL_MYSQLUTILS_H
#define LSST_QSERV_MYSQL_MYSQLUTILS_H

// System headers
#include <cassert>
#include <limits>
#include <stdexcept>

// Third-party headers
#include "nlohmann/json.hpp"

/// Forward declarations.
namespace lsst::qserv::mysql {
class MySqlConfig;
}  // namespace lsst::qserv::mysql

/// This header declarations.
namespace lsst::qserv::mysql {

/**
 * Class MySqlQueryError represents exceptions to be throw on specific errors
 * detected when attempting to execute the queries.
 */
class MySqlQueryError : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/**
 * Class MySqlUtils is the utility class providing a collection of useful queries reporting
 * small result sets.
 * @note Each tool of the collection does its own connection handling (opening/etc.).
 */
class MySqlUtils {
public:
    /**
     * Report info on the on-going queries using 'SHOW [FULL] PROCESSLIST'.
     * @param A scope of the operaton depends on the user credentials privided
     *   in the configuration object. Normally, a subset of queries which belong
     *   to the specified user will be reported.
     * @param config Configuration parameters of the MySQL connector.
     * @param full The optional modifier which (if set) allows seeing the full text
     *   of the queries.
     * @return A collection of queries encoded as the JSON object. Please, see the code
     *   for further details on the schema of the object.
     * @throws MySqlQueryError on errors detected during query execution/processing.
     */
    static nlohmann::json processList(MySqlConfig const& config, bool full = false);
};

/**
 * Escape a bytestring for LOAD DATA INFILE, as specified by MySQL doc:
 * https://dev.mysql.com/doc/refman/5.1/en/load-data.html
 *
 * This implementation is limited to:
 *
 *   Char  Escape Sequence
 *   ----  ----------------
 *   \0    An ASCII NUL (0x00) character
 *   \b    A backspace character
 *   \n    A newline (linefeed) character
 *   \r    A carriage return character
 *   \t    A tab character.
 *   \Z    ASCII 26 (Control+Z)
 *   \N    NULL
 *
 *  @return the number of bytes written to dest
 */
template <typename Iter, typename CIter>
inline int escapeString(Iter destBegin, CIter srcBegin, CIter srcEnd) {
    // mysql_real_escape_string(_mysql, cursor, col, r.lengths[i]);
    // empty string isn't escaped
    if (srcEnd == srcBegin) return 0;
    assert(srcEnd - srcBegin > 0);
    assert(srcEnd - srcBegin < std::numeric_limits<int>::max() / 2);
    Iter destI = destBegin;
    for (CIter i = srcBegin; i != srcEnd; ++i) {
        switch (*i) {
            case '\0':
                *destI++ = '\\';
                *destI++ = '0';
                break;
            case '\b':
                *destI++ = '\\';
                *destI++ = 'b';
                break;
            case '\n':
                *destI++ = '\\';
                *destI++ = 'n';
                break;
            case '\r':
                *destI++ = '\\';
                *destI++ = 'r';
                break;
            case '\t':
                *destI++ = '\\';
                *destI++ = 't';
                break;
            case '\032':
                *destI++ = '\\';
                *destI++ = 'Z';
                break;
            case '\\': {
                auto const nextI = i + 1;
                if (srcEnd == nextI) {
                    *destI++ = *i;
                } else if (*nextI != 'N') {
                    *destI++ = '\\';
                    *destI++ = '\\';
                } else {
                    // in this case don't modify anything, because Null (\N) is not treated by escaping in
                    // this context.
                    *destI++ = *i;
                }
                break;
            }
            default:
                *destI++ = *i;
                break;
        }
    }
    return destI - destBegin;
}

/// The specialized version of the function for the char* type.
int escapeString(char* dest, char const* src, int srcLength);

/**
 * The specialized version of the function for the std::string type.
 *
 * The function will append the result to the destination string. The destination string
 * will be resized to accommodate the result. The string will be enclosed by tge optionally
 * specified quote character if requested.
 *
 * @note The function will not add the terminating zero to the destination string.
 * @return The number of bytes added to the destination string.
 */
int escapeAppendString(std::string& dest, char const* srcData, size_t srcSize, bool quote = true,
                       char quoteChar = '\'');

}  // namespace lsst::qserv::mysql

#endif  // LSST_QSERV_MYSQL_MYSQLUTILS_H
