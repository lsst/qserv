
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
#include "replica/mysql/DatabaseMySQLUtils.h"

// Qserv headers
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLExceptions.h"

// System headers
#include <cctype>
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace {

// Bi-directional translation maps.
// Note that the translation is always between a single character and and a string that
// has exactly 5 characters. This assumption is used in the implementation of the methods below.
// The first character of the string is always '@', and the rest of the characters are
// the hexadecimal representation of the character in the ASCII encoding.

map<char, string> const obj2fsMap = {
        {' ', "@0020"},  {'!', "@0021"},  {'"', "@0022"}, {'#', "@0023"}, {'$', "@0024"}, {'%', "@0025"},
        {'&', "@0026"},  {'\'', "@0027"}, {'(', "@0028"}, {')', "@0029"}, {'*', "@002a"}, {'+', "@002b"},
        {',', "@002c"},  {'-', "@002d"},  {'.', "@002e"}, {'/', "@002f"}, {':', "@003a"}, {';', "@003b"},
        {'<', "@003c"},  {'=', "@003d"},  {'>', "@003e"}, {'?', "@003f"}, {'@', "@0040"}, {'[', "@005b"},
        {'\\', "@005c"}, {']', "@005d"},  {'^', "@005e"}, {'`', "@0060"}, {'{', "@007b"}, {'|', "@007c"},
        {'}', "@007d"},  {'~', "@007e"}};

map<string, char> const fs2objMap = {
        {"@0020", ' '},  {"@0021", '!'},  {"@0022", '"'}, {"@0023", '#'}, {"@0024", '$'}, {"@0025", '%'},
        {"@0026", '&'},  {"@0027", '\''}, {"@0028", '('}, {"@0029", ')'}, {"@002a", '*'}, {"@002b", '+'},
        {"@002c", ','},  {"@002d", '-'},  {"@002e", '.'}, {"@002f", '/'}, {"@003a", ':'}, {"@003b", ';'},
        {"@003c", '<'},  {"@003d", '='},  {"@003e", '>'}, {"@003f", '?'}, {"@0040", '@'}, {"@005b", '['},
        {"@005c", '\\'}, {"@005d", ']'},  {"@005e", '^'}, {"@0060", '`'}, {"@007b", '{'}, {"@007c", '|'},
        {"@007d", '}'},  {"@007e", '~'}};
}  // namespace

namespace lsst::qserv::replica::database::mysql {
namespace detail {

bool selectSingleValueImpl(shared_ptr<Connection> const& conn, string const& query,
                           function<bool(Row&)> const& onEachRow, bool noMoreThanOne) {
    string const context = "DatabaseMySQLUtils::" + string(__func__) + " ";
    conn->execute(query);
    if (!conn->hasResult()) {
        throw logic_error(context + "wrong query type - the query doesn't have any result set.");
    }
    bool isNotNull = false;
    size_t numRows = 0;
    Row row;
    while (conn->next(row)) {
        // Only the very first row matters
        if (numRows == 0) isNotNull = onEachRow(row);
        // Have to read the rest of the result set to avoid problems with
        // the MySQL protocol
        ++numRows;
    }
    switch (numRows) {
        case 0:
            throw EmptyResultSetError(context + "result set is empty.");
        case 1:
            return isNotNull;
        default:
            if (!noMoreThanOne) return isNotNull;
    }
    throw logic_error(context + "result set has more than 1 row");
}

}  // namespace detail

json processList(shared_ptr<Connection> const& conn, bool full) {
    string const query = "SHOW" + string(full ? " FULL" : "") + " PROCESSLIST";
    json result;
    result["queries"] = json::object({{"columns", json::array()}, {"rows", json::array()}});
    conn->executeInOwnTransaction([&](auto conn) {
        conn->execute(query);
        if (conn->hasResult()) {
            result["queries"]["columns"] = conn->columnNames();
            auto& rows = result["queries"]["rows"];
            Row row;
            while (conn->next(row)) {
                json resultRow = json::array();
                for (size_t colIdx = 0, numColumns = row.numColumns(); colIdx < numColumns; ++colIdx) {
                    resultRow.push_back(row.getAs<string>(colIdx, string()));
                }
                rows.push_back(resultRow);
            }
        }
    });
    return result;
}

std::string obj2fs(std::string const& objectName) {
    if (objectName.empty()) throw invalid_argument("Object name is empty");
    string result;
    result.reserve(objectName.size() * 5);  // Reserve enough space for the worst case
    for (char c : objectName) {
        auto it = ::obj2fsMap.find(c);
        if (it != ::obj2fsMap.end()) {
            result += it->second;
        } else {
            result += c;
        }
    }
    return result;
}

std::string fs2obj(std::string const& fileSystemName) {
    if (fileSystemName.empty()) throw invalid_argument("File system name is empty");
    string result = fileSystemName;
    size_t pos = 0;
    while ((pos = result.find('@', pos)) != string::npos) {
        // Ensure that at least 5 characters are remaining to form a valid translation
        if (pos + 5 > result.size()) break;

        auto it = ::fs2objMap.find(result.substr(pos, 5));
        if (it != ::fs2objMap.end()) {
            result.replace(pos, 5, string(1, it->second));
        }
        pos += 1;  // Move to the next character after the '@'
    }
    return result;
}

bool isValidObjectName(std::string const& objectName) {
    if (objectName.empty()) throw invalid_argument("Object name is empty");
    for (char c : objectName) {
        if (isalnum(c) || c == '_') continue;
        if (::obj2fsMap.find(c) != ::obj2fsMap.end()) continue;
        return false;
    }
    return true;
}

}  // namespace lsst::qserv::replica::database::mysql
