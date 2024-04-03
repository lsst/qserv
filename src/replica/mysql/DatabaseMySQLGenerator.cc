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
#include "replica/mysql/DatabaseMySQLGenerator.h"

// System headers
#include <stdexcept>
#include <sstream>
#include <type_traits>

// Qserv headers
#include "replica/mysql/DatabaseMySQL.h"

using namespace std;

namespace lsst::qserv::replica::database::mysql {

[[noreturn]] void throwOnInvalidScope(string const& func, SqlVarScope scope) {
    string const msg = "QueryGenerator::" + func + " unsupported scope: " +
                       to_string(static_cast<typename underlying_type<SqlVarScope>::type>(scope));
    throw invalid_argument(msg);
}

Sql const Sql::NULL_{"NULL"};

Sql const Sql::LAST_INSERT_ID{"LAST_INSERT_ID()"};

Sql const Sql::COUNT_STAR{"COUNT(*)"};

Sql const Sql::STAR{"*"};

Sql const Sql::DATABASE{"DATABASE()"};

Sql const Sql::NOW{"NOW()"};

Sql Sql::MAX_(SqlId const& sqlId) { return Sql("MAX(" + sqlId.str + ")"); }

Sql Sql::UNIX_TIMESTAMP(SqlId const& sqlId) { return Sql("UNIX_TIMESTAMP(" + sqlId.str + ")"); }

Sql Sql::TIMESTAMPDIFF(string const& resolution, SqlId const& lhs, SqlId const& rhs) {
    return Sql("TIMESTAMPDIFF(" + resolution + "," + lhs.str + "," + rhs.str + ")");
}

Sql Sql::QSERV_MANAGER(DoNotProcess const& sqlVal) { return Sql("QSERV_MANAGER(" + sqlVal.str + ")"); }

QueryGenerator::QueryGenerator(shared_ptr<Connection> conn) : _conn(conn) {}

string QueryGenerator::escape(string const& str) const { return _conn == nullptr ? str : _conn->escape(str); }

DoNotProcess QueryGenerator::val(vector<string> const& coll) const {
    string str;
    for (auto&& v : coll) {
        if (!str.empty()) str += ',';
        str += v;
    }
    return val(str);
}

string QueryGenerator::limit(unsigned int num, unsigned int offset) const {
    if (num == 0) return string();
    string str = " LIMIT " + to_string(num);
    if (offset != 0) str += " OFFSET " + to_string(offset);
    return str;
}

string QueryGenerator::createTable(SqlId const& sqlId, bool ifNotExists, list<SqlColDef> const& columns,
                                   list<string> const& keys, string const& engine,
                                   string const& comment) const {
    string sql = "CREATE TABLE ";
    if (ifNotExists) sql += "IF NOT EXISTS ";
    sql += sqlId.str + " (";
    string sqlColumns;
    for (auto&& column : columns) {
        if (!sqlColumns.empty()) sqlColumns += ",";
        sqlColumns += id(column.name).str + " " + column.type;
    }
    sql += sqlColumns;
    for (auto&& key : keys) {
        sql += "," + key;
    }
    sql += ") ENGINE=" + engine;
    if (!comment.empty()) sql += " COMMENT=" + val(comment).str;
    return sql;
}

string QueryGenerator::insertPacked(string const& tableName, string const& packedColumns,
                                    vector<string> const& packedValues) const {
    _assertNotEmpty(__func__, packedValues);
    string sql = "INSERT INTO " + id(tableName).str + " (" + packedColumns + ") VALUES ";
    for (size_t i = 0, size = packedValues.size(); i < size; ++i) {
        if (i != 0) sql += ",";
        sql += "(" + packedValues[i] + ")";
    }
    return sql;
}

vector<string> QueryGenerator::insertPacked(string const& tableName, string const& packedColumns,
                                            vector<string> const& packedValues,
                                            size_t const maxQueryLength) const {
    _assertNotEmpty(__func__, packedValues);
    vector<string> queries;
    string sql;
    size_t numRowsPacked = 0;
    for (vector<string>::const_iterator itr = packedValues.cbegin(); itr != packedValues.cend();) {
        string const& row = *itr;
        if (sql.empty()) {
            sql = "INSERT INTO " + id(tableName).str + " (" + packedColumns + ") VALUES ";
        }
        // 2 more characters are needed for injecting the first row: "(" + row + ")"
        // And 1 more - for subsequent rows: ",(" + row + ")"
        size_t const extraSpacePerRow = (numRowsPacked == 0 ? 2 : 3);
        size_t const projectedQueryLength = sql.size() + extraSpacePerRow + row.size();
        if (projectedQueryLength <= maxQueryLength) {
            // -- Extend the current query and move on to the next row (if any)
            if (numRowsPacked != 0) sql += ",";
            sql += "(" + row + ")";
            numRowsPacked++;
            ++itr;
        } else {
            // -- Flush the current query and start building the next one
            if (numRowsPacked == 0) {
                string const msg = "QueryGenerator::" + string(__func__) + " the generated query length " +
                                   to_string(projectedQueryLength) + " exceeds the limit " +
                                   to_string(maxQueryLength);
                throw invalid_argument(msg);
            }
            queries.push_back(move(sql));
            sql = string();
            numRowsPacked = 0;
        }
    }
    // -- Flush the current query
    if (!sql.empty()) queries.push_back(move(sql));
    return queries;
}

void QueryGenerator::_assertNotEmpty(string const& func, vector<string> const& coll) {
    if (coll.empty()) {
        throw invalid_argument("QueryGenerator::" + func + " the input collection is empty.");
    }
}

string QueryGenerator::showVars(SqlVarScope scope, string const& pattern) const {
    string const like = pattern.empty() ? string() : " LIKE " + val(pattern).str;
    switch (scope) {
        case SqlVarScope::SESSION:
            return "SHOW VARIABLES" + like;
        case SqlVarScope::GLOBAL:
            return "SHOW GLOBAL VARIABLES" + like;
    }
    throwOnInvalidScope(__func__, scope);
}

string QueryGenerator::call(DoNotProcess const& packedProcAndArgs) const {
    if (packedProcAndArgs.str.empty()) {
        string const msg = "QueryGenerator::" + string(__func__) +
                           " the packed procedure and its arguments can not be empty.";
        throw invalid_argument(msg);
    }
    return "CALL " + packedProcAndArgs.str;
}

string QueryGenerator::_setVars(SqlVarScope scope, string const& packedVars) const {
    if (packedVars.empty()) {
        string const msg = "QueryGenerator::" + string(__func__) +
                           " the collection of the packed variables/values can not be empty.";
        throw invalid_argument(msg);
    }
    switch (scope) {
        case SqlVarScope::SESSION:
            return "SET " + packedVars;
        case SqlVarScope::GLOBAL:
            return "SET GLOBAL " + packedVars;
    }
    throwOnInvalidScope(__func__, scope);
}

string QueryGenerator::_createIndex(SqlId const& tableId, string const& indexName, string const& spec,
                                    list<tuple<string, unsigned int, bool>> const& keys,
                                    string const& comment) const {
    string packedKeys;
    for (auto&& key : keys) {
        if (!packedKeys.empty()) packedKeys += ",";
        packedKeys += id(get<0>(key)).str;
        unsigned int const length = get<1>(key);
        if (length != 0) packedKeys += "(" + to_string(length) + ")";
        bool const ascending = get<2>(key);
        packedKeys += ascending ? " ASC" : " DESC";
    }
    string sql = "CREATE ";
    if (!spec.empty()) sql += spec + " ";
    sql += "INDEX " + id(indexName).str + " ON " + id(tableId).str + " (" + packedKeys + ")" + " COMMENT " +
           val(comment).str;
    return sql;
}

}  // namespace lsst::qserv::replica::database::mysql
