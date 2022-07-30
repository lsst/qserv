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
#include "replica/DatabaseMySQLGenerator.h"

// System headers
#include <sstream>

// Qserv headers
#include "replica/DatabaseMySQL.h"

using namespace std;

namespace lsst::qserv::replica::database::mysql {

Keyword const Keyword::SQL_NULL{"NULL"};

Function const Function::LAST_INSERT_ID{"LAST_INSERT_ID()"};

Function const Function::COUNT_STAR{"COUNT(*)"};

Function const Function::STAR{"*"};

Function const Function::DATABASE{"DATABASE()"};

Function const Function::NOW{"NOW()"};

Function Function::UNIX_TIMESTAMP(SqlId const& id) { return Function("UNIX_TIMESTAMP(" + id.str + ")"); }

Function Function::TIMESTAMPDIFF(string const& resolution, SqlId const& lhs, SqlId const& rhs) {
    return Function("TIMESTAMPDIFF(" + resolution + "," + lhs.str + "," + rhs.str + ")");
}

QueryGenerator::QueryGenerator(shared_ptr<Connection> conn) : _conn(conn) {}

string QueryGenerator::escape(string const& str) const { return _conn == nullptr ? str : _conn->escape(str); }

DoNotProcess QueryGenerator::sqlValue(vector<string> const& coll) const {
    ostringstream values;
    for (auto&& v : coll) {
        values << v << ',';
    }
    return sqlValue(values.str());
}

string QueryGenerator::sqlCreateTable(SqlId const& id, bool ifNotExists, list<SqlColDef> const& columns,
                                      list<string> const& keys, string const& engine,
                                      string const& comment) const {
    string sql = "CREATE TABLE ";
    if (ifNotExists) sql += "IF NOT EXISTS ";
    sql += id.str + " (";
    string sqlColumns;
    for (auto&& column : columns) {
        if (!sqlColumns.empty()) sqlColumns += ",";
        sqlColumns += sqlId(column.name).str + " " + column.type;
    }
    sql += sqlColumns;
    for (auto&& key : keys) {
        sql += "," + key;
    }
    sql += ") ENGINE=" + engine;
    if (!comment.empty()) sql += " COMMENT=" + sqlValue(comment).str;
    return sql;
}

string QueryGenerator::_sqlCreateIndex(SqlId const& tableId, string const& indexName, string const& spec,
                                       list<tuple<string, unsigned int, bool>> const& keys,
                                       string const& comment) const {
    string packedKeys;
    for (auto&& key : keys) {
        if (!packedKeys.empty()) packedKeys += ",";
        packedKeys += sqlId(get<0>(key)).str;
        unsigned int const length = get<1>(key);
        if (length != 0) packedKeys += "(" + to_string(length) + ")";
        bool const ascending = get<2>(key);
        packedKeys += ascending ? " ASC" : " DESC";
    }
    string sql = "CREATE ";
    if (!spec.empty()) sql += spec + " ";
    sql += "INDEX " + sqlId(indexName).str + " ON " + sqlId(tableId).str + " (" + packedKeys + ")" +
           " COMMENT " + sqlValue(comment).str;
    return sql;
}

}  // namespace lsst::qserv::replica::database::mysql
