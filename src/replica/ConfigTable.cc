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
#include "replica/ConfigTable.h"

// System headers
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <tuple>

// Qserv headers
#include "global/constants.h"
#include "replica/Performance.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

DirectorTableRef::DirectorTableRef(string const& databaseTableName, string const& primaryKeyColumn)
        : _primaryKeyColumn(primaryKeyColumn) {
    string::size_type const pos = databaseTableName.find_first_of('.');
    if (pos == string::npos) {
        _tableName = databaseTableName;
    } else {
        _databaseName = databaseTableName.substr(0, pos);
        _tableName = databaseTableName.substr(pos + 1);
    }
    if (!_databaseName.empty() && _tableName.empty())
        throw invalid_argument("DirectorTableRef: invalid database & table spec '" + databaseTableName + "'");
    if (!_tableName.empty() && _primaryKeyColumn.empty())
        throw invalid_argument("DirectorTableRef: primary key column name can not be empty");
}

string DirectorTableRef::databaseTableName() const {
    if (_databaseName.empty()) return _tableName;
    return _databaseName + "." + _tableName;
}

bool DirectorTableRef::empty() const {
    return _databaseName.empty() && _tableName.empty() && _primaryKeyColumn.empty();
}

json DirectorTableRef::toJson() const {
    json result = json::object();
    result["database_name"] = _databaseName;
    result["table_name"] = _tableName;
    result["primary_key_column"] = _primaryKeyColumn;
    return result;
}

bool operator==(DirectorTableRef const& lhs, DirectorTableRef const& rhs) {
    return (lhs.databaseName() == rhs.databaseName()) && (lhs.tableName() == rhs.tableName()) &&
           (lhs.primaryKeyColumn() == rhs.primaryKeyColumn());
}

ostream& operator<<(ostream& os, DirectorTableRef const& table) {
    os << "DirectorTableRef: " << table.toJson().dump();
    return os;
}

vector<string> TableInfo::columnNames() const {
    vector<string> names;
    transform(columns.cbegin(), columns.cend(), back_inserter(names),
              [](SqlColDef const& column) { return column.name; });
    return names;
}

bool TableInfo::isDependantOf(string const& table) const {
    if (table.empty()) {
        string const context = "TableInfo::" + string(__func__) + " ";
        throw invalid_argument(context + "the name of the director table can't be empty.");
    }
    return (directorTable.databaseTableName() == table) || (directorTable2.databaseTableName() == table);
}

string TableInfo::schema4css() const {
    string schema;
    for (auto&& column : columns) {
        schema += string(schema.empty() ? "(" : ", ") + "`" + column.name + "` " + column.type;
    }
    schema += ")";
    return schema;
}

json TableInfo::toJson() const {
    json result = json::object();
    result["name"] = name;
    result["database"] = database;
    result["is_published"] = isPublished ? 1 : 0;
    result["create_time"] = createTime;
    result["publish_time"] = publishTime;
    result["is_partitioned"] = isPartitioned ? 1 : 0;
    result["is_director"] = isDirector() ? 1 : 0;
    result["is_ref_match"] = isRefMatch() ? 1 : 0;
    result["director_table"] = directorTable.databaseTableName();
    result["director_database_name"] = directorTable.databaseName();
    result["director_table_name"] = directorTable.tableName();
    result["director_key"] = directorTable.primaryKeyColumn();
    result["director_table2"] = directorTable2.databaseTableName();
    result["director_database_name2"] = directorTable2.databaseName();
    result["director_table_name2"] = directorTable2.tableName();
    result["director_key2"] = directorTable2.primaryKeyColumn();
    result["flag"] = flagColName;
    result["ang_sep"] = angSep;
    result["unique_primary_key"] = uniquePrimaryKey ? 1 : 0;
    result["latitude_key"] = latitudeColName;
    result["longitude_key"] = longitudeColName;
    // The array representation is required to preserve the relative order
    // of the table's columns.
    json columnsJson = json::array();
    for (auto&& column : columns) {
        json columnJson = json::object();
        columnJson["name"] = column.name;
        columnJson["type"] = column.type;
        columnsJson.push_back(columnJson);
    }
    result["columns"] = columnsJson;
    return result;
}

bool operator==(TableInfo const& lhs, TableInfo const& rhs) {
    return (lhs.columns == rhs.columns) && (lhs.name == rhs.name) && (lhs.database == rhs.database) &&
           (lhs.isPartitioned == rhs.isPartitioned) && (lhs.directorTable == rhs.directorTable) &&
           (lhs.directorTable2 == rhs.directorTable2) && (lhs.flagColName == rhs.flagColName) &&
           (lhs.uniquePrimaryKey == rhs.uniquePrimaryKey) && (lhs.latitudeColName == rhs.latitudeColName) &&
           (lhs.longitudeColName == rhs.longitudeColName);
}

ostream& operator<<(ostream& os, TableInfo const& info) {
    os << "TableInfo: " << info.toJson().dump();
    return os;
}

}  // namespace lsst::qserv::replica
