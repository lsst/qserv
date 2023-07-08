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
#include "replica/ConfigDatabase.h"

// System headers
#include <algorithm>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "global/constants.h"
#include "replica/ConfigDatabaseFamily.h"
#include "replica/Performance.h"
#include "util/TimeUtils.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

bool columnInSchema(string const& name, list<SqlColDef> const& columns) {
    return columns.end() != find_if(columns.begin(), columns.end(),
                                    [&name](SqlColDef const& column) { return column.name == name; });
}
}  // namespace

namespace lsst::qserv::replica {

DatabaseInfo DatabaseInfo::create(string const& name, string const family) {
    DatabaseInfo info;
    info.name = name;
    info.family = family;
    info.createTime = util::TimeUtils::now();
    return info;
}

DatabaseInfo DatabaseInfo::parse(json const& obj, map<string, DatabaseFamilyInfo> const& families,
                                 map<string, DatabaseInfo> const& databases) {
    string const context = "DatabaseInfo::DatabaseInfo(json): ";
    if (!obj.is_object()) throw invalid_argument(context + "a JSON object is required.");
    if (obj.empty()) throw invalid_argument(context + "a JSON object is empty.");
    if (families.empty()) throw invalid_argument(context + "a collection of families is empty.");
    DatabaseInfo database;
    try {
        database.name = obj.at("database").get<string>();
        database.family = obj.at("family_name").get<string>();
        if (families.count(database.family) == 0) {
            throw invalid_argument(context + "unknown family name '" + database.family +
                                   "' specified in the JSON object for the database '" + database.name +
                                   "'.");
        }
        database.isPublished = obj.at("is_published").get<int>() != 0;
        database.createTime = obj.at("create_time").get<uint64_t>();
        database.publishTime = obj.at("publish_time").get<uint64_t>();
        if (obj.count("tables") != 0) {
            for (auto&& tableJson : obj.at("tables")) {
                TableInfo table;
                table.name = tableJson.at("name").get<string>();
                table.database = database.name;
                table.isPublished = tableJson.at("is_published").get<int>() != 0;
                table.createTime = tableJson.at("create_time").get<uint64_t>();
                table.publishTime = tableJson.at("publish_time").get<uint64_t>();
                table.isPartitioned = tableJson.at("is_partitioned").get<int>() != 0;
                if (table.isPartitioned) {
                    table.directorTable = DirectorTableRef(tableJson.at("director_table").get<string>(),
                                                           tableJson.at("director_key").get<string>());
                    table.directorTable2 = DirectorTableRef(tableJson.at("director_table2").get<string>(),
                                                            tableJson.at("director_key2").get<string>());
                    table.flagColName = tableJson.at("flag").get<string>();
                    table.angSep = tableJson.at("ang_sep").get<double>();
                    table.uniquePrimaryKey = tableJson.at("unique_primary_key").get<int>() != 0;
                    table.latitudeColName = tableJson.at("latitude_key").get<string>();
                    table.longitudeColName = tableJson.at("longitude_key").get<string>();
                    table.isDirector = table.isPartitioned && table.directorTable.tableName().empty();
                    table.isRefMatch = table.isPartitioned && !table.directorTable2.tableName().empty();
                }
                if (tableJson.count("columns") != 0) {
                    json const& columns = tableJson.at("columns");
                    if (!columns.is_array()) {
                        throw invalid_argument(context + "a JSON array is required for columns.");
                    }
                    for (auto&& column : columns) {
                        table.columns.push_back(
                                SqlColDef(column.at("name").get<string>(), column.at("type").get<string>()));
                    }
                }
                database.addTable(databases, table);
            }
        }
    } catch (invalid_argument const& ex) {
        throw;
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
    return database;
}

json DatabaseInfo::toJson() const {
    json result;
    result["database"] = name;
    result["family_name"] = family;
    result["is_published"] = isPublished ? 1 : 0;
    result["create_time"] = createTime;
    result["publish_time"] = publishTime;
    result["tables"] = json::array();
    for (auto&& itr : _tables) {
        result["tables"].push_back(itr.second.toJson());
    }
    return result;
}

vector<string> DatabaseInfo::tables() const {
    vector<string> result;
    for (auto&& itr : _tables) {
        result.push_back(itr.first);
    }
    return result;
}

vector<string> DatabaseInfo::regularTables() const {
    vector<string> result;
    for (auto&& itr : _tables) {
        if (!itr.second.isPartitioned) result.push_back(itr.first);
    }
    return result;
}

vector<string> DatabaseInfo::partitionedTables() const {
    vector<string> result;
    for (auto&& itr : _tables) {
        if (itr.second.isPartitioned) result.push_back(itr.first);
    }
    return result;
}

vector<string> DatabaseInfo::directorTables() const {
    vector<string> result;
    for (auto&& itr : _tables) {
        if (itr.second.isDirector) result.push_back(itr.first);
    }
    return result;
}

vector<string> DatabaseInfo::refMatchTables() const {
    vector<string> result;
    for (auto&& itr : _tables) {
        if (itr.second.isRefMatch) result.push_back(itr.first);
    }
    return result;
}

bool DatabaseInfo::tableExists(string const& tableName) const { return _tables.count(tableName) != 0; }

TableInfo const& DatabaseInfo::findTable(std::string const& tableName) const {
    auto itr = _tables.find(tableName);
    if (itr != _tables.end()) return itr->second;
    throw invalid_argument("DatabaseInfo::" + string(__func__) + " no such table '" + tableName +
                           "' found in the database '" + this->name + "'.");
}

TableInfo DatabaseInfo::addTable(map<string, DatabaseInfo> const& databases, TableInfo const& table_,
                                 bool validate_, bool sanitize_) {
    if (validate_) {
        TableInfo const table = validate(databases, table_, sanitize_);
        _tables[table.name] = table;
        return table;
    } else {
        _tables[table_.name] = table_;
        return table_;
    }
}

TableInfo DatabaseInfo::validate(map<std::string, DatabaseInfo> const& databases, TableInfo const& table_,
                                 bool sanitize_) const {
    TableInfo const table = sanitize_ ? this->sanitize(table_) : table_;
    auto const throwIf = [&table, &database = this->name, func = __func__](bool cond, string const& message) {
        if (!cond) return;
        throw invalid_argument("DatabaseInfo::" + string(func) + " " + message + " [database=" + database +
                               "] " + table.toJson().dump());
    };
    auto const throwIfNot = [&throwIf](bool cond, string const& message) { throwIf(!cond, message); };

    throwIf(table.name.empty(), "table name is empty");
    throwIf(tableExists(table.name), "table already exists");
    throwIf(table.database.empty(), "database name is empty");
    throwIf(table.database != this->name, "database name doesn't match the current database");
    throwIf(table.isPublished && (table.publishTime <= table.createTime),
            "inconsistent timestamps of the published table");
    throwIf(!table.isPublished && (table.publishTime != 0),
            "the publish timestamp of the non-published table is not 0");

    bool const isRegularType = !table.isPartitioned && !(table.isDirector || table.isRefMatch);
    bool const isPartitionedType = table.isPartitioned && !(table.isDirector && table.isRefMatch);
    throwIfNot(isRegularType || isPartitionedType, "ambiguous table type definition");

    if (table.isPartitioned) {
        // This collection will get populated with special columns required for
        // the table, depending on its declared type.
        map<string, string> colDefs;

        if (table.isDirector) {
            throwIfNot(table.directorTable.tableName().empty() && table.directorTable2.empty(),
                       "the director table can't be the dependant of other director(s)");

            // This is the required PK of the director table that will be references by
            // to by the corresponding FKs of the dependent tables.
            throwIf(table.directorTable.primaryKeyColumn().empty(),
                    "the director table definition requires a valid director key");
            colDefs.insert({"directorTable.primaryKeyColumn()", table.directorTable.primaryKeyColumn()});

            throwIfNot(table.directorTable2.primaryKeyColumn().empty(),
                       "the director table definition can't have the second director's key");

            // The spatial coordinate columns are required for the director tables
            throwIf(table.latitudeColName.empty() || table.longitudeColName.empty(),
                    "the director table requires both spatial coordinate columns");
            colDefs.insert({"latitudeColName", table.latitudeColName});
            colDefs.insert({"longitudeColName", table.longitudeColName});

            // This column is required for the director tables to allow Qserv materialize
            // sub-chunks in the near-neighbour queries.
            colDefs.insert({"subChunkIdColName", lsst::qserv::SUB_CHUNK_COLUMN});
        } else if (table.isRefMatch) {
            throwIf(table.directorTable.empty() || table.directorTable2.empty(),
                    "incomplete definition of the directors for the RefMatch table");
            throwIf(table.directorTable == table.directorTable2,
                    "the director tables of the RefMatch table can't be the same");

            // Director tables references by the RefMatch tables can be in other
            // than the current one databases.
            for (auto&& tableRef : {table.directorTable, table.directorTable2}) {
                DatabaseInfo const* database = this;
                if (!tableRef.databaseName().empty()) {
                    auto const itr = databases.find(tableRef.databaseName());
                    throwIf(itr == databases.cend(),
                            "non-existing database '" + tableRef.databaseName() +
                                    "' referenced in the spec of the director table '" +
                                    tableRef.tableName() + "'");
                    database = &(itr->second);
                }
                throwIfNot(database->tableExists(tableRef.tableName()),
                           "non-existing director '" + tableRef.tableName() +
                                   "' referenced in the RefMatch definition");
                throwIfNot(database->findTable(tableRef.tableName()).isDirector,
                           "table '" + tableRef.tableName() +
                                   "' referenced in the RefMatch definition isn't the director");
            }

            // These columns are required since they're pointing to the matched objects
            // at the corresponding director tables.
            throwIf(table.directorTable.primaryKeyColumn().empty() ||
                            table.directorTable2.primaryKeyColumn().empty(),
                    "incomplete definition of the director table keys for the RefMatch table");
            throwIf(table.directorTable.primaryKeyColumn() == table.directorTable2.primaryKeyColumn(),
                    "the director table keys of the RefMatch table can't be the same");
            colDefs.insert({"directorTable.primaryKeyColumn()", table.directorTable.primaryKeyColumn()});
            colDefs.insert({"directorTable2.primaryKeyColumn()", table.directorTable2.primaryKeyColumn()});

            // The columns with flags is also required
            throwIf(table.flagColName.empty(),
                    "the RefMatch table requires the special column to store flags");
            colDefs.insert({"flagColName", table.flagColName});

            throwIfNot(table.angSep > 0,
                       "the RefMatch table requires the angular separation to be more than 0");

            throwIfNot(table.latitudeColName.empty() && table.longitudeColName.empty(),
                       "the RefMatch table can't have spatial coordinate columns");
        } else {
            throwIf(table.directorTable.empty(), "the dependent table requires a valid director");
            throwIfNot(table.directorTable.databaseName().empty(),
                       "the database name isn't allowed in the director"
                       " table spec of the dependent tables");
            throwIfNot(tableExists(table.directorTable.tableName()),
                       "non-existing director table referenced in the dependent table definition");
            throwIfNot(findTable(table.directorTable.tableName()).isDirector,
                       "a table referenced in the dependent table definition isn't the director table");
            throwIfNot(table.directorTable2.empty(), "the dependent table can't have the second director");

            // This is the required FK to the corresponding director table.
            throwIf(table.directorTable.primaryKeyColumn().empty(),
                    "the director table definition requires a valid director key");
            colDefs.insert({"directorTable.primaryKeyColumn()", table.directorTable.primaryKeyColumn()});

            throwIfNot(table.directorTable2.primaryKeyColumn().empty(),
                       "the dependent table can't have the second director key");

            // The dependent table is allowed not to have the spatial coordinates since
            // it's guaranteed to have the direct association with its director table
            // via FK -> PK (see the test above for the value of the table.directorTable.primaryKeyColumn()).
            // However, if the coordinates are provided then they must be provided both.
            // The following will enforce the consistency later during the column
            // verification stage.
            throwIf(table.latitudeColName.empty() != table.longitudeColName.empty(),
                    "inconsistent definition of the spatial coordinate columns");
            if (!table.latitudeColName.empty()) {
                colDefs.insert({"latitudeColName", table.latitudeColName});
                colDefs.insert({"longitudeColName", table.longitudeColName});
            }
        }

        // Verify if the special columns exist in the schema provided to the method.
        for (auto&& column : colDefs) {
            string const& role = column.first;
            string const& name = column.second;
            throwIf(name.empty(), "a valid column name must be provided for parameter '" + role + "'");
            throwIfNot(::columnInSchema(name, table.columns),
                       "no matching column '" + name + "' found in schema for parameter '" + role + "'");
        }
    } else {
        throwIfNot(table.directorTable.empty() && table.directorTable2.empty(),
                   "fully replicated tables can't depend on director(s)");
        throwIfNot(table.directorTable.primaryKeyColumn().empty() &&
                           table.directorTable2.primaryKeyColumn().empty(),
                   "fully replicated tables can't have the director keys");
        throwIfNot(table.latitudeColName.empty() && table.longitudeColName.empty(),
                   "fully replicated tables can't have spatial coordinate columns");
    }
    return table;
}

TableInfo DatabaseInfo::sanitize(TableInfo const& table_) const {
    TableInfo table = table_;
    table.database = this->name;
    if (table.createTime == 0) {
        table.createTime = util::TimeUtils::now();
    }
    if (table.publishTime == 0 && !table.isPublished) {
        table.publishTime = 0;
    }
    if (table.isPartitioned) {
        if (table.isDirector != table.isRefMatch) {
            // For the known specialization of the partitioned table type sanitize
            // other attributes depending on the type. Note that such explicit
            // specialization always takes precedence.
            if (table.isDirector) {
                table.directorTable = DirectorTableRef("", table.directorTable.primaryKeyColumn());
                table.directorTable2 = DirectorTableRef();
                table.flagColName = string();
                table.angSep = 0;
            } else {
                table.latitudeColName = string();
                table.longitudeColName = string();
            }
        } else if (table.isDirector && table.isRefMatch) {
            // It's impossible to do anything here due to the explicitly
            // made table type ambiguity.
            ;
        } else {
            // If neither type flags were set then try deducing the table type based
            // on the presence of the director table columns.
            if (table.directorTable.tableName().empty()) {
                table.isDirector = true;
                table.isRefMatch = false;
                table.directorTable = DirectorTableRef("", table.directorTable.primaryKeyColumn());
                table.directorTable2 = DirectorTableRef();
                table.flagColName = string();
                table.angSep = 0;
            } else {
                if (table.directorTable2.tableName().empty()) {
                    table.isDirector = false;
                    table.isRefMatch = false;
                    table.directorTable2 = DirectorTableRef();
                    table.flagColName = string();
                    table.angSep = 0;
                } else {
                    table.isDirector = false;
                    table.isRefMatch = true;
                    table.latitudeColName = string();
                    table.longitudeColName = string();
                }
            }
        }
    } else {
        table.isDirector = false;
        table.isRefMatch = false;
        table.directorTable = DirectorTableRef();
        table.directorTable2 = DirectorTableRef();
        table.latitudeColName = string();
        table.longitudeColName = string();
        table.flagColName = string();
        table.angSep = 0;
    }
    return table;
}

void DatabaseInfo::removeTable(string const& tableName) {
    string const context = "DatabaseInfo::" + string(__func__) + " ";
    if (tableName.empty()) throw invalid_argument(context + "the table name can't be empty.");
    auto thisTableItr = _tables.find(tableName);
    if (thisTableItr == _tables.end()) {
        throw invalid_argument(context + "no such table '" + tableName + "' in the database '" + this->name +
                               "'.");
    }
    TableInfo& thisTableInfo = thisTableItr->second;
    if (thisTableInfo.isDirector) {
        // Make sure no dependent tables exists for this director
        // among other partitioned tables.
        for (auto&& itr : _tables) {
            TableInfo const& tableInfo = itr.second;
            if (!tableInfo.isPartitioned || (tableInfo.name == thisTableInfo.name)) continue;
            if (tableInfo.isDependantOf(thisTableInfo.name)) {
                throw invalid_argument(context + "can't remove the director table '" + tableName +
                                       "' from the database '" + this->name +
                                       "' because it has dependent tables.");
            }
        }
    }
    _tables.erase(thisTableItr);
}

ostream& operator<<(ostream& os, DatabaseInfo const& info) {
    os << "DatabaseInfo: " << info.toJson().dump();
    return os;
}

}  // namespace lsst::qserv::replica
