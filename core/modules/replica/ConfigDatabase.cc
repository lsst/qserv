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
#include "replica/ConfigDatabaseFamily.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

bool columnInSchema(string const& colName, list<SqlColDef> const& columns) {
    return columns.end() != find_if(
            columns.begin(), columns.end(),
            [&colName] (SqlColDef const& coldef) { return coldef.name == colName; });
}
} // namespace

namespace lsst {
namespace qserv {
namespace replica {

DatabaseInfo::DatabaseInfo(json const& obj,
                           map<string, DatabaseFamilyInfo> const& families) {
    string const context = "DatabaseInfo::DatabaseInfo(json): ";
    if (obj.empty()) return;    // the default construction
    if (!obj.is_object()) throw invalid_argument(context + "a JSON object is required.");
    try {
        name = obj.at("database").get<string>();
        family = obj.at("family_name").get<string>();
        if (families.count(family) == 0) {
            throw invalid_argument(
                    context + "unknown family name '" + family + "' specified in the JSON object.");
        }
        isPublished  = obj.at("is_published").get<int>() != 0;
        if (obj.count("tables") != 0) {
            for (auto&& itr: obj.at("tables").items()) {
                string const& table = itr.key();
                json const& tableJson = itr.value();
                if (table != tableJson.at("name").get<string>()) {
                    throw invalid_argument(
                            context + "the table name '" + table + "' found in a dictionary of the database's " + name 
                            + "' tables is not consistent within the table's name '" + tableJson.at("name").get<string>()
                            + "' within the JSON object representing the table.");
                }
                bool const isPartitioned = tableJson.at("is_partitioned").get<int>() != 0;
                if (isPartitioned) {
                    partitionedTables.push_back(table);
                    latitudeColName[table]  = tableJson.at("latitude_key").get<string>();
                    longitudeColName[table] = tableJson.at("longitude_key").get<string>();
                } else {
                    regularTables.push_back(table);
                    latitudeColName[table]  = string();
                    longitudeColName[table] = string();
                }
            }
        }
        if (obj.count("columns") != 0) {
            for (auto&& itr: obj.at("columns").items()) {
                auto&& table = itr.key();
                auto&& columnsJson = itr.value();
                list<SqlColDef>& tableColumns = columns[table];
                for (auto&& coldefJson: columnsJson) {
                    tableColumns.push_back(
                        SqlColDef(coldefJson.at("name").get<string>(), coldefJson.at("type").get<string>()));
                }
            }
        }
        directorTable     = obj.at("director_table").get<string>();
        directorTableKey  = obj.at("director_key").get<string>();
        chunkIdColName    = obj.at("chunk_id_key").get<string>();
        subChunkIdColName = obj.at("sub_chunk_id_key").get<string>();
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}


vector<string> DatabaseInfo::tables() const {
    vector<string> result = partitionedTables;
    result.insert(result.end(), regularTables.begin(), regularTables.end());
    return result;
}


json DatabaseInfo::toJson() const {
    json infoJson;
    infoJson["database"] = name;
    infoJson["family_name"] = family;
    infoJson["is_published"] = isPublished ? 1 : 0;
    infoJson["tables"] = json::object();
    for (auto&& name: partitionedTables) {
        infoJson["tables"][name] = json::object({
            {"name", name},
            {"is_partitioned", 1},
            {"latitude_key", latitudeColName.at(name)},
            {"longitude_key", longitudeColName.at(name)}
        });
    }
    for (auto&& name: regularTables) {
        infoJson["tables"][name] = json::object({
            {"name", name},
            {"is_partitioned", 0},
            {"latitude_key", ""},
            {"longitude_key", ""}
        });
    }
    for (auto&& columnsEntry: columns) {
        string const& table = columnsEntry.first;
        auto const& coldefs = columnsEntry.second;
        json coldefsJson;
        for (auto&& coldef: coldefs) {
            json coldefJson;
            coldefJson["name"] = coldef.name;
            coldefJson["type"] = coldef.type;
            coldefsJson.push_back(coldefJson);
        }
        infoJson["columns"][table] = coldefsJson;
    }
    infoJson["director_table"] = directorTable;
    infoJson["director_key"] = directorTableKey;
    infoJson["chunk_id_key"] = chunkIdColName;
    infoJson["sub_chunk_id_key"] = subChunkIdColName;
    return infoJson;
}


string DatabaseInfo::schema4css(string const& table) const {
    string schema;
    for (auto const& coldef: columns.at(table)) {
        schema += string(schema.empty() ? "(" : ", ") + "`" + coldef.name + "` " + coldef.type;
    }
    schema += ")";
    return schema;
}


bool DatabaseInfo::isPartitioned(string const& table) const {
    if (partitionedTables.end() != find(partitionedTables.begin(), partitionedTables.end(), table))  return true;
    if (regularTables.end() != find(regularTables.begin(), regularTables.end(), table)) return false;
    throw invalid_argument(
            "DatabaseInfo::" + string(__func__) +
            "no such table '" + table + "' found in database '" + name + "'");
}


bool DatabaseInfo::isDirector(string const& table) const {
    // This test will also ensure the table is known. Otherwise, an exception
    // will be thrown.
    if (not isPartitioned(table)) return false;
    return table == directorTable;
}


bool DatabaseInfo::hasTable(std::string const& table) const {
    for (auto const collection: {&partitionedTables, &regularTables}) {
        if (collection->cend() != find(collection->cbegin(), collection->cend(), table)) return true;
    }
    return false;
}


void DatabaseInfo::addTable(
        string const& table, list<SqlColDef> const& columns_,
        bool isPartitioned, bool isDirectorTable, string const& directorTableKey_,
        string const& chunkIdColName_, string const& subChunkIdColName_,
        string const& latitudeColName_, string const& longitudeColName_) {

    string const context = "DatabaseInfo::" + string(__func__) + " ";

    if (hasTable(table)) {
        throw invalid_argument(context + "table '" + table + "' already exists.");
    }
    if (isPartitioned) {
        map<string, string> const colDefs = {
            {"chunkIdColName", chunkIdColName_},
            {"subChunkIdColName", subChunkIdColName_}
        };
        for (auto&& entry: colDefs) {
            string const& role = entry.first;
            string const& colName = entry.second;
            if (colName.empty()) {
                throw invalid_argument(
                        context + "a valid column name must be provided"
                        " for the '" + role + "' parameter of the partitioned table");
            }
            if (!columnInSchema(colName, columns_)) {
                throw invalid_argument(
                        context + "no matching column found in the provided"
                        " schema for name '" + colName + " as required by parameter '" + role +
                        "' of the partitioned table: '" + table + "'");
            }
        }
        if (isDirectorTable) {
            if (!directorTable.empty()) {
                throw invalid_argument(
                        context + "another table '" + directorTable +
                        "' was already claimed as the 'director' table.");
            }
            if (directorTableKey_.empty()) {
                throw invalid_argument(
                        context + "a valid column name must be provided"
                        " for the 'director' table");
            }
            if (!columnInSchema(directorTableKey_, columns_)) {
                throw invalid_argument(
                        context + "a value of parameter 'directorTableKey'"
                        " provided for the 'director' table '" + table + "' doesn't match any column"
                        " in the table schema");
            }
            if (!latitudeColName_.empty()) {
                if (!columnInSchema(latitudeColName_, columns_)) {
                    throw invalid_argument(
                            context + "a value '" + latitudeColName_ + "' of parameter 'latitudeColName'"
                            " provided for the partitioned table '" + table + "' doesn't match any column"
                            " in the table schema");
                }
            }
            if (!longitudeColName_.empty()) {
                if (!columnInSchema(longitudeColName_, columns_)) {
                    throw invalid_argument(
                            context + "a value '" + longitudeColName_ + "' of parameter 'longitudeColName'"
                            " provided for the partitioned table '" + table + "' doesn't match any column"
                            " in the table schema");
                }
            }
            directorTable = table;
            directorTableKey = directorTableKey_;
            chunkIdColName = chunkIdColName_;
            subChunkIdColName = subChunkIdColName_;
        }
        latitudeColName[table] = latitudeColName_;
        longitudeColName[table] = longitudeColName_;
        partitionedTables.push_back(table);
    } else {
        if (isDirectorTable) {
            throw invalid_argument(context + "non-partitioned tables can't be the 'director' ones");
        }
        regularTables.push_back(table);
    }
    columns[table] = columns_;
}


void DatabaseInfo::removeTable(std::string const& table) {
    bool const partitioned = isPartitioned(table);
    bool const director = isDirector(table);
    if (partitioned) {
        partitionedTables.erase(
            find(partitionedTables.begin(),
                 partitionedTables.end(),
                 table)
        );
        if (director) {
            // These attributes are set for the director table only.
            directorTable = "";
            directorTableKey = "";
            chunkIdColName = "";
            subChunkIdColName = "";
        }
        latitudeColName.erase(table);
        longitudeColName.erase(table);
    } else {
       regularTables.erase(
            find(regularTables.begin(),
                 regularTables.end(),
                 table)
        );
    }
    columns.erase(table);
}


ostream& operator <<(ostream& os, DatabaseInfo const& info) {
    os  << "DatabaseInfo: " << info.toJson().dump();
    return os;
}

}}} // namespace lsst::qserv::replica
