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

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

bool columnInSchema(string const& colName, list<SqlColDef> const& columns) {
    return columns.end() != find_if(columns.begin(), columns.end(),
                                    [&colName](SqlColDef const& coldef) { return coldef.name == colName; });
}
}  // namespace

namespace lsst::qserv::replica {

DatabaseInfo::DatabaseInfo(json const& obj, map<string, DatabaseFamilyInfo> const& families) {
    string const context = "DatabaseInfo::DatabaseInfo(json): ";
    if (obj.empty()) return;  // the default construction
    if (!obj.is_object()) throw invalid_argument(context + "a JSON object is required.");
    try {
        name = obj.at("database").get<string>();
        family = obj.at("family_name").get<string>();
        if (families.count(family) == 0) {
            throw invalid_argument(context + "unknown family name '" + family +
                                   "' specified in the JSON object.");
        }
        isPublished = obj.at("is_published").get<int>() != 0;
        if (obj.count("tables") != 0) {
            for (auto&& itr : obj.at("tables").items()) {
                string const& table = itr.key();
                json const& tableJson = itr.value();
                if (table != tableJson.at("name").get<string>()) {
                    throw invalid_argument(context + "the table name '" + table +
                                           "' found in a dictionary of the database's " + name +
                                           "' tables is not consistent within the table's name '" +
                                           tableJson.at("name").get<string>() +
                                           "' within the JSON object representing the table.");
                }
                bool const isPartitioned = tableJson.at("is_partitioned").get<int>() != 0;
                if (isPartitioned) {
                    partitionedTables.push_back(table);
                    string const director = tableJson.at("director").get<string>();
                    string const directorKey = tableJson.at("director_key").get<string>();
                    string const latitudeCol = tableJson.at("latitude_key").get<string>();
                    string const longitudeCol = tableJson.at("longitude_key").get<string>();
                    if (directorKey.empty()) {
                        throw invalid_argument(context + "the director key of the partitioned table '" +
                                               table + "' can't be empty.");
                    }
                    if (director.empty()) {
                        if (latitudeCol.empty()) {
                            throw invalid_argument(context + "the latitude column of the director table '" +
                                                   table + "' can't be empty.");
                        }
                        if (longitudeCol.empty()) {
                            throw invalid_argument(context + "the longitude column of the director table '" +
                                                   table + "' can't be empty.");
                        }
                    } else {
                        if (latitudeCol.empty() != longitudeCol.empty()) {
                            throw invalid_argument(context +
                                                   "inconsistent values of the latitude and longitude "
                                                   "columns of the dependent table '" +
                                                   table +
                                                   "'. The columns must be both defined or not defined.");
                        }
                    }
                    directorTable[table] = director;
                    directorTableKey[table] = directorKey;
                    latitudeColName[table] = latitudeCol;
                    longitudeColName[table] = longitudeCol;
                } else {
                    regularTables.push_back(table);
                }
            }
        }
        if (obj.count("columns") != 0) {
            for (auto&& itr : obj.at("columns").items()) {
                auto&& table = itr.key();
                auto&& columnsJson = itr.value();
                list<SqlColDef>& tableColumns = columns[table];
                for (auto&& coldefJson : columnsJson) {
                    tableColumns.push_back(SqlColDef(coldefJson.at("name").get<string>(),
                                                     coldefJson.at("type").get<string>()));
                }
            }
        }
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}

vector<string> DatabaseInfo::tables() const {
    vector<string> result = partitionedTables;
    result.insert(result.end(), regularTables.begin(), regularTables.end());
    return result;
}

vector<std::string> DatabaseInfo::directorTables() const {
    vector<string> result;
    for (auto&& itr : directorTable) {
        // "Director" tables can't have "directors"
        if (itr.second.empty()) result.push_back(itr.first);
    }
    return result;
}

json DatabaseInfo::toJson() const {
    json infoJson;
    infoJson["database"] = name;
    infoJson["family_name"] = family;
    infoJson["is_published"] = isPublished ? 1 : 0;
    infoJson["tables"] = json::object();
    for (auto&& name : partitionedTables) {
        infoJson["tables"][name] = json::object({{"name", name},
                                                 {"is_partitioned", 1},
                                                 {"director", directorTable.at(name)},
                                                 {"director_key", directorTableKey.at(name)},
                                                 {"latitude_key", latitudeColName.at(name)},
                                                 {"longitude_key", longitudeColName.at(name)}});
    }
    for (auto&& name : regularTables) {
        infoJson["tables"][name] = json::object({{"name", name}, {"is_partitioned", 0}});
    }
    for (auto&& columnsEntry : columns) {
        string const& table = columnsEntry.first;
        auto const& coldefs = columnsEntry.second;
        json coldefsJson;
        for (auto&& coldef : coldefs) {
            json coldefJson;
            coldefJson["name"] = coldef.name;
            coldefJson["type"] = coldef.type;
            coldefsJson.push_back(coldefJson);
        }
        infoJson["columns"][table] = coldefsJson;
    }
    return infoJson;
}

string DatabaseInfo::schema4css(string const& table) const {
    string schema;
    for (auto const& coldef : columns.at(table)) {
        schema += string(schema.empty() ? "(" : ", ") + "`" + coldef.name + "` " + coldef.type;
    }
    schema += ")";
    return schema;
}

bool DatabaseInfo::isPartitioned(string const& table) const {
    if (partitionedTables.cend() != find(partitionedTables.cbegin(), partitionedTables.cend(), table))
        return true;
    if (regularTables.cend() != find(regularTables.cbegin(), regularTables.cend(), table)) return false;
    throw invalid_argument("DatabaseInfo::" + string(__func__) + "no such table '" + table +
                           "' found in database '" + name + "'");
}

bool DatabaseInfo::isDirector(string const& table) const {
    return isPartitioned(table) && directorTable.at(table).empty();
}

bool DatabaseInfo::hasTable(std::string const& table) const {
    for (auto const collection : {&partitionedTables, &regularTables}) {
        if (collection->cend() != find(collection->cbegin(), collection->cend(), table)) return true;
    }
    return false;
}

void DatabaseInfo::addTable(string const& table, list<SqlColDef> const& columns_, bool isPartitioned,
                            bool isDirector, string const& directorTable_, string const& directorTableKey_,
                            string const& latitudeColName_, string const& longitudeColName_) {
    string const context = "DatabaseInfo::" + string(__func__) + " ";

    if (hasTable(table)) {
        throw invalid_argument(context + "table '" + table + "' already exists.");
    }
    if (isPartitioned) {
        // This will get populated with special columns required for the table, depeniding
        // on its type (director or dependent).
        map<string, string> colDefs;

        // Required for both director and dependent tables in order to establish
        // the direct FK-PK association between them.
        colDefs.insert({"directorTableKey", directorTableKey_});

        if (isDirector) {
            if (!directorTable_.empty()) {
                throw invalid_argument(context + "the director table '" + table +
                                       "' can't be the dependent table of another director table '" +
                                       directorTable_ + "'.");
            }

            // This column is required for all partitioned tables for materializing
            // sub-chunks.
            colDefs.insert({"subChunkIdColName", lsst::qserv::SUB_CHUNK_COLUMN});

            // These are required for the director tables
            colDefs.insert({"latitudeColName", latitudeColName_});
            colDefs.insert({"longitudeColName", longitudeColName_});

        } else {
            if (directorTable_.empty()) {
                throw invalid_argument(
                        context + "the dependent table '" + table +
                        "' requires the name of the corresponding 'director' table to be provided.");
            }
            auto const directors = directorTables();
            if (directors.cend() == find(directors.cbegin(), directors.cend(), directorTable_)) {
                throw invalid_argument(
                        context + "the dependent  table '" + table + "' requires the director table '" +
                        directorTable_ +
                        "' that is not registered yet in Qserv. Please, make sure the"
                        " director tables get registered before the corresponding dependent tables.");
            }

            // The dependent table is allowed not to have the spatial coordinates since
            // it's guaranteed to have the direct association with its director table
            // via FK -> PK (see the test above for the value of the directorTableKey_).
            // However, if the coordinates are provided then they must be provided both.
            // The following will enforce the consistency later during the column
            // verification stage.
            if (!latitudeColName_.empty() || !longitudeColName_.empty()) {
                colDefs.insert({"latitudeColName", latitudeColName_});
                colDefs.insert({"longitudeColName", longitudeColName_});
            }
        }
        partitionedTables.push_back(table);

        // Verify if the special columns exist in the schema provided the method
        for (auto&& entry : colDefs) {
            string const& role = entry.first;
            string const& colName = entry.second;
            if (colName.empty()) {
                throw invalid_argument(context +
                                       "a valid column name must be provided"
                                       " for the '" +
                                       role + "' parameter of the partitioned table");
            }
            if (!columnInSchema(colName, columns_)) {
                throw invalid_argument(context +
                                       "no matching column found in the provided"
                                       " schema for name '" +
                                       colName + " as required by parameter '" + role +
                                       "' of the partitioned table: '" + table + "'");
            }
        }

        // Each partitioned table, regardless of its type will have an entry in each
        // collection even if the right value may be empty. Those special cases (of
        // the empty values have already been verified above).
        directorTable[table] = directorTable_;
        directorTableKey[table] = directorTableKey_;
        latitudeColName[table] = latitudeColName_;
        longitudeColName[table] = longitudeColName_;

    } else {
        regularTables.push_back(table);
    }
    columns[table] = columns_;
}

void DatabaseInfo::removeTable(std::string const& table) {
    string const context = "DatabaseInfo::" + string(__func__) + " ";
    bool const partitioned = isPartitioned(table);
    bool const director = isDirector(table);
    if (partitioned) {
        if (director) {
            for (auto const& itr : directorTable) {
                if (itr.second == table) {
                    throw invalid_argument(context + "can't removed the director table '" + table +
                                           "' because it has dependent tables, including '" + itr.second +
                                           "'.");
                }
            }
        }
        partitionedTables.erase(find(partitionedTables.begin(), partitionedTables.end(), table));
        directorTable.erase(table);
        directorTableKey.erase(table);
        latitudeColName.erase(table);
        longitudeColName.erase(table);
    } else {
        regularTables.erase(find(regularTables.begin(), regularTables.end(), table));
    }
    columns.erase(table);
}

ostream& operator<<(ostream& os, DatabaseInfo const& info) {
    os << "DatabaseInfo: " << info.toJson().dump();
    return os;
}

}  // namespace lsst::qserv::replica
