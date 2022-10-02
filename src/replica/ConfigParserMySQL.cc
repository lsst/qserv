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
#include "replica/ConfigParserMySQL.h"

// System headers
#include <list>
#include <stdexcept>

// Qserv headers
#include "replica/ConfigurationExceptions.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

using namespace database::mysql;

int const ConfigParserMySQL::expectedSchemaVersion = 12;

ConfigParserMySQL::ConfigParserMySQL(Connection::Ptr const& conn, json& data,
                                     map<string, WorkerInfo>& workers,
                                     map<string, DatabaseFamilyInfo>& databaseFamilies,
                                     map<string, DatabaseInfo>& databases)
        : _conn(conn),
          _g(conn),
          _data(data),
          _workers(workers),
          _databaseFamilies(databaseFamilies),
          _databases(databases) {}

void ConfigParserMySQL::parse() {
    _parseVersion();
    _parseWorkers();
    _parseDatabaseFamilies();
    _parseDatabases();
}

void ConfigParserMySQL::_parseVersion() {
    string const table = "QMetadata";
    auto const databaseTableSql = _g.id(_conn->connectionParams().database, table);
    if (!_conn->tableExists(table)) {
        throw ConfigVersionMismatch(_context + " the metadata table " + databaseTableSql.str +
                                    " doesn't exist.");
    }
    string const column = "value";
    string const query =
            _g.select(column) + _g.from(databaseTableSql) + _g.where(_g.eq("metakey", "version"));
    int version;
    bool const isNotNull = _conn->executeSingleValueSelect(query, column, version);
    if (!isNotNull) {
        throw ConfigVersionMismatch(_context + " the metadata table " + databaseTableSql.str +
                                    " doesn't have the schema version.");
    }
    if (version != expectedSchemaVersion) {
        string const msg = _context + " schema version " + to_string(version) +
                           " found in the metadata table " + databaseTableSql.str +
                           " doesn't match the required version " + to_string(expectedSchemaVersion) + ".";
        throw ConfigVersionMismatch(msg, version, expectedSchemaVersion);
    }
}

void ConfigParserMySQL::_parseWorkers() {
    string const query = _g.select(Sql::STAR) + _g.from("config_worker");
    _conn->execute(query);
    while (_conn->next(_row)) {
        WorkerInfo worker;
        worker.name = _parseParam<string>("name");
        worker.isEnabled = _parseParam<int>("is_enabled") != 0;
        worker.isReadOnly = _parseParam<int>("is_read_only") != 0;
        _workers[worker.name] = worker;
    }
}

void ConfigParserMySQL::_parseDatabaseFamilies() {
    _conn->execute(_g.select(Sql::STAR) + _g.from("config_database_family"));
    while (_conn->next(_row)) {
        DatabaseFamilyInfo family;
        family.name = _parseParam<string>("name");
        family.replicationLevel = _parseParam<unsigned int>("min_replication_level");
        family.numStripes = _parseParam<unsigned int>("num_stripes");
        family.numSubStripes = _parseParam<unsigned int>("num_sub_stripes");
        family.overlap = _parseParam<double>("overlap");
        _databaseFamilies[family.name] = family;
    }
}

void ConfigParserMySQL::_parseDatabases() {
    _conn->execute(_g.select(Sql::STAR) + _g.from("config_database"));
    while (_conn->next(_row)) {
        DatabaseInfo database;
        database.name = _parseParam<string>("database");
        database.family = _parseParam<string>("family_name");
        database.isPublished = _parseParam<int>("is_published") != 0;
        database.createTime = _parseParam<uint64_t>("create_time");
        database.publishTime = _parseParam<uint64_t>("publish_time");
        _databases[database.name] = database;
    }

    // Read database-specific table definitions and extend the corresponding database entries.
    // Table definitions are going to be stored in the temporary collection to allow extending
    // each definition later with the table schema before pushing the tables into the configuration.
    list<TableInfo> tables;
    _conn->execute(_g.select(Sql::STAR) + _g.from("config_database_table"));
    while (_conn->next(_row)) {
        TableInfo table;
        table.name = _parseParam<string>("table");
        table.database = _parseParam<string>("database");
        table.isPublished = _parseParam<int>("is_published") != 0;
        table.createTime = _parseParam<uint64_t>("create_time");
        table.publishTime = _parseParam<uint64_t>("publish_time");
        table.directorTable =
                DirectorTableRef(_parseParam<string>("director_table"), _parseParam<string>("director_key"));
        table.directorTable2 = DirectorTableRef(_parseParam<string>("director_table2"),
                                                _parseParam<string>("director_key2"));
        table.flagColName = _parseParam<string>("flag");
        table.angSep = _parseParam<double>("ang_sep");
        table.latitudeColName = _parseParam<string>("latitude_key");
        table.longitudeColName = _parseParam<string>("longitude_key");
        table.isPartitioned = _parseParam<int>("is_partitioned") != 0;
        table.isDirector = table.isPartitioned && table.directorTable.tableName().empty() &&
                           !table.directorTable.primaryKeyColumn().empty() && table.directorTable2.empty();
        table.isRefMatch =
                table.isPartitioned && !table.directorTable.empty() && !table.directorTable2.empty();
        tables.emplace_back(table);
    }

    // Read schema for each table.
    for (auto&& table : tables) {
        string const query = _g.select("col_name", "col_type") + _g.from("config_database_table_schema") +
                             _g.where(_g.eq("database", table.database), _g.eq("table", table.name)) +
                             _g.orderBy(make_pair("col_position", "ASC"));
        _conn->execute(query);
        while (_conn->next(_row)) {
            table.columns.push_back(
                    SqlColDef(_parseParam<string>("col_name"), _parseParam<string>("col_type")));
        }
    }

    // Register tables in the configuration in two phases, starting with the "director"
    // tables, and ending with the rest. Note that "directors" have to be known to
    // the configuration before attempting to register the corresponding dependent tables.
    // This algorithm will enforce the referential integrity between the partitioned tables.
    // Pushing partitioned tables in the wrong order will fail the registration.
    for (auto&& table : tables) {
        if (table.isDirector) _databases[table.database].addTable(_databases, table);
    }
    for (auto&& table : tables) {
        if (!table.isDirector) _databases[table.database].addTable(_databases, table);
    }
}

}  // namespace lsst::qserv::replica
