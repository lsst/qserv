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
#include "replica/ConfigApp.h"

// System headers
#include <stdexcept>
#include <iomanip>
#include <iostream>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "util/TablePrinter.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ConfigApp");

string const description =
        "The command-line tool for viewing and manipulating the configuration data"
        " of the Replication system stored in the MySQL/MariaDB database.";

/**
 * Register an option with a parser (which could also represent a command).
 * @param parser The handler responsible for processing options
 * @param param Parameter handler.`
 */
template <class PARSER, typename T>
void addCommandOption(PARSER& parser, T& param) {
    parser.option(param.key, param.description(), param.value);
}

}  // namespace

namespace lsst::qserv::replica {

ConfigApp::Ptr ConfigApp::create(int argc, char* argv[]) { return Ptr(new ConfigApp(argc, argv)); }

ConfigApp::ConfigApp(int argc, char* argv[]) : ConfigAppBase(argc, argv, description) {
    parser().commands("command",
                      {"DUMP", "CONFIG_INIT_FILE", "UPDATE_WORKER", "ADD_WORKER", "DELETE_WORKER",
                       "ADD_DATABASE_FAMILY", "DELETE_DATABASE_FAMILY", "ADD_DATABASE", "PUBLISH_DATABASE",
                       "UNPUBLISH_DATABASE", "DELETE_DATABASE", "ADD_TABLE", "DELETE_TABLE"},
                      _command);

    parser().command("DUMP")
            .description(
                    "Dump existing configuration parameters. The command allows specifying"
                    " an optional scope to limit the output. If the parameter 'scope' is"
                    " omitted then complete configuration will be printed. Allowed scopes:"
                    " 'WORKERS', 'FAMILIES', 'DATABASES'.")
            .optional("scope",
                      "This optional parameter narrows a scope of the operation down to a specific"
                      " context. If no scope is specified then everything will be dumped.",
                      _dumpScope, vector<string>({"WORKERS", "FAMILIES", "DATABASES"}));

    parser().command("CONFIG_INIT_FILE")
            .description(
                    "Dump the configuration initialization script for the specified format."
                    " Note that the only format supported by this implementation is 'JSON'.")
            .required("format",
                      "The format of the initialization file to be produced with this option."
                      " The only format supported by the implementation is: 'JSON'.",
                      _format, vector<string>({"JSON"}));

    auto& updateWorkerCommand =
            parser().command("UPDATE_WORKER")
                    .description("Update a configuration of a worker.")
                    .required("worker", "The name of a worker to be updated.", _worker.name);
    _configureWorkerOptions(updateWorkerCommand);

    auto& addWorkerCommand =
            parser().command("ADD_WORKER")
                    .description(
                            "Register a new worker in the configuration. Note that the minimal configuration"
                            " requires just one mandatory parameter - the unique name (identifier) of the "
                            "worker.")
                    .required("worker", "The name of a worker to be added.", _worker.name);
    _configureWorkerOptions(addWorkerCommand);

    parser().command("DELETE_WORKER")
            .required("worker", "The name of a worker to be deleted.", _worker.name)
            .description(
                    "Remove the specified worker from the configuration. ATTENTION: any data associated"
                    " with the deleted worker will be automatically deleted from the database."
                    " This includes: replicas info and transaction contribution records.");

    parser().command("ADD_DATABASE_FAMILY")
            .description(
                    "Register a new database family. Note that all parameters of this command are"
                    " mandatory and positional.")
            .required("name", "The name of a new database family.", _family.name)
            .required("replication-level", "The minimum replication level desired (1..N).",
                      _family.replicationLevel)
            .required("num-stripes", "The number of stripes (from the CSS partitioning configuration).",
                      _family.numStripes)
            .required("num-sub-stripes",
                      "The number of sub-stripes (from the CSS partitioning configuration).",
                      _family.numSubStripes)
            .required("overlap", "The default overlap for tables that do not specify their own overlap.",
                      _family.overlap);

    parser().command("DELETE_DATABASE_FAMILY")
            .description(
                    "Remove the specified database family from the configuration. ATTENTION:"
                    " any data associated with the deleted family will be automatically deleted from"
                    " the configuration. This includes: any metadata associated with databases - members"
                    " of the deleted family, replicas info, transaction contribution records, etc.")
            .required("name",
                      "The name of an existing database family to be deleted. ATTENTION: all databases that"
                      " are members of the family will be deleted as well, along with the relevant info"
                      " about replicas of all chunks of the databases.",
                      _family.name);

    parser().command("ADD_DATABASE")
            .description(
                    "Register a new database in the configuration. No tables or any other metadata, such"
                    " as the names of 'director' tables, etc. will be added by this operations. Those"
                    " can be added later when the corresponding tables will be registered in a scope"
                    " of the database.")
            .required("name", "The name of a new database.", _database.name)
            .required("family", "The name of an existing family the new database will join.",
                      _database.family);

    parser().command("PUBLISH_DATABASE")
            .description(
                    "Change the current status of the database as being 'published'. Note that"
                    " databases in this state will be tracked by the Replication system to ensure"
                    " the sufficient number of replicas (as defined by the corresponding parameter"
                    " in the databases's family definition) will be maintained. Note that databases"
                    " are published only after they're fully ingested. Normally, the change of the"
                    " database status should be requested via the REST API of the Master Replication"
                    " Controller, rather than using this tool.")
            .required("name", "The name of an existing database.", _database.name);

    parser().command("UNPUBLISH_DATABASE")
            .description(
                    "Temporarily change the current status of the database as being 'un-published'"
                    " in order to ingest more tables. Note that databases in this state won't be"
                    " tracked by the Replication system to maintain the sufficient replication level."
                    " Normally, the change of the database status should be requested via the REST API"
                    " of the Master Replication Controller, rather than using this tool.")
            .required("name", "The name of an existing database.", _database.name);

    parser().command("DELETE_DATABASE")
            .description("Remove the database and all relevant metadata from the configuration.")
            .required("name",
                      "The name of an existing database to be deleted. ATTENTION: all relevant info that"
                      " is associated with the database (replicas of all chunks, etc.) will get deleted as "
                      "well.",
                      _database.name);

    parser().command("ADD_TABLE")
            .description("Register a new table a configuration of the given database.")
            .required("database", "The name of an existing database.", _table.database)
            .required("table", "The name of a new table.", _table.name)
            .flag("partitioned", "The flag indicating (if present) that a table is partitioned.",
                  _table.isPartitioned)
            .option("director-table",
                    "The name of the first director table. This parameter is required for"
                    " all dependent tables. For the RefMatch tables the value may also include"
                    " the name of a database. In this case the syntax would be <database>.<table>",
                    _directorDatabaseTable)
            .option("director-key",
                    "The name of a column in the 'director' table of the database."
                    " Note that this option must be provided for the 'director' tables.",
                    _primaryKeyColumn)
            .option("director-table2",
                    "The name of the second director table. This parameter is required for"
                    " the 'RefMatch' tables only. A presence of this table specializes the dependent"
                    " table into the 'RefMatch' one. The value may also include the name of"
                    " a database. In this case the syntax would be <database>.<table>",
                    _directorDatabaseTable2)
            .option("director-key2",
                    "The name of a column in the second 'director' table."
                    " Note that this option must be provided for the 'RefMatch' tables only.",
                    _primaryKeyColumn2)
            .option("flag",
                    "The name of a column in the 'RefMatch' that stores flags."
                    " Note that this option must be provided for the 'RefMatch' tables only."
                    " This parameter is ignored for other tables.",
                    _table.flagColName)
            .option("ang-sep",
                    "The angular separation parameter."
                    " Note that this option must be provided for the 'RefMatch' tables only."
                    " This parameter is ignored for other tables.",
                    _table.angSep)
            .flag("non-unique-primary-key",
                  "The optional flag that is related to the 'director' tables only, and it's ignored"
                  " for any other table types. The flag affects the construction of the 'director'"
                  " indexes of such tables during catalog ingest or when the index is built for"
                  " the published table. Setting a value of this parameter to 'true' would drop"
                  " the 'UNIQUE' constraint in a definition of the corresponiding key of the 'director'"
                  " table schema when the index is constructed at the transaction commit time",
                  _nonUniquePrimaryKey)
            .option("latitude-key",
                    "The name of a column in the 'partitioned' table indicating a column which"
                    " stores latitude (declination) of the object/sources. This parameter is optional.",
                    _table.latitudeColName)
            .option("longitude-key",
                    "The name of a column in the 'partitioned' table indicating a column which"
                    " stores longitude (right ascension) of the object/sources. This parameter is optional.",
                    _table.longitudeColName);

    parser().command("DELETE_TABLE")
            .description("Remove the table and all relevant metadata from the configuration.")
            .required("database", "The name of an existing database.", _table.database)
            .required(
                    "table",
                    "The name of an existing table to be deleted. ATTENTION: all relevant info that"
                    " is associated with the table (replicas of all chunks, etc.) will get deleted as well.",
                    _table.name);
}

int ConfigApp::runSubclassImpl() {
    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (_command == "DUMP") return _dump();
    if (_command == "CONFIG_INIT_FILE") return _configInitFile();
    if (_command == "UPDATE_WORKER") return _updateWorker();
    if (_command == "ADD_WORKER") return _addWorker();
    if (_command == "DELETE_WORKER") return _deleteWorker();
    if (_command == "ADD_DATABASE_FAMILY") return _addFamily();
    if (_command == "DELETE_DATABASE_FAMILY") return _deleteFamily();
    if (_command == "ADD_DATABASE") return _addDatabase();
    if (_command == "PUBLISH_DATABASE") return _publishDatabase(true);
    if (_command == "UNPUBLISH_DATABASE") return _publishDatabase(false);
    if (_command == "DELETE_DATABASE") return _deleteDatabase();
    if (_command == "ADD_TABLE") return _addTable();
    if (_command == "DELETE_TABLE") return _deleteTable();

    LOGS(_log, LOG_LVL_ERROR, context << "unsupported command: '" + _command + "'");
    return 1;
}

void ConfigApp::_configureWorkerOptions(detail::Command& command) {
    command.option("enabled", "Set to '0' if the worker is turned into disabled mode upon creation.",
                   _worker.isEnabled)
            .option("read-only",
                    "Set to '0' if the worker is NOT turned into the read-only mode upon creation.",
                    _worker.isReadOnly);
}

int ConfigApp::_dump() const {
    string const indent = "  ";
    cout << "\n"
         << indent << "CONFIG_URL: " << configUrl() << "\n"
         << "\n";
    if (_dumpScope.empty() or _dumpScope == "WORKERS") dumpWorkersAsTable(indent);
    if (_dumpScope.empty() or _dumpScope == "FAMILIES") dumpFamiliesAsTable(indent);
    if (_dumpScope.empty() or _dumpScope == "DATABASES") dumpDatabasesAsTable(indent);
    return 0;
}

int ConfigApp::_configInitFile() const {
    if ("JSON" != _format) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": unsupported format '" << _format << "'.");
        return 1;
    }
    try {
        cout << config()->toJson().dump() << endl;
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_updateWorker() const {
    // Configuration changes will be updated in the transient object obtained from
    // the database and then be saved to the the persistent configuration.
    try {
        auto info = config()->workerInfo(_worker.name);
        WorkerInfo::update(_workerEnable, info.isEnabled);
        WorkerInfo::update(_workerReadOnly, info.isReadOnly);
        auto const updatedInfo = config()->updateWorker(info);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_addWorker() const {
    try {
        config()->addWorker(_worker);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_deleteWorker() const {
    try {
        config()->deleteWorker(_worker.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_addFamily() {
    try {
        config()->addDatabaseFamily(_family);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_deleteFamily() {
    try {
        config()->deleteDatabaseFamily(_family.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_addDatabase() {
    try {
        config()->addDatabase(_database.name, _database.family);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_publishDatabase(bool publish) {
    try {
        if (publish) {
            config()->publishDatabase(_database.name);
        } else {
            config()->unPublishDatabase(_database.name);
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_deleteDatabase() {
    try {
        config()->deleteDatabase(_database.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_addTable() {
    try {
        _table.directorTable = DirectorTableRef(_directorDatabaseTable, _primaryKeyColumn);
        _table.directorTable2 = DirectorTableRef(_directorDatabaseTable2, _primaryKeyColumn2);
        _table.uniquePrimaryKey = !_nonUniquePrimaryKey;
        config()->addTable(_table);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

int ConfigApp::_deleteTable() {
    try {
        config()->deleteTable(_table.database, _table.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

}  // namespace lsst::qserv::replica
