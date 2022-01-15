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

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ConfigApp::Ptr ConfigApp::create(int argc, char* argv[]) {
    return Ptr(new ConfigApp(argc, argv));
}


ConfigApp::ConfigApp(int argc, char* argv[])
    :   ConfigAppBase(argc, argv, description) {

    parser().commands(
        "command",
        {"DUMP",
         "CONFIG_INIT_FILE",
         "UPDATE_GENERAL",
         "UPDATE_WORKER", "ADD_WORKER", "DELETE_WORKER",
         "ADD_DATABASE_FAMILY", "DELETE_DATABASE_FAMILY",
         "ADD_DATABASE", "PUBLISH_DATABASE", "DELETE_DATABASE",
         "ADD_TABLE", "DELETE_TABLE"
        },
        _command
    );

    parser().command(
        "DUMP"
    ).description(
        "Dump existing configuration parameters. The command allows specifying"
        " an optional scope to limit the output. If the parameter 'scope' is"
        " omitted then complete configuration will be printed. Allowed scopes:"
        " 'GENERAL', 'WORKERS', 'FAMILIES', 'DATABASES'."
    ).optional(
        "scope",
        "This optional parameter narrows a scope of the operation down to a specific"
        " context. If no scope is specified then everything will be dumped.",
        _dumpScope,
        vector<string>({"GENERAL", "WORKERS", "FAMILIES", "DATABASES"})
    );

    parser().command(
        "CONFIG_INIT_FILE"
    ).description(
        "Dump the configuration initialization script for the specified format."
        " Note that the only format supported by this implementation is 'JSON'."
    ).required(
        "format",
        "The format of the initialization file to be produced with this option."
        " The only format supported by the implementation is: 'JSON'.",
        _format,
        vector<string>({"JSON"})
    );

    auto& updateWorkerCommand = parser().command(
        "UPDATE_WORKER"
    ).description(
        "Update a configuration of a worker."
    ).required(
        "worker",
        "The name of a worker to be updated.",
        _workerInfo.name
    ).option(
        "service-host",
        "The new DNS name or an IP address where the worker runs.",
        _workerInfo.svcHost
    );
    _configureWorkerOptions(updateWorkerCommand);

    auto& addWorkerCommand = parser().command(
        "ADD_WORKER"
    ).description(
        "Register a new worker in the configuration. Note that the minimal configuration"
        " requires two mandatory parameters: the unique name (identifier) of the worker and"
        " the DNS name (or an IP address) of a host where the worker service would run."
        " Other parameters are optional. The following defines rules for the optional parameters."
        " If no location will be given for some other service a location of the 'service-host' will"
        " be assumed. If no specific port will be provided for a service then the corresponding"
        " default port defined in the 'worker-defaults' of the general configuration category"
        " will be assumed. The later rule also applies to the temporary folders of all services."
    ).required(
        "worker",
        "The name of a worker to be added.",
        _workerInfo.name
    ).required(
        "service-host",
        "The DNS name or an IP address where the worker runs.",
        _workerInfo.svcHost
    );
    _configureWorkerOptions(addWorkerCommand);

    parser().command("DELETE_WORKER").required(
        "worker",
        "The name of a worker to be deleted.",
        _workerInfo.name
    ).description(
        "Remove the specified worker from the configuration. ATTENTION: any data associated"
        " with the deleted worker will be automatically deleted from the database."
        " This includes: replicas info and transaction contribution records."
    );

    // Add options for the general parameters named as:
    //   --<category>.<param>=<string>
    // Note that since no database connection is available at this time (that would have
    // required knowing a value of the parameter 'configUrl', and no parsing has been made
    // yet) then the loop below will set the default value of each option to be the empty
    // string. Any changes from that will be detected when processing the input. 
    auto&& updateGeneralCmd = parser().command(
        "UPDATE_GENERAL"
    ).description(
        "Update the general configuration parameters."
    );
    for (auto&& itr: ConfigurationSchema::parameters()) {
        string const& category = itr.first;
        for (auto&& param: itr.second) {
            // The read-only parameters can't be updated programmatically.
            if (ConfigurationSchema::readOnly(category, param)) continue;
            _generalParams[category][param] = ConfigurationSchema::defaultValueAsString(category, param);
            updateGeneralCmd.option(
                category + "-" + param,
                ConfigurationSchema::description(category, param),
                _generalParams[category][param]);
        }
    }

    parser().command(
        "ADD_DATABASE_FAMILY"
    ).description(
        "Register a new database family. Note that all parameters of this command are"
        " mandatory and positional."
    ).required(
        "name",
        "The name of a new database family.",
        _familyInfo.name
    ).required(
        "replication-level",
        "The minimum replication level desired (1..N).",
        _familyInfo.replicationLevel
    ).required(
        "num-stripes",
        "The number of stripes (from the CSS partitioning configuration).",
        _familyInfo.numStripes
    ).required(
        "num-sub-stripes",
        "The number of sub-stripes (from the CSS partitioning configuration).",
        _familyInfo.numSubStripes
    ).required(
        "overlap",
        "The default overlap for tables that do not specify their own overlap.",
        _familyInfo.overlap
    );

    parser().command(
        "DELETE_DATABASE_FAMILY"
    ).description(
        "Remove the specified database family from the configuration. ATTENTION:"
        " any data associated with the deleted family will be automatically deleted from"
        " the configuration. This includes: any metadata associated with databases - members"
        " of the deleted family, replicas info, transaction contribution records, etc."
    ).required(
        "name",
        "The name of an existing database family to be deleted. ATTENTION: all databases that"
        " are members of the family will be deleted as well, along with the relevant info"
        " about replicas of all chunks of the databases.",
        _familyInfo.name
    );
    
    parser().command(
        "ADD_DATABASE"
    ).description(
        "Register a new database in the configuration. No tables or any other metadata, such"
        " as the names of 'director' tables, etc. will be added by this operations. Those"
        " can be added later when the corresponding tables will be registered in a scope"
        " of the database."
    ).required(
        "name",
        "The name of a new database.",
        _databaseInfo.name
    ).required(
        "family",
        "The name of an existing family the new database will join.",
        _databaseInfo.family
    );

    parser().command(
        "PUBLISH_DATABASE"
    ).description(
        "Change the current status of the database as being 'published'. Note that"
        " databases in this state will be tracked by the Replication system to ensure"
        " the sufficient number of replicas (as defined by the corresponding parameter"
        " in the databases's family definition) will be maintained. Note that databases"
        " are published only after they're fully ingested. Normally, the change of the"
        " database status is made by the Ingest system, not by this tool."
    ).required(
        "name",
        "The name of an existing database.",
        _databaseInfo.name
    );

    parser().command(
        "DELETE_DATABASE"
    ).description(
        "Remove the database and all relevant metadata from the configuration."
    ).required(
        "name",
        "The name of an existing database to be deleted. ATTENTION: all relevant info that"
        " is associated with the database (replicas of all chunks, etc.) will get deleted as well.",
        _databaseInfo.name
    );

    parser().command(
        "ADD_TABLE"
    ).description(
        "Register a new table a configuration of the given database."
    ).required(
        "database",
        "The name of an existing database.",
        _database
    ).required(
        "table",
        "The name of a new table.",
        _table
    ).flag(
        "partitioned",
        "The flag indicating (if present) that a table is partitioned.",
        _isPartitioned
    ).flag(
        "director",
        "The flag indicating (if present) that this is a 'director' table of the database"
        " Note that this flag only applies to the partitioned tables.",
        _isDirector
    ).option(
        "director-key",
        "The name of a column in the 'director' table of the database."
        " Note that this option must be provided for the 'director' tables.",
        _directorKey
    ).option(
        "latitude-key",
        "The name of a column in the 'partitioned' table indicating a column which"
        " stores latitude (declination) of the object/sources. This parameter is optional.",
        _latitudeColName
    ).option(
        "longitude-key",
        "The name of a column in the 'partitioned' table indicating a column which"
        " stores longitude (right ascension) of the object/sources. This parameter is optional.",
        _longitudeColName
    );

    parser().command(
        "DELETE_TABLE"
    ).description(
        "Remove the table and all relevant metadata from the configuration."
    ).required(
        "database",
        "The name of an existing database.",
        _database
    ).required(
        "table",
        "The name of an existing table to be deleted. ATTENTION: all relevant info that"
        " is associated with the table (replicas of all chunks, etc.) will get deleted as well.",
        _table
    );
}


int ConfigApp::runSubclassImpl() {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (_command == "DUMP") return _dump();
    if (_command == "CONFIG_INIT_FILE") return _configInitFile();
    if (_command == "UPDATE_GENERAL") return _updateGeneral();
    if (_command == "UPDATE_WORKER") return _updateWorker();
    if (_command == "ADD_WORKER") return _addWorker();
    if (_command == "DELETE_WORKER") return _deleteWorker();
    if (_command == "ADD_DATABASE_FAMILY") return _addFamily();
    if (_command == "DELETE_DATABASE_FAMILY") return _deleteFamily();
    if (_command == "ADD_DATABASE") return _addDatabase();
    if (_command == "PUBLISH_DATABASE") return _publishDatabase();
    if (_command == "DELETE_DATABASE") return _deleteDatabase();
    if (_command == "ADD_TABLE") return _addTable();
    if (_command == "DELETE_TABLE") return _deleteTable();

    LOGS(_log, LOG_LVL_ERROR, context << "unsupported command: '" + _command + "'");
    return 1;
}


void ConfigApp::_configureWorkerOptions(detail::Command& command) {
    command.option(
        "service-port",
        "The port number of the worker service.",
        _workerInfo.svcPort
    ).option(
        "fs-host",
        "The DNS name or an IP address where the worker's File Server runs.",
        _workerInfo.fsHost
    ).option(
        "fs-port",
        "The port number of the worker's File Server.",
        _workerInfo.fsPort
    ).option(
        "data-dir",
        "The data directory of the worker.",
        _workerInfo.dataDir
    ).option(
        "enabled",
        "Set to '0' if the worker is turned into disabled mode upon creation.",
        _workerInfo.isEnabled
    ).option(
        "read-only",
        "Set to '0' if the worker is NOT turned into the read-only mode upon creation.",
        _workerInfo.isReadOnly
    ).option(
        "loader-host",
        "The DNS name or an IP address where the worker's Catalog Ingest Server runs.",
        _workerInfo.loaderHost
    ).option(
        "loader-port",
        "The port number of the worker's Catalog Ingest Server.",
        _workerInfo.loaderPort
    ).option(
        "loader-tmp-dir",
        "The temporay directory of the worker's Ingest Service.",
        _workerInfo.loaderTmpDir
    ).option(
        "exporter-host",
        "The DNS name or an IP address where the worker's Data Exporting Server runs.",
        _workerInfo.exporterHost
    ).option(
        "exporter-port",
        "The port number of the worker's Data Exporting Server.",
        _workerInfo.exporterPort
    ).option(
        "exporter-tmp-dir",
        "The temporay directory of the worker's Data Exporting Service.",
        _workerInfo.exporterTmpDir
    ).option(
        "http-loader-host",
        "The DNS name or an IP address where the worker's HTTP-based Catalog Ingest Server runs.",
        _workerInfo.httpLoaderHost
    ).option(
        "http-loader-port",
        "The port number of the worker's HTTP-based Catalog Ingest Server.",
        _workerInfo.httpLoaderPort
    ).option(
        "http-loader-tmp-dir",
        "The temporay directory of the worker's HTTP-based Catalog Ingest Service",
        _workerInfo.httpLoaderTmpDir
    );
} 


int ConfigApp::_dump() const {
    string const indent = "  ";
    cout << "\n" << indent << "CONFIG_URL: " << configUrl() << "\n" << "\n";
    if (_dumpScope.empty() or _dumpScope == "GENERAL") dumpGeneralAsTable(indent);
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


int ConfigApp::_updateGeneral() {
    try {
        // Note that options specified by a user will have non-empty values.
        for (auto&& categoryItr: _generalParams) {
            string const& category = categoryItr.first;
            for (auto&& paramItr: categoryItr.second) {
                string const& param = paramItr.first;
                string const& value = paramItr.second;
                if (!value.empty()) {
                    config()->setFromString(category, param, value);
                }
            }
        }
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
        auto info = config()->workerInfo(_workerInfo.name);
        WorkerInfo::update(_workerEnable, info.isEnabled);
        WorkerInfo::update(_workerReadOnly, info.isReadOnly);
        WorkerInfo::update(_workerInfo.svcHost, info.svcHost);
        WorkerInfo::update(_workerInfo.svcPort, info.svcPort);
        WorkerInfo::update(_workerInfo.fsHost, info.fsHost);
        WorkerInfo::update(_workerInfo.fsPort, info.fsPort);
        WorkerInfo::update(_workerInfo.dataDir, info.dataDir);
        WorkerInfo::update(_workerInfo.loaderHost, info.loaderHost);
        WorkerInfo::update(_workerInfo.loaderPort, info.loaderPort);
        WorkerInfo::update(_workerInfo.loaderTmpDir, info.loaderTmpDir);
        WorkerInfo::update(_workerInfo.exporterHost, info.exporterHost);
        WorkerInfo::update(_workerInfo.exporterPort, info.exporterPort);
        WorkerInfo::update(_workerInfo.exporterTmpDir, info.exporterTmpDir);
        WorkerInfo::update(_workerInfo.httpLoaderHost, info.httpLoaderHost);
        WorkerInfo::update(_workerInfo.httpLoaderPort, info.httpLoaderPort);
        WorkerInfo::update(_workerInfo.httpLoaderTmpDir, info.httpLoaderTmpDir);
        auto const updatedInfo = config()->updateWorker(info);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_addWorker() const {
    try {
        config()->addWorker(_workerInfo);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_deleteWorker() const {
    try {
        config()->deleteWorker(_workerInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_addFamily() {    
    try {
        config()->addDatabaseFamily(_familyInfo);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_deleteFamily() {
    try {
        config()->deleteDatabaseFamily(_familyInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_addDatabase() {    
    try {
        config()->addDatabase(_databaseInfo.name, _databaseInfo.family);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_publishDatabase() {
    try {
        config()->publishDatabase(_databaseInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_deleteDatabase() {
    try {
        config()->deleteDatabase(_databaseInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_addTable() {
    try {
        list<SqlColDef> noColumns;
        config()->addTable(_database, _table, _isPartitioned, noColumns,
                          _isDirector, _directorKey,
                          _latitudeColName, _longitudeColName);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}


int ConfigApp::_deleteTable() {
    try {
        config()->deleteTable(_database, _table);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "ConfigApp::" << __func__ << ": " << ex.what());
        throw;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
