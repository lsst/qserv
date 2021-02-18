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

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This application is the tool for viewing and manipulating"
    " the configuration data of the Replication system stored in the MySQL/MariaDB.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;
bool const injectXrootdOptions = false;

/**
 * Register an option with a parser (which could also represent a command)
 *
 * @param parser
 *   the handler responsible for processing options
 *
 * @param struct_
 *   the option descriptor
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
    return Ptr(
        new ConfigApp(argc, argv)
    );
}


ConfigApp::ConfigApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            description,
            injectDatabaseOptions,
            boostProtobufVersionCheck,
            enableServiceProvider,
            injectXrootdOptions
        ),
        _log(LOG_GET("lsst.qserv.replica.ConfigApp")),
        _configUrl("mysql://qsreplica@localhost:3306/qservReplica") {

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
    ).option(
        "config",
        "Configuration URL (a configuration file or a database connection string).",
        _configUrl
    ).flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in dumps.",
        _verticalSeparator
    );

    parser().command(
        "DUMP"
    ).optional(
        "scope",
        "This optional parameter narrows a scope of the operation down to a specific"
        " context. If no scope is specified then everything will be dumped.",
        _dumpScope,
        vector<string>({"GENERAL", "WORKERS", "FAMILIES", "DATABASES"})
    );

    parser().command(
        "CONFIG_INIT_FILE"
    ).required(
        "format",
        "The format of the initialization file to be produced with this option."
        " Allowed values: JSON.",
        _format,
        vector<string>({"JSON"})
    );

    parser().command(
        "UPDATE_WORKER"
    ).required(
        "worker",
        "The name of a worker to be updated.",
        _workerInfo.name
    ).option(
        "worker-service-host",
        "The new DNS name or an IP address where the worker runs.",
        _workerInfo.svcHost
    ).option(
        "worker-service-port",
        "The port number of the worker service.",
        _workerInfo.svcPort
    ).option(
        "worker-fs-host",
        "The new DNS name or an IP address where the worker's File Server runs.",
        _workerInfo.fsHost
    ).option(
        "worker-fs-port",
        "The port number of the worker's File Server.",
        _workerInfo.fsPort
    ).option(
        "worker-data-dir",
        "The data directory of the worker.",
        _workerInfo.dataDir
    ).option(
        "worker-db-host",
        "The new DNS name or an IP address where the worker's database service runs.",
        _workerInfo.dbHost
    ).option(
        "worker-db-port",
        "The port number of the worker's database service.",
        _workerInfo.dbPort
    ).option(
        "worker-db-user",
        "The name of a user account for the worker's database service.",
        _workerInfo.dbUser
    ).option(
        "worker-enable",
        "Enable the worker if 1 (or any positive number), disable if 0."
        " Negative numbers are ignored.",
        _workerEnable
    ).option(
        "worker-read-only",
        "Turn the worker into the read-write mode if 1 (or any positive number),"
        ", turn it int the read-write mode if 0.",
        _workerReadOnly
    ).option(
        "worker-loader-host",
        "The new DNS name or an IP address where the worker's Catalog Ingest service runs.",
        _workerInfo.loaderHost
    ).option(
        "worker-loader-port",
        "The port number of the worker's Catalog Ingest service.",
        _workerInfo.loaderPort
    ).option(
        "worker-loader-tmp-dir",
        "The name of a user account for a temporary folder of the worker's Catalog Ingest service.",
        _workerInfo.loaderTmpDir
    ).option(
        "worker-exporter-host",
        "The new DNS name or an IP address where the worker's Data Exporting service runs.",
        _workerInfo.exporterHost
    ).option(
        "worker-exporter-port",
        "The port number of the worker's Data Exporting service.",
        _workerInfo.exporterPort
    ).option(
        "worker-exporter-tmp-dir",
        "The name of a user account for a temporary folder of the worker's Data Exporting service.",
        _workerInfo.exporterTmpDir
    ).option(
        "worker-http-loader-host",
        "The new DNS name or an IP address where the worker's Catalog REST-based Ingest service runs.",
        _workerInfo.httpLoaderHost
    ).option(
        "worker-http-loader-port",
        "The port number of the worker's Catalog REST-based Ingest service.",
        _workerInfo.httpLoaderPort
    ).option(
        "worker-http-loader-tmp-dir",
        "The name of a user account for a temporary folder of the worker's Catalog REST-based Ingest service.",
        _workerInfo.httpLoaderTmpDir
    );

    parser().command(
        "ADD_WORKER"
    ).required(
        "worker",
        "The name of a worker to be added.",
        _workerInfo.name
    ).required(
        "service-host",
        "The DNS name or an IP address where the worker runs.",
        _workerInfo.svcHost
    ).optional(
        "service-port",
        "The port number of the worker service",
        _workerInfo.svcPort
    ).required(
        "fs-host",
        "The DNS name or an IP address where the worker's File Server runs.",
        _workerInfo.fsHost
    ).optional(
        "fs-port",
        "The port number of the worker's File Server.",
        _workerInfo.fsPort
    ).optional(
        "data-dir",
        "The data directory of the worker",
        _workerInfo.dataDir
    ).optional(
        "enabled",
        "Set to '0' if the worker is turned into disabled mode upon creation.",
        _workerInfo.isEnabled
    ).optional(
        "read-only",
        "Set to '0' if the worker is NOT turned into the read-only mode upon creation.",
        _workerInfo.isReadOnly
    ).required(
        "db-host",
        "The DNS name or an IP address where the worker's Database Service runs.",
        _workerInfo.dbHost
    ).optional(
        "db-port",
        "The port number of the worker's Database Service.",
        _workerInfo.dbPort
    ).optional(
        "db-user",
        "The name of the MySQL user for the worker's Database Service",
        _workerInfo.dbUser
    ).required(
        "loader-host",
        "The DNS name or an IP address where the worker's Catalog Ingest Server runs.",
        _workerInfo.loaderHost
    ).optional(
        "loader-port",
        "The port number of the worker's Catalog Ingest Server.",
        _workerInfo.loaderPort
    ).optional(
        "loader-tmp-dir",
        "The temporay directory of the worker's Ingest Service",
        _workerInfo.loaderTmpDir
    ).required(
        "exporter-host",
        "The DNS name or an IP address where the worker's Data Exporting Server runs.",
        _workerInfo.exporterHost
    ).optional(
        "exporter-port",
        "The port number of the worker's Data Exporting Server.",
        _workerInfo.exporterPort
    ).optional(
        "exporter-tmp-dir",
        "The temporay directory of the worker's Data Exporting Service",
        _workerInfo.exporterTmpDir
    ).required(
        "http-loader-host",
        "The DNS name or an IP address where the worker's HTTP-based Catalog Ingest Server runs.",
        _workerInfo.httpLoaderHost
    ).optional(
        "http-loader-port",
        "The port number of the worker's HTTP-based Catalog Ingest Server.",
        _workerInfo.httpLoaderPort
    ).optional(
        "http-loader-tmp-dir",
        "The temporay directory of the worker's HTTP-based Catalog Ingest Service",
        _workerInfo.httpLoaderTmpDir
    );


    parser().command("DELETE_WORKER").required(
        "worker",
        "The name of a worker to be deleted.",
        _workerInfo.name
    );

    auto&& updateGeneralCmd = parser().command("UPDATE_GENERAL");
    ::addCommandOption(updateGeneralCmd, _general.requestBufferSizeBytes);
    ::addCommandOption(updateGeneralCmd, _general.retryTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.controllerThreads);
    ::addCommandOption(updateGeneralCmd, _general.controllerRequestTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.jobTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.jobHeartbeatTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.controllerHttpPort);
    ::addCommandOption(updateGeneralCmd, _general.controllerHttpThreads);
    ::addCommandOption(updateGeneralCmd, _general.controllerEmptyChunksDir);
    ::addCommandOption(updateGeneralCmd, _general.xrootdAutoNotify);
    ::addCommandOption(updateGeneralCmd, _general.xrootdHost);
    ::addCommandOption(updateGeneralCmd, _general.xrootdPort);
    ::addCommandOption(updateGeneralCmd, _general.xrootdTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.databaseServicesPoolSize);
    ::addCommandOption(updateGeneralCmd, _general.qservMasterDatabaseHost);
    ::addCommandOption(updateGeneralCmd, _general.qservMasterDatabasePort);
    ::addCommandOption(updateGeneralCmd, _general.qservMasterDatabaseUser);
    ::addCommandOption(updateGeneralCmd, _general.qservMasterDatabaseName);
    ::addCommandOption(updateGeneralCmd, _general.qservMasterDatabaseServicesPoolSize);
    ::addCommandOption(updateGeneralCmd, _general.qservMasterDatabaseTmpDir);
    ::addCommandOption(updateGeneralCmd, _general.workerTechnology);
    ::addCommandOption(updateGeneralCmd, _general.workerNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _general.fsNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _general.workerFsBufferSizeBytes);
    ::addCommandOption(updateGeneralCmd, _general.loaderNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _general.exporterNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _general.httpLoaderNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultSvcPort);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultFsPort);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultDataDir);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultDbPort);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultDbUser);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultLoaderPort);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultLoaderTmpDir);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultExporterPort);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultExporterTmpDir);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultHttpLoaderPort);
    ::addCommandOption(updateGeneralCmd, _general.workerDefaultHttpLoaderTmpDir);

    parser().command(
        "ADD_DATABASE_FAMILY"
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
    ).required(
        "name",
        "The name of an existing database family to be deleted. ATTENTION: all databases that"
        " are members of the family will be deleted as well, along with the relevant info"
        " about replicas of all chunks of the databases.",
        _familyInfo.name
    );
    
    parser().command(
        "ADD_DATABASE"
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
    ).required(
        "name",
        "The name of an existing database.",
        _databaseInfo.name
    );

    parser().command(
        "DELETE_DATABASE"
    ).required(
        "name",
        "The name of an existing database to be deleted. ATTENTION: all relevant info that"
        " is associated with the database (replicas of all chunks, etc.) will get deleted as well.",
        _databaseInfo.name
    );

    parser().command(
        "ADD_TABLE"
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
        "chunk-id-key",
        "The name of a column in the 'partitioned' table indicating a column which"
        " stores identifiers of chunks. Note that this option must be provided"
        " for the 'partitioned' tables.",
        _chunkIdColName
    ).option(
        "sub-chunk-id-key",
        "The name of a column in the 'partitioned' table indicating a column which"
        " stores identifiers of sub-chunks. Note that this option must be provided"
        " for the 'partitioned' tables.",
        _subChunkIdColName
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


int ConfigApp::runImpl() {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    _config = Configuration::load(_configUrl);

    if (_command == "DUMP")                   return _dump();
    if (_command == "CONFIG_INIT_FILE")       return _configInitFile();
    if (_command == "UPDATE_GENERAL")         return _updateGeneral();
    if (_command == "UPDATE_WORKER")          return _updateWorker();
    if (_command == "ADD_WORKER")             return _addWorker();
    if (_command == "DELETE_WORKER")          return _deleteWorker();
    if (_command == "ADD_DATABASE_FAMILY")    return _addFamily();
    if (_command == "DELETE_DATABASE_FAMILY") return _deleteFamily();
    if (_command == "ADD_DATABASE")           return _addDatabase();
    if (_command == "PUBLISH_DATABASE")       return _publishDatabase();
    if (_command == "DELETE_DATABASE")        return _deleteDatabase();
    if (_command == "ADD_TABLE")              return _addTable();
    if (_command == "DELETE_TABLE"   )        return _deleteTable();

    LOGS(_log, LOG_LVL_ERROR, context << "unsupported command: '" + _command + "'");
    return 1;
}


int ConfigApp::_dump() const {

    string const indent = "  ";

    cout << "\n"
         << indent << "CONFIG_URL: " << _configUrl << "\n";

    if (_dumpScope.empty() or _dumpScope == "GENERAL") {
        cout << "\n";
        _dumpGeneralAsTable(indent);
    }
    if (_dumpScope.empty() or _dumpScope == "WORKERS") {
        cout << "\n";
        _dumpWorkersAsTable(indent);
    }
    if (_dumpScope.empty() or _dumpScope == "FAMILIES") {
        cout << "\n";
        _dumpFamiliesAsTable(indent);
    }
    if (_dumpScope.empty() or _dumpScope == "DATABASES") {
        cout << "\n";
        _dumpDatabasesAsTable(indent);
    }
    cout << endl;

    return 0;
}

void ConfigApp::_dumpGeneralAsTable(string const& indent) const {

    // Extract general attributes and put them into the corresponding
    // columns. Translate tables cell values into strings when required.

    vector<string> parameter;
    vector<string> value;
    vector<string> description;

    parameter.  push_back(_general.metaVersion.key);
    value.      push_back(_general.metaVersion.str(*_config));
    description.push_back(_general.metaVersion.description());

    parameter.  push_back(_general.requestBufferSizeBytes.key);
    value.      push_back(_general.requestBufferSizeBytes.str(*_config));
    description.push_back(_general.requestBufferSizeBytes.description());

    parameter.  push_back(_general.retryTimeoutSec.key);
    value.      push_back(_general.retryTimeoutSec.str(*_config));
    description.push_back(_general.retryTimeoutSec.description());

    parameter.  push_back(_general.controllerThreads.key);
    value.      push_back(_general.controllerThreads.str(*_config));
    description.push_back(_general.controllerThreads.description());

    parameter.  push_back(_general.controllerRequestTimeoutSec.key);
    value.      push_back(_general.controllerRequestTimeoutSec.str(*_config));
    description.push_back(_general.controllerRequestTimeoutSec.description());

    parameter.  push_back(_general.jobTimeoutSec.key);
    value.      push_back(_general.jobTimeoutSec.str(*_config));
    description.push_back(_general.jobTimeoutSec.description());

    parameter.  push_back(_general.jobHeartbeatTimeoutSec.key);
    value.      push_back(_general.jobHeartbeatTimeoutSec.str(*_config));
    description.push_back(_general.jobHeartbeatTimeoutSec.description());

    parameter.  push_back(_general.controllerHttpPort.key);
    value.      push_back(_general.controllerHttpPort.str(*_config));
    description.push_back(_general.controllerHttpPort.description());

    parameter.  push_back(_general.controllerHttpThreads.key);
    value.      push_back(_general.controllerHttpThreads.str(*_config));
    description.push_back(_general.controllerHttpThreads.description());

    parameter.  push_back(_general.controllerEmptyChunksDir.key);
    value.      push_back(_general.controllerEmptyChunksDir.str(*_config));
    description.push_back(_general.controllerEmptyChunksDir.description());

    parameter.  push_back(_general.xrootdAutoNotify.key);
    value.      push_back(_general.xrootdAutoNotify.str(*_config));
    description.push_back(_general.xrootdAutoNotify.description());

    parameter.  push_back(_general.xrootdHost.key);
    value.      push_back(_general.xrootdHost.str(*_config));
    description.push_back(_general.xrootdHost.description());

    parameter.  push_back(_general.xrootdPort.key);
    value.      push_back(_general.xrootdPort.str(*_config));
    description.push_back(_general.xrootdPort.description());

    parameter.  push_back(_general.xrootdTimeoutSec.key);
    value.      push_back(_general.xrootdTimeoutSec.str(*_config));
    description.push_back(_general.xrootdTimeoutSec.description());

    parameter.  push_back(_general.databaseServicesPoolSize.key);
    value.      push_back(_general.databaseServicesPoolSize.str(*_config));
    description.push_back(_general.databaseServicesPoolSize.description());

    parameter.  push_back(_general.databaseHost.key);
    value.      push_back(_general.databaseHost.str(*_config));
    description.push_back(_general.databaseHost.description());

    parameter.  push_back(_general.databasePort.key);
    value.      push_back(_general.databasePort.str(*_config));
    description.push_back(_general.databasePort.description());

    parameter.  push_back(_general.databaseUser.key);
    value.      push_back(_general.databaseUser.str(*_config));
    description.push_back(_general.databaseUser.description());

    parameter.  push_back(_general.databaseName.key);
    value.      push_back(_general.databaseName.str(*_config));
    description.push_back(_general.databaseName.description());

    parameter.  push_back(_general.qservMasterDatabaseServicesPoolSize.key);
    value.      push_back(_general.qservMasterDatabaseServicesPoolSize.str(*_config));
    description.push_back(_general.qservMasterDatabaseServicesPoolSize.description());

    parameter.  push_back(_general.qservMasterDatabaseHost.key);
    value.      push_back(_general.qservMasterDatabaseHost.str(*_config));
    description.push_back(_general.qservMasterDatabaseHost.description());

    parameter.  push_back(_general.qservMasterDatabasePort.key);
    value.      push_back(_general.qservMasterDatabasePort.str(*_config));
    description.push_back(_general.qservMasterDatabasePort.description());

    parameter.  push_back(_general.qservMasterDatabaseUser.key);
    value.      push_back(_general.qservMasterDatabaseUser.str(*_config));
    description.push_back(_general.qservMasterDatabaseUser.description());

    parameter.  push_back(_general.qservMasterDatabaseName.key);
    value.      push_back(_general.qservMasterDatabaseName.str(*_config));
    description.push_back(_general.qservMasterDatabaseName.description());

    parameter.  push_back(_general.qservMasterDatabaseTmpDir.key);
    value.      push_back(_general.qservMasterDatabaseTmpDir.str(*_config));
    description.push_back(_general.qservMasterDatabaseTmpDir.description());

    parameter.  push_back(_general.workerTechnology.key);
    value.      push_back(_general.workerTechnology.str(*_config));
    description.push_back(_general.workerTechnology.description());

    parameter.  push_back(_general.workerNumProcessingThreads.key);
    value.      push_back(_general.workerNumProcessingThreads.str(*_config));
    description.push_back(_general.workerNumProcessingThreads.description());

    parameter.  push_back(_general.fsNumProcessingThreads.key);
    value.      push_back(_general.fsNumProcessingThreads.str(*_config));
    description.push_back(_general.fsNumProcessingThreads.description());

    parameter.  push_back(_general.workerFsBufferSizeBytes.key);
    value.      push_back(_general.workerFsBufferSizeBytes.str(*_config));
    description.push_back(_general.workerFsBufferSizeBytes.description());

    parameter.  push_back(_general.loaderNumProcessingThreads.key);
    value.      push_back(_general.loaderNumProcessingThreads.str(*_config));
    description.push_back(_general.loaderNumProcessingThreads.description());

    parameter.  push_back(_general.exporterNumProcessingThreads.key);
    value.      push_back(_general.exporterNumProcessingThreads.str(*_config));
    description.push_back(_general.exporterNumProcessingThreads.description());

    parameter.  push_back(_general.httpLoaderNumProcessingThreads.key);
    value.      push_back(_general.httpLoaderNumProcessingThreads.str(*_config));
    description.push_back(_general.httpLoaderNumProcessingThreads.description());

    parameter.  push_back(_general.workerDefaultSvcPort.key);
    value.      push_back(_general.workerDefaultSvcPort.str(*_config));
    description.push_back(_general.workerDefaultSvcPort.description());

    parameter.  push_back(_general.workerDefaultFsPort.key);
    value.      push_back(_general.workerDefaultFsPort.str(*_config));
    description.push_back(_general.workerDefaultFsPort.description());

    parameter.  push_back(_general.workerDefaultDataDir.key);
    value.      push_back(_general.workerDefaultDataDir.str(*_config));
    description.push_back(_general.workerDefaultDataDir.description());

    parameter.  push_back(_general.workerDefaultDbPort.key);
    value.      push_back(_general.workerDefaultDbPort.str(*_config));
    description.push_back(_general.workerDefaultDbPort.description());

    parameter.  push_back(_general.workerDefaultDbUser.key);
    value.      push_back(_general.workerDefaultDbUser.str(*_config));
    description.push_back(_general.workerDefaultDbUser.description());

    parameter.  push_back(_general.workerDefaultLoaderPort.key);
    value.      push_back(_general.workerDefaultLoaderPort.str(*_config));
    description.push_back(_general.workerDefaultLoaderPort.description());

    parameter.  push_back(_general.workerDefaultLoaderTmpDir.key);
    value.      push_back(_general.workerDefaultLoaderTmpDir.str(*_config));
    description.push_back(_general.workerDefaultLoaderTmpDir.description());

    parameter.  push_back(_general.workerDefaultExporterPort.key);
    value.      push_back(_general.workerDefaultExporterPort.str(*_config));
    description.push_back(_general.workerDefaultExporterPort.description());

    parameter.  push_back(_general.workerDefaultExporterTmpDir.key);
    value.      push_back(_general.workerDefaultExporterTmpDir.str(*_config));
    description.push_back(_general.workerDefaultExporterTmpDir.description());

    parameter.  push_back(_general.workerDefaultHttpLoaderPort.key);
    value.      push_back(_general.workerDefaultHttpLoaderPort.str(*_config));
    description.push_back(_general.workerDefaultHttpLoaderPort.description());

    parameter.  push_back(_general.workerDefaultHttpLoaderTmpDir.key);
    value.      push_back(_general.workerDefaultHttpLoaderTmpDir.str(*_config));
    description.push_back(_general.workerDefaultHttpLoaderTmpDir.description());

    util::ColumnTablePrinter table("GENERAL PARAMETERS:", indent, _verticalSeparator);

    table.addColumn("parameter",   parameter,   util::ColumnTablePrinter::LEFT);
    table.addColumn("value",       value);
    table.addColumn("description", description, util::ColumnTablePrinter::LEFT);

    table.print(cout, false, false);
}


void ConfigApp::_dumpWorkersAsTable(string const& indent) const {

    // Extract attributes of each worker and put them into the corresponding
    // columns. Translate tables cell values into strings when required.

    vector<string> name;
    vector<string> isEnabled;
    vector<string> isReadOnly;
    vector<string> dataDir;
    vector<string> svcHost;
    vector<string> svcPort;
    vector<string> fsHost;
    vector<string> fsPort;
    vector<string> dbHost;
    vector<string> dbPort;
    vector<string> dbUser;
    vector<string> loaderHost;
    vector<string> loaderPort;
    vector<string> loaderTmpDir;
    vector<string> exporterHost;
    vector<string> exporterPort;
    vector<string> exporterTmpDir;

    for (auto&& worker: _config->allWorkers()) {
        auto const wi = _config->workerInfo(worker);
        name.push_back(wi.name);
        isEnabled.push_back(wi.isEnabled  ? "yes" : "no");
        isReadOnly.push_back(wi.isReadOnly ? "yes" : "no");
        dataDir.push_back(wi.dataDir);
        svcHost.push_back(wi.svcHost);
        svcPort.push_back(to_string(wi.svcPort));
        fsHost.push_back(wi.fsHost);
        fsPort.push_back(to_string(wi.fsPort));
        dbHost.push_back(wi.dbHost);
        dbPort.push_back(to_string(wi.dbPort));
        dbUser.push_back(wi.dbUser);
        loaderHost.push_back(wi.loaderHost);
        loaderPort.push_back(to_string(wi.loaderPort));
        loaderTmpDir.push_back(wi.loaderTmpDir);
        exporterHost.push_back(wi.exporterHost);
        exporterPort.push_back(to_string(wi.exporterPort));
        exporterTmpDir.push_back(wi.exporterTmpDir);
    }

    util::ColumnTablePrinter table("WORKERS:", indent, _verticalSeparator);

    table.addColumn("name", name, util::ColumnTablePrinter::LEFT);
    table.addColumn("enabled", isEnabled);
    table.addColumn("read-only", isReadOnly);
    table.addColumn("Data directory", dataDir, util::ColumnTablePrinter::LEFT);
    table.addColumn("Replication server", svcHost, util::ColumnTablePrinter::LEFT);
    table.addColumn(":port", svcPort);
    table.addColumn("File server", fsHost, util::ColumnTablePrinter::LEFT);
    table.addColumn(":port", fsPort);
    table.addColumn("Database server", dbHost, util::ColumnTablePrinter::LEFT);
    table.addColumn(":port", dbPort);
    table.addColumn(":user", dbUser, util::ColumnTablePrinter::LEFT);
    table.addColumn("Ingest server", loaderHost, util::ColumnTablePrinter::LEFT);
    table.addColumn(":port", loaderPort);
    table.addColumn(":tmp", loaderTmpDir, util::ColumnTablePrinter::LEFT);
    table.addColumn("Export server", exporterHost, util::ColumnTablePrinter::LEFT);
    table.addColumn(":port", exporterPort);
    table.addColumn(":tmp", exporterTmpDir, util::ColumnTablePrinter::LEFT);

    table.print(cout, false, false);
}


void ConfigApp::_dumpFamiliesAsTable(string const& indent) const {

    // Extract attributes of each family and put them into the corresponding
    // columns.

    vector<string>       name;
    vector<size_t>       replicationLevel;
    vector<unsigned int> numStripes;
    vector<unsigned int> numSubStripes;

    for (auto&& family: _config->databaseFamilies()) {
        auto const fi = _config->databaseFamilyInfo(family);
        name.push_back(fi.name);
        replicationLevel.push_back(fi.replicationLevel);
        numStripes.push_back(fi.numStripes);
        numSubStripes.push_back(fi.numSubStripes);
    }

    util::ColumnTablePrinter table("DATABASE FAMILIES:", indent, _verticalSeparator);

    table.addColumn("name", name, util::ColumnTablePrinter::LEFT);
    table.addColumn("replication level", replicationLevel);
    table.addColumn("stripes", numStripes);
    table.addColumn("sub-stripes", numSubStripes);

    table.print(cout, false, false);
}


void ConfigApp::_dumpDatabasesAsTable(string const& indent) const {

    // Extract attributes of each database and put them into the corresponding
    // columns.

    vector<string> familyName;
    vector<string> databaseName;
    vector<string> isPublished;
    vector<string> tableName;
    vector<string> isPartitioned;
    vector<string> isDirector;
    vector<string> directorKey;
    vector<string> chunkIdColName;
    vector<string> subChunkIdColName;

    string const noSpecificFamily;
    bool const allDatabases = true;
    for (auto&& database: _config->databases(noSpecificFamily, allDatabases)) {
        auto const di = _config->databaseInfo(database);
        for (auto& table: di.partitionedTables) {
            familyName.push_back(di.family);
            databaseName.push_back(di.name);
            isPublished.push_back(di.isPublished ? "yes" : "no");
            tableName.push_back(table);
            isPartitioned.push_back("yes");
            if (table == di.directorTable) {
                isDirector.push_back("yes");
                directorKey.push_back(di.directorTableKey);
            } else {
                isDirector.push_back("no");
                directorKey.push_back("");
            }
            chunkIdColName.push_back(di.chunkIdColName);
            subChunkIdColName.push_back(di.subChunkIdColName);
        }
        for (auto& table: di.regularTables) {
            familyName.push_back(di.family);
            databaseName.push_back(di.name);
            isPublished.push_back(di.isPublished ? "yes" : "no");
            tableName.push_back(table);
            isPartitioned.push_back("no");
            isDirector.push_back("no");
            directorKey.push_back("");
            chunkIdColName.push_back("");
            subChunkIdColName.push_back("");
        }
        if (di.partitionedTables.empty() and di.regularTables.empty()) {
            familyName.push_back(di.family);
            databaseName.push_back(di.name);
            isPublished.push_back(di.isPublished ? "yes" : "no");
            tableName.push_back("<no tables>");
            isPartitioned.push_back("n/a");
            isDirector.push_back("n/a");
            directorKey.push_back("n/a");
            chunkIdColName.push_back("n/a");
            subChunkIdColName.push_back("n/a");
        }
    }

    util::ColumnTablePrinter table("DATABASES & TABLES:", indent, _verticalSeparator);

    table.addColumn("family",       familyName,   util::ColumnTablePrinter::LEFT);
    table.addColumn("database",     databaseName, util::ColumnTablePrinter::LEFT);
    table.addColumn(":published",   isPublished);
    table.addColumn("table",        tableName,    util::ColumnTablePrinter::LEFT);
    table.addColumn(":partitioned", isPartitioned);
    table.addColumn(":director",     isDirector);
    table.addColumn(":director-key", directorKey);
    table.addColumn(":chunk-id-key",     chunkIdColName);
    table.addColumn(":sub-chunk-id-key", subChunkIdColName);

    table.print(cout, false, false);
}


int ConfigApp::_configInitFile() const {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    try {
        if ("JSON" == _format) { cout << _config->toJson().dump() << endl; }
        else {
            LOGS(_log, LOG_LVL_ERROR, context << "operation failed, unsupported format: " << _format);
            return 1;
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: "
              << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_updateGeneral() {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    try {
        _general.requestBufferSizeBytes.save(*_config);
        _general.retryTimeoutSec.save(*_config);
        _general.controllerRequestTimeoutSec.save(*_config);
        _general.jobTimeoutSec.save(*_config);
        _general.jobHeartbeatTimeoutSec.save(*_config);
        _general.controllerThreads.save(*_config);
        _general.controllerHttpPort.save(*_config);
        _general.controllerHttpThreads.save(*_config);
        _general.controllerEmptyChunksDir.save(*_config);
        _general.xrootdAutoNotify.save(*_config);
        _general.xrootdHost.save(*_config);
        _general.xrootdPort.save(*_config);
        _general.xrootdTimeoutSec.save(*_config);
        _general.databaseServicesPoolSize.save(*_config);
        _general.qservMasterDatabaseHost.save(*_config);
        _general.qservMasterDatabasePort.save(*_config);
        _general.qservMasterDatabaseUser.save(*_config);
        _general.qservMasterDatabaseName.save(*_config);
        _general.qservMasterDatabaseServicesPoolSize.save(*_config);
        _general.qservMasterDatabaseTmpDir.save(*_config);
        _general.workerTechnology.save(*_config);
        _general.workerNumProcessingThreads.save(*_config);
        _general.fsNumProcessingThreads.save(*_config);
        _general.workerFsBufferSizeBytes.save(*_config);
        _general.loaderNumProcessingThreads.save(*_config);
        _general.exporterNumProcessingThreads.save(*_config);
        _general.httpLoaderNumProcessingThreads.save(*_config);
        _general.workerDefaultSvcPort.save(*_config);
        _general.workerDefaultFsPort.save(*_config);
        _general.workerDefaultDataDir.save(*_config);
        _general.workerDefaultDbPort.save(*_config);
        _general.workerDefaultDbUser.save(*_config);
        _general.workerDefaultLoaderPort.save(*_config);
        _general.workerDefaultLoaderTmpDir.save(*_config);
        _general.workerDefaultExporterPort.save(*_config);
        _general.workerDefaultExporterTmpDir.save(*_config);
        _general.workerDefaultHttpLoaderPort.save(*_config);
        _general.workerDefaultHttpLoaderTmpDir.save(*_config);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_updateWorker() const {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (!_config->isKnownWorker(_workerInfo.name)) {
        LOGS(_log, LOG_LVL_ERROR, context << "unknown worker: '" << _workerInfo.name << "'");
        return 1;
    }

    // Configuration changes will be updated in the transient object obtained from
    // the database and then be saved to the the persistent configuration.
    try {
        auto info = _config->workerInfo(_workerInfo.name);

        WorkerInfo::update(_workerEnable,   info.isEnabled);
        WorkerInfo::update(_workerReadOnly, info.isReadOnly);

        WorkerInfo::update(_workerInfo.svcHost, info.svcHost);
        WorkerInfo::update(_workerInfo.svcPort, info.svcPort);

        WorkerInfo::update(_workerInfo.fsHost,  info.fsHost);
        WorkerInfo::update(_workerInfo.fsPort,  info.fsPort);
        WorkerInfo::update(_workerInfo.dataDir, info.dataDir);

        WorkerInfo::update(_workerInfo.dbHost,  info.dbHost);
        WorkerInfo::update(_workerInfo.dbPort,  info.dbPort);
        WorkerInfo::update(_workerInfo.dbUser,  info.dbUser);

        WorkerInfo::update(_workerInfo.loaderHost,   info.loaderHost);
        WorkerInfo::update(_workerInfo.loaderPort,   info.loaderPort);
        WorkerInfo::update(_workerInfo.loaderTmpDir, info.loaderTmpDir);

        WorkerInfo::update(_workerInfo.exporterHost,   info.exporterHost);
        WorkerInfo::update(_workerInfo.exporterPort,   info.exporterPort);
        WorkerInfo::update(_workerInfo.exporterTmpDir, info.exporterTmpDir);

        WorkerInfo::update(_workerInfo.httpLoaderHost,   info.httpLoaderHost);
        WorkerInfo::update(_workerInfo.httpLoaderPort,   info.httpLoaderPort);
        WorkerInfo::update(_workerInfo.httpLoaderTmpDir, info.httpLoaderTmpDir);

        auto const updatedInfo = _config->updateWorker(info);

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_addWorker() const {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (_config->isKnownWorker(_workerInfo.name)) {
        LOGS(_log, LOG_LVL_ERROR, context << "the worker already exists: '" << _workerInfo.name << "'");
        return 1;
    }
    try {
        _config->addWorker(_workerInfo);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_deleteWorker() const {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (not _config->isKnownWorker(_workerInfo.name)) {
        LOGS(_log, LOG_LVL_ERROR, context << "the worker doesn't exists: '" << _workerInfo.name << "'");
        return 1;
    }

    auto const info = _config->workerInfo(_workerInfo.name);
    try {
        _config->deleteWorker(_workerInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_addFamily() {

    string const context = "ConfigApp::" + string(__func__) + "  ";
    
    if (_familyInfo.name.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the family name can't be empty");
        return 1;
    }
    if (_familyInfo.replicationLevel == 0) {
        LOGS(_log, LOG_LVL_ERROR, context << "the replication level can't be 0");
        return 1;
    }
    if (_familyInfo.numStripes == 0) {
        LOGS(_log, LOG_LVL_ERROR, context << "the number of stripes level can't be 0");
        return 1;
    }
    if (_familyInfo.numSubStripes == 0) {
        LOGS(_log, LOG_LVL_ERROR, context << "the number of sub-stripes level can't be 0");
        return 1;
    }
    try {
        _config->addDatabaseFamily(_familyInfo);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_deleteFamily() {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (_familyInfo.name.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the family name can't be empty");
        return 1;
    }
    try {
        _config->deleteDatabaseFamily(_familyInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_addDatabase() {

    string const context = "ConfigApp::" + string(__func__) + "  ";
    
    if (_databaseInfo.name.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the database name can't be empty");
        return 1;
    }
    if (_databaseInfo.family.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the family name can't be empty");
        return 1;
    }
    try {
        _config->addDatabase(_databaseInfo.name, _databaseInfo.family);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_publishDatabase() {

    string const context = "ConfigApp::" + string(__func__) + "  ";
    
    if (_databaseInfo.name.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the database name can't be empty");
        return 1;
    }
    try {
        _config->publishDatabase(_databaseInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_deleteDatabase() {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (_databaseInfo.name.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the database name can't be empty");
        return 1;
    }
    try {
        _config->deleteDatabase(_databaseInfo.name);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_addTable() {

    string const context = "ConfigApp::" + string(__func__) + "  ";
    
    if (_database.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the database name can't be empty");
        return 1;
    }
    if (_table.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the table name can't be empty");
        return 1;
    }
    try {
        list<SqlColDef> noColumns;
        _config->addTable(_database, _table, _isPartitioned, noColumns,
                          _isDirector, _directorKey,
                          _chunkIdColName, _subChunkIdColName, _latitudeColName, _longitudeColName);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_deleteTable() {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (_database.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the database name can't be empty");
        return 1;
    }
    if (_table.empty()) {
        LOGS(_log, LOG_LVL_ERROR, context << "the table name can't be empty");
        return 1;
    }
    try {
        _config->deleteTable(_database, _table);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
