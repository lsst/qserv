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
#include "replica/ConfigurationFile.h"
#include "replica/ConfigurationMySQL.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This application is the tool for viewing and manipulating"
    " the configuration data of the Replication system stored in the MySQL/MariaDB.";

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
void addCommandOption(PARSER& parser, T& struct_) {
    parser.option(struct_.key, struct_.description, struct_.value);
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
            ::description,
            true  /* injectDatabaseOptions */,
            false /* boostProtobufVersionCheck */,
            false /* enableServiceProvider */
        ),
        _log(LOG_GET("lsst.qserv.replica.ConfigApp")),
        _configUrl("file:replication.cfg") {

    // Configure the command line parser

    parser().commands(
        "command",
        {"DUMP",
         "CONFIG_INIT_FILE",
         "UPDATE_GENERAL",
         "UPDATE_WORKER", "ADD_WORKER", "DELETE_WORKER",
         "ADD_DATABASE_FAMILY", "DELETE_DATABASE_FAMILY",
         "ADD_DATABASE", "DELETE_DATABASE",
         "ADD_TABLE", "DELETE_TABLE"
        },
        _command);

    // Parameters, options and flags shared by all commands

    parser().option(
        "config",
        "Configuration URL (a configuration file or a set of database connection parameters).",
        _configUrl);

    parser().flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in dumps.",
        _verticalSeparator);

    // Command-specific parameters, options and flags

    auto&& dumpCmd = parser().command("DUMP");

    dumpCmd.optional(
        "scope",
        "This optional parameter narrows a scope of the operation down to a specific"
        " context. If no scope is specified then everything will be dumped.",
        _dumpScope,
        vector<string>({"GENERAL", "WORKERS", "FAMILIES", "DATABASES"}));

    dumpCmd.flag(
        "db-show-password",
        "Show the actual database password when making the dump of the GENERAL parameters.",
        _dumpDbShowPassword);

    // Command-specific parameters, options and flags

    parser().command("CONFIG_INIT_FILE").required(
        "format",
        "The format of the initialization file to be produced with this option."
        " Allowed values: MYSQL, INI",
        _format,
        vector<string>({"MYSQL", "INI"}));

    // Command-specific parameters, options and flags

    auto&& updateWorkerCmd = parser().command("UPDATE_WORKER");

    updateWorkerCmd.required(
        "worker",
        "The name of a worker to be updated.",
        _workerInfo.name);

    updateWorkerCmd.option(
        "worker-service-host",
        "The new DNS name or an IP address where the worker runs.",
        _workerInfo.svcHost);

    updateWorkerCmd.option(
        "worker-service-port",
        "The port number of the worker service.",
        _workerInfo.svcPort);

    updateWorkerCmd.option(
        "worker-fs-host",
        "The new DNS name or an IP address where the worker's File Server runs.",
        _workerInfo.fsHost);

    updateWorkerCmd.option(
        "worker-fs-port",
        "The port number of the worker's File Server.",
        _workerInfo.fsPort);

    updateWorkerCmd.option(
        "worker-data-dir",
        "The data directory of the worker.",
        _workerInfo.dataDir);

    updateWorkerCmd.option(
        "worker-db-host",
        "The new DNS name or an IP address where the worker's database service runs.",
        _workerInfo.dbHost);

    updateWorkerCmd.option(
        "worker-db-port",
        "The port number of the worker's database service.",
        _workerInfo.dbPort);

    updateWorkerCmd.option(
        "worker-db-user",
        "The name of a user account for the worker's database service.",
        _workerInfo.dbUser);

    updateWorkerCmd.option(
        "worker-enable",
        "Enable the worker if 1 (or any positive number), disable if 0."
        " Negative numbers are ignored.",
        _workerEnable);

    updateWorkerCmd.option(
        "worker-read-only",
        "Turn the worker into the read-write mode if 1 (or any positive number),"
        ", turn it int the read-write mode if 0.",
        _workerReadOnly);

    // Command-specific parameters, options and flags

    auto&& addWorkerCmd = parser().command("ADD_WORKER");

    addWorkerCmd.required(
        "worker",
        "The name of a worker to be added.",
        _workerInfo.name);

    addWorkerCmd.required(
        "service-host",
        "The DNS name or an IP address where the worker runs.",
        _workerInfo.svcHost);

    addWorkerCmd.required(
        "service-port",
        "The port number of the worker service",
        _workerInfo.svcPort);

    addWorkerCmd.required(
        "fs-host",
        "The DNS name or an IP address where the worker's File Server runs.",
        _workerInfo.fsHost);

    addWorkerCmd.required(
        "fs-port",
        "The port number of the worker's File Server.",
        _workerInfo.fsPort);

    addWorkerCmd.required(
        "data-dir",
        "The data directory of the worker",
        _workerInfo.dataDir);

    addWorkerCmd.required(
        "enabled",
        "Set to '0' if the worker is turned into disabled mode upon creation.",
        _workerInfo.isEnabled);

    addWorkerCmd.required(
        "read-only",
        "Set to '0' if the worker is NOT turned into the read-only mode upon creation.",
        _workerInfo.isReadOnly);

    // Command-specific parameters, options and flags

    parser().command("DELETE_WORKER").required(
        "worker",
        "The name of a worker to be deleted.",
        _workerInfo.name);

    // Command-specific parameters, options and flags

    auto&& updateGeneralCmd = parser().command("UPDATE_GENERAL");

    ::addCommandOption(updateGeneralCmd, _general.requestBufferSizeBytes);
    ::addCommandOption(updateGeneralCmd, _general.retryTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.controllerThreads);
    ::addCommandOption(updateGeneralCmd, _general.controllerHttpPort);
    ::addCommandOption(updateGeneralCmd, _general.controllerHttpThreads);
    ::addCommandOption(updateGeneralCmd, _general.controllerRequestTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.jobTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.jobHeartbeatTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.xrootdAutoNotify);
    ::addCommandOption(updateGeneralCmd, _general.xrootdHost);
    ::addCommandOption(updateGeneralCmd, _general.xrootdPort);
    ::addCommandOption(updateGeneralCmd, _general.xrootdTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _general.databaseServicesPoolSize);
    ::addCommandOption(updateGeneralCmd, _general.workerTechnology);
    ::addCommandOption(updateGeneralCmd, _general.workerNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _general.fsNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _general.workerFsBufferSizeBytes);

    // Command-specific parameters, options and flags

    auto&& addFamilyCmd = parser().command("ADD_DATABASE_FAMILY");

    addFamilyCmd.required(
        "name",
        "The name of a new database family.",
        _familyInfo.name);

    addFamilyCmd.required(
        "replication-level",
        "The minimum replication level desired (1..N).",
        _familyInfo.replicationLevel);

    addFamilyCmd.required(
        "num-stripes",
        "The number of stripes (from the CSS partitioning configuration).",
        _familyInfo.numStripes);

    addFamilyCmd.required(
        "num-sub-stripes",
        "The number of sub-stripes (from the CSS partitioning configuration).",
        _familyInfo.numSubStripes);

    // Command-specific parameters, options and flags

    parser().command("DELETE_DATABASE_FAMILY").required(
        "name",
        "The name of an existing database family to be deleted. ATTENTION: all databases that"
        " are members of the family will be deleted as well, along with the relevant info"
        " about replicas of all chunks of the databases.",
        _familyInfo.name);
    
    // Command-specific parameters, options and flags

    auto&& addDatabaseCmd = parser().command("ADD_DATABASE");

    addDatabaseCmd.required(
        "name",
        "The name of a new database.",
        _databaseInfo.name);

    addDatabaseCmd.required(
        "family",
        "The name of an existing family the new database will join.",
        _databaseInfo.family);

    // Command-specific parameters, options and flags

    parser().command("DELETE_DATABASE").required(
        "name",
        "The name of an existing database to be deleted. ATTENTION: all relevant info that"
        " is associated with the database (replicas of all chunks, etc.) will get deleted as well.",
        _databaseInfo.name);

    // Command-specific parameters, options and flags

    auto&& addTableCmd = parser().command("ADD_TABLE");

    addTableCmd.required(
        "database",
        "The name of an existing database.",
        _database);

    addTableCmd.required(
        "table",
        "The name of a new table.",
        _table);

    addTableCmd.flag(
        "partitioned",
        "The flag indicating (if present) that a table is partitioned.",
        _isPartitioned);

    // Command-specific parameters, options and flags

    auto&& deleteTableCmd = parser().command("DELETE_TABLE");

    deleteTableCmd.required(
        "database",
        "The name of an existing database.",
        _database );

    deleteTableCmd.required(
        "table",
        "The name of an existing table to be deleted. ATTENTION: all relevant info that"
        " is associated with the table (replicas of all chunks, etc.) will get deleted as well.",
        _table);
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
    if (_command == "DELETE_DATABASE")        return _deleteDatabase();
    if (_command == "ADD_TABLE")              return _addTable();
    if (_command == "DELETE_TABLE"   )        return _deleteTable();

    LOGS(_log, LOG_LVL_ERROR, context << "unsupported command: '" + _command + "'");
    return 1;
}


int ConfigApp::_dump() const {

    string const indent = "  ";

    cout << "\n"
         << indent << "CONFIG_URL: " << _config->configUrl() << "\n";

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

    parameter.  push_back(_general.requestBufferSizeBytes.key);
    value.      push_back(_general.requestBufferSizeBytes.str(_config));
    description.push_back(_general.requestBufferSizeBytes.description);

    parameter.  push_back(_general.retryTimeoutSec.key);
    value.      push_back(_general.retryTimeoutSec.str(_config));
    description.push_back(_general.retryTimeoutSec.description);

    parameter.  push_back(_general.controllerThreads.key);
    value.      push_back(_general.controllerThreads.str(_config));
    description.push_back(_general.controllerThreads.description);

    parameter.  push_back(_general.controllerHttpPort.key);
    value.      push_back(_general.controllerHttpPort.str(_config));
    description.push_back(_general.controllerHttpPort.description);

    parameter.  push_back(_general.controllerHttpThreads.key);
    value.      push_back(_general.controllerHttpThreads.str(_config));
    description.push_back(_general.controllerHttpThreads.description);

    parameter.  push_back(_general.controllerRequestTimeoutSec.key);
    value.      push_back(_general.controllerRequestTimeoutSec.str(_config));
    description.push_back(_general.controllerRequestTimeoutSec.description);

    parameter.  push_back(_general.jobTimeoutSec.key);
    value.      push_back(_general.jobTimeoutSec.str(_config));
    description.push_back(_general.jobTimeoutSec.description);

    parameter.  push_back(_general.jobHeartbeatTimeoutSec.key);
    value.      push_back(_general.jobHeartbeatTimeoutSec.str(_config));
    description.push_back(_general.jobHeartbeatTimeoutSec.description);

    parameter.  push_back(_general.xrootdAutoNotify.key);
    value.      push_back(_general.xrootdAutoNotify.str(_config));
    description.push_back(_general.xrootdAutoNotify.description);

    parameter.  push_back(_general.xrootdHost.key);
    value.      push_back(_general.xrootdHost.str(_config));
    description.push_back(_general.xrootdHost.description);

    parameter.  push_back(_general.xrootdPort.key);
    value.      push_back(_general.xrootdPort.str(_config));
    description.push_back(_general.xrootdPort.description);

    parameter.  push_back(_general.xrootdTimeoutSec.key);
    value.      push_back(_general.xrootdTimeoutSec.str(_config));
    description.push_back(_general.xrootdTimeoutSec.description);

    parameter.  push_back(_general.databaseTechnology.key);
    value.      push_back(_general.databaseTechnology.str(_config));
    description.push_back(_general.databaseTechnology.description);

    parameter.  push_back(_general.databaseHost.key);
    value.      push_back(_general.databaseHost.str(_config));
    description.push_back(_general.databaseHost.description);

    parameter.  push_back(_general.databasePort.key);
    value.      push_back(_general.databasePort.str(_config));
    description.push_back(_general.databasePort.description);

    parameter.  push_back(_general.databaseUser.key);
    value.      push_back(_general.databaseUser.str(_config));
    description.push_back(_general.databaseUser.description);

    bool const scrambleDbPassword = not _dumpDbShowPassword;
    parameter.  push_back(_general.databasePassword.key);
    value.      push_back(_general.databasePassword.str(_config, scrambleDbPassword));
    description.push_back(_general.databasePassword.description);

    parameter.  push_back(_general.databaseName.key);
    value.      push_back(_general.databaseName.str(_config));
    description.push_back(_general.databaseName.description);

    parameter.  push_back(_general.databaseServicesPoolSize.key);
    value.      push_back(_general.databaseServicesPoolSize.str(_config));
    description.push_back(_general.databaseServicesPoolSize.description);

    parameter.  push_back(_general.qservMasterDatabaseHost.key);
    value.      push_back(_general.qservMasterDatabaseHost.str(_config));
    description.push_back(_general.qservMasterDatabaseHost.description);

    parameter.  push_back(_general.qservMasterDatabasePort.key);
    value.      push_back(_general.qservMasterDatabasePort.str(_config));
    description.push_back(_general.qservMasterDatabasePort.description);

    parameter.  push_back(_general.qservMasterDatabaseUser.key);
    value.      push_back(_general.qservMasterDatabaseUser.str(_config));
    description.push_back(_general.qservMasterDatabaseUser.description);

    parameter.  push_back(_general.qservMasterDatabasePassword.key);
    value.      push_back(_general.qservMasterDatabasePassword.str(_config, scrambleDbPassword));
    description.push_back(_general.qservMasterDatabasePassword.description);

    parameter.  push_back(_general.qservMasterDatabaseName.key);
    value.      push_back(_general.qservMasterDatabaseName.str(_config));
    description.push_back(_general.qservMasterDatabaseName.description);

    parameter.  push_back(_general.qservMasterDatabaseServicesPoolSize.key);
    value.      push_back(_general.qservMasterDatabaseServicesPoolSize.str(_config));
    description.push_back(_general.qservMasterDatabaseServicesPoolSize.description);

    parameter.  push_back(_general.workerTechnology.key);
    value.      push_back(_general.workerTechnology.str(_config));
    description.push_back(_general.workerTechnology.description);

    parameter.  push_back(_general.workerNumProcessingThreads.key);
    value.      push_back(_general.workerNumProcessingThreads.str(_config));
    description.push_back(_general.workerNumProcessingThreads.description);

    parameter.  push_back(_general.fsNumProcessingThreads.key);
    value.      push_back(_general.fsNumProcessingThreads.str(_config));
    description.push_back(_general.fsNumProcessingThreads.description);

    parameter.  push_back(_general.workerFsBufferSizeBytes.key);
    value.      push_back(_general.workerFsBufferSizeBytes.str(_config));
    description.push_back(_general.workerFsBufferSizeBytes.description);

    util::ColumnTablePrinter table("GENERAL PARAMETERS:", indent, _verticalSeparator);

    table.addColumn("parameter",   parameter,   util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("value",       value);
    table.addColumn("description", description, util::ColumnTablePrinter::Alignment::LEFT);

    table.print(cout, false, false);
}


void ConfigApp::_dumpWorkersAsTable(string const& indent) const {

    // Extract attributes of each worker and put them into the corresponding
    // columns. Translate tables cell values into strings when required.

    vector<string> name;
    vector<string> isEnabled;
    vector<string> isReadOnly;
    vector<string> svcHost;
    vector<string> svcPort;
    vector<string> fsHost;
    vector<string> fsPort;
    vector<string> dataDir;
    vector<string> dbHost;
    vector<string> dbPort;
    vector<string> dbUser;

    for (auto&& worker: _config->allWorkers()) {
        auto const wi = _config->workerInfo(worker);
        name       .push_back(wi.name);
        isEnabled  .push_back(wi.isEnabled  ? "yes" : "no");
        isReadOnly .push_back(wi.isReadOnly ? "yes" : "no");
        svcHost    .push_back(wi.svcHost);
        svcPort    .push_back(to_string(wi.svcPort));
        fsHost     .push_back(wi.fsHost);
        fsPort     .push_back(to_string(wi.fsPort));
        dbHost     .push_back(wi.dbHost);
        dbPort     .push_back(to_string(wi.dbPort));
        dbUser     .push_back(wi.dbUser);
        dataDir    .push_back(wi.dataDir);
    }

    util::ColumnTablePrinter table("WORKERS:", indent, _verticalSeparator);

    table.addColumn("name",               name,        util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("enabled",            isEnabled);
    table.addColumn("read-only",          isReadOnly);
    table.addColumn("Replication server", svcHost,     util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn(":port",              svcPort);
    table.addColumn("File server",        fsHost,      util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn(":port",              fsPort);
    table.addColumn("Database server",    dbHost,      util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn(":port",              dbPort);
    table.addColumn(":user",              dbUser,      util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("Data directory",     dataDir,     util::ColumnTablePrinter::Alignment::LEFT);

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
        name            .push_back(fi.name);
        replicationLevel.push_back(fi.replicationLevel);
        numStripes      .push_back(fi.numStripes);
        numSubStripes   .push_back(fi.numSubStripes);
    }

    util::ColumnTablePrinter table("DATABASE FAMILIES:", indent, _verticalSeparator);

    table.addColumn("name", name, util::ColumnTablePrinter::Alignment::LEFT);
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
    vector<string> tableName;
    vector<string> isPartitioned;

    for (auto&& database: _config->databases()) {
        auto const di = _config->databaseInfo(database);
        for (auto& table: di.partitionedTables) {
            familyName    .push_back(di.family);
            databaseName .push_back(di.name);
            tableName    .push_back(table);
            isPartitioned.push_back("yes");
        }
        for (auto& table: di.regularTables) {
            familyName   .push_back(di.family);
            databaseName .push_back(di.name);
            tableName    .push_back(table);
            isPartitioned.push_back("no");
        }
        if (di.partitionedTables.empty() and di.regularTables.empty()) {
            familyName   .push_back(di.family);
            databaseName .push_back(di.name);
            tableName    .push_back("<no tables>");
            isPartitioned.push_back("n/a");
        }
    }

    util::ColumnTablePrinter table("DATABASES & TABLES:", indent, _verticalSeparator);

    table.addColumn("family",      familyName,   util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("database",    databaseName, util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("table",       tableName,    util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("partitioned", isPartitioned);

    table.print(cout, false, false);
}


int ConfigApp::_configInitFile() const {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    try {
        if      ("MYSQL" == _format) { cout << ConfigurationMySQL::dump2init(_config) << endl; }
        else if ("INI"   == _format) { cout << ConfigurationFile::dump2init(_config) << endl; }
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
        _general.requestBufferSizeBytes     .save(_config);
        _general.retryTimeoutSec            .save(_config);
        _general.controllerThreads          .save(_config);
        _general.controllerHttpPort         .save(_config);
        _general.controllerHttpThreads      .save(_config);
        _general.controllerRequestTimeoutSec.save(_config);
        _general.jobTimeoutSec              .save(_config);
        _general.jobHeartbeatTimeoutSec     .save(_config);
        _general.xrootdAutoNotify           .save(_config);
        _general.xrootdHost                 .save(_config);
        _general.xrootdPort                 .save(_config);
        _general.xrootdTimeoutSec           .save(_config);
        _general.databaseServicesPoolSize   .save(_config);
        _general.workerTechnology           .save(_config);
        _general.workerNumProcessingThreads .save(_config);
        _general.fsNumProcessingThreads     .save(_config);
        _general.workerFsBufferSizeBytes    .save(_config);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_updateWorker() const {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (not _config->isKnownWorker(_workerInfo.name)) {
        LOGS(_log, LOG_LVL_ERROR, context << "unknown worker: '" << _workerInfo.name << "'");
        return 1;
    }
    auto const info = _config->workerInfo(_workerInfo.name);

    try {
        if (not _workerInfo.svcHost.empty()
            and _workerInfo.svcHost != info.svcHost) {

            _config->setWorkerSvcHost(_workerInfo.name,
                                      _workerInfo.svcHost);
        }
        if (_workerInfo.svcPort != 0 and
            _workerInfo.svcPort != info.svcPort) {

            _config->setWorkerSvcPort(_workerInfo.name,
                                      _workerInfo.svcPort);
        }
        if (not _workerInfo.fsHost.empty()
            and _workerInfo.fsHost != info.fsHost) {

            _config->setWorkerFsHost(_workerInfo.name,
                                     _workerInfo.fsHost);
        }
        if (_workerInfo.fsPort != 0 and
            _workerInfo.fsPort != info.fsPort) {

            _config->setWorkerFsPort(_workerInfo.name,
                                     _workerInfo.fsPort);
        }
        if (not _workerInfo.dataDir.empty()
            and _workerInfo.dataDir != info.dataDir) {

            _config->setWorkerDataDir(_workerInfo.name,
                                      _workerInfo.dataDir);
        }

        if (not _workerInfo.dbHost.empty()
            and _workerInfo.dbHost != info.dbHost) {

            _config->setWorkerDbHost(_workerInfo.name,
                                     _workerInfo.dbHost);
        }
        if (_workerInfo.dbPort != 0 and
            _workerInfo.dbPort != info.dbPort) {

            _config->setWorkerDbPort(_workerInfo.name,
                                     _workerInfo.dbPort);
        }
        if (not _workerInfo.dbUser.empty()
            and _workerInfo.dbUser != info.dbUser) {

            _config->setWorkerDbUser(_workerInfo.name,
                                     _workerInfo.dbUser);
        }
        if (_workerEnable > 0 and not info.isEnabled) {
            _config->disableWorker(_workerInfo.name, false);
        }
        if (_workerEnable == 0 and info.isEnabled) {
            _config->disableWorker(_workerInfo.name);
        }
        if (_workerReadOnly > 0 and not info.isReadOnly) {
            _config->setWorkerReadOnly(_workerInfo.name);
        }
        if (_workerReadOnly == 0 and info.isReadOnly) {
            _config->setWorkerReadOnly(_workerInfo.name, false);
        }
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
        _config->addDatabase(_databaseInfo);
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
        _config->addTable(_database, _table, _isPartitioned);
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
