/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

string const description {
    "This application is the tool for viewing and manipulating"
    " the configuration data of the Replication system stored in the MySQL/MariaDB"
};

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

ConfigApp::Ptr ConfigApp::create(int argc,
                                 const char* const argv[]) {
    return Ptr(
        new ConfigApp(
            argc,
            argv
        )
    );
}


ConfigApp::ConfigApp(int argc,
                     const char* const argv[])
    :   Application(
            argc,
            argv,
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
        "Print vertical separator when displaying tabular data in dumps",
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
        "show the actual database password when making the dump of the GENERAL parameters",
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
        "The name of a worker to be updated",
        _workerInfo.name);

    updateWorkerCmd.option(
        "worker-service-host",
        "The new DNS name or an IP address where the worker runs",
        _workerInfo.svcHost);

    updateWorkerCmd.option(
        "worker-service-port",
        "The port number of the worker service",
        _workerInfo.svcPort);

    updateWorkerCmd.option(
        "worker-fs-host",
        "The new DNS name or an IP address where the worker's File Server runs",
        _workerInfo.fsHost);

    updateWorkerCmd.option(
        "worker-fs-port",
        "The port number of the worker's File Server",
        _workerInfo.fsPort);

    updateWorkerCmd.option(
        "worker-data-dir",
        "The data directory of the worker",
        _workerInfo.dataDir);

    updateWorkerCmd.flag(
        "worker-enable",
        "Enable the worker. ATTENTION: this flag can't be used together with flag --worker-disable",
        _workerEnable);

    updateWorkerCmd.flag(
        "worker-disable",
        "Disable the worker. ATTENTION: this flag can't be used together with flag --worker-enable",
        _workerDisable);

    updateWorkerCmd.flag(
        "worker-read-only",
        "Turn the worker into the read-only mode. ATTENTION: this flag can't be"
        " used together with flag --worker-read-write",
        _workerReadOnly);

    updateWorkerCmd.flag(
        "worker-read-write",
        "Turn the worker into the read-write mode. ATTENTION: this flag can't be"
        " used together with flag --worker-read-only",
        _workerReadWrite);

    // Command-specific parameters, options and flags

    auto&& addWorkerCmd = parser().command("ADD_WORKER");

    addWorkerCmd.required(
        "worker",
        "The name of a worker to be added",
        _workerInfo.name);

    addWorkerCmd.required(
        "service-host",
        "The DNS name or an IP address where the worker runs",
        _workerInfo.svcHost);

    addWorkerCmd.required(
        "service-port",
        "The port number of the worker service",
        _workerInfo.svcPort);

    addWorkerCmd.required(
        "fs-host",
        "The DNS name or an IP address where the worker's File Server runs",
        _workerInfo.fsHost);

    addWorkerCmd.required(
        "fs-port",
        "The port number of the worker's File Server",
        _workerInfo.fsPort);

    addWorkerCmd.required(
        "data-dir",
        "The data directory of the worker",
        _workerInfo.dataDir);

    addWorkerCmd.required(
        "enabled",
        "Set to '0' if the worker is turned into disabled mode upon creation",
        _workerInfo.isEnabled);

    addWorkerCmd.required(
        "read-only",
        "Set to '0' if the worker is NOT turned into the read-only mode upon creation",
        _workerInfo.isReadOnly);

    // Command-specific parameters, options and flags

    parser().command("DELETE_WORKER").required(
        "worker",
        "The name of a worker to be deleted",
        _workerInfo.name);

    // Command-specific parameters, options and flags

    auto&& updateGeneralCmd = parser().command("UPDATE_GENERAL");

    ::addCommandOption(updateGeneralCmd, _requestBufferSizeBytes);
    ::addCommandOption(updateGeneralCmd, _retryTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _controllerThreads);
    ::addCommandOption(updateGeneralCmd, _controllerHttpPort);
    ::addCommandOption(updateGeneralCmd, _controllerHttpThreads);
    ::addCommandOption(updateGeneralCmd, _controllerRequestTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _jobTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _jobHeartbeatTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _xrootdAutoNotify);
    ::addCommandOption(updateGeneralCmd, _xrootdHost);
    ::addCommandOption(updateGeneralCmd, _xrootdPort);
    ::addCommandOption(updateGeneralCmd, _xrootdTimeoutSec);
    ::addCommandOption(updateGeneralCmd, _databaseServicesPoolSize);
    ::addCommandOption(updateGeneralCmd, _workerTechnology);
    ::addCommandOption(updateGeneralCmd, _workerNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _fsNumProcessingThreads);
    ::addCommandOption(updateGeneralCmd, _workerFsBufferSizeBytes);

    // Command-specific parameters, options and flags

    auto&& addFamilyCmd = parser().command("ADD_DATABASE_FAMILY");

    addFamilyCmd.required(
        "name",
        "The name of a new database family",
        _familyInfo.name);

    addFamilyCmd.required(
        "replication-level",
        "The minimum replication level desired (1..N)",
        _familyInfo.replicationLevel);

    addFamilyCmd.required(
        "num-stripes",
        "The number of stripes (from the CSS partitioning configuration)",
        _familyInfo.numStripes);

    addFamilyCmd.required(
        "num-sub-stripes",
        "The number of sub-stripes (from the CSS partitioning configuration)",
        _familyInfo.numSubStripes);

    // Command-specific parameters, options and flags

    parser().command("DELETE_DATABASE_FAMILY").required(
        "name",
        "The name of an existing database family to be deleted. ATTENTION: all databases that"
        " are members of the family will be deleted as well, along with the relevant info"
        " about replicas of all chunks of the databases",
        _familyInfo.name);
    
    // Command-specific parameters, options and flags

    auto&& addDatabaseCmd = parser().command("ADD_DATABASE");

    addDatabaseCmd.required(
        "name",
        "The name of a new database",
        _databaseInfo.name);

    addDatabaseCmd.required(
        "family",
        "The name of an existing family the new database will join",
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
        "The name of an existing database",
        _database);

    addTableCmd.required(
        "table",
        "The name of a new table",
        _table);

    addTableCmd.flag(
        "partitioned",
        "The flag indicating (if present) that a table is partitioned",
        _isPartitioned);

    // Command-specific parameters, options and flags

    auto&& deleteTableCmd = parser().command("DELETE_TABLE");

    deleteTableCmd.required(
        "database",
        "The name of an existing database",
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
    /*
    if (_config->prefix() != "mysql") {
        LOGS(_log, LOG_LVL_ERROR, context << "configuration with prefix '" << _config->prefix()
             << "' is not allowed by this application");
        return 1;
    }
     * */
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

    parameter.  push_back(                  _requestBufferSizeBytes.key);
    value.      push_back(to_string(_config->requestBufferSizeBytes()));
    description.push_back(                  _requestBufferSizeBytes.description);

    parameter.  push_back(                  _retryTimeoutSec.key);
    value.      push_back(to_string(_config->retryTimeoutSec()));
    description.push_back(                  _retryTimeoutSec.description);

    parameter.  push_back(                  _controllerThreads.key);
    value.      push_back(to_string(_config->controllerThreads()));
    description.push_back(                  _controllerThreads.description);

    parameter.  push_back(                  _controllerHttpPort.key);
    value.      push_back(to_string(_config->controllerHttpPort()));
    description.push_back(                  _controllerHttpPort.description);

    parameter.  push_back(                  _controllerHttpThreads.key);
    value.      push_back(to_string(_config->controllerHttpThreads()));
    description.push_back(                  _controllerHttpThreads.description);

    parameter.  push_back(                  _controllerRequestTimeoutSec.key);
    value.      push_back(to_string(_config->controllerRequestTimeoutSec()));
    description.push_back(                  _controllerRequestTimeoutSec.description);

    parameter.  push_back(                  _jobTimeoutSec.key);
    value.      push_back(to_string(_config->jobTimeoutSec()));
    description.push_back(                  _jobTimeoutSec.description);

    parameter.  push_back(                  _jobHeartbeatTimeoutSec.key);
    value.      push_back(to_string(_config->jobHeartbeatTimeoutSec()));
    description.push_back(                  _jobHeartbeatTimeoutSec.description);

    parameter.  push_back(        _xrootdAutoNotify.key);
    value.      push_back(_config->xrootdAutoNotify() ? "yes" : "no");
    description.push_back(        _xrootdAutoNotify.description);

    parameter.  push_back(        _xrootdHost.key);
    value.      push_back(_config->xrootdHost());
    description.push_back(        _xrootdHost.description);

    parameter.  push_back(          _xrootdPort.key);
    value.      push_back(to_string(_config->xrootdPort()));
    description.push_back(          _xrootdPort.description);

    parameter.  push_back(                  _xrootdTimeoutSec.key);
    value.      push_back(to_string(_config->xrootdTimeoutSec()));
    description.push_back(                  _xrootdTimeoutSec.description);

    parameter.  push_back(        _databaseTechnology.key);
    value.      push_back(_config->databaseTechnology());
    description.push_back(        _databaseTechnology.description);

    parameter.  push_back(        _databaseHost.key);
    value.      push_back(_config->databaseHost());
    description.push_back(        _databaseHost.description);

    parameter.  push_back(                  _databasePort.key);
    value.      push_back(to_string(_config->databasePort()));
    description.push_back(                  _databasePort.description);

    parameter.  push_back(        _databaseUser.key);
    value.      push_back(_config->databaseUser());
    description.push_back(        _databaseUser.description);

    parameter.  push_back(                              _databasePassword.key);
    value.      push_back(_dumpDbShowPassword ? _config->databasePassword() : "xxxxxx");
    description.push_back(                              _databasePassword.description);

    parameter.  push_back(        _databaseName.key);
    value.      push_back(_config->databaseName());
    description.push_back(        _databaseName.description);

    parameter.  push_back(                  _databaseServicesPoolSize.key);
    value.      push_back(to_string(_config->databaseServicesPoolSize()));
    description.push_back(                  _databaseServicesPoolSize.description);

    parameter.  push_back(        _workerTechnology.key);
    value.      push_back(_config->workerTechnology());
    description.push_back(        _workerTechnology.description);

    parameter.  push_back(                  _workerNumProcessingThreads.key);
    value.      push_back(to_string(_config->workerNumProcessingThreads()));
    description.push_back(                  _workerNumProcessingThreads.description);

    parameter.  push_back(                  _fsNumProcessingThreads.key);
    value.      push_back(to_string(_config->fsNumProcessingThreads()));
    description.push_back(                  _fsNumProcessingThreads.description);

    parameter.  push_back(                  _workerFsBufferSizeBytes.key);
    value.      push_back(to_string(_config->workerFsBufferSizeBytes()));
    description.push_back(                  _workerFsBufferSizeBytes.description);

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
    vector<string> svcHostPort;
    vector<string> fsHostPort;
    vector<string> dataDir;

    for (auto&& worker: _config->allWorkers()) {
        auto const wi = _config->workerInfo(worker);
        name       .push_back(wi.name);
        isEnabled  .push_back(wi.isEnabled  ? "yes" : "no");
        isReadOnly .push_back(wi.isReadOnly ? "yes" : "no");
        svcHostPort.push_back(wi.svcHost + ":" + to_string(wi.svcPort));
        fsHostPort .push_back(wi.fsHost  + ":" + to_string(wi.fsPort));
        dataDir    .push_back(wi.dataDir);
    }

    util::ColumnTablePrinter table("WORKERS:", indent, _verticalSeparator);

    table.addColumn("name",                name,        util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("enabled",             isEnabled);
    table.addColumn("read-only",           isReadOnly);
    table.addColumn("replication service", svcHostPort, util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("file service",        fsHostPort,  util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("MySQL directory",     dataDir,     util::ColumnTablePrinter::Alignment::LEFT);

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
    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_updateGeneral() {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    try {
        _requestBufferSizeBytes     .save(_config);
        _retryTimeoutSec            .save(_config);
        _controllerThreads          .save(_config);
        _controllerHttpPort         .save(_config);
        _controllerHttpThreads      .save(_config);
        _controllerRequestTimeoutSec.save(_config);
        _jobTimeoutSec              .save(_config);
        _jobHeartbeatTimeoutSec     .save(_config);
        _xrootdAutoNotify           .save(_config);
        _xrootdHost                 .save(_config);
        _xrootdPort                 .save(_config);
        _xrootdTimeoutSec           .save(_config);
        _databaseServicesPoolSize   .save(_config);
        _workerTechnology           .save(_config);
        _workerNumProcessingThreads .save(_config);
        _fsNumProcessingThreads     .save(_config);
        _workerFsBufferSizeBytes    .save(_config);
    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}


int ConfigApp::_updateWorker() const {

    string const context = "ConfigApp::" + string(__func__) + "  ";

    if (_workerEnable and _workerDisable) {
        LOGS(_log, LOG_LVL_ERROR, context << "flags --worker-enable and --worker-disable"
             << " can't be used simultaneously");
        return 1;
    }
    if (_workerReadOnly and _workerReadWrite) {
        LOGS(_log, LOG_LVL_ERROR, context << "flags --worker-read-only and --worker-read-write"
             << " can't be used simultaneously");
        return 1;
    }
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
        if (_workerEnable and not info.isEnabled) {
            _config->disableWorker(_workerInfo.name, false);
        }
        if (_workerDisable and info.isEnabled) {
            _config->disableWorker(_workerInfo.name);
        }
        if (_workerReadOnly and not info.isReadOnly) {
            _config->setWorkerReadOnly(_workerInfo.name);
        }
        if (_workerReadWrite and info.isReadOnly) {
            _config->setWorkerReadOnly(_workerInfo.name, false);
        }
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
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
    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "operation failed, exception: " << ex.what());
        return 1;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
