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
#include "util/TablePrinter.h"


using namespace std;

namespace {

string const description {
    "This application is the tool for viewing and manipulating"
    " the configuration data of the Replication system stored in the MySQL/MariaDB"
};

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
        _configUrl("file:replication.cfg"),
        _log(LOG_GET("lsst.qserv.replica.ConfigApp")) {

    // Configure the command line parser

    parser().commands(
        "command",
        {"DUMP", "UPDATE_WORKER"},
        _command
    ).option(
        "config",
        "Configuration URL (a configuration file or a set of database connection parameters).",
        _configUrl
    );
    parser().command("DUMP").optional(
        "scope",
        "This optional parameter narrows a scope of the operation down to a specific"
        " context. If no scope is specified then everything will be dumped.",
        _dumpScope,
        vector<string>({"GENERAL", "WORKERS", "FAMILIES", "DATABASES"})
    ).flag(
        "db-show-password",
        "show the actual database password when making the dump of the GENERAL parameters",
        _dumpDbShowPassword
    );
    parser().command("UPDATE_WORKER").required(
        "worker",
        "The name of a worker to be updated",
        _workerInfo.name
    ).option(
        "worker-service-host",
        "The new DNS name or an IP address where the worker runs",
        _workerInfo.svcHost
    ).option(
        "worker-service-port",
        "The port number of the worker service",
        _workerInfo.svcPort
    ).option(
        "worker-fs-host",
        "The new DNS name or an IP address where the worker's File Server runs",
        _workerInfo.fsHost
    ).option(
        "worker-fs-port",
        "The port number of the worker's File Server",
        _workerInfo.fsPort
    ).option(
        "worker-data-dir",
        "The data directory of the worker",
        _workerInfo.dataDir
    ).flag(
        "worker-enable",
        "Enable the worker. ATTENTION: this flag can't be used together with flag --worker-disable",
        _workerEnable
    ).flag(
        "worker-disable",
        "Disable the worker. ATTENTION: this flag can't be used together with flag --worker-enable",
        _workerDisable
    ).flag(
        "worker-read-only",
        "Turn the worker into the read-only mode. ATTENTION: this flag can't be"
        " used together with flag --worker-read-write",
        _workerReadOnly
    ).flag(
        "worker-read-write",
        "Turn the worker into the read-write mode. ATTENTION: this flag can't be"
        " used together with flag --worker-read-only",
        _workerReadWrite
    );
}


int ConfigApp::runImpl() {

    char const* context = "ConfigApp::runImpl  ";

    _config = Configuration::load(_configUrl);
    if (_config->prefix() != "mysql") {
        LOGS(_log, LOG_LVL_ERROR, context << "configuration with prefix '" << _config->prefix()
             << "' is not allowed by this application");
        return 1;
    }
    if (_command == "DUMP")          return _dump();
    if (_command == "UPDATE_WORKER") return _updateWorker();

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

void ConfigApp::_dumpGeneralAsTable(string const indent) const {

    // Extract general attributes and put them into the corresponding
    // columns. Translate tables cell values into strings when required.

    vector<string> parameter;
    vector<string> value;
    vector<string> description;

    parameter.  push_back("NET_BUF_SIZE_BYTES");
    value.      push_back(to_string(_config->requestBufferSizeBytes()));
    description.push_back("default buffer size for network communications");

    parameter.  push_back("NET_RETRY_TIMEOUT_SEC");
    value.      push_back(to_string(_config->retryTimeoutSec()));
    description.push_back("default retry timeout for network communications");

    parameter.  push_back("CONTR_NUM_THREADS");
    value.      push_back(to_string(_config->controllerThreads()));
    description.push_back("number of threads managed by BOOST ASIO");

    parameter.  push_back("CONTR_HTTP_PORT");
    value.      push_back(to_string(_config->controllerHttpPort()));
    description.push_back("port number for the controller's HTTP server");

    parameter.  push_back("CONTR_NUM_HTTP_THREADS");
    value.      push_back(to_string(_config->controllerHttpThreads()));
    description.push_back("number of threads managed by BOOST ASIO for the HTTP server");

    parameter.  push_back("CONTR_REQUEST_TIMEOUT_SEC");
    value.      push_back(to_string(_config->controllerRequestTimeoutSec()));
    description.push_back("default timeout for completing worker requests");

    parameter.  push_back("CONTR_JOB_TIMEOUT_SEC");
    value.      push_back(to_string(_config->jobTimeoutSec()));
    description.push_back("default timeout for completing jobs");

    parameter.  push_back("CONTR_JOB_HEARTBEAT_SEC");
    value.      push_back(to_string(_config->jobHeartbeatTimeoutSec()));
    description.push_back("heartbeat interval for jobs");

    parameter.  push_back("QSERV_AUTO_NOTIFY");
    value.      push_back(_config->xrootdAutoNotify() ? "yes" : "no");
    description.push_back("automatically notify Qserv on changes in replica disposition");

    parameter.  push_back("XROOTD_HOST_PORT");
    value.      push_back(_config->xrootdHost() + ":" + to_string(_config->xrootdPort()));
    description.push_back("service location of XRootD/SSI for communications with Qserv");

    parameter.  push_back("XROOT_COMM_TIMEOUT_SEC");
    value.      push_back(to_string(_config->xrootdTimeoutSec()));
    description.push_back("default timeout for communications with Qserv over XRootD/SSI");

    parameter.  push_back("DB_TECHNOLOGY");
    value.      push_back(_config->databaseTechnology());
    description.push_back("name of a database technology for the persistent state");

    parameter.  push_back("DB_HOST_PORT");
    value.      push_back(_config->databaseHost() + ":" + to_string(_config->databasePort()));
    description.push_back("database service location");

    parameter.  push_back("DB_USER");
    value.      push_back(_config->databaseUser());
    description.push_back("user name for connecting to the database service");

    parameter.  push_back("DB_PASSWORD");
    value.      push_back(_dumpDbShowPassword ? _config->databasePassword() : "xxxxxx");
    description.push_back("password for connecting to the database service");

    parameter.  push_back("DB_NAME");
    value.      push_back(_config->databaseName());
    description.push_back("the name of the default database schema");

    parameter.  push_back("DB_SVC_POOL_SIZE");
    value.      push_back(to_string(_config->databaseServicesPoolSize()));
    description.push_back("the pool size at the client database services connector");

    parameter.  push_back("WORKER_TECHNOLOGY");
    value.      push_back(_config->workerTechnology());
    description.push_back("name of a technology for implementing requests");

    parameter.  push_back("WORKER_NUM_PROC_THREADS");
    value.      push_back(to_string(_config->workerNumProcessingThreads()));
    description.push_back("number of request processing threads in each worker service");

    parameter.  push_back("WORKER_FS_NUM_PROC_THREADS");
    value.      push_back(to_string(_config->fsNumProcessingThreads()));
    description.push_back("number of request processing threads in each worker's file server");

    parameter.  push_back("WORKER_FS_BUF_SIZE_BYTES");
    value.      push_back(to_string(_config->workerFsBufferSizeBytes()));
    description.push_back("buffer size for file and network operations at worker's file server");

    util::ColumnTablePrinter table("GENERAL PARAMETERS:", indent);

    table.addColumn("parameter",   parameter,   util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("value",       value);
    table.addColumn("description", description, util::ColumnTablePrinter::Alignment::LEFT);

    table.print(cout, false, false);
}


void ConfigApp::_dumpWorkersAsTable(string const indent) const {

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

    util::ColumnTablePrinter table("WORKERS:", indent);

    table.addColumn("name",                name,        util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("enabled",             isEnabled);
    table.addColumn("read-only",           isReadOnly);
    table.addColumn("replication service", svcHostPort, util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("file service",        fsHostPort,  util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("MySQL directory",     dataDir,     util::ColumnTablePrinter::Alignment::LEFT);

    table.print(cout, false, false);
}


void ConfigApp::_dumpFamiliesAsTable(string const indent) const {

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

    util::ColumnTablePrinter table("DATABASE FAMILIES:", indent);

    table.addColumn("name", name, util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("replication level", replicationLevel);
    table.addColumn("stripes", numStripes);
    table.addColumn("sub-stripes", numSubStripes);

    table.print(cout, false, false);
}


void ConfigApp::_dumpDatabasesAsTable(string const indent) const {

    // Extract attributes of each database and put them into the corresponding
    // columns.

    vector<string> familyName;
    vector<string> databaseName;
    vector<string> tableName;
    vector<string> isPartitionable;

    for (auto&& database: _config->databases()) {
        auto const di = _config->databaseInfo(database);
        for (auto& table: di.partitionedTables) {
            familyName     .push_back(di.family);
            databaseName   .push_back(di.name);
            tableName      .push_back(table);
            isPartitionable.push_back("yes");
        }
        for (auto& table: di.regularTables) {
            familyName     .push_back(di.family);
            databaseName   .push_back(di.name);
            tableName      .push_back(table);
            isPartitionable.push_back("no");
        }
    }

    util::ColumnTablePrinter table("DATABASES & TABLES:", indent);

    table.addColumn("family",        familyName,   util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("database",      databaseName, util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("table",         tableName,    util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("partitionable", isPartitionable);

    table.print(cout, false, false);
}


int ConfigApp::_updateWorker() const {

    char const* context = "ConfigApp::_updateWorker  ";

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


}}} // namespace lsst::qserv::replica
