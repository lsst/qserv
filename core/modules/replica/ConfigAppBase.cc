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
#include "replica/ConfigAppBase.h"

// System headers
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "util/TablePrinter.h"

using namespace std;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;
bool const injectXrootdOptions = false;

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ConfigAppBase::ConfigAppBase(int argc, char* argv[], string const& description)
    :   Application(
            argc, argv,
            description,
            injectDatabaseOptions,
            boostProtobufVersionCheck,
            enableServiceProvider,
            injectXrootdOptions
        ),
        _configUrl("mysql://qsreplica@localhost:3306/qservReplica") {

    parser().option(
        "config",
        "Configuration URL (a database connection string).",
        _configUrl
    ).flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in dumps.",
        _verticalSeparator
    );
}


int ConfigAppBase::runImpl() {
    _config = Configuration::load(_configUrl);
    return runSubclassImpl();
}


void ConfigAppBase::dumpGeneralAsTable(string const& indent) const {

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


void ConfigAppBase::dumpWorkersAsTable(string const& indent, string const& caption) const {

    // Extract attributes of each worker and put them into the corresponding
    // columns. Translate tables cell values into strings when required.

    vector<string> name;
    vector<string> isEnabled;
    vector<string> isReadOnly;
    vector<string> dataDir;
    vector<string> svcHostPort;
    vector<string> fsHostPort;
    vector<string> dbHostPort;
    vector<string> dbUser;
    vector<string> loaderHostPort;
    vector<string> loaderTmpDir;
    vector<string> exporterHostPort;
    vector<string> exporterTmpDir;
    vector<string> httpLoaderHostPort;
    vector<string> httpLoaderTmpDir;

    for (auto&& worker: _config->allWorkers()) {
        auto const wi = _config->workerInfo(worker);
        name.push_back(wi.name);
        isEnabled.push_back(wi.isEnabled  ? "yes" : "no");
        isReadOnly.push_back(wi.isReadOnly ? "yes" : "no");
        dataDir.push_back(wi.dataDir);
        svcHostPort.push_back(wi.svcHost + ":" + to_string(wi.svcPort));
        fsHostPort.push_back(wi.fsHost + ":" + to_string(wi.fsPort));
        dbHostPort.push_back(wi.dbHost + ":" + to_string(wi.dbPort));
        dbUser.push_back(wi.dbUser);
        loaderHostPort.push_back(wi.loaderHost + ":" + to_string(wi.loaderPort));
        loaderTmpDir.push_back(wi.loaderTmpDir);
        exporterHostPort.push_back(wi.exporterHost + ":" + to_string(wi.exporterPort));
        exporterTmpDir.push_back(wi.exporterTmpDir);
        httpLoaderHostPort.push_back(wi.httpLoaderHost + ":" + to_string(wi.httpLoaderPort));
        httpLoaderTmpDir.push_back(wi.httpLoaderTmpDir);
    }

    util::ColumnTablePrinter table(caption, indent, _verticalSeparator);

    table.addColumn("name", name, util::ColumnTablePrinter::LEFT);
    table.addColumn("enabled", isEnabled);
    table.addColumn("read-only", isReadOnly);
    table.addColumn("Qserv data directory", dataDir, util::ColumnTablePrinter::LEFT);
    table.addColumn("Repl. svc", svcHostPort, util::ColumnTablePrinter::LEFT);
    table.addColumn("File svc", fsHostPort, util::ColumnTablePrinter::LEFT);
    table.addColumn("Qserv db", dbHostPort, util::ColumnTablePrinter::LEFT);
    table.addColumn(":user", dbUser, util::ColumnTablePrinter::LEFT);
    table.addColumn("Binary ingest", loaderHostPort, util::ColumnTablePrinter::LEFT);
    table.addColumn(":tmp", loaderTmpDir, util::ColumnTablePrinter::LEFT);
    table.addColumn("Export svc", exporterHostPort, util::ColumnTablePrinter::LEFT);
    table.addColumn(":tmp", exporterTmpDir, util::ColumnTablePrinter::LEFT);
    table.addColumn("HTTP ingest", httpLoaderHostPort, util::ColumnTablePrinter::LEFT);
    table.addColumn(":tmp", httpLoaderTmpDir, util::ColumnTablePrinter::LEFT);

    table.print(cout, false, false);
    cout << endl;
}


void ConfigAppBase::dumpFamiliesAsTable(string const& indent, string const& caption) const {

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

    util::ColumnTablePrinter table(caption, indent, _verticalSeparator);

    table.addColumn("name", name, util::ColumnTablePrinter::LEFT);
    table.addColumn("replication level", replicationLevel);
    table.addColumn("stripes", numStripes);
    table.addColumn("sub-stripes", numSubStripes);

    table.print(cout, false, false);
    cout << endl;
}


void ConfigAppBase::dumpDatabasesAsTable(string const& indent, string const& caption) const {

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

    util::ColumnTablePrinter table(caption, indent, _verticalSeparator);

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
    cout << endl;
}

}}} // namespace lsst::qserv::replica
