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
            enableServiceProvider
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
    vector<string> categories;
    vector<string> parameters;
    vector<string> values;
    vector<string> descriptions;
    for (auto&& itr: ConfigurationSchema::parameters()) {
        string const& category = itr.first;
        for (auto&& param: itr.second) {
            categories.push_back(category);
            parameters.push_back(param);
            values.push_back(ConfigurationSchema::securityContext(category, param) ?
                    "xxxxxx" : _config->getAsString(category, param)
            );
            descriptions.push_back(ConfigurationSchema::description(category, param));
        }
    }
    util::ColumnTablePrinter table("GENERAL PARAMETERS:", indent, verticalSeparator());
    table.addColumn("category", categories, util::ColumnTablePrinter::LEFT);
    table.addColumn("param", parameters, util::ColumnTablePrinter::LEFT);
    table.addColumn("value", values);
    table.addColumn("description", descriptions, util::ColumnTablePrinter::LEFT);
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
        loaderHostPort.push_back(wi.loaderHost + ":" + to_string(wi.loaderPort));
        loaderTmpDir.push_back(wi.loaderTmpDir);
        exporterHostPort.push_back(wi.exporterHost + ":" + to_string(wi.exporterPort));
        exporterTmpDir.push_back(wi.exporterTmpDir);
        httpLoaderHostPort.push_back(wi.httpLoaderHost + ":" + to_string(wi.httpLoaderPort));
        httpLoaderTmpDir.push_back(wi.httpLoaderTmpDir);
    }

    util::ColumnTablePrinter table(caption, indent, verticalSeparator());

    table.addColumn("name", name, util::ColumnTablePrinter::LEFT);
    table.addColumn("enabled", isEnabled);
    table.addColumn("read-only", isReadOnly);
    table.addColumn("Qserv data directory", dataDir, util::ColumnTablePrinter::LEFT);
    table.addColumn("Repl. svc", svcHostPort, util::ColumnTablePrinter::LEFT);
    table.addColumn("File svc", fsHostPort, util::ColumnTablePrinter::LEFT);
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

    util::ColumnTablePrinter table(caption, indent, verticalSeparator());

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
    vector<string> directorTable;
    vector<string> directorKey;
    vector<string> latitudeColName;
    vector<string> longitudeColName;
    vector<string> numColumns;

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
            if (di.isDirector(table)) {
                isDirector.push_back("yes");
                directorTable.push_back("");
            } else {
                isDirector.push_back("no");
                directorTable.push_back(di.directorTable.at(table));
            }
            directorKey.push_back(di.directorTableKey.at(table));
            latitudeColName.push_back(di.latitudeColName.at(table));
            longitudeColName.push_back(di.longitudeColName.at(table));
            numColumns.push_back(to_string(di.columns.at(table).size()));
       }
        for (auto& table: di.regularTables) {
            familyName.push_back(di.family);
            databaseName.push_back(di.name);
            isPublished.push_back(di.isPublished ? "yes" : "no");
            tableName.push_back(table);
            isPartitioned.push_back("no");
            isDirector.push_back("no");
            directorTable.push_back("");
            directorKey.push_back("");
            latitudeColName.push_back("");
            longitudeColName.push_back("");
            numColumns.push_back(to_string(di.columns.at(table).size()));
        }
        if (di.partitionedTables.empty() and di.regularTables.empty()) {
            familyName.push_back(di.family);
            databaseName.push_back(di.name);
            isPublished.push_back(di.isPublished ? "yes" : "no");
            tableName.push_back("<no tables>");
            isPartitioned.push_back("n/a");
            isDirector.push_back("n/a");
            directorTable.push_back("n/a");
            directorKey.push_back("n/a");
            latitudeColName.push_back("n/a");
            longitudeColName.push_back("n/a");
            numColumns.push_back("n/a");
        }
    }

    util::ColumnTablePrinter table(caption, indent, verticalSeparator());

    table.addColumn("family", familyName, util::ColumnTablePrinter::LEFT);
    table.addColumn("database", databaseName, util::ColumnTablePrinter::LEFT);
    table.addColumn(":published", isPublished);
    table.addColumn("table", tableName, util::ColumnTablePrinter::LEFT);
    table.addColumn(":partitioned", isPartitioned);
    table.addColumn(":director", isDirector);
    table.addColumn(":director-table", directorTable);
    table.addColumn(":director-key", directorKey);
    table.addColumn(":latitude-key", latitudeColName);
    table.addColumn(":longitude-key", longitudeColName);
    table.addColumn(":num-columns", numColumns);

    table.print(cout, false, false);
    cout << endl;
}

}}} // namespace lsst::qserv::replica
