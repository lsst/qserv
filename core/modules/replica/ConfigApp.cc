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
#include "replica/ConfigurationSchema.h"
#include "util/TablePrinter.h"

using namespace std;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

string const description =
    "This application is the tool for viewing and manipulating"
    " the configuration data of the Replication system stored in the MySQL/MariaDB.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;
bool const injectXrootdOptions = false;

/**
 * Register an option with a parser (which could also represent a command).
 * @param parser The handler responsible for processing options
 * @param param Parameter handler.`
 */
template <class PARSER, typename T>
void addCommandOption(PARSER& parser, T& param) {
    parser.option(param.key, param.description(), param.value);
}

// Color enhanced strings for better reporting.
string const COLOR_BEGIN_GREEN = "\001\033[0;32m\002";
string const COLOR_BEGIN_RED   = "\001\033[0;31m\002";
string const COLOR_RESET       = "\001\033[0m\002";
string const PASSED_STR = "[" + COLOR_BEGIN_GREEN + "PASSED" + COLOR_RESET + "]";
string const FAILED_STR = "[" + COLOR_BEGIN_RED   + "FAILED" + COLOR_RESET + "]";
string const OK_STR  = COLOR_BEGIN_GREEN + "OK" + COLOR_RESET;
string const VALUE_MISMATCH_STR = COLOR_BEGIN_RED + "VALUE MISMATCH" + COLOR_RESET;
string const TYPE_MISMATCH_STR = COLOR_BEGIN_RED + "TYPE MISMATCH" + COLOR_RESET;
string const MISSING_STR = COLOR_BEGIN_RED + "MISSING" + COLOR_RESET;
string const NOT_TESTED_STR = COLOR_BEGIN_RED + "NOT TESTED" + COLOR_RESET;

/**
 * The class TestGeneral facilitates testing and reporting values of
 * the general defaults.
 */
class TestGeneral {
public:
    TestGeneral() = delete;
    TestGeneral(TestGeneral const&) = delete;
    TestGeneral& operator=(TestGeneral const&) = delete;

    TestGeneral(Configuration::Ptr const& config,
                string const& capture,
                string const& indent,
                bool verticalSeparator)
        :   _config(config),
            _capture(capture),
            _indent(indent),
            _verticalSeparator(verticalSeparator) {
    }

    template <typename T>
    void verify(string const& category, string const& parameter, T const& expectedValue) {
        _testedParameters[category].insert(parameter);
        _category.push_back(category);
        _parameter.push_back(parameter);
        try {
            T const actualValue = _config->get<T>(category, parameter);
            bool const equal = actualValue == expectedValue;
            _result.push_back(equal ? OK_STR : VALUE_MISMATCH_STR);
            _actual.push_back(detail::ConfigParamHandlerTrait<T>::to_string(actualValue));
            if (!equal) ++_failed;
        } catch(invalid_argument const& ex) {
            _result.push_back(MISSING_STR);
            _actual.push_back("");
            ++_failed;
        } catch(ConfigTypeMismatch const& ex) {
            _result.push_back(TYPE_MISMATCH_STR);
            _actual.push_back("");
            ++_failed;
        } catch(exception const& ex) {
            _result.push_back(ex.what());
            _actual.push_back("");
            ++_failed;
        }
        _expected.push_back(detail::ConfigParamHandlerTrait<T>::to_string(expectedValue));
    }

    /// @return 'true' if the test was successful.
    bool reportResults() {
        // Locate and report parameters that as not been tested.
        map<string, set<string>> const knownParameters = _config->parameters();
        if (knownParameters != _testedParameters) {
            for (auto&& categoryItr: knownParameters) {
                string const& category = categoryItr.first;
                for (string const& parameter: categoryItr.second) {
                    if ((_testedParameters.count(category) == 0) ||
                        (_testedParameters.at(category).count(parameter) == 0)) {
                        _result.push_back(NOT_TESTED_STR);
                        _category.push_back(category);
                        _parameter.push_back(parameter);
                        _actual.push_back("");
                        _expected.push_back("");
                        ++_failed;
                    }
                }
            }
        }
        string const capture = (_failed == 0 ? PASSED_STR : FAILED_STR) + " " + _capture;
        util::ColumnTablePrinter table(capture, _indent, _verticalSeparator);
        table.addColumn("result", _result);
        table.addColumn("category", _category, util::ColumnTablePrinter::LEFT);
        table.addColumn("parameter", _parameter, util::ColumnTablePrinter::LEFT);
        table.addColumn("actual", _actual);
        table.addColumn("expected", _expected);
        table.print(cout, false, false);
        return _failed == 0;
    }

private:
    // Input parameters
    Configuration::Ptr const _config;
    string const _capture;
    string const _indent;
    bool const _verticalSeparator;

    // Parameters that have been tested so far
    map<string, set<string>> _testedParameters;

    // The number of failed tests.
    int _failed = 0;

    // Values accumulated along table columns.
    vector<string> _result;
    vector<string> _category;
    vector<string> _parameter;
    vector<string> _actual;
    vector<string> _expected;
};

/**
 * The class ComparatorBase represents the base class for specific comparators
 * for workers, database families or databases. The class encapsulates common
 * aspects of the final comparatos.
 */
class ComparatorBase {
public:
    ComparatorBase() = delete;
    ComparatorBase(ComparatorBase const&) = delete;
    ComparatorBase& operator=(ComparatorBase const&) = delete;

    /// @return 'true' if the test was successful.
    bool reportResults() {
        string const capture = (_failed == 0 ? PASSED_STR : FAILED_STR) + " " + _capture;
        util::ColumnTablePrinter table(capture, _indent, _verticalSeparator);
        table.addColumn("result", _result);
        table.addColumn("attribute", _attribute, util::ColumnTablePrinter::LEFT);
        table.addColumn("actual", _actual);
        table.addColumn("expected", _expected);
        table.print(cout, false, false);
        return _failed == 0;
    }

protected:
    ComparatorBase(string const& capture, string const& indent, bool verticalSeparator)
        :   _capture(capture), _indent(indent), _verticalSeparator(verticalSeparator) {
    }

    template <typename T>
    void verifyImpl(string const& attribute,
                T const& actualValue,
                T const& expectedValue) {
        bool const equal = actualValue == expectedValue;
        _result.push_back(equal ? OK_STR : VALUE_MISMATCH_STR);
        _attribute.push_back(attribute);
        _actual.push_back(detail::ConfigParamHandlerTrait<T>::to_string(actualValue));
        _expected.push_back(detail::ConfigParamHandlerTrait<T>::to_string(expectedValue));
        if (!equal) ++_failed;
    }

private:
    // Input parameters
    string const _capture;
    string const _indent;
    bool const _verticalSeparator;

    // The number of failed tests.
    int _failed = 0;

    // Values accumulated along table columns.
    vector<string> _result;
    vector<string> _attribute;
    vector<string> _actual;
    vector<string> _expected;
};

/**
 * The class CompareWorkerAtributes compares values of the coresponding attrubutes
 * of two workers and reports differences.
 */
class CompareWorkerAtributes: public ComparatorBase {
public:
    CompareWorkerAtributes() = delete;
    CompareWorkerAtributes(CompareWorkerAtributes const&) = delete;
    CompareWorkerAtributes& operator=(CompareWorkerAtributes const&) = delete;

    CompareWorkerAtributes(string const& capture, string const& indent, bool verticalSeparator,
                           Configuration::Ptr const& config)
        :   ComparatorBase(capture, indent, verticalSeparator),
            _config(config) {
    }

    /**
     * Compare values of the the corresponding attributes of two workers.
     * @param actual The actual worker descriptor obtained from the database after adding or
     *   updating the worker.
     * @param desired The input worker descriptor that was used in worker specification.
     * @param compareWithDefault The optional flag that if 'true' will modify the behavior
     *   of the test by pulling expected values of the default attributes either from
     *   the database defaults, or (for host names) from the host name configured
     *   for the main replication service SVC.
     */
    void verify(WorkerInfo const& actual, WorkerInfo const& desired, bool compareWithDefault=false) {
        _verify("name",         actual.name, desired.name);
        _verify("is_enabled",   actual.isEnabled,  desired.isEnabled);
        _verify("is_read_only", actual.isReadOnly, desired.isReadOnly);
        _verify("svc_host", actual.svcHost, desired.svcHost);
        _verify("svc_port", actual.svcPort, desired.svcPort, compareWithDefault);
        _verify("fs_host",  actual.fsHost,  compareWithDefault ? desired.svcHost : desired.fsHost);
        _verify("fs_port",  actual.fsPort,  desired.fsPort,  compareWithDefault);
        _verify("data_dir", actual.dataDir, desired.dataDir, compareWithDefault);
        _verify("db_host", actual.dbHost, compareWithDefault ? desired.svcHost : desired.dbHost);
        _verify("db_port", actual.dbPort, desired.dbPort, compareWithDefault);
        _verify("db_user", actual.dbUser, desired.dbUser, compareWithDefault);
        _verify("loader_host",    actual.loaderHost,   compareWithDefault ? desired.svcHost : desired.loaderHost);
        _verify("loader_port",    actual.loaderPort,   desired.loaderPort,   compareWithDefault);
        _verify("loader_tmp_dir", actual.loaderTmpDir, desired.loaderTmpDir, compareWithDefault);
        _verify("exporter_host",    actual.exporterHost,   compareWithDefault ? desired.svcHost : desired.exporterHost);
        _verify("exporter_port",    actual.exporterPort,   desired.exporterPort,   compareWithDefault);
        _verify("exporter_tmp_dir", actual.exporterTmpDir, desired.exporterTmpDir, compareWithDefault);
        _verify("http_loader_host",    actual.httpLoaderHost,   compareWithDefault ? desired.svcHost  : desired.httpLoaderHost);
        _verify("http_loader_port",    actual.httpLoaderPort,   desired.httpLoaderPort,   compareWithDefault);
        _verify("http_loader_tmp_dir", actual.httpLoaderTmpDir, desired.httpLoaderTmpDir, compareWithDefault);
    }

    template <typename T>
    void _verify(string const& attribute,
                 T const& actualValue,
                 T const& expectedValue,
                 bool compareWithDefault=false) {
        verifyImpl<T>(attribute, actualValue,
                      compareWithDefault ? _config->get<T>("worker_defaults", attribute) : expectedValue);
    }

private:
    // Input parameters
    Configuration::Ptr const _config;
};

/**
 * The class CompareWorkerAtributes compares values of the coresponding
 * attrubutes of two database families and reports differences.
 */
class CompareFamilyAtributes: public ComparatorBase {
public:
    CompareFamilyAtributes() = delete;
    CompareFamilyAtributes(CompareFamilyAtributes const&) = delete;
    CompareFamilyAtributes& operator=(CompareFamilyAtributes const&) = delete;

    CompareFamilyAtributes(string const& capture, string const& indent, bool verticalSeparator)
        :  ComparatorBase(capture, indent, verticalSeparator) {}

    /**
     * Compare values of the the corresponding attributes of two families.
     * @param actual The actual family descriptor obtained from the database
     *   after adding or updating the family.
     * @param desired The input family descriptor that was used in family
     *   specification.
     */
    void verify(DatabaseFamilyInfo const& actual, DatabaseFamilyInfo const& desired) {
        verifyImpl("name", actual.name, desired.name);
        verifyImpl("min_replication_level", actual.replicationLevel, desired.replicationLevel);
        verifyImpl("num_stripes", actual.numStripes, desired.numStripes);
        verifyImpl("num_sub_stripes", actual.numSubStripes, desired.numSubStripes);
        verifyImpl("overlap", actual.overlap, desired.overlap);
    }
};

/**
 * The class CompareDatabaseAtributes compares values of the coresponding
 * attrubutes of two databases and reports differences.
 */
class CompareDatabaseAtributes: public ComparatorBase {
public:
    CompareDatabaseAtributes() = delete;
    CompareDatabaseAtributes(CompareDatabaseAtributes const&) = delete;
    CompareDatabaseAtributes& operator=(ComparatorBase const&) = delete;

    CompareDatabaseAtributes(string const& capture, string const& indent, bool verticalSeparator)
        :  ComparatorBase(capture, indent, verticalSeparator) {}

    /**
     * Compare values of the the corresponding attributes of two databases.
     * @param actual The actual family descriptor obtained from the database
     *   after adding or updating the database.
     * @param desired The input database descriptor that was used in database
     *   specification.
     */
    void verify(DatabaseInfo const& actual, DatabaseInfo const& desired) {
        verifyImpl("name", actual.name, desired.name);
        verifyImpl("family_name", actual.family, desired.family);
        verifyImpl("is_published", actual.isPublished, desired.isPublished);
        verifyImpl("partitioned_tables.empty()", actual.partitionedTables.empty(), desired.partitionedTables.empty());
        verifyImpl("regular_tables.empty()", actual.regularTables.empty(), desired.regularTables.empty());
        verifyImpl("columns.empty()", actual.columns.empty(), desired.columns.empty());
        verifyImpl("director_table", actual.directorTable, desired.directorTable);
        verifyImpl("director_key", actual.directorTableKey, desired.directorTableKey);
        verifyImpl("chunk_id_key", actual.chunkIdColName, desired.chunkIdColName);
        verifyImpl("sub_chunk_id_key", actual.subChunkIdColName, desired.subChunkIdColName);
    }
};

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
        {"MYSQL_CREATE", "MYSQL_SCHEMA_UPGRADE", "MYSQL_SCHEMA_DUMP", "MYSQL_TEST",
         "SCHEMA_VERSION",
         "DUMP",
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
        "MYSQL_CREATE"
    ).description(
        "Create the configuration database in MySQL at a location specified in the configuration"
        " URL parameter '--config=<url>'. Populate the database with the current schema. Preload"
        " default values of a minimal set of parameters required by the Replication/Ingest system"
        " to begin operating. NOTE: If the database already exist one would have to use the optional"
        " flag '--reset' to destroy the specified database before re-creating it from scratch. Plan"
        " carefully when using this flag to avoid destroying any valuable data."
    ).flag(
        "reset",
        "This flag allow resetting the content of the configuration database to the default"
        " state, before initializing it with the current schema and defaults. ATTENTION: use"
        " this flag with caution as it may result in destroying valuable information.",
        _reset
    );

    parser().command(
        "MYSQL_SCHEMA_UPGRADE"
    ).description(
        "Upgrade schema of an existing configuration database to the current version if needed."
        " The MySQL database location is specified by the configuration URL parameter '--config=<url>'."
    );

    parser().command(
        "MYSQL_SCHEMA_DUMP"
    ).description(
        "Print the current MySQL schema of the configuration database."
    ).flag(
        "exclude-default-parameters",
        "The flag that prevents printing additional 'INSERT INTO ...' statements for initializing"
        " the default parameters required by the Replication/Ingest system to begin operating.",
        _excludeDefaultParameters
    );

    parser().command(
        "MYSQL_TEST"
    ).description(
        "The complete integration test for the Configuration service. The test will create"
        " (or re-create) the Configuration database in MySQL at a location (and credential)"
        " specified via configuration URL parameter '--config=<url>'. NOTE: If the database"
        " already exist one would have to use the optional flag '--reset' to destroy"
        " the database before re-creating it from scratch. Plan carefully when using this flag"
        " to avoid destroying any valuable data. Avoid running this command in the production"
        " environment."
    ).optional(
        "scope",
        "This optional parameter narrows a scope of the operation down to a specific"
        " context. If no scope is specified then everything will be tested.",
        _testScope,
        vector<string>({"GENERAL", "WORKERS", "DATABASES_AND_FAMILIES"})
    ).flag(
        "reset",
        "This flag allow resetting the content of the configuration database to the default"
        " state, before initializing it with the current schema and defaults. ATTENTION: use"
        " this flag with caution as it may result in destroying valuable information.",
        _reset
    );

    parser().command(
        "SCHEMA_VERSION"
    ).description(
        "Print the schema version expected by the application."
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

    if (_command == "SCHEMA_VERSION") {
        cout << ConfigurationSchema::version << endl;
        return 0;
    }

    // Do not attept loading the configuration since the database may not exist
    // at this time, or it may require special handling.
    if (_command == "MYSQL_CREATE") return _create();

    // This operation opens the Configuration in a special mode to allow automated
    // upgrade if needed.
    if (_command == "MYSQL_SCHEMA_UPGRADE") return _upgrade();

    // This command doesn't require any configuration URL.
    if (_command == "MYSQL_SCHEMA_DUMP") return _schema();

    // This comand will create (or re-create) a database at the specified URL.
    if (_command == "MYSQL_TEST") return _test();

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


int ConfigApp::_create() const {
    auto const config = ConfigurationSchema::create(_configUrl, _reset);
    return 0;
}


int ConfigApp::_upgrade() const {
    bool const autoMigrateSchema = true;
    auto const config = Configuration::load(_configUrl, autoMigrateSchema);
    return 0;
}


int ConfigApp::_schema() const {
    bool const includeInitStatements = !_excludeDefaultParameters;
    for (auto&& sql: ConfigurationSchema::schema(includeInitStatements)) {
        cout << sql << ";" << endl;
    }
    return 0;
}


int ConfigApp::_test() {
    _config = ConfigurationSchema::create(_configUrl, _reset);
    bool success = true;
    if (_testScope.empty() or _testScope == "GENERAL") {
        success = success && _testGeneral();
    }
    if (_testScope.empty() or _testScope == "WORKERS") {
        success = success && _testWorkers();
    }
    if (_testScope.empty() or _testScope == "DATABASES_AND_FAMILIES") {
        success = success && _testDatabasesAndFamilies();
    }
    return success ? 0 : 1;
}


bool ConfigApp::_testGeneral() {

    string const indent = "";
    bool success = true;

    // Testing reading the default values using the generic API. results will be reported
    // asa table onto the standard output.  Note that the last argument in each
    // call represents an expected value of the parameter's value.
    {
        TestGeneral test(_config, "READING DEAFULT STATE OF THE GENERAL PARAMETERS:", indent, _verticalSeparator);
        test.verify<int>(           "meta", "version", ConfigurationSchema::version);
        test.verify<size_t>(        "common", "request_buf_size_bytes", 131072);
        test.verify<unsigned int>(  "common", "request_retry_interval_sec", 1);
        test.verify<size_t>(        "controller", "num_threads", 2);
        test.verify<size_t>(        "controller", "http_server_threads", 2);
        test.verify<uint16_t>(      "controller", "http_server_port", 25081);
        test.verify<unsigned int>(  "controller", "request_timeout_sec", 600);
        test.verify<unsigned int>(  "controller", "job_timeout_sec", 600);
        test.verify<unsigned int>(  "controller", "job_heartbeat_sec", 0);
        test.verify<std::string>(   "controller", "empty_chunks_dir", "/qserv/data/qserv");
        test.verify<size_t>(        "database", "services_pool_size", 2);
        test.verify<std::string>(   "database", "host", "localhost");
        test.verify<uint16_t>(      "database", "port", 23306);
        test.verify<std::string>(   "database", "user", "root");
        test.verify<std::string>(   "database", "password", "CHANGEME");
        test.verify<std::string>(   "database", "name", "qservReplica");
        test.verify<std::string>(   "database", "qserv_master_host", "localhost");
        test.verify<uint16_t>(      "database", "qserv_master_port", 3306);
        test.verify<std::string>(   "database", "qserv_master_user", "qsmaster");
        test.verify<std::string>(   "database", "qserv_master_name", "qservMeta");
        test.verify<size_t>(        "database", "qserv_master_services_pool_size", 2);
        test.verify<std::string>(   "database", "qserv_master_tmp_dir", "/qserv/data/ingest");
        test.verify<unsigned int>(  "xrootd", "auto_notify", 1);
        test.verify<unsigned int>(  "xrootd", "request_timeout_sec", 180);
        test.verify<std::string>(   "xrootd", "host", "localhost");
        test.verify<uint16_t>(      "xrootd", "port", 1094);
        test.verify<std::string>(   "worker", "technology", "FS");
        test.verify<size_t>(        "worker", "num_svc_processing_threads", 2);
        test.verify<size_t>(        "worker", "num_fs_processing_threads", 2);
        test.verify<size_t>(        "worker", "fs_buf_size_bytes", 4194304);
        test.verify<size_t>(        "worker", "num_loader_processing_threads", 2);
        test.verify<size_t>(        "worker", "num_exporter_processing_threads", 2);
        test.verify<size_t>(        "worker", "num_http_loader_processing_threads", 2);
        test.verify<uint16_t>(      "worker_defaults", "svc_port", 25000);
        test.verify<uint16_t>(      "worker_defaults", "fs_port", 25001);
        test.verify<std::string>(   "worker_defaults", "data_dir", "/qserv/data/mysql");
        test.verify<uint16_t>(      "worker_defaults", "db_port", 3306);
        test.verify<std::string>(   "worker_defaults", "db_user", "root");
        test.verify<uint16_t>(      "worker_defaults", "loader_port", 25002);
        test.verify<std::string>(   "worker_defaults", "loader_tmp_dir", "/qserv/data/ingest");
        test.verify<uint16_t>(      "worker_defaults", "exporter_port", 25003);
        test.verify<std::string>(   "worker_defaults", "exporter_tmp_dir", "/qserv/data/export");
        test.verify<uint16_t>(      "worker_defaults", "http_loader_port", 25004);
        test.verify<std::string>(   "worker_defaults", "http_loader_tmp_dir", "/qserv/data/ingest");
        success = success && test.reportResults();
        cout << "\n";
    }

    // Test updating the defaults and reading them back after reopening the database
    // to ensure the values are read from the database rather than from the transinet state.
    //
    // Note that database parameters that are computed from the configUrl can't be updated
    // in this way:
    //   "database.host"
    //   "database.port"
    //   "database.user"
    //   "database.password"
    //   "database.name"
    // These parameters be be skipped in the update sequence.
    {
        _config->set<size_t>(        "common", "request_buf_size_bytes", 131072 + 1);
        _config->set<unsigned int>(  "common", "request_retry_interval_sec", 1 + 1);
        _config->set<size_t>(        "controller", "num_threads", 2 + 1);
        _config->set<size_t>(        "controller", "http_server_threads", 2 + 1);
        _config->set<uint16_t>(      "controller", "http_server_port", 25081 + 1);
        _config->set<unsigned int>(  "controller", "request_timeout_sec", 600 + 1);
        _config->set<unsigned int>(  "controller", "job_timeout_sec", 600 + 1);
        _config->set<unsigned int>(  "controller", "job_heartbeat_sec", 0 + 1);
        _config->set<std::string>(   "controller", "empty_chunks_dir", "/qserv/data/qserv-1");
        _config->set<size_t>(        "database", "services_pool_size", 2 + 1);
        // These 5 parameters are deduced from 'configUrl'. Changing them here won't make
        // a sense since they're not stored within MySQL.
        if (false) {
            _config->set<std::string>("database", "host", "localhost");
            _config->set<uint16_t>(   "database", "port", 23306);
            _config->set<std::string>("database", "user", "root");
            _config->set<std::string>("database", "password", "CHANGEME");
            _config->set<std::string>("database", "name", "qservReplica");
        }
        _config->set<std::string>(   "database", "qserv_master_host", "localhost-1");
        _config->set<uint16_t>(      "database", "qserv_master_port", 3306 + 1);
        _config->set<std::string>(   "database", "qserv_master_user", "qsmaster-1");
        _config->set<std::string>(   "database", "qserv_master_name", "qservMeta-1");
        _config->set<size_t>(        "database", "qserv_master_services_pool_size", 2 + 1);
        _config->set<std::string>(   "database", "qserv_master_tmp_dir", "/qserv/data/ingest-1");
        _config->set<unsigned int>(  "xrootd", "auto_notify", 0);
        _config->set<unsigned int>(  "xrootd", "request_timeout_sec", 180 + 1);
        _config->set<std::string>(   "xrootd", "host", "localhost-1");
        _config->set<uint16_t>(      "xrootd", "port", 1094 + 1);
        _config->set<std::string>(   "worker", "technology", "FS-1");
        _config->set<size_t>(        "worker", "num_svc_processing_threads", 2 + 1);
        _config->set<size_t>(        "worker", "num_fs_processing_threads", 2 + 1);
        _config->set<size_t>(        "worker", "fs_buf_size_bytes", 4194304 + 1);
        _config->set<size_t>(        "worker", "num_loader_processing_threads", 2 + 1);
        _config->set<size_t>(        "worker", "num_exporter_processing_threads", 2 + 1);
        _config->set<size_t>(        "worker", "num_http_loader_processing_threads", 2 + 1);
        _config->set<uint16_t>(      "worker_defaults", "svc_port", 25000 + 1);
        _config->set<uint16_t>(      "worker_defaults", "fs_port", 25001 + 1);
        _config->set<std::string>(   "worker_defaults", "data_dir", "/qserv/data/mysql-1");
        _config->set<uint16_t>(      "worker_defaults", "db_port", 3306 + 1);
        _config->set<std::string>(   "worker_defaults", "db_user", "root-1");
        _config->set<uint16_t>(      "worker_defaults", "loader_port", 25002 + 1);
        _config->set<std::string>(   "worker_defaults", "loader_tmp_dir", "/qserv/data/ingest-1");
        _config->set<uint16_t>(      "worker_defaults", "exporter_port", 25003 + 1);
        _config->set<std::string>(   "worker_defaults", "exporter_tmp_dir", "/qserv/data/export-1");
        _config->set<uint16_t>(      "worker_defaults", "http_loader_port", 25004 + 1);
        _config->set<std::string>(   "worker_defaults", "http_loader_tmp_dir", "/qserv/data/ingest-1");

        _config->reload();
        TestGeneral test(_config, "READING UPDATED GENERAL PARAMETERS:", indent, _verticalSeparator);
        test.verify<int>(           "meta", "version", ConfigurationSchema::version);
        test.verify<size_t>(        "common", "request_buf_size_bytes", 131072 + 1);
        test.verify<unsigned int>(  "common", "request_retry_interval_sec", 1 + 1);
        test.verify<size_t>(        "controller", "num_threads", 2 + 1);
        test.verify<size_t>(        "controller", "http_server_threads", 2 + 1);
        test.verify<uint16_t>(      "controller", "http_server_port", 25081 + 1);
        test.verify<unsigned int>(  "controller", "request_timeout_sec", 600 + 1);
        test.verify<unsigned int>(  "controller", "job_timeout_sec", 600 + 1);
        test.verify<unsigned int>(  "controller", "job_heartbeat_sec", 0 + 1);
        test.verify<std::string>(   "controller", "empty_chunks_dir", "/qserv/data/qserv-1");
        test.verify<size_t>(        "database", "services_pool_size", 2 + 1);

        // These 5 parameters deduced from 'configUrl' shouldn't be changed.
        test.verify<std::string>(   "database", "host", "localhost");
        test.verify<uint16_t>(      "database", "port", 23306);
        test.verify<std::string>(   "database", "user", "root");
        test.verify<std::string>(   "database", "password", "CHANGEME");
        test.verify<std::string>(   "database", "name", "qservReplica");

        test.verify<std::string>(   "database", "qserv_master_host", "localhost-1");
        test.verify<uint16_t>(      "database", "qserv_master_port", 3306 + 1);
        test.verify<std::string>(   "database", "qserv_master_user", "qsmaster-1");
        test.verify<std::string>(   "database", "qserv_master_name", "qservMeta-1");
        test.verify<size_t>(        "database", "qserv_master_services_pool_size", 2 + 1);
        test.verify<std::string>(   "database", "qserv_master_tmp_dir", "/qserv/data/ingest-1");
        test.verify<unsigned int>(  "xrootd", "auto_notify", 0);
        test.verify<unsigned int>(  "xrootd", "request_timeout_sec", 180 + 1);
        test.verify<std::string>(   "xrootd", "host", "localhost-1");
        test.verify<uint16_t>(      "xrootd", "port", 1094 + 1);
        test.verify<std::string>(   "worker", "technology", "FS-1");
        test.verify<size_t>(        "worker", "num_svc_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "num_fs_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "fs_buf_size_bytes", 4194304 + 1);
        test.verify<size_t>(        "worker", "num_loader_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "num_exporter_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "num_http_loader_processing_threads", 2 + 1);
        test.verify<uint16_t>(      "worker_defaults", "svc_port", 25000 + 1);
        test.verify<uint16_t>(      "worker_defaults", "fs_port", 25001 + 1);
        test.verify<std::string>(   "worker_defaults", "data_dir", "/qserv/data/mysql-1");
        test.verify<uint16_t>(      "worker_defaults", "db_port", 3306 + 1);
        test.verify<std::string>(   "worker_defaults", "db_user", "root-1");
        test.verify<uint16_t>(      "worker_defaults", "loader_port", 25002 + 1);
        test.verify<std::string>(   "worker_defaults", "loader_tmp_dir", "/qserv/data/ingest-1");
        test.verify<uint16_t>(      "worker_defaults", "exporter_port", 25003 + 1);
        test.verify<std::string>(   "worker_defaults", "exporter_tmp_dir", "/qserv/data/export-1");
        test.verify<uint16_t>(      "worker_defaults", "http_loader_port", 25004 + 1);
        test.verify<std::string>(   "worker_defaults", "http_loader_tmp_dir", "/qserv/data/ingest-1");
        success = success && test.reportResults();
        cout << "\n";
        _config = ConfigurationSchema::create(_configUrl, _reset);
    }
    return success;
}


bool ConfigApp::_testWorkers() {

    // IMPORTANT: This test will reload configuration from the database after
    // each modification to ensure the modifications were actually saved in
    // the persistent store.

    bool success = true;
    string const indent = "";

    // No workers should exist right after initializing the configuration.
    {
        vector<string> const workers = _config->allWorkers();
        cout << (workers.empty() ? PASSED_STR : FAILED_STR) << " NO WORKERS SHOULD EXIST AFTER INITIALIZATION" << "\n";
        _dumpWorkersAsTable(indent, "");
        success = success && workers.empty();
    }

    // Adding a worker using full specification.
    {
        WorkerInfo workerSpec;
        workerSpec.name = "worker-A";
        workerSpec.isEnabled = true;
        workerSpec.isReadOnly = false;
        workerSpec.svcHost = "host-A";
        workerSpec.svcPort = 15000;
        workerSpec.fsHost = "host-A";
        workerSpec.fsPort = 15001;
        workerSpec.dataDir = "/data/A";
        workerSpec.dbHost = "host-A";
        workerSpec.dbPort = 13306;
        workerSpec.dbUser = "default";
        workerSpec.loaderHost = "host-A";
        workerSpec.loaderPort = 15002;
        workerSpec.loaderTmpDir = "/tmp/A";
        workerSpec.exporterHost = "host-A";
        workerSpec.exporterPort = 15003;
        workerSpec.exporterTmpDir = "/tmp/A";
        workerSpec.httpLoaderHost = "host-A";
        workerSpec.httpLoaderPort = 15004;
        workerSpec.httpLoaderTmpDir = "/tmp/http/A";
        string error;
        CompareWorkerAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED WORKER VS ITS SPECIFICATIONS:",
                                          indent, _verticalSeparator,
                                          _config);
        try {
            _config->addWorker(workerSpec);
            _config->reload();
            WorkerInfo const addedWorker = _config->workerInfo(workerSpec.name);
            comparator.verify(addedWorker, workerSpec);
        } catch (exception const& ex) {
            error = "failed to add worker '" + workerSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR ) << " ADDING WORKERS WITH FULL SPECIFICATION" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // Adding a worker using partial specification.
    {
        WorkerInfo workerSpec;
        // The only required fields are these two. The host names for other services should
        // be set to be the same of the main Replication service. The port numbers and directory
        // paths will be pulled from the worker defaults.
        workerSpec.name = "worker-B";
        workerSpec.svcHost = "host-B";
        string error;
        CompareWorkerAtributes comparator("COMPARING ATRIBUTES OF THE ADDED WORKER VS ITS SPECIFICATIONS:",
                                          indent, _verticalSeparator,
                                          _config);
        try {
            _config->addWorker(workerSpec);
            _config->reload();
            WorkerInfo const addedWorker = _config->workerInfo(workerSpec.name);
            // Compare against defaults for everything but the name of the worker and the name
            // of a host where it runs.
            bool const compareWithDefault = true;
            comparator.verify(addedWorker, workerSpec, compareWithDefault);
        } catch (exception const& ex) {
            error = "failed to add worker '" + workerSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING WORKERS WITH PARTIAL SPECIFICATION" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // Updating an existing worker using partial modifications.
    {
        WorkerInfo workerSpec = _config->workerInfo("worker-B");

        // The only required fields are these two. The host names for other services should
        // be set to be the same of the main Replication service. The port numbers and directory
        // paths will be pulled from the worker defaults.
        workerSpec.isEnabled = true;
        workerSpec.isReadOnly = true;
        workerSpec.svcHost = "host-B-1";
        workerSpec.svcPort = 16000;
        workerSpec.fsHost = "host-B-1";
        workerSpec.fsPort = 16001;
        workerSpec.dataDir = "/qserv/data/worker-B/mysql";
        string error;
        CompareWorkerAtributes comparator("COMPARING ATRIBUTES OF THE UPDATED WORKER VS ITS SPECIFICATIONS:",
                                          indent, _verticalSeparator,
                                          _config);
        try {
            _config->updateWorker(workerSpec);
            _config->reload();
            WorkerInfo const updatedWorker = _config->workerInfo(workerSpec.name);
            comparator.verify(updatedWorker, workerSpec);
        } catch (exception const& ex) {
            error = "failed to update worker '" + workerSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " UPDATING WORKERS" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // Test worker selectors
    {
        vector<string> const workers = _config->allWorkers();
        bool const passed = workers.size() == 2;
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR) << " 2 WORKERS SHOULD EXIST AT THIS POINT" << "\n";
        _dumpWorkersAsTable(indent, "");
    }
    {
        // Assuming default selectors passed into the method:
        //  
        vector<string> const workers = _config->workers();
        bool const passed = (workers.size() == 1) && (workers[0] == "worker-A");
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR) << " 1 ENABLED & READ-WRITE WORKER SHOULD EXIST AT THIS POINT" << "\n";
        _dumpWorkersAsTable(indent, "");
    }
    {
        bool const isEnabled = true;
        bool const isReadOnly = true;
        vector<string> const workers = _config->workers(isEnabled, isReadOnly);
        bool const passed = ((workers.size() == 1) && (workers[0] == "worker-B"));
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR) << " 1 READ-ONLY WORKER SHOULD EXIST AT THIS POINT" << "\n";
        _dumpWorkersAsTable(indent, "");
    }

    // Delete both workers
    {
        vector<string> errors;
        for (auto&& worker: _config->allWorkers()) {
            try {
                _config->deleteWorker(worker);
                _config->reload();
            } catch (exception const& ex) {
                errors.push_back("failed to delete worker '" + worker + "', ex: " + string(ex.what()));
            }
        }
        success = success && errors.empty();
        cout << (errors.empty() ? PASSED_STR : FAILED_STR) << " DELETING ALL WORKERS" << "\n";
        _dumpWorkersAsTable(indent, "");
        if (!errors.empty()) {
            for (auto&& error: errors) {
                cout << indent << " ERROR: " << error << "\n";
            }
            cout << "\n";
        }
    }

    // No workers should exist right after deleting them all at the previous step.
    {
        vector<string> const workers = _config->allWorkers();
        success = success && workers.empty();
        cout << (workers.empty() ? PASSED_STR : FAILED_STR) << " NO WORKERS SHOULD EXIST AFTER DELETING THEM ALL" << "\n";
        _dumpWorkersAsTable(indent, "");
    }
    return success;
}


bool ConfigApp::_testDatabasesAndFamilies() {

    // IMPORTANT: This test involves operatons on database families and databases
    // due to a dependency of the later to the former.

    bool success = true;
    string const indent = "";

    // No families should exist right after initializing the configuration.
    {
        vector<string> const families = _config->databaseFamilies();
        bool const passed = families.empty();
        cout << (passed ? PASSED_STR : FAILED_STR) << " NO FAMILIES SHOULD EXIST AFTER INITIALIZATION" << "\n";
        _dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

    // Adding the first family
    {
        DatabaseFamilyInfo familySpec;
        familySpec.name = "test";
        familySpec.replicationLevel = 1;
        familySpec.numStripes = 340;
        familySpec.numSubStripes = 3;
        familySpec.overlap = 0.01667;
        string error;
        CompareFamilyAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED FAMILY VS ITS SPECIFICATIONS:",
                                          indent, _verticalSeparator);
        try {
            _config->addDatabaseFamily(familySpec);
            _config->reload();
            DatabaseFamilyInfo const addedFamily = _config->databaseFamilyInfo(familySpec.name);
            comparator.verify(addedFamily, familySpec);
        } catch (exception const& ex) {
            error = "failed to add family '" + familySpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING FAMILIES WITH FULL SPECIFICATION" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // Adding the second family
    {
        DatabaseFamilyInfo familySpec;
        familySpec.name = "production";
        familySpec.replicationLevel = 2;
        familySpec.numStripes = 170;
        familySpec.numSubStripes = 6;
        familySpec.overlap = 0.01;
        string error;
        CompareFamilyAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED FAMILY VS ITS SPECIFICATIONS:",
                                          indent, _verticalSeparator);
        try {
            _config->addDatabaseFamily(familySpec);
            _config->reload();
            DatabaseFamilyInfo const addedFamily = _config->databaseFamilyInfo(familySpec.name);
            comparator.verify(addedFamily, familySpec);
        } catch (exception const& ex) {
            error = "failed to add family '" + familySpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING FAMILIES WITH FULL SPECIFICATION" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // Two families should exist at this point
    {
        vector<string> const families = _config->databaseFamilies();
        bool const passed = families.size() == 2;
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 2 FAMILIES SHOULD EXIST NOW" << "\n";
        _dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

   // No database should exist at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = _config->databases(family, allDatabases);
        bool const passed = databases.empty();
        cout << (passed ? PASSED_STR : FAILED_STR ) << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        _dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding a database that will depend on the previously created family
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = "db1";
        databaseSpec.family = "test";
        string error;
        CompareDatabaseAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED DATABASE VS ITS SPECIFICATIONS:",
                                            indent, _verticalSeparator);
        try {
            _config->addDatabase(databaseSpec);
            _config->reload();
            DatabaseInfo const addedDatabase = _config->databaseInfo(databaseSpec.name);
            comparator.verify(addedDatabase, databaseSpec);
        } catch (exception const& ex) {
            error = "failed to add database '" + databaseSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING DATABASES" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // One database should exist at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = _config->databases(family, allDatabases);
        bool const passed = (databases.size() == 1) && (databases[0] == "db1");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        _dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Add the second database
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = "db2";
        databaseSpec.family = "production";
        string error;
        CompareDatabaseAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED DATABASE VS ITS SPECIFICATIONS:",
                                            indent, _verticalSeparator);
        try {
            _config->addDatabase(databaseSpec);
            _config->reload();
            DatabaseInfo const addedDatabase = _config->databaseInfo(databaseSpec.name);
            comparator.verify(addedDatabase, databaseSpec);
        } catch (exception const& ex) {
            error = "failed to add database '" + databaseSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING DATABASES" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // Two databases should exist at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = _config->databases(family, allDatabases);
        bool const passed = databases.size() == 2;
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        _dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Publish one database
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = "db2";
        databaseSpec.family = "production";
        databaseSpec.isPublished = true;
        string error;
        CompareDatabaseAtributes comparator("COMPARING ATTRIBUTES OF THE PUBLISHED DATABASE VS ITS ORIGINALE:",
                                            indent, _verticalSeparator);
        try {
            _config->publishDatabase(databaseSpec.name);
            _config->reload();
            DatabaseInfo const publishedDatabase = _config->databaseInfo(databaseSpec.name);
            comparator.verify(publishedDatabase, databaseSpec);
        } catch (exception const& ex) {
            error = "failed to publish database '" + databaseSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " PUBLISHING DATABASES" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        } else {
            success = success && comparator.reportResults();
            cout << "\n";
        }
    }

    // Test database selectors (one published and one unpublished databases are expected)
    {
        string const family;    // all families if empty
        bool const allDatabases = false;

        vector<string> databases = _config->databases(family, allDatabases, true);
        bool passed = (databases.size() == 1) && (databases[0] == "db2");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 PUBLISHED DATABASE SHOULD EXIST" << "\n";
        success = success && passed;

        databases = _config->databases(family, allDatabases, false);
        passed = (databases.size() == 1) && (databases[0] == "db1");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 NON-PUBLISHED DATABASE SHOULD EXIST" << "\n";
        success = success && passed;

        _dumpDatabasesAsTable(indent, "");
    }

    // Remove one database
    {
        string const name = "db1";
        string error;
        try {
            _config->deleteDatabase(name);
            _config->reload();
        } catch (exception const& ex) {
            error = "failed to delete database '" + name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " DELETING DATABASES" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        }
    }

    // One database should still remain at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = _config->databases(family, allDatabases);
        bool const passed = (databases.size() == 1) && (databases[0] == "db2");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        _dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Remove the database family corresponding to the remaining database
    {
        string const name = "production";
        string error;
        try {
            _config->deleteDatabaseFamily(name);
            _config->reload();
        } catch (exception const& ex) {
            error = "failed to delete database family '" + name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " DELETING DATABASE FAMILIES" << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        }
    }

    // One database family should exist at this point
    {
        vector<string> const families = _config->databaseFamilies();
        bool const passed = (families.size() == 1) && (families[0] == "test");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 FAMILY SHOULD EXIST NOW" << "\n";
        _dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

    // No databases should exist at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = _config->databases(family, allDatabases);
        bool const passed = databases.empty();
        cout << (passed ? PASSED_STR : FAILED_STR ) << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        _dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }
    return success;
}


int ConfigApp::_dump() const {
    string const indent = "  ";
    cout << "\n" << indent << "CONFIG_URL: " << _configUrl << "\n" << "\n";
    if (_dumpScope.empty() or _dumpScope == "GENERAL") _dumpGeneralAsTable(indent);
    if (_dumpScope.empty() or _dumpScope == "WORKERS") _dumpWorkersAsTable(indent);
    if (_dumpScope.empty() or _dumpScope == "FAMILIES") _dumpFamiliesAsTable(indent);
    if (_dumpScope.empty() or _dumpScope == "DATABASES") _dumpDatabasesAsTable(indent);
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


void ConfigApp::_dumpWorkersAsTable(string const& indent, string const& capture) const {

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

    util::ColumnTablePrinter table(capture, indent, _verticalSeparator);

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


void ConfigApp::_dumpFamiliesAsTable(string const& indent, string const& capture) const {

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

    util::ColumnTablePrinter table(capture, indent, _verticalSeparator);

    table.addColumn("name", name, util::ColumnTablePrinter::LEFT);
    table.addColumn("replication level", replicationLevel);
    table.addColumn("stripes", numStripes);
    table.addColumn("sub-stripes", numSubStripes);

    table.print(cout, false, false);
    cout << endl;
}


void ConfigApp::_dumpDatabasesAsTable(string const& indent, string const& capture) const {

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

    util::ColumnTablePrinter table(capture, indent, _verticalSeparator);

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

    // Configuration changes will be amended into the transient object obtained from
    // the database and then be saved to the the persistent configuration.
    try {
        auto info = _config->workerInfo(_workerInfo.name);

        WorkerInfo::amend(_workerEnable,   info.isEnabled);
        WorkerInfo::amend(_workerReadOnly, info.isReadOnly);

        WorkerInfo::amend(_workerInfo.svcHost, info.svcHost);
        WorkerInfo::amend(_workerInfo.svcPort, info.svcPort);

        WorkerInfo::amend(_workerInfo.fsHost,  info.fsHost);
        WorkerInfo::amend(_workerInfo.fsPort,  info.fsPort);
        WorkerInfo::amend(_workerInfo.dataDir, info.dataDir);

        WorkerInfo::amend(_workerInfo.dbHost,  info.dbHost);
        WorkerInfo::amend(_workerInfo.dbPort,  info.dbPort);
        WorkerInfo::amend(_workerInfo.dbUser,  info.dbUser);

        WorkerInfo::amend(_workerInfo.loaderHost,   info.loaderHost);
        WorkerInfo::amend(_workerInfo.loaderPort,   info.loaderPort);
        WorkerInfo::amend(_workerInfo.loaderTmpDir, info.loaderTmpDir);

        WorkerInfo::amend(_workerInfo.exporterHost,   info.exporterHost);
        WorkerInfo::amend(_workerInfo.exporterPort,   info.exporterPort);
        WorkerInfo::amend(_workerInfo.exporterTmpDir, info.exporterTmpDir);

        WorkerInfo::amend(_workerInfo.httpLoaderHost,   info.httpLoaderHost);
        WorkerInfo::amend(_workerInfo.httpLoaderPort,   info.httpLoaderPort);
        WorkerInfo::amend(_workerInfo.httpLoaderTmpDir, info.httpLoaderTmpDir);

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
        _config->addDatabase(_databaseInfo);
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
