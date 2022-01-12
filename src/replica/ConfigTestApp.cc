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
#include "replica/ConfigTestApp.h"

// System headers
#include <cmath>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <vector>

// Third-party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "util/TablePrinter.h"

using namespace std;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

string const description =
    "This application represents the complete integration test for"
    " the Configuration service. The test is supposed to be run against"
    " the Configuration database in MySQL at a location (and credential)"
    " specified via configuration URL parameter '--config=<url>'. The database is required"
    " to exist and be compatible with the application's requirements."
    " ATTENTION: Plan carefully when using this flag to avoid destroying any"
    " valuable data. Avoid running this command in the production environment.";

/**
 * Register an option with a parser (which could also represent a command).
 * @param parser The handler responsible for processing options
 * @param param Parameter handler.`
 */
template <class PARSER, typename T>
void addCommandOption(PARSER& parser, T& param) {
    parser.option(param.key, param.description(), param.value);
}

// The strings for operaton completion reporting.

string const PASSED_STR = "[PASSED]";
string const FAILED_STR = "[FAILED]";
string const OK_STR  = "OK";
string const VALUE_MISMATCH_STR = "VALUE MISMATCH";
string const TYPE_MISMATCH_STR = "TYPE MISMATCH";
string const MISSING_STR = "MISSING";
string const NOT_TESTED_STR = "NOT TESTED";

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
                string const& caption,
                string const& indent,
                bool verticalSeparator)
        :   _config(config),
            _caption(caption),
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
            _actual.push_back(detail::TypeConversionTrait<T>::to_string(actualValue));
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
        _expected.push_back(detail::TypeConversionTrait<T>::to_string(expectedValue));
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
        string const caption = (_failed == 0 ? PASSED_STR : FAILED_STR) + " " + _caption;
        util::ColumnTablePrinter table(caption, _indent, _verticalSeparator);
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
    string const _caption;
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
        string const caption = (_failed == 0 ? PASSED_STR : FAILED_STR) + " " + _caption;
        util::ColumnTablePrinter table(caption, _indent, _verticalSeparator);
        table.addColumn("result", _result);
        table.addColumn("attribute", _attribute, util::ColumnTablePrinter::LEFT);
        table.addColumn("actual", _actual);
        table.addColumn("expected", _expected);
        table.print(cout, false, false);
        return _failed == 0;
    }

protected:
    ComparatorBase(string const& caption, string const& indent, bool verticalSeparator)
        :   _caption(caption), _indent(indent), _verticalSeparator(verticalSeparator) {
    }

    template <typename T>
    void verifyImpl(string const& attribute,
                    T const& actualValue,
                    T const& expectedValue) {
        bool const equal = actualValue == expectedValue;
        _result.push_back(equal ? OK_STR : VALUE_MISMATCH_STR);
        _attribute.push_back(attribute);
        _actual.push_back(detail::TypeConversionTrait<T>::to_string(actualValue));
        _expected.push_back(detail::TypeConversionTrait<T>::to_string(expectedValue));
        if (!equal) ++_failed;
    }

    void verifyImpl(string const& attribute,
                    double actualValue,
                    double expectedValue) {
        bool const equal = std::abs(actualValue - expectedValue) <= std::numeric_limits<double>::epsilon();
        _result.push_back(equal ? OK_STR : VALUE_MISMATCH_STR);
        _attribute.push_back(attribute);
        _actual.push_back(detail::TypeConversionTrait<double>::to_string(actualValue));
        _expected.push_back(detail::TypeConversionTrait<double>::to_string(expectedValue));
        if (!equal) ++_failed;
    }

private:
    // Input parameters
    string const _caption;
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
 * The class CompareWorkerAtributes compares values of the corresponding attrubutes
 * of two workers and reports differences.
 */
class CompareWorkerAtributes: public ComparatorBase {
public:
    CompareWorkerAtributes() = delete;
    CompareWorkerAtributes(CompareWorkerAtributes const&) = delete;
    CompareWorkerAtributes& operator=(CompareWorkerAtributes const&) = delete;

    CompareWorkerAtributes(string const& caption, string const& indent, bool verticalSeparator,
                           Configuration::Ptr const& config)
        :   ComparatorBase(caption, indent, verticalSeparator),
            _config(config) {
    }

    /**
     * Compare values of the the corresponding attributes of two workers.
     * @param actual The actual worker descriptor obtained from the database after adding or
     *   updating the worker.
     * @param desired The input worker descriptor that was used in worker specification.
     * @param compareWithDefault The optional flag that if 'true' will modify the behavior
     *   of the test by pulling expected values of the default attributes either from
     *   the database defaults, or (for host names) from the host name where
     *   the main replication service runs.
     */
    void verify(WorkerInfo const& actual, WorkerInfo const& desired, bool compareWithDefault=false) {
        _verify("name", actual.name, desired.name);
        _verify("is_enabled", actual.isEnabled, desired.isEnabled);
        _verify("is_read_only", actual.isReadOnly, desired.isReadOnly);
        _verify("svc_host", actual.svcHost, desired.svcHost);
        _verify("svc_port", actual.svcPort, desired.svcPort, compareWithDefault);
        _verify("fs_host", actual.fsHost, compareWithDefault ? desired.svcHost : desired.fsHost);
        _verify("fs_port", actual.fsPort, desired.fsPort, compareWithDefault);
        _verify("data_dir", actual.dataDir, desired.dataDir, compareWithDefault);
        _verify("loader_host", actual.loaderHost, compareWithDefault ? desired.svcHost : desired.loaderHost);
        _verify("loader_port", actual.loaderPort, desired.loaderPort, compareWithDefault);
        _verify("loader_tmp_dir", actual.loaderTmpDir, desired.loaderTmpDir, compareWithDefault);
        _verify("exporter_host", actual.exporterHost, compareWithDefault ? desired.svcHost : desired.exporterHost);
        _verify("exporter_port", actual.exporterPort, desired.exporterPort, compareWithDefault);
        _verify("exporter_tmp_dir", actual.exporterTmpDir, desired.exporterTmpDir, compareWithDefault);
        _verify("http_loader_host", actual.httpLoaderHost, compareWithDefault ? desired.svcHost  : desired.httpLoaderHost);
        _verify("http_loader_port", actual.httpLoaderPort, desired.httpLoaderPort, compareWithDefault);
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

    CompareFamilyAtributes(string const& caption, string const& indent, bool verticalSeparator)
        :  ComparatorBase(caption, indent, verticalSeparator) {}

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
 * The class CompareDatabaseAtributes compares values of the corresponding
 * attrubutes of two databases and reports differences.
 */
class CompareDatabaseAtributes: public ComparatorBase {
public:
    CompareDatabaseAtributes() = delete;
    CompareDatabaseAtributes(CompareDatabaseAtributes const&) = delete;
    CompareDatabaseAtributes& operator=(ComparatorBase const&) = delete;

    CompareDatabaseAtributes(string const& caption, string const& indent, bool verticalSeparator)
        :  ComparatorBase(caption, indent, verticalSeparator) {}

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
        verifyImpl("director_table.size()", actual.directorTable.size(), desired.directorTable.size());
        verifyImpl("director_tables.size()", actual.directorTables().size(), desired.directorTables().size());
        verifyImpl("director_key.size()", actual.directorTableKey.size(), desired.directorTableKey.size());
        verifyImpl("latitude_key.size()", actual.latitudeColName.size(), desired.latitudeColName.size());
        verifyImpl("longitude_key.size()", actual.longitudeColName.size(), desired.longitudeColName.size());
    }
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ConfigTestApp::Ptr ConfigTestApp::create(int argc, char* argv[]) {
    return Ptr(new ConfigTestApp(argc, argv));
}


ConfigTestApp::ConfigTestApp(int argc, char* argv[])
    :   ConfigAppBase(argc, argv, description) {

    parser().optional(
        "scope",
        "This optional parameter narrows a scope of the operation down to a specific"
        " context. Allowed values: ALL, GENERAL, WORKERS, DATABASES_AND_FAMILIES, TABLES.",
        _testScope,
        vector<string>({"ALL", "GENERAL", "WORKERS", "DATABASES_AND_FAMILIES", "TABLES"})
    );
}


int ConfigTestApp::runSubclassImpl() {
    int result = 0;
    if (_testScope == "ALL") {
        result += _testGeneral() ? 0 : 1;
        result += _testWorkers() ? 0 : 1;
        result += _testDatabasesAndFamilies()  ? 0 : 1;
        result += _testTables() ? 0 : 1;
    } else if (_testScope == "GENERAL") {
        result += _testGeneral() ? 0 : 1;
    } else if (_testScope == "WORKERS") {
        result += _testWorkers() ? 0 : 1;
    } else if (_testScope == "DATABASES_AND_FAMILIES") {
        result += _testDatabasesAndFamilies() ? 0 : 1;
    } else if (_testScope == "TABLES") {
        result += _testTables() ? 0 : 1;
    }
    return result;
}


bool ConfigTestApp::_testGeneral() {

    string const indent = "";
    bool success = true;

    // Testing reading the default values using the generic API. results will be reported
    // as a table onto the standard output.  Note that the last argument in each
    // call represents an expected value of the parameter's value.
    {
        TestGeneral test(config(), "READING DEAFULT STATE OF THE GENERAL PARAMETERS:", indent, verticalSeparator());
        test.verify<size_t>(        "common", "request_buf_size_bytes", 131072);
        test.verify<unsigned int>(  "common", "request_retry_interval_sec", 1);
        test.verify<size_t>(        "controller", "num_threads", 2);
        test.verify<size_t>(        "controller", "http_server_threads", 2);
        test.verify<uint16_t>(      "controller", "http_server_port", 25081);
        test.verify<unsigned int>(  "controller", "http_max_listen_conn",
                                    boost::asio::socket_base::max_listen_connections);
        test.verify<unsigned int>(  "controller", "request_timeout_sec", 600);
        test.verify<unsigned int>(  "controller", "job_timeout_sec", 600);
        test.verify<unsigned int>(  "controller", "job_heartbeat_sec", 0);
        test.verify<std::string>(   "controller", "empty_chunks_dir", "/qserv/data/qserv");
        test.verify<int>(           "controller", "worker_evict_priority_level", PRIORITY_VERY_HIGH);
        test.verify<int>(           "controller", "health_monitor_priority_level", PRIORITY_VERY_HIGH);
        test.verify<int>(           "controller", "ingest_priority_level", PRIORITY_HIGH);
        test.verify<int>(           "controller", "catalog_management_priority_level", PRIORITY_LOW);

        test.verify<size_t>(        "database", "services_pool_size", 2);
        test.verify<std::string>(   "database", "host", "localhost");
        test.verify<uint16_t>(      "database", "port", 23306);
        test.verify<std::string>(   "database", "user", "root");
        test.verify<std::string>(   "database", "password", "CHANGEME");
        test.verify<std::string>(   "database", "name", "qservReplica");
        test.verify<std::string>(   "database", "qserv_master_user", "qsmaster");
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
        test.verify<size_t>(        "worker", "num_async_loader_processing_threads", 2);
        test.verify<unsigned int>(  "worker", "async_loader_auto_resume", 1);
        test.verify<unsigned int>(  "worker", "async_loader_cleanup_on_resume", 1);
        test.verify<unsigned int>(  "worker", "http_max_listen_conn",
                                    boost::asio::socket_base::max_listen_connections);
        test.verify<uint16_t>(      "worker_defaults", "svc_port", 25000);
        test.verify<uint16_t>(      "worker_defaults", "fs_port", 25001);
        test.verify<std::string>(   "worker_defaults", "data_dir", "/qserv/data/mysql");
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
        config()->set<size_t>(        "common", "request_buf_size_bytes", 131072 + 1);
        config()->set<unsigned int>(  "common", "request_retry_interval_sec", 1 + 1);
        config()->set<size_t>(        "controller", "num_threads", 2 + 1);
        config()->set<size_t>(        "controller", "http_server_threads", 2 + 1);
        config()->set<uint16_t>(      "controller", "http_server_port", 25081 + 1);
        config()->set<unsigned int>(  "controller", "http_max_listen_conn",
                                      boost::asio::socket_base::max_listen_connections * 2);
        config()->set<unsigned int>(  "controller", "request_timeout_sec", 600 + 1);
        config()->set<unsigned int>(  "controller", "job_timeout_sec", 600 + 1);
        config()->set<unsigned int>(  "controller", "job_heartbeat_sec", 0 + 1);
        config()->set<std::string>(   "controller", "empty_chunks_dir", "/qserv/data/qserv-1");
        config()->set<int>(           "controller", "worker_evict_priority_level", PRIORITY_VERY_HIGH + 1);
        config()->set<int>(           "controller", "health_monitor_priority_level", PRIORITY_VERY_HIGH + 1);
        config()->set<int>(           "controller", "ingest_priority_level", PRIORITY_HIGH + 1);
        config()->set<int>(           "controller", "catalog_management_priority_level", PRIORITY_LOW + 1);

        config()->set<size_t>(        "database", "services_pool_size", 2 + 1);
        // These 5 parameters are deduced from 'configUrl'. Changing them here won't make
        // a sense since they're not stored within MySQL.
        if (false) {
            config()->set<std::string>("database", "host", "localhost");
            config()->set<uint16_t>(   "database", "port", 23306);
            config()->set<std::string>("database", "user", "root");
            config()->set<std::string>("database", "password", "CHANGEME");
            config()->set<std::string>("database", "name", "qservReplica");
        }
        config()->set<std::string>(   "database", "qserv_master_user", "qsmaster-1");
        config()->set<size_t>(        "database", "qserv_master_services_pool_size", 2 + 1);
        config()->set<std::string>(   "database", "qserv_master_tmp_dir", "/qserv/data/ingest-1");
        config()->set<unsigned int>(  "xrootd", "auto_notify", 0);
        config()->set<unsigned int>(  "xrootd", "request_timeout_sec", 180 + 1);
        config()->set<std::string>(   "xrootd", "host", "localhost-1");
        config()->set<uint16_t>(      "xrootd", "port", 1094 + 1);
        config()->set<std::string>(   "worker", "technology", "POSIX");
        config()->set<size_t>(        "worker", "num_svc_processing_threads", 2 + 1);
        config()->set<size_t>(        "worker", "num_fs_processing_threads", 2 + 1);
        config()->set<size_t>(        "worker", "fs_buf_size_bytes", 4194304 + 1);
        config()->set<size_t>(        "worker", "num_loader_processing_threads", 2 + 1);
        config()->set<size_t>(        "worker", "num_exporter_processing_threads", 2 + 1);
        config()->set<size_t>(        "worker", "num_http_loader_processing_threads", 2 + 1);
        config()->set<size_t>(        "worker", "num_async_loader_processing_threads", 2 + 1);
        config()->set<unsigned int>(  "worker", "async_loader_auto_resume", 0);
        config()->set<unsigned int>(  "worker", "async_loader_cleanup_on_resume", 0);
        config()->set<unsigned int>(  "worker", "http_max_listen_conn",
                                      boost::asio::socket_base::max_listen_connections * 4);
        config()->set<uint16_t>(      "worker_defaults", "svc_port", 25000 + 1);
        config()->set<uint16_t>(      "worker_defaults", "fs_port", 25001 + 1);
        config()->set<std::string>(   "worker_defaults", "data_dir", "/qserv/data/mysql-1");
        config()->set<uint16_t>(      "worker_defaults", "loader_port", 25002 + 1);
        config()->set<std::string>(   "worker_defaults", "loader_tmp_dir", "/qserv/data/ingest-1");
        config()->set<uint16_t>(      "worker_defaults", "exporter_port", 25003 + 1);
        config()->set<std::string>(   "worker_defaults", "exporter_tmp_dir", "/qserv/data/export-1");
        config()->set<uint16_t>(      "worker_defaults", "http_loader_port", 25004 + 1);
        config()->set<std::string>(   "worker_defaults", "http_loader_tmp_dir", "/qserv/data/ingest-1");

        config()->reload();
        TestGeneral test(config(), "READING UPDATED GENERAL PARAMETERS:", indent, verticalSeparator());
        test.verify<size_t>(        "common", "request_buf_size_bytes", 131072 + 1);
        test.verify<unsigned int>(  "common", "request_retry_interval_sec", 1 + 1);
        test.verify<size_t>(        "controller", "num_threads", 2 + 1);
        test.verify<size_t>(        "controller", "http_server_threads", 2 + 1);
        test.verify<uint16_t>(      "controller", "http_server_port", 25081 + 1);
        test.verify<unsigned int>(  "controller", "http_max_listen_conn",
                                    boost::asio::socket_base::max_listen_connections * 2);
        test.verify<unsigned int>(  "controller", "request_timeout_sec", 600 + 1);
        test.verify<unsigned int>(  "controller", "job_timeout_sec", 600 + 1);
        test.verify<unsigned int>(  "controller", "job_heartbeat_sec", 0 + 1);
        test.verify<std::string>(   "controller", "empty_chunks_dir", "/qserv/data/qserv-1");
        test.verify<int>(           "controller", "worker_evict_priority_level", PRIORITY_VERY_HIGH + 1);
        test.verify<int>(           "controller", "health_monitor_priority_level", PRIORITY_VERY_HIGH + 1);
        test.verify<int>(           "controller", "ingest_priority_level", PRIORITY_HIGH + 1);
        test.verify<int>(           "controller", "catalog_management_priority_level", PRIORITY_LOW + 1);
        test.verify<size_t>(        "database", "services_pool_size", 2 + 1);

        // These 5 parameters deduced from 'configUrl' shouldn't be changed.
        test.verify<std::string>(   "database", "host", "localhost");
        test.verify<uint16_t>(      "database", "port", 23306);
        test.verify<std::string>(   "database", "user", "root");
        test.verify<std::string>(   "database", "password", "CHANGEME");
        test.verify<std::string>(   "database", "name", "qservReplica");

        test.verify<std::string>(   "database", "qserv_master_user", "qsmaster-1");
        test.verify<size_t>(        "database", "qserv_master_services_pool_size", 2 + 1);
        test.verify<std::string>(   "database", "qserv_master_tmp_dir", "/qserv/data/ingest-1");
        test.verify<unsigned int>(  "xrootd", "auto_notify", 0);
        test.verify<unsigned int>(  "xrootd", "request_timeout_sec", 180 + 1);
        test.verify<std::string>(   "xrootd", "host", "localhost-1");
        test.verify<uint16_t>(      "xrootd", "port", 1094 + 1);
        test.verify<std::string>(   "worker", "technology", "POSIX");
        test.verify<size_t>(        "worker", "num_svc_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "num_fs_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "fs_buf_size_bytes", 4194304 + 1);
        test.verify<size_t>(        "worker", "num_loader_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "num_exporter_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "num_http_loader_processing_threads", 2 + 1);
        test.verify<size_t>(        "worker", "num_async_loader_processing_threads", 2 + 1);
        test.verify<unsigned int>(  "worker", "async_loader_auto_resume", 0);
        test.verify<unsigned int>(  "worker", "async_loader_cleanup_on_resume", 0);
        test.verify<unsigned int>(  "worker", "http_max_listen_conn",
                                    boost::asio::socket_base::max_listen_connections * 4);
        test.verify<uint16_t>(      "worker_defaults", "svc_port", 25000 + 1);
        test.verify<uint16_t>(      "worker_defaults", "fs_port", 25001 + 1);
        test.verify<std::string>(   "worker_defaults", "data_dir", "/qserv/data/mysql-1");
        test.verify<uint16_t>(      "worker_defaults", "loader_port", 25002 + 1);
        test.verify<std::string>(   "worker_defaults", "loader_tmp_dir", "/qserv/data/ingest-1");
        test.verify<uint16_t>(      "worker_defaults", "exporter_port", 25003 + 1);
        test.verify<std::string>(   "worker_defaults", "exporter_tmp_dir", "/qserv/data/export-1");
        test.verify<uint16_t>(      "worker_defaults", "http_loader_port", 25004 + 1);
        test.verify<std::string>(   "worker_defaults", "http_loader_tmp_dir", "/qserv/data/ingest-1");
        success = success && test.reportResults();
        cout << "\n";
    }
    return success;
}


bool ConfigTestApp::_testWorkers() {

    // IMPORTANT: This test will reload configuration from the database after
    // each modification to ensure the modifications were actually saved in
    // the persistent store.

    bool success = true;
    string const indent = "";

    // No workers should exist right after initializing the configuration.
    {
        vector<string> const workers = config()->allWorkers();
        cout << (workers.empty() ? PASSED_STR : FAILED_STR) << " NO WORKERS SHOULD EXIST AFTER INITIALIZATION" << "\n";
        dumpWorkersAsTable(indent, "");
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
                                          indent, verticalSeparator(),
                                          config());
        try {
            config()->addWorker(workerSpec);
            config()->reload();
            WorkerInfo const addedWorker = config()->workerInfo(workerSpec.name);
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
                                          indent, verticalSeparator(),
                                          config());
        try {
            config()->addWorker(workerSpec);
            config()->reload();
            WorkerInfo const addedWorker = config()->workerInfo(workerSpec.name);
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
        WorkerInfo workerSpec = config()->workerInfo("worker-B");

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
                                          indent, verticalSeparator(),
                                          config());
        try {
            config()->updateWorker(workerSpec);
            config()->reload();
            WorkerInfo const updatedWorker = config()->workerInfo(workerSpec.name);
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
        vector<string> const workers = config()->allWorkers();
        bool const passed = workers.size() == 2;
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR) << " 2 WORKERS SHOULD EXIST AT THIS POINT" << "\n";
        dumpWorkersAsTable(indent, "");
    }
    {
        // Assuming default selectors passed into the method:
        //  
        vector<string> const workers = config()->workers();
        bool const passed = (workers.size() == 1) && (workers[0] == "worker-A");
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR) << " 1 ENABLED & READ-WRITE WORKER SHOULD EXIST AT THIS POINT" << "\n";
        dumpWorkersAsTable(indent, "");
    }
    {
        bool const isEnabled = true;
        bool const isReadOnly = true;
        vector<string> const workers = config()->workers(isEnabled, isReadOnly);
        bool const passed = ((workers.size() == 1) && (workers[0] == "worker-B"));
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR) << " 1 READ-ONLY WORKER SHOULD EXIST AT THIS POINT" << "\n";
        dumpWorkersAsTable(indent, "");
    }

    // Delete both workers
    {
        vector<string> errors;
        for (auto&& worker: config()->allWorkers()) {
            try {
                config()->deleteWorker(worker);
                config()->reload();
            } catch (exception const& ex) {
                errors.push_back("failed to delete worker '" + worker + "', ex: " + string(ex.what()));
            }
        }
        success = success && errors.empty();
        cout << (errors.empty() ? PASSED_STR : FAILED_STR) << " DELETING ALL WORKERS" << "\n";
        dumpWorkersAsTable(indent, "");
        if (!errors.empty()) {
            for (auto&& error: errors) {
                cout << indent << " ERROR: " << error << "\n";
            }
            cout << "\n";
        }
    }

    // No workers should exist right after deleting them all at the previous step.
    {
        vector<string> const workers = config()->allWorkers();
        success = success && workers.empty();
        cout << (workers.empty() ? PASSED_STR : FAILED_STR) << " NO WORKERS SHOULD EXIST AFTER DELETING THEM ALL" << "\n";
        dumpWorkersAsTable(indent, "");
    }
    return success;
}


bool ConfigTestApp::_testDatabasesAndFamilies() {

    // IMPORTANT: This test involves operatons on database families and databases
    // due to a dependency of the later to the former.

    bool success = true;
    string const indent = "";

    // No families should exist right after initializing the configuration.
    {
        vector<string> const families = config()->databaseFamilies();
        bool const passed = families.empty();
        cout << (passed ? PASSED_STR : FAILED_STR) << " NO FAMILIES SHOULD EXIST AFTER INITIALIZATION" << "\n";
        dumpFamiliesAsTable(indent, "");
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
                                          indent, verticalSeparator());
        try {
            config()->addDatabaseFamily(familySpec);
            config()->reload();
            DatabaseFamilyInfo const addedFamily = config()->databaseFamilyInfo(familySpec.name);
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
                                          indent, verticalSeparator());
        try {
            config()->addDatabaseFamily(familySpec);
            config()->reload();
            DatabaseFamilyInfo const addedFamily = config()->databaseFamilyInfo(familySpec.name);
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
        vector<string> const families = config()->databaseFamilies();
        bool const passed = families.size() == 2;
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 2 FAMILIES SHOULD EXIST NOW" << "\n";
        dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

    // No database should exist at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = databases.empty();
        cout << (passed ? PASSED_STR : FAILED_STR ) << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding a database that will depend on the previously created family
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = "db1";
        databaseSpec.family = "test";
        string error;
        CompareDatabaseAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED DATABASE VS ITS SPECIFICATIONS:",
                                            indent, verticalSeparator());
        try {
            config()->addDatabase(databaseSpec.name, databaseSpec.family);
            config()->reload();
            DatabaseInfo const addedDatabase = config()->databaseInfo(databaseSpec.name);
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
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = (databases.size() == 1) && (databases[0] == "db1");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Add the second database
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = "db2";
        databaseSpec.family = "production";
        string error;
        CompareDatabaseAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED DATABASE VS ITS SPECIFICATIONS:",
                                            indent, verticalSeparator());
        try {
            config()->addDatabase(databaseSpec.name, databaseSpec.family);
            config()->reload();
            DatabaseInfo const addedDatabase = config()->databaseInfo(databaseSpec.name);
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
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = databases.size() == 2;
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        dumpDatabasesAsTable(indent, "");
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
                                            indent, verticalSeparator());
        try {
            config()->publishDatabase(databaseSpec.name);
            config()->reload();
            DatabaseInfo const publishedDatabase = config()->databaseInfo(databaseSpec.name);
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

        vector<string> databases = config()->databases(family, allDatabases, true);
        bool passed = (databases.size() == 1) && (databases[0] == "db2");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 PUBLISHED DATABASE SHOULD EXIST" << "\n";
        success = success && passed;

        databases = config()->databases(family, allDatabases, false);
        passed = (databases.size() == 1) && (databases[0] == "db1");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 NON-PUBLISHED DATABASE SHOULD EXIST" << "\n";
        success = success && passed;

        dumpDatabasesAsTable(indent, "");
    }

    // Remove one database
    {
        string const name = "db1";
        string error;
        try {
            config()->deleteDatabase(name);
            config()->reload();
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
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = (databases.size() == 1) && (databases[0] == "db2");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Remove the database family corresponding to the remaining database
    {
        string const name = "production";
        string error;
        try {
            config()->deleteDatabaseFamily(name);
            config()->reload();
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
        vector<string> const families = config()->databaseFamilies();
        bool const passed = (families.size() == 1) && (families[0] == "test");
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 FAMILY SHOULD EXIST NOW" << "\n";
        dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

    // No databases should exist at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = databases.empty();
        cout << (passed ? PASSED_STR : FAILED_STR ) << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Remove the remaining family
    {
        string const name = "test";
        string error;
        try {
            config()->deleteDatabaseFamily(name);
            config()->reload();
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

    // No families should exist at this point.
    {
        vector<string> const families = config()->databaseFamilies();
        bool const passed = families.empty();
        cout << (passed ? PASSED_STR : FAILED_STR) << " NO FAMILIES SHOULD EXIST AFTER THE CLEANUP!" << "\n";
        dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

    return success;
}


bool ConfigTestApp::_testTables() {

    // IMPORTANT: This test involves operatons on database families, databases and tables
    // due to a dependency of the later to the former.

    bool success = true;
    string const indent = "";

    // No families should exist right after initializing the configuration.
    {
        vector<string> const families = config()->databaseFamilies();
        if (!families.empty()) {
            cout << FAILED_STR << " NO FAMILIES SHOULD EXIST BEFORE THE TEST OF TABLES" << "\n";
            dumpFamiliesAsTable(indent, "");
            return false;
        }
    }

    // No database should exist at this point
    {
        string const family;    // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        if (!databases.empty()) {
            cout << PASSED_STR << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST BEFORE THE TEST OF TABLES" << "\n";
            dumpDatabasesAsTable(indent, "");
            return false;
        }
    }

    // Adding the family
    string const family = "test";
    {
        DatabaseFamilyInfo familySpec;
        familySpec.name = family;
        familySpec.replicationLevel = 1;
        familySpec.numStripes = 340;
        familySpec.numSubStripes = 3;
        familySpec.overlap = 0.01667;
        try {
            config()->addDatabaseFamily(familySpec);
            config()->reload();
            DatabaseFamilyInfo const addedFamily = config()->databaseFamilyInfo(familySpec.name);
        } catch (exception const& ex) {
            cout << "\n";
            cout << indent << " ERROR: " << "failed to add family '" << familySpec.name << "', ex: "
                << string(ex.what()) << ", ABORTING THE TEST OF TABLES\n";
            cout << "\n";
            return false;
        }
    }

    // Adding a database that will depend on the previously created family
    string const database = "db1";
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = database;
        databaseSpec.family = family;
        try {
            config()->addDatabase(databaseSpec.name, databaseSpec.family);
            config()->reload();
            DatabaseInfo const addedDatabase = config()->databaseInfo(databaseSpec.name);
        } catch (exception const& ex) {
            cout << "\n";
            cout << indent << " ERROR: " << "failed to add database '" << databaseSpec.name << "', ex: "
                << string(ex.what()) << ", ABORTING THE TEST OF TABLES\n";

            cout << "\n";
            return false;
        }
    }

    auto const addTable = [&](string const& database, string const& table,
                              bool isPartitioned, bool isDirector, string const& directorTable,
                              string const& directorTableKey, string const& latitudeColName,
                              string const& longitudeColName, list<SqlColDef> const& coldefs) -> bool {
        try {
            config()->addTable(database, table, isPartitioned, coldefs,
                               isDirector, directorTable, directorTableKey,
                               latitudeColName, longitudeColName);
            config()->reload();
            DatabaseInfo const updatedDatabase = config()->databaseInfo(database);
            return true;
        } catch (exception const& ex) {
            cout << "\n";
            cout << indent << " ERROR: " << "failed to add table '" << table << "' to database '" << database << "', ex: "
                << string(ex.what()) << ", ABORTING THE TEST OF TABLES\n";
            cout << "\n";
            return false;
        }
    };

    // Adding the first director table to the database. This is is going to be the "stand-alone"
    // director that won't have any dependents.
    string const table1 = "director-1";
    bool const isPartitioned1 = true;
    bool isDirector1 = true;
    string const directorTable1;
    string const directorTableKey1 = "objectId";
    string const latitudeColName1 = "decl";
    string const longitudeColName1 = "ra";
    list<SqlColDef> coldefs1;
    coldefs1.emplace_back(directorTableKey1, "INT UNSIGNED");
    coldefs1.emplace_back(latitudeColName1, "DOUBLE");
    coldefs1.emplace_back(longitudeColName1, "DOUBLE");
    coldefs1.emplace_back(lsst::qserv::SUB_CHUNK_COLUMN, "INT");

    success = success && addTable(database, table1, isPartitioned1, isDirector1, directorTable1,
                                  directorTableKey1, latitudeColName1, longitudeColName1, coldefs1);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(database);
        auto const tables = databaseInfo.tables();
        bool const passed = (tables.size() == 1)
                && (find(tables.cbegin(), tables.cend(), table1) != tables.cend())
                && (databaseInfo.isPartitioned(table1) == isPartitioned1)
                && (databaseInfo.isDirector(table1) == isDirector1)
                && databaseInfo.directorTable.at(table1).empty()
                && (databaseInfo.directorTableKey.at(table1) == directorTableKey1)
                && (databaseInfo.latitudeColName.at(table1) == latitudeColName1)
                && (databaseInfo.longitudeColName.at(table1) == longitudeColName1)
                && (databaseInfo.columns.at(table1).size() == coldefs1.size());
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 1 TABLE SHOULD EXIST NOW" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding the second director table to the database. This table will have dependents.
    string const table2 = "director-2";
    bool const isPartitioned2 = true;
    bool isDirector2 = true;
    string const directorTable2;
    string const directorTableKey2 = "id";
    string const latitudeColName2 = "coord_decl";
    string const longitudeColName2 = "coord_ra";
    list<SqlColDef> coldefs2;
    coldefs2.emplace_back(directorTableKey2, "INT UNSIGNED");
    coldefs2.emplace_back(latitudeColName2, "DOUBLE");
    coldefs2.emplace_back(longitudeColName2, "DOUBLE");
    coldefs2.emplace_back(lsst::qserv::SUB_CHUNK_COLUMN, "INT");

    success = success && addTable(database, table2, isPartitioned2, isDirector2, directorTable2,
                                  directorTableKey2, latitudeColName2, longitudeColName2, coldefs2);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(database);
        auto const tables = databaseInfo.tables();
        bool const passed = (tables.size() == 2)
                && (find(tables.cbegin(), tables.cend(), table2) != tables.cend())
                && (databaseInfo.isPartitioned(table2) == isPartitioned2)
                && (databaseInfo.isDirector(table2) == isDirector2)
                && databaseInfo.directorTable.at(table2).empty()
                && (databaseInfo.directorTableKey.at(table2) == directorTableKey2)
                && (databaseInfo.latitudeColName.at(table2) == latitudeColName2)
                && (databaseInfo.longitudeColName.at(table2) == longitudeColName2)
                && (databaseInfo.columns.at(table2).size() == coldefs2.size());
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 2 TABLES SHOULD EXIST NOW" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding the first dependent table connected to the second director.
    string const table12 = "dependent-1-of-2";
    bool const isPartitioned12 = true;
    bool isDirector12 = false;
    string const directorTable12 = "director-2";
    string const directorTableKey12 = "director_id";
    string const latitudeColName12 = "";
    string const longitudeColName12 = "";
    list<SqlColDef> coldefs12;
    coldefs12.emplace_back(directorTableKey12, "INT UNSIGNED");

    success = success && addTable(database, table12, isPartitioned12, isDirector12, directorTable12,
                                  directorTableKey12, latitudeColName12, longitudeColName12, coldefs12);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(database);
        auto const tables = databaseInfo.tables();
        bool const passed = (tables.size() == 3)
                && (find(tables.cbegin(), tables.cend(), table12) != tables.cend())
                && (databaseInfo.isPartitioned(table12) == isPartitioned12)
                && (databaseInfo.isDirector(table12) == isDirector12)
                && (databaseInfo.directorTable.at(table12) == directorTable12)
                && (databaseInfo.directorTableKey.at(table12) == directorTableKey12)
                && (databaseInfo.latitudeColName.at(table12) == latitudeColName12)
                && (databaseInfo.longitudeColName.at(table12) == longitudeColName12)
                && (databaseInfo.columns.at(table12).size() == coldefs12.size());
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 3 TABLES SHOULD EXIST NOW" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding the second dependent table connected to the second director.
    string const table22 = "dependent-2-of-2";
    bool const isPartitioned22 = true;
    bool isDirector22 = false;
    string const directorTable22 = "director-2";
    string const directorTableKey22 = "director_id_key";
    string const latitudeColName22 = "decl";
    string const longitudeColName22 = "ra";
    list<SqlColDef> coldefs22;
    coldefs22.emplace_back(directorTableKey22, "INT UNSIGNED");
    coldefs22.emplace_back(latitudeColName22, "DOUBLE");
    coldefs22.emplace_back(longitudeColName22, "DOUBLE");

    success = success && addTable(database, table22, isPartitioned22, isDirector22, directorTable22,
                                  directorTableKey22, latitudeColName22, longitudeColName22, coldefs22);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(database);
        auto const tables = databaseInfo.tables();
        bool const passed = (tables.size() == 4)
                && (find(tables.cbegin(), tables.cend(), table22) != tables.cend())
                && (databaseInfo.isPartitioned(table22) == isPartitioned22)
                && (databaseInfo.isDirector(table22) == isDirector22)
                && (databaseInfo.directorTable.at(table22) == directorTable22)
                && (databaseInfo.directorTableKey.at(table22) == directorTableKey22)
                && (databaseInfo.latitudeColName.at(table22) == latitudeColName22)
                && (databaseInfo.longitudeColName.at(table22) == longitudeColName22)
                && (databaseInfo.columns.at(table22).size() == coldefs22.size());
        cout << (passed ? PASSED_STR : FAILED_STR ) << " EXACTLY 4 TABLES SHOULD EXIST NOW" << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Remove the database family to clean up everything created by this test
    {
        string error;
        try {
            config()->deleteDatabaseFamily(family);
            config()->reload();
        } catch (exception const& ex) {
            cout << "\n";
            cout << indent << " ERROR: " << "failed to delete database family '" << family << "', ex: "
                << string(ex.what()) << "\n";
            cout << "\n";
            return false;
        }
    }
    return success;
}

}}} // namespace lsst::qserv::replica
