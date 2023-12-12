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

// The strings for operaton completion reporting.

string const PASSED_STR = "[PASSED]";
string const FAILED_STR = "[FAILED]";
string const OK_STR = "OK";
string const VALUE_MISMATCH_STR = "VALUE MISMATCH";

/**
 * The class ComparatorBase represents the base class for specific comparators
 * for workers, database families or databases. The class encapsulates common
 * aspects of the final comparators.
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
            : _caption(caption), _indent(indent), _verticalSeparator(verticalSeparator) {}

    template <typename T>
    void verifyImpl(string const& attribute, T const& actualValue, T const& expectedValue) {
        bool const equal = actualValue == expectedValue;
        _result.push_back(equal ? OK_STR : VALUE_MISMATCH_STR);
        _attribute.push_back(attribute);
        _actual.push_back(detail::TypeConversionTrait<T>::to_string(actualValue));
        _expected.push_back(detail::TypeConversionTrait<T>::to_string(expectedValue));
        if (!equal) ++_failed;
    }

    void verifyImpl(string const& attribute, double actualValue, double expectedValue) {
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
class CompareWorkerAtributes : public ComparatorBase {
public:
    CompareWorkerAtributes() = delete;
    CompareWorkerAtributes(CompareWorkerAtributes const&) = delete;
    CompareWorkerAtributes& operator=(CompareWorkerAtributes const&) = delete;

    CompareWorkerAtributes(string const& caption, string const& indent, bool verticalSeparator,
                           Configuration::Ptr const& config)
            : ComparatorBase(caption, indent, verticalSeparator), _config(config) {}

    /**
     * Compare values of the the corresponding attributes of two workers.
     * @param actual The actual worker descriptor obtained from the database after adding or
     *   updating the worker.
     * @param desired The input worker descriptor that was used in worker specification.
     */
    void verify(ConfigWorker const& actual, ConfigWorker const& desired) {
        _verify("name", actual.name, desired.name);
        _verify("is-enabled", actual.isEnabled, desired.isEnabled);
        _verify("is-read-only", actual.isReadOnly, desired.isReadOnly);
    }

    template <typename T>
    void _verify(string const& attribute, T const& actualValue, T const& expectedValue) {
        verifyImpl<T>(attribute, actualValue, expectedValue);
    }

private:
    // Input parameters
    Configuration::Ptr const _config;
};

/**
 * The class CompareFamilyAtributes compares values of the coresponding
 * attrubutes of two database families and reports differences.
 */
class CompareFamilyAtributes : public ComparatorBase {
public:
    CompareFamilyAtributes() = delete;
    CompareFamilyAtributes(CompareFamilyAtributes const&) = delete;
    CompareFamilyAtributes& operator=(CompareFamilyAtributes const&) = delete;

    CompareFamilyAtributes(string const& caption, string const& indent, bool verticalSeparator)
            : ComparatorBase(caption, indent, verticalSeparator) {}

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
class CompareDatabaseAtributes : public ComparatorBase {
public:
    CompareDatabaseAtributes() = delete;
    CompareDatabaseAtributes(CompareDatabaseAtributes const&) = delete;
    CompareDatabaseAtributes& operator=(ComparatorBase const&) = delete;

    CompareDatabaseAtributes(string const& caption, string const& indent, bool verticalSeparator)
            : ComparatorBase(caption, indent, verticalSeparator) {}

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
        verifyImpl("tables.empty()", actual.tables().empty(), desired.tables().empty());
        verifyImpl("partitioned_tables.empty()", actual.partitionedTables().empty(),
                   desired.partitionedTables().empty());
        verifyImpl("director_tables.empty()", actual.directorTables().empty(),
                   desired.directorTables().empty());
        verifyImpl("ref_match_tables.empty()", actual.refMatchTables().empty(),
                   desired.refMatchTables().empty());
        verifyImpl("regular_tables.empty()", actual.regularTables().empty(), desired.regularTables().empty());
    }
};

}  // namespace

namespace lsst::qserv::replica {

ConfigTestApp::Ptr ConfigTestApp::create(int argc, char* argv[]) {
    return Ptr(new ConfigTestApp(argc, argv));
}

ConfigTestApp::ConfigTestApp(int argc, char* argv[]) : ConfigAppBase(argc, argv, description) {
    parser().optional("scope",
                      "This optional parameter narrows a scope of the operation down to a specific"
                      " context. Allowed values: ALL, WORKERS, DATABASES_AND_FAMILIES, TABLES.",
                      _testScope, vector<string>({"ALL", "WORKERS", "DATABASES_AND_FAMILIES", "TABLES"}));
}

int ConfigTestApp::runSubclassImpl() {
    int result = 0;
    if (_testScope == "ALL") {
        result += _testWorkers() ? 0 : 1;
        result += _testDatabasesAndFamilies() ? 0 : 1;
        result += _testTables() ? 0 : 1;
    } else if (_testScope == "WORKERS") {
        result += _testWorkers() ? 0 : 1;
    } else if (_testScope == "DATABASES_AND_FAMILIES") {
        result += _testDatabasesAndFamilies() ? 0 : 1;
    } else if (_testScope == "TABLES") {
        result += _testTables() ? 0 : 1;
    }
    return result;
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
        cout << (workers.empty() ? PASSED_STR : FAILED_STR) << " NO WORKERS SHOULD EXIST AFTER INITIALIZATION"
             << "\n";
        dumpWorkersAsTable(indent, "");
        success = success && workers.empty();
    }

    // Adding a worker using full specification.
    {
        ConfigWorker workerSpec;
        workerSpec.name = "worker-A";
        workerSpec.isEnabled = true;
        workerSpec.isReadOnly = false;
        string error;
        CompareWorkerAtributes comparator("COMPARING ATTRIBUTES OF THE ADDED WORKER VS ITS SPECIFICATIONS:",
                                          indent, verticalSeparator(), config());
        try {
            config()->addWorker(workerSpec);
            config()->reload();
            ConfigWorker const addedWorker = config()->worker(workerSpec.name);
            comparator.verify(addedWorker, workerSpec);
        } catch (exception const& ex) {
            error = "failed to add worker '" + workerSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING WORKERS WITH FULL SPECIFICATION"
             << "\n";
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
        ConfigWorker workerSpec;
        // The only required fields are these two. The host names for other services should
        // be set to be the same of the main Replication service. The port numbers and directory
        // paths will be pulled from the worker defaults.
        workerSpec.name = "worker-B";
        string error;
        CompareWorkerAtributes comparator("COMPARING ATRIBUTES OF THE ADDED WORKER VS ITS SPECIFICATIONS:",
                                          indent, verticalSeparator(), config());
        try {
            config()->addWorker(workerSpec);
            config()->reload();
            ConfigWorker const addedWorker = config()->worker(workerSpec.name);
            // Compare against defaults for everything but the name of the worker and the name
            // of a host where it runs.
            comparator.verify(addedWorker, workerSpec);
        } catch (exception const& ex) {
            error = "failed to add worker '" + workerSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING WORKERS WITH PARTIAL SPECIFICATION"
             << "\n";
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
        ConfigWorker workerSpec = config()->worker("worker-B");

        // The only required fields are these two. The host names for other services should
        // be set to be the same of the main Replication service. The port numbers and directory
        // paths will be pulled from the worker defaults.
        workerSpec.isEnabled = true;
        workerSpec.isReadOnly = true;
        string error;
        CompareWorkerAtributes comparator("COMPARING ATRIBUTES OF THE UPDATED WORKER VS ITS SPECIFICATIONS:",
                                          indent, verticalSeparator(), config());
        try {
            config()->updateWorker(workerSpec);
            config()->reload();
            ConfigWorker const updatedWorker = config()->worker(workerSpec.name);
            comparator.verify(updatedWorker, workerSpec);
        } catch (exception const& ex) {
            error = "failed to update worker '" + workerSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " UPDATING WORKERS"
             << "\n";
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
        cout << (passed ? PASSED_STR : FAILED_STR) << " 2 WORKERS SHOULD EXIST AT THIS POINT"
             << "\n";
        dumpWorkersAsTable(indent, "");
    }
    {
        // Assuming default selectors passed into the method:
        //
        vector<string> const workers = config()->workers();
        bool const passed = (workers.size() == 1) && (workers[0] == "worker-A");
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR)
             << " 1 ENABLED & READ-WRITE WORKER SHOULD EXIST AT THIS POINT"
             << "\n";
        dumpWorkersAsTable(indent, "");
    }
    {
        bool const isEnabled = true;
        bool const isReadOnly = true;
        vector<string> const workers = config()->workers(isEnabled, isReadOnly);
        bool const passed = ((workers.size() == 1) && (workers[0] == "worker-B"));
        success = success && passed;
        cout << (passed ? PASSED_STR : FAILED_STR) << " 1 READ-ONLY WORKER SHOULD EXIST AT THIS POINT"
             << "\n";
        dumpWorkersAsTable(indent, "");
    }

    // Delete both workers
    {
        vector<string> errors;
        for (auto&& worker : config()->allWorkers()) {
            try {
                config()->deleteWorker(worker);
                config()->reload();
            } catch (exception const& ex) {
                errors.push_back("failed to delete worker '" + worker + "', ex: " + string(ex.what()));
            }
        }
        success = success && errors.empty();
        cout << (errors.empty() ? PASSED_STR : FAILED_STR) << " DELETING ALL WORKERS"
             << "\n";
        dumpWorkersAsTable(indent, "");
        if (!errors.empty()) {
            for (auto&& error : errors) {
                cout << indent << " ERROR: " << error << "\n";
            }
            cout << "\n";
        }
    }

    // No workers should exist right after deleting them all at the previous step.
    {
        vector<string> const workers = config()->allWorkers();
        success = success && workers.empty();
        cout << (workers.empty() ? PASSED_STR : FAILED_STR)
             << " NO WORKERS SHOULD EXIST AFTER DELETING THEM ALL"
             << "\n";
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
        cout << (passed ? PASSED_STR : FAILED_STR) << " NO FAMILIES SHOULD EXIST AFTER INITIALIZATION"
             << "\n";
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
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING FAMILIES WITH FULL SPECIFICATION"
             << "\n";
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
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING FAMILIES WITH FULL SPECIFICATION"
             << "\n";
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
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 2 FAMILIES SHOULD EXIST NOW"
             << "\n";
        dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

    // No database should exist at this point
    {
        string const family;  // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = databases.empty();
        cout << (passed ? PASSED_STR : FAILED_STR)
             << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST"
             << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding a database that will depend on the previously created family
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = "db1";
        databaseSpec.family = "test";
        string error;
        CompareDatabaseAtributes comparator(
                "COMPARING ATTRIBUTES OF THE ADDED DATABASE VS ITS SPECIFICATIONS:", indent,
                verticalSeparator());
        try {
            config()->addDatabase(databaseSpec.name, databaseSpec.family);
            config()->reload();
            DatabaseInfo const addedDatabase = config()->databaseInfo(databaseSpec.name);
            comparator.verify(addedDatabase, databaseSpec);
        } catch (exception const& ex) {
            error = "failed to add database '" + databaseSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING DATABASES"
             << "\n";
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
        string const family;  // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = (databases.size() == 1) && (databases[0] == "db1");
        cout << (passed ? PASSED_STR : FAILED_STR)
             << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST"
             << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Add the second database
    {
        DatabaseInfo databaseSpec;
        databaseSpec.name = "db2";
        databaseSpec.family = "production";
        string error;
        CompareDatabaseAtributes comparator(
                "COMPARING ATTRIBUTES OF THE ADDED DATABASE VS ITS SPECIFICATIONS:", indent,
                verticalSeparator());
        try {
            config()->addDatabase(databaseSpec.name, databaseSpec.family);
            config()->reload();
            DatabaseInfo const addedDatabase = config()->databaseInfo(databaseSpec.name);
            comparator.verify(addedDatabase, databaseSpec);
        } catch (exception const& ex) {
            error = "failed to add database '" + databaseSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " ADDING DATABASES"
             << "\n";
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
        string const family;  // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = databases.size() == 2;
        cout << (passed ? PASSED_STR : FAILED_STR)
             << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST"
             << "\n";
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
        CompareDatabaseAtributes comparator(
                "COMPARING ATTRIBUTES OF THE PUBLISHED DATABASE VS ITS ORIGINALE:", indent,
                verticalSeparator());
        try {
            config()->publishDatabase(databaseSpec.name);
            config()->reload();
            DatabaseInfo const publishedDatabase = config()->databaseInfo(databaseSpec.name);
            comparator.verify(publishedDatabase, databaseSpec);
        } catch (exception const& ex) {
            error = "failed to publish database '" + databaseSpec.name + "', ex: " + string(ex.what());
        }
        success = success && error.empty();
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " PUBLISHING DATABASES"
             << "\n";
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
        string const family;  // all families if empty
        bool const allDatabases = false;

        vector<string> databases = config()->databases(family, allDatabases, true);
        bool passed = (databases.size() == 1) && (databases[0] == "db2");
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 1 PUBLISHED DATABASE SHOULD EXIST"
             << "\n";
        success = success && passed;

        databases = config()->databases(family, allDatabases, false);
        passed = (databases.size() == 1) && (databases[0] == "db1");
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 1 NON-PUBLISHED DATABASE SHOULD EXIST"
             << "\n";
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
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " DELETING DATABASES"
             << "\n";
        if (!error.empty()) {
            cout << "\n";
            cout << indent << " ERROR: " << error << "\n";
            cout << "\n";
        }
    }

    // One database should still remain at this point
    {
        string const family;  // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = (databases.size() == 1) && (databases[0] == "db2");
        cout << (passed ? PASSED_STR : FAILED_STR)
             << " EXACTLY 1 DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST"
             << "\n";
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
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " DELETING DATABASE FAMILIES"
             << "\n";
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
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 1 FAMILY SHOULD EXIST NOW"
             << "\n";
        dumpFamiliesAsTable(indent, "");
        success = success && passed;
    }

    // No databases should exist at this point
    {
        string const family;  // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        bool const passed = databases.empty();
        cout << (passed ? PASSED_STR : FAILED_STR)
             << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST"
             << "\n";
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
        cout << (error.empty() ? PASSED_STR : FAILED_STR) << " DELETING DATABASE FAMILIES"
             << "\n";
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
        cout << (passed ? PASSED_STR : FAILED_STR) << " NO FAMILIES SHOULD EXIST AFTER THE CLEANUP!"
             << "\n";
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
            cout << FAILED_STR << " NO FAMILIES SHOULD EXIST BEFORE THE TEST OF TABLES"
                 << "\n";
            dumpFamiliesAsTable(indent, "");
            return false;
        }
    }

    // No database should exist at this point
    {
        string const family;  // all families if empty
        bool const allDatabases = true;
        vector<string> const databases = config()->databases(family, allDatabases);
        if (!databases.empty()) {
            cout << PASSED_STR
                 << " NO DATABASE OF ANY FAMILY AND IN ANY STATE SHOULD EXIST BEFORE THE TEST OF TABLES"
                 << "\n";
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
            cout << indent << " ERROR: "
                 << "failed to add family '" << familySpec.name << "', ex: " << string(ex.what())
                 << ", ABORTING THE TEST OF TABLES\n";
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
            cout << indent << " ERROR: "
                 << "failed to add database '" << databaseSpec.name << "', ex: " << string(ex.what())
                 << ", ABORTING THE TEST OF TABLES\n";

            cout << "\n";
            return false;
        }
    }

    auto const addTable = [&](TableInfo const& table) -> bool {
        try {
            config()->addTable(table);
            config()->reload();
            DatabaseInfo const updatedDatabase = config()->databaseInfo(table.database);
            return true;
        } catch (exception const& ex) {
            cout << "\n";
            cout << indent << " ERROR: "
                 << "failed to add table '" << table.name << "' to database '" << table.database
                 << "', ex: " << string(ex.what()) << ", ABORTING THE TEST OF TABLES\n";
            cout << "\n";
            return false;
        }
    };

    // Adding the first director table to the database. This is is going to be the "stand-alone"
    // director that won't have any dependents.
    TableInfo table1;
    table1.name = "director-1";
    table1.database = database;
    table1.isPartitioned = true;
    table1.directorTable = DirectorTableRef("", "objectId");
    table1.uniquePrimaryKey = false;
    table1.latitudeColName = "decl";
    table1.longitudeColName = "ra";
    table1.columns.emplace_back(table1.directorTable.primaryKeyColumn(), "INT UNSIGNED");
    table1.columns.emplace_back(table1.latitudeColName, "DOUBLE");
    table1.columns.emplace_back(table1.longitudeColName, "DOUBLE");
    table1.columns.emplace_back(lsst::qserv::SUB_CHUNK_COLUMN, "INT");

    success = success && addTable(table1);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(table1.database);
        bool passed = (databaseInfo.tables().size() == 1) && databaseInfo.tableExists(table1.name) &&
                      (databaseInfo.findTable(table1.name) == table1);
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 1 TABLE SHOULD EXIST NOW"
             << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding the second director table to the database. This table will have dependents.
    TableInfo table2;
    table2.name = "director-2";
    table2.database = database;
    table2.isPartitioned = true;
    table2.directorTable = DirectorTableRef("", "id");
    table2.latitudeColName = "coord_decl";
    table2.longitudeColName = "coord_ra";
    table2.columns.emplace_back(table2.directorTable.primaryKeyColumn(), "INT UNSIGNED");
    table2.columns.emplace_back(table2.latitudeColName, "DOUBLE");
    table2.columns.emplace_back(table2.longitudeColName, "DOUBLE");
    table2.columns.emplace_back(lsst::qserv::SUB_CHUNK_COLUMN, "INT");

    success = success && addTable(table2);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(table2.database);
        bool passed = (databaseInfo.tables().size() == 2) && databaseInfo.tableExists(table2.name) &&
                      (databaseInfo.findTable(table2.name) == table2);
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 2 TABLES SHOULD EXIST NOW"
             << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding the first dependent table connected to the second director.
    TableInfo table1of2;
    table1of2.name = "dependent-1-of-2";
    table1of2.database = database;
    table1of2.isPartitioned = true;
    table1of2.directorTable = DirectorTableRef("director-2", "director_id");
    table1of2.columns.emplace_back(table1of2.directorTable.primaryKeyColumn(), "INT UNSIGNED");

    success = success && addTable(table1of2);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(database);
        bool passed = (databaseInfo.tables().size() == 3) && databaseInfo.tableExists(table1of2.name) &&
                      (databaseInfo.findTable(table1of2.name) == table1of2);
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 3 TABLES SHOULD EXIST NOW"
             << "\n";
        dumpDatabasesAsTable(indent, "");
        success = success && passed;
    }

    // Adding the second dependent table connected to the second director.
    TableInfo table2of2;
    table2of2.name = "dependent-2-of-2";
    table2of2.isPartitioned = true;
    table2of2.directorTable = DirectorTableRef("director-2", "director_id_key");
    table2of2.latitudeColName = "decl";
    table2of2.longitudeColName = "ra";
    table2of2.columns.emplace_back(table2of2.directorTable.primaryKeyColumn(), "INT UNSIGNED");
    table2of2.columns.emplace_back(table2of2.latitudeColName, "DOUBLE");
    table2of2.columns.emplace_back(table2of2.longitudeColName, "DOUBLE");

    success = success && addTable(table2of2);
    {
        DatabaseInfo const databaseInfo = config()->databaseInfo(table2of2.database);
        bool passed = (databaseInfo.tables().size() == 4) && databaseInfo.tableExists(table2of2.name) &&
                      (databaseInfo.findTable(table2of2.name) == table2of2);
        cout << (passed ? PASSED_STR : FAILED_STR) << " EXACTLY 4 TABLES SHOULD EXIST NOW"
             << "\n";
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
            cout << indent << " ERROR: "
                 << "failed to delete database family '" << family << "', ex: " << string(ex.what()) << "\n";
            cout << "\n";
            return false;
        }
    }
    return success;
}

}  // namespace lsst::qserv::replica
