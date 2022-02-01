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
#include "replica/MySQLTestApp.h"

#include <fstream>
#include <iostream>
#include <stdexcept>
#include <streambuf>

// Qserv headers
#include "replica/Configuration.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "This application is for testing the MySQL API used by"
    " the Replication system implementation.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = true;

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

MySQLTestApp::Ptr MySQLTestApp::create(int argc, char* argv[]) {
    return Ptr(new MySQLTestApp(argc, argv));
}


MySQLTestApp::MySQLTestApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            ::injectDatabaseOptions,
            ::boostProtobufVersionCheck,
            ::enableServiceProvider
        ) {

    // Configure the command line parser

    parser().commands(
        "operation",
        {"TEST_TRANSACTIONS", "CREATE_DATABASE", "DROP_DATABASE", "QUERY", "QUERY_WAIT"},
        _operation
    );

    parser().command(
        "TEST_TRANSACTIONS"
    ).description(
        "Test the transactions API by beginning, committing or rolling back transactions."
    );

    parser().command(
        "CREATE_DATABASE"
    ).description(
        "Create a new database."
    ).required(
        "database",
        "The name of a database to be created.",
        _databaseName
    );

    parser().command(
        "DROP_DATABASE"
    ).description(
        "Drop an existing database."
    ).required(
        "database",
        "The name of a database to be deleted.",
        _databaseName
    );

    parser().command(
        "QUERY"
    ).description(
        "Read a query from a file and execute it using the traditional method which"
        " wouldn't attempt to repeat the transaction after connection loss and"
        " subsequent reconnects."
    ).required(
        "query",
        "The name of a file from which to read a SQL statement."
        " If the file name is set to '-' then statement will be read"
        " from the standard input stream.",
        _fileName
    );

    parser().command(
        "QUERY_WAIT"
    ).description(
        "Read a query from a file and execute it using the advanced method which"
        " would attempt to repeat the transaction after connection looses and"
        " subsequent reconnects."
    ).required(
        "query",
        "The name of a file from which to read a SQL statement."
        " If the file name is set to '-' then statement will be read"
        " from the standard input stream.",
        _fileName
    );
}


int MySQLTestApp::runImpl() {

    string query;
    if (("QUERY" == _operation) or ("QUERY_WAIT" == _operation)) {
        query = _getQuery();
        if (query.empty()) {
            cerr << "error: no query provided" << endl;
            return 1;
        }
    }

    auto const config = serviceProvider()->config();
    database::mysql::ConnectionParams const connectionParams(
        config->get<string>("database", "host"),
        config->get<uint16_t>("database", "port"),
        config->get<string>("database", "user"),
        config->get<string>("database", "password"),
        config->get<string>("database", "name")
    );
    _conn = database::mysql::Connection::open(connectionParams);

    util::BlockPost blockPost(_iterDelayMillisec, _iterDelayMillisec + 1);

    for (unsigned int i = 0; i < _numIter; ++i) {
        if      ("TEST_TRANSACTIONS" == _operation) _testTransactions();
        else if ("CREATE_DATABASE"   == _operation) _createDatabase();
        else if ("DROP_DATABASE"     == _operation) _dropDatabase();
        else if ("QUERY"             == _operation) _executeQuery(query);
        else if ("QUERY_WAIT"        == _operation) _executeQueryWait(query);
        if (_iterDelayMillisec > 0) blockPost.wait();
    }
    return 0;
}


void MySQLTestApp::_runTransactionTest(string const& testName,
                                       function<void(database::mysql::Connection::Ptr const&)> func) const {
    try {
        cout << "transaction is " << (_conn->inTransaction() ? "" : "NOT ") << "active" << endl;
        func(_conn);
        cout << "transaction test [PASSED]: '" << testName << "'" << endl;
    } catch (logic_error const& ex) {
        cout << "transaction test [FAILED]: '" << testName << "' " << ex.what() << endl;
    }
}


void MySQLTestApp::_testTransactions() const {

    _runTransactionTest("begin,commit", [] (decltype(_conn) const& conn) {
        conn->begin();
        conn->commit();
    });
    _runTransactionTest("begin,rollback", [] (decltype(_conn) const& conn) {
        conn->begin();
        conn->rollback();
    });
    _runTransactionTest("begin,begin", [] (decltype(_conn) const& conn) {
        conn->begin();
        conn->begin();
    });
    _runTransactionTest("commit", [] (decltype(_conn) const& conn) {
        conn->commit();
    });
    _runTransactionTest("rollback", [] (decltype(_conn) const& conn) {
        conn->rollback();
    });
    _runTransactionTest("begin,commit,rollback", [] (decltype(_conn) const& conn) {
        conn->begin();
        conn->commit();
        conn->rollback();
    });
    _runTransactionTest("begin,rollback,commit", [] (decltype(_conn) const& conn) {
        conn->begin();
        conn->rollback();
        conn->commit();
    });
}


void MySQLTestApp::_createDatabase() const {
    try {
        _conn->execute("CREATE DATABASE " + _databaseName);
    } catch (logic_error const& ex) {
        cout << ex.what() << endl;
    }
}


void MySQLTestApp::_dropDatabase() const {
    try {
        _conn->execute ("DROP DATABASE " + _databaseName);
    } catch (logic_error const& ex) {
        cout << ex.what() << endl;
    }
}


void MySQLTestApp::_executeQuery(string const& query) const {
    try {
        if (not _noTransaction) _conn->begin();

        _conn->execute(query);
        cout << "hasResult: " << (_conn->hasResult() ? "true" : "false") << endl;

        if (_conn->hasResult()) {
            if (not _noResultSet) {
                if (not _resultSummaryOnly) {

                    // Print the result set content
                    cout << "Columns:   ";
                    for (auto&& name: _conn->columnNames()) {
                        cout << "'" << name << "', ";
                    }
                    cout << "\n" << endl;
        
                    database::mysql::Row row;
                    while (_conn->next(row)) {

                        // Since this is a test/demo application for the MySQL API then cells
                        // from each row are printed twice: first - via their names, second
                        // time - via their relative numbers.
                        for (auto&& name: _conn->columnNames()) {
                            string val;
                            bool const notNull = row.get(name, val);
                            cout << name << ": " << (notNull ? "'" + val + "'" : "NULL") << ", ";
                        }
                        cout << "\n";
                        for (size_t i = 0; i < row.numColumns(); ++i) {
                            string val;
                            bool const notNull = row.get(i, val);
                            cout << i << ": " << (notNull ? "'" + val + "'" : "NULL") << ", ";
                        }
                        cout << "\n";
                    }
                } else {
                    // Just report the number of rows in the result set
                    size_t numRows = 0;
                    database::mysql::Row row;
                    while (_conn->next(row)) {
                        ++numRows;
                    }
                    cout << "numRows:   " << numRows << "\n";
                }
            }
        }
        if (not _noTransaction) _conn->commit();
        return;
    } catch (logic_error const& ex) {
        cout << ex.what() << endl;
    }
    if (not _noTransaction) {
        if (_conn->inTransaction()) _conn->rollback();
    }
}


void MySQLTestApp::_executeQueryWait(string const& query) const {
    _conn->execute([&] (decltype(_conn) const& conn) {
        _executeQuery(query);
    });
}


string MySQLTestApp::_getQuery() const {
    string query;
    if (_fileName == "-") {
        query = string(istreambuf_iterator<char>(cin),istreambuf_iterator<char>());
    } else {
        // Note a little optimization in which the algorithm determines the file
        // size and pre-allocates the string  buffer before
        // performing the actual read.
        ifstream fs(_fileName);
        if (!fs) {
            cerr << "failed to read the contents of file: " << _fileName << endl;
            return query;
        }
        getline(fs, query, '\0');
    }
    cout << "Query: " << query << endl;
    return query;
}

}}} // namespace lsst::qserv::replica
