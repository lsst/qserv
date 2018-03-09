/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// replica_mysql_test.cc is for testing the MySQL API used by
/// the Replication system implementation.

// System headers
#include <functional>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <streambuf>
#include <string>

// Qserv headers
#include "replica/DatabaseMySQL.h"
#include "replica/FileUtils.h"
#include "util/CmdLineParser.h"

namespace replica  = lsst::qserv::replica;
namespace database = lsst::qserv::replica::database::mysql;
namespace util     = lsst::qserv::util;

namespace {

// Command line parameters

std::string operation;

bool noAutoReconnect;
bool noTransaction;
bool noResultSet;

database::ConnectionParams connectionParams;

std::string databaseName;
std::string fileName;


// Run various test on transactions
void runTransactionTest(database::Connection::pointer const& conn,
                        std::string const& testName,
                        std::function<void(database::Connection::pointer const& conn)> func) {
    try {
        std::cout << "transaction is " << (conn->inTransaction() ? "" : "NOT ") << "active" << std::endl;
        func(conn);
        std::cout << "transaction test [PASSED]: '" << testName << "'" << std::endl;   
    } catch (std::logic_error const& ex) {
        std::cout << "transaction test [FAILED]: '" << testName << "' " << ex.what() << std::endl; 
    }
}
void testTransactions(database::Connection::pointer const& conn) {

    runTransactionTest(conn, "begin,commit", [] (database::Connection::pointer const& conn) {
        conn->begin();
        conn->commit();
    });
    runTransactionTest(conn, "begin,rollback", [] (database::Connection::pointer const& conn) {
        conn->begin();
        conn->rollback();
    });
    runTransactionTest(conn, "begin,begin", [] (database::Connection::pointer const& conn) {
        conn->begin();
        conn->begin();
    });
    runTransactionTest(conn, "commit", [] (database::Connection::pointer const& conn) {
        conn->commit();
    });
    runTransactionTest(conn, "rollback", [] (database::Connection::pointer const& conn) {
        conn->rollback();
    });
    runTransactionTest(conn, "begin,commit,rollback", [] (database::Connection::pointer const& conn) {
        conn->begin();
        conn->commit();
        conn->rollback();
    });
    runTransactionTest(conn, "begin,rollback,commit", [] (database::Connection::pointer const& conn) {
        conn->begin();
        conn->rollback();
        conn->commit();
    });
}

/// Create a new database
void createDatabase(database::Connection::pointer const& conn) {
    try {
        conn->execute("CREATE DATABASE " + databaseName);
    } catch (std::logic_error const& ex) {
        std::cout << ex.what() << std::endl; 
    }
}

/// Drop an existing database
void dropDatabase(database::Connection::pointer const& conn) {
    try {
        conn->execute ("DROP DATABASE " + databaseName);
    } catch (std::logic_error const& ex) {
        std::cout << ex.what() << std::endl; 
    }
}

/// Read a query from a file, execute it and (if requested)
/// explore ita results
void query(database::Connection::pointer const& conn) {

    // Read q query from the standard input or from a file into a string.

    std::string query;
    if (fileName == "-") {
        query = std::string((std::istreambuf_iterator<char>(std::cin)),
                             std::istreambuf_iterator<char>());
    } else {

        // Note a little optimization in which the algorithm detemines the file
        // size and preallocates the string buffer before
        // perforмing the actual read.

        std::ifstream fs(fileName);
        if (not fs) {
            std::cerr << "faild to read the contents of file: " << fileName << std::endl;
            return;
        }
        fs.seekg(0, std::ios::end);   
        query.reserve(fs.tellg());
        fs.seekg(0, std::ios::beg);
        query.assign((std::istreambuf_iterator<char>(fs)),
                      std::istreambuf_iterator<char>());
    }    
    std::cout << "Query: " << query << std::endl;

    // Execute the query

    try {
        if (not noTransaction) { conn->begin(); }
        {
            conn->execute(query);
            std::cout << "hasResult: " << (conn->hasResult() ? "true" : "false") << std::endl;

            if (not noResultSet and conn->hasResult()) {
                std::cout << "Columns:   ";
                for (std::string const& name: conn->columnNames()) {
                    std::cout << "'" << name << "', ";
                }
                std::cout << "\n" << std::endl;

                database::Row row;
                while (conn->next(row)) {
                    for (std::string const& name: conn->columnNames()) {
                        std::string val;
                        bool const notNull = row.get(name, val);
                        std::cout << name << ": " << (notNull ? "'" + val + "'" : "NULL") << ", ";
                    }
                    std::cout << "\n";
                    for (size_t i = 0; i < row.numColumns(); ++i) {
                        std::string val;
                        bool const notNull = row.get(i, val);
                        std::cout << i << ": " << (notNull ? "'" + val + "'" : "NULL") << ", ";   
                    }
                    std::cout << "\n";
                }
            }
        }
        if (not noTransaction) { conn->commit(); }

    } catch (std::logic_error const& ex) {
        std::cout << ex.what() << std::endl; 
    }
}

/// Run the test
bool test() {

    try {
        database::Connection::pointer const conn =
            database::Connection::open(connectionParams, !noAutoReconnect);
        
        if      ("TEST_TRANSACTIONS" == operation) { testTransactions(conn); }
        else if ("CREATE_DATABASE"   == operation) { createDatabase(conn); }
        else if ("DROP_DATABASE"     == operation) { dropDatabase(conn); }
        else if ("QUERY"             == operation) { query(conn); }

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return true;
}
} /// namespace

int main(int argc, const char* const argv[]) {

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <operation> [<parameter> [<parameter> [...]]]\n"
            "              [--no-auto-reconnect] [--no-transaction] [--no-result-set]\n"
            "              [--host=<name>] [--port=<number>]\n"
            "              [--user=<name>] [--password=<secret>]\n"
            "              [--default-database=<name>]\n"
            "\n"
            "Supported operations and mandatory parameters:\n"
            "    TEST_TRANSACTIONS\n"
            "\n"
            "    CREATE_DATABASE <database>\n"
            "    DROP_DATABASE   <database>\n"
            "\n"
            "    QUERY <file>\n"
            "\n"
            "Parameters:\n"
            "  database             - the name of a database\n"
            "  file                 - the name of a file from which to read a SQL statement.\n"
            "                         If the file name is set to '-' then statement will be read\n"
            "                         from the standard input stream.\n"
            "Flags and options:\n"
            "  --no-auto-reconnect  - do *NOT* auto-reconnect to the service if its drops the connection\n"
            "                         after a long period of the applicatin inactivity\n"
            "  --no-transaction     - do *NOT* start/commit transactions when executing\n"
            "                         database queries\n"
            "  --no-result-set      - do *NOT* explore the result set after executing statements\n"
            "  --host               - the DNS name or IP address of a host where the service runs\n"
            "                         [ DEFAULT: localhost]\n"
            "  --port               - the port number for the MySQL service\n"
            "                         [ DEFAULT: 3306]\n"
            "  --user               - the name of the MySQL user account\n"
            "  --password           - a password to log into the MySQL user account\n"
            "  --default-database   - the name of the default database to connect to\n"
            "                         [ DEFAULT: '']\n");

        ::operation = parser.parameterRestrictedBy(
            1, {"TEST_TRANSACTIONS",
                "CREATE_DATABASE",
                "DROP_DATABASE",
                "QUERY"}
        );
        if (parser.in(::operation, {
            "CREATE_DATABASE",
            "DROP_DATABASE"})) {
            ::databaseName = parser.parameter<std::string>(2);
        }
        if (parser.in(::operation, {
            "QUERY"})) {
            ::fileName = parser.parameter<std::string>(2);
        }
        ::noAutoReconnect           = parser.flag("no-auto-reconnect");
        ::noTransaction             = parser.flag("no-transaction");
        ::noResultSet               = parser.flag("no-result-set");
        ::connectionParams.host     = parser.option<std::string>("host", "localhost");
        ::connectionParams.port     = parser.option<uint16_t>("port", 3306);
        ::connectionParams.user     = parser.option<std::string>("user", replica::FileUtils::getEffectiveUser());
        ::connectionParams.password = parser.option<std::string>("password", "");
        ::connectionParams.database = parser.option<std::string>("default-database", "");

    } catch (std::exception const& ex) {
        return 1;
    }  
    ::test();
    return 0;
}