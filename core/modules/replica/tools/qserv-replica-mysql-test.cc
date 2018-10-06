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

/// qserv-replica-mysql-test.cc is for testing the MySQL API used by
/// the Replication system implementation.

// System headers
#include <functional>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <streambuf>
#include <string>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/FileUtils.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv;
namespace database = lsst::qserv::replica::database::mysql;

namespace {

// Command line parameters

std::string operation;

bool         databaseAllowReconnect        = replica::Configuration::databaseAllowReconnect();
unsigned int databaseConnectTimeoutSec     = replica::Configuration::databaseConnectTimeoutSec();
unsigned int databaseMaxReconnects         = replica::Configuration::databaseMaxReconnects();
unsigned int databaseTransactionTimeoutSec = replica::Configuration::databaseTransactionTimeoutSec();

bool noTransaction;
bool noResultSet;
bool resultSummaryOnly;

database::ConnectionParams connectionParams;

std::string databaseName;
std::string fileName;

unsigned int numIter = 1;           // run just once
unsigned int iterDelayMillisec = 0; // no delay between iterations

// Run various test on transactions
void runTransactionTest(database::Connection::Ptr const& conn,
                        std::string const& testName,
                        std::function<void(database::Connection::Ptr const& conn)> func) {
    try {
        std::cout << "transaction is " << (conn->inTransaction() ? "" : "NOT ") << "active" << std::endl;
        func(conn);
        std::cout << "transaction test [PASSED]: '" << testName << "'" << std::endl;
    } catch (std::logic_error const& ex) {
        std::cout << "transaction test [FAILED]: '" << testName << "' " << ex.what() << std::endl;
    }
}
void testTransactions(database::Connection::Ptr const& conn) {

    runTransactionTest(conn, "begin,commit", [] (database::Connection::Ptr const& conn) {
        conn->begin();
        conn->commit();
    });
    runTransactionTest(conn, "begin,rollback", [] (database::Connection::Ptr const& conn) {
        conn->begin();
        conn->rollback();
    });
    runTransactionTest(conn, "begin,begin", [] (database::Connection::Ptr const& conn) {
        conn->begin();
        conn->begin();
    });
    runTransactionTest(conn, "commit", [] (database::Connection::Ptr const& conn) {
        conn->commit();
    });
    runTransactionTest(conn, "rollback", [] (database::Connection::Ptr const& conn) {
        conn->rollback();
    });
    runTransactionTest(conn, "begin,commit,rollback", [] (database::Connection::Ptr const& conn) {
        conn->begin();
        conn->commit();
        conn->rollback();
    });
    runTransactionTest(conn, "begin,rollback,commit", [] (database::Connection::Ptr const& conn) {
        conn->begin();
        conn->rollback();
        conn->commit();
    });
}

/// Create a new database
void createDatabase(database::Connection::Ptr const& conn) {
    try {
        conn->execute("CREATE DATABASE " + databaseName);
    } catch (std::logic_error const& ex) {
        std::cout << ex.what() << std::endl;
    }
}

/// Drop an existing database
void dropDatabase(database::Connection::Ptr const& conn) {
    try {
        conn->execute ("DROP DATABASE " + databaseName);
    } catch (std::logic_error const& ex) {
        std::cout << ex.what() << std::endl;
    }
}

/// Execute the query and (if requested) explore its result set
void executeQuery(database::Connection::Ptr const& conn,
                  std::string const& query) {

    try {

        if (not noTransaction) conn->begin();

        conn->execute(query);
        std::cout << "hasResult: " << (conn->hasResult() ? "true" : "false") << std::endl;

        if (conn->hasResult()) {
        
            if (not noResultSet) {

                if (not resultSummaryOnly) {

                    // Print the result set content

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

                } else {

                    // Just report the number of rows in the result set
                    size_t numRows = 0;
                    database::Row row;
                    while (conn->next(row)) {
                        ++numRows;
                    }
                    std::cout << "numRows:   " << numRows << "\n";
                }
            }
        }
        if (not noTransaction) conn->commit();
        return;

    } catch (std::logic_error const& ex) {
        std::cout << ex.what() << std::endl;
    }
    if (not noTransaction) {
        if (conn->inTransaction()) conn->rollback();
    }
}

/// Execute it and (if requested) explore its results
void executeQueryWait(database::Connection::Ptr const& conn,
                      std::string const& query) {
    conn->execute(
        std::bind(
            executeQuery,
            std::placeholders::_1,
            query
        )
    );
}

/// Read a query from the standard input or from a file into a string.
std::string getQuery() {

    std::string query;
    if (fileName == "-") {
        query = std::string((std::istreambuf_iterator<char>(std::cin)),
                             std::istreambuf_iterator<char>());
    } else {

        // Note a little optimization in which the algorithm detemines the file
        // size and preallocates the string buffer before
        // performing the actual read.

        std::ifstream fs(fileName);
        if (not fs) {
            std::cerr << "failed to read the contents of file: " << fileName << std::endl;
            return query;
        }
        fs.seekg(0, std::ios::end);
        query.reserve(fs.tellg());
        fs.seekg(0, std::ios::beg);
        query.assign(std::istreambuf_iterator<char>(fs),
                     std::istreambuf_iterator<char>());
    }
    std::cout << "Query: " << query << std::endl;

    return query;
}

/// Run the test
void test() {

    try {

        // Change default parameters of the database connectors

        replica::Configuration::setDatabaseAllowReconnect(databaseAllowReconnect);
        replica::Configuration::setDatabaseConnectTimeoutSec(databaseConnectTimeoutSec);
        replica::Configuration::setDatabaseMaxReconnects(databaseMaxReconnects);
        replica::Configuration::setDatabaseTransactionTimeoutSec(databaseTransactionTimeoutSec);

        std::string query;
        if (("QUERY" == operation) or ("QUERY_WAIT" == operation)) {
            query = getQuery();
            if (query.empty()) {
                std::cerr << "error: no query provided" << std::endl;
                return;
            }
        }

        auto conn = database::Connection::open(connectionParams);

        util::BlockPost blockPost(iterDelayMillisec, iterDelayMillisec + 1);

        for (unsigned int i = 0; i < numIter; ++i) {

            if      ("TEST_TRANSACTIONS" == operation) testTransactions(conn);
            else if ("CREATE_DATABASE"   == operation) createDatabase(conn);
            else if ("DROP_DATABASE"     == operation) dropDatabase(conn);
            else if ("QUERY"             == operation) executeQuery(conn, query);
            else if ("QUERY_WAIT"        == operation) executeQueryWait(conn, query);

            if (iterDelayMillisec > 0) blockPost.wait();
        }

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return;
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
            "\n"
            "  <operation> [<parameter> [<parameter> [...]]]\n"
            "\n"
            "              [--db-allow-reconnect=<flag>]\n"
            "              [--db-reconnect-timeout=<sec>]\n"
            "              [--db-max-reconnects=<num>]\n"
            "              [--db-transaction-timeout=<sec>]\n"
            "\n"
            "              [--no-transaction]\n"
            "              [--no-result-set]\n"
            "              [--result-summary-only]\n"
            "\n"
            "              [--host=<name>]\n"
            "              [--port=<number>]\n"
            "              [--user=<name>]\n"
            "              [--password=<secret>]\n"
            "              [--default-database=<name>]\n"
            "\n"
            "              [--iter=<num>]\n"
            "              [--iter-delay=<ms>]\n"
            "\n"
            "Supported operations and mandatory parameters:\n"
            "\n"
            "    TEST_TRANSACTIONS\n"
            "\n"
            "    CREATE_DATABASE <database>\n"
            "    DROP_DATABASE   <database>\n"
            "\n"
            "    QUERY      <file>\n"
            "    QUERY_WAIT <file>\n"
            "\n"
            "Parameters:\n"
            "\n"
            "    <database> \n"
            "\n"
            "      the name of a database\n"
            "\n"
            "    <file> \n"
            "\n"
            "      the name of a file from which to read a SQL statement.\n"
            "      If the file name is set to '-' then statement will be read\n"
            "      from the standard input stream.\n"
            "\n"
            "Flags and options:\n"
            "\n"
            "    --db-allow-reconnect \n"
            "\n"
            "      change the default database connecton handling node. Set 0 to disable automatic\n"
            "      reconnects. Any other number would man an opposite scenario.\n"
            "      DEFAULT: " + std::to_string(::databaseAllowReconnect ? 1 : 0) + "\n"
            "\n"
            "    --db-reconnect-timeout \n"
            "\n"
            "      change the default value limiting a duration of time for making automatic\n"
            "      reconnects to a database server before failing and reporting error (if the server\n"
            "      is not up, or if it's not reachable for some reason)\n"
            "      DEFAULT: " + std::to_string(::databaseConnectTimeoutSec) + "\n"
            "\n"
            "    --db-max-reconnects\n"
            "\n"
            "      change the default value limiting a number of attempts to repeat a sequence\n"
            "      of queries due to connection losses and subsequent reconnects before to fail.\n"
            "      DEFAULT: " + std::to_string(::databaseMaxReconnects) + "\n"
            "\n"
            "    --db-transaction-timeout \n"
            "\n"
            "      change the default value limiting a duration of each attempt to execute\n"
            "      a database transaction before to fail.\n"
            "      DEFAULT: " + std::to_string(::databaseTransactionTimeoutSec) + "\n"
            "\n"
            "    --no-transaction \n"
            "\n"
            "      do *NOT* start/commit transactions when executing\n"
            "      database queries\n"
            "\n"
            "    --no-result-set \n"
            "\n"
            "      do *NOT* explore the result set after executing statements\n"
            "\n"
            "    --result-summary-only \n"
            "\n"
            "      print the number of rows for queries instead of their full content\n"
            "\n"
            "    --host \n"
            "\n"
            "      the DNS name or IP address of a host where the service runs."
            "      DEFAULT: '" + ::connectionParams.host + "'\n"
            "\n"
            "    --port \n"
            "\n"
            "      the port number for the MySQL service\n"
            "      DEFAULT: " + std::to_string(::connectionParams.port) + "\n"
             "\n"
            "    --user \n"
            "\n"
            "      the name of the MySQL user account\n"
            "      DEFAULT: '" + ::connectionParams.user + "'\n"
            "\n"
            "    --password \n"
            "\n"
            "      user password to log into the MySQL user account\n"
            "      DEFAULT: '" + ::connectionParams.password + "'\n"
            "\n"
            "    --default-database \n"
            "\n"
            "      the name of the default database to connect to\n"
            "      DEFAULT: '" + ::connectionParams.database + "'\n"
            "\n"
            "    --iter \n"
            "\n"
            "      the number of iterations\n"
            "      DEFAULT: " + std::to_string(::numIter) + "\n"
            "\n"
            "    --iter-delay \n"
            "\n"
            "      interval (milliseconds) between iterations\n"
            "      DEFAULT: " + std::to_string(::iterDelayMillisec) + "\n");

        ::operation = parser.parameterRestrictedBy(
            1, {"TEST_TRANSACTIONS",
                "CREATE_DATABASE",
                "DROP_DATABASE",
                "QUERY",
                "QUERY_WAIT"}
        );
        if (parser.in(::operation, {
            "CREATE_DATABASE",
            "DROP_DATABASE"})) {
            ::databaseName = parser.parameter<std::string>(2);
        }
        if (parser.in(::operation, {
            "QUERY",
            "QUERY_WAIT"})) {
            ::fileName = parser.parameter<std::string>(2);
        }
        ::noTransaction     = parser.flag("no-transaction");
        ::noResultSet       = parser.flag("no-result-set");
        ::resultSummaryOnly = parser.flag("result-summary-only");

        ::connectionParams.host     = parser.option<std::string>("host",             ::connectionParams.host);
        ::connectionParams.port     = parser.option<uint16_t>(   "port",             ::connectionParams.port);
        ::connectionParams.user     = parser.option<std::string>("user",             ::connectionParams.user);
        ::connectionParams.password = parser.option<std::string>("password",         ::connectionParams.password);
        ::connectionParams.database = parser.option<std::string>("default-database", ::connectionParams.database);

        ::databaseAllowReconnect        = parser.option<unsigned int>("db-allow-reconnect",     ::databaseAllowReconnect);
        ::databaseConnectTimeoutSec     = parser.option<unsigned int>("db-reconnect-timeout",   ::databaseConnectTimeoutSec);
        ::databaseMaxReconnects         = parser.option<unsigned int>("db-max-reconnects",      ::databaseMaxReconnects);
        ::databaseTransactionTimeoutSec = parser.option<unsigned int>("db-transaction-timeout", ::databaseTransactionTimeoutSec);

        ::numIter              = parser.option<unsigned int>("iter",       ::numIter);
        ::iterDelayMillisec    = parser.option<unsigned int>("iter-delay", ::iterDelayMillisec);

    } catch (std::exception const& ex) {
        return 1;
    }
    ::test();
    return 0;
}
