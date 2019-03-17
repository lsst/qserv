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
#ifndef LSST_QSERV_REPLICA_MYSQLTESTAPP_H
#define LSST_QSERV_REPLICA_MYSQLTESTAPP_H

// System headers
#include <functional>
#include <string>

// Qserv headers
#include "replica/Application.h"
#include "replica/DatabaseMySQL.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class MySQLTestApp implements a tool for testing the MySQL API used by
 * the Replication system implementation.
 */
class MySQLTestApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MySQLTestApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    // Default construction and copy semantics are prohibited

    MySQLTestApp()=delete;
    MySQLTestApp(MySQLTestApp const&)=delete;
    MySQLTestApp& operator=(MySQLTestApp const&)=delete;

    ~MySQLTestApp() final=default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see MySQLTestApp::create()
    MySQLTestApp(int argc, char* argv[]);

    /**
     * Run various test on transactions
     * 
     * @param testName
     *   the name of a test
     * 
     * @param func
     *   the function to be executed as a test
     */
    void _runTransactionTest(std::string const& testName,
                             std::function<void(database::mysql::Connection::Ptr const&)> func) const;

    /**
     * The actual test for transactions is here
     */
    void _testTransactions() const;

    /// Create a new database
    void _createDatabase() const;

    /// Drop an existing database
    void _dropDatabase() const;

    /**
     * Execute the query and (if requested) explore its result set
     * 
     * @param query
     *   the query to be executed
     */
    void _executeQuery(std::string const& query) const;

    /**
     * Execute the query and (if requested) explore its result set
     * 
     * @param query
     *   the query to be executed
     */
    void _executeQueryWait(std::string const& query) const;

    /**
     * @return return a query to be read from the standard input or from a file 
     */
    std::string _getQuery() const;

    /// The name of a test
    std::string _operation;

    /// Do NOT start/commit transactions when executing database queries
    bool _noTransaction = false;

    /// Do NOT explore the result set after executing statements
    bool _noResultSet = false;

    /// Print the number of rows for queries instead of their full content
    bool _resultSummaryOnly = false;

    /// The name of a database
    std::string _databaseName;

    /// The name of a file from which to read a SQL statement.
    /// If the file name is set to '-' then statement will be read
    /// from the standard input stream.
    std::string _fileName;

    /// The number of iterations (the times the same operation would be repeated))
    unsigned int _numIter = 1;
    
    /// The optional delay between iterations
    unsigned int _iterDelayMillisec = 0;

    /// The connection to be open before performing the tests
    database::mysql::Connection::Ptr _conn;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_MYSQLTESTAPP_H */
