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
#ifndef LSST_QSERV_REPLICA_SQLAPP_H
#define LSST_QSERV_REPLICA_SQLAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class SqlApp implements a tool which executes the same query
 * against worker databases of select workers. Result sets
 * will be reported upon a completion of the application.
 */
class SqlApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlApp> Ptr;

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

    SqlApp() = delete;
    SqlApp(SqlApp const&) = delete;
    SqlApp& operator=(SqlApp const&) = delete;

    ~SqlApp() final = default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see SqlApp::create()
    SqlApp(int argc, char* argv[]);

    /// Worker-side MySQL user account for executing the query
    std::string _user;

    /// Password for the MySQL account
    std::string _password;

    /// The query to be executed
    std::string _query;

    /// The maximum number of rows to be pulled from result
    /// sets at workers
    /// @note
    ///   This parameter nothing to do with the SQL's 'LIMIT <num-rows>'.
    ///   It serves as an additional fail safe mechanism preventing
    ///   protocol buffers from being overloaded by huge result sets
    ///   which might be accidentally initiated by users.
    uint64_t _maxRows = 10000;

    /// Permanently delete a worker from the Configuration
    bool _allWorkers = false;

    /// The maximum timeout to wait for the completion of the queries
    unsigned int _timeoutSec = 300;

    /// The number of rows in the table of replicas (0 means no pages)
    size_t _pageSize = 100;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_SQLAPP_H */
