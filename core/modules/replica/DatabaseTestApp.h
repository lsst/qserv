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
#ifndef LSST_QSERV_REPLICA_DATABASETESTAPP_H
#define LSST_QSERV_REPLICA_DATABASETESTAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class DatabaseTestApp implements a tool for testing the DatabaseServices API
 * used by the Replication system implementation.
 */
class DatabaseTestApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseTestApp> Ptr;

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

    DatabaseTestApp()=delete;
    DatabaseTestApp(DatabaseTestApp const&)=delete;
    DatabaseTestApp& operator=(DatabaseTestApp const&)=delete;

    ~DatabaseTestApp() override=default;

protected:

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /**
     * @see DatabaseTestApp::create()
     */
    DatabaseTestApp(int argc, char* argv[]);

private:
    /// The name of a test
    std::string _operation;

    /// The maximum number of replicas to be returned
    size_t _maxReplicas = 1;

    /// Limit a scope of an operation to workers which are presently enabled in
    /// the Replication system.
    bool _enabledWorkersOnly = false;

    /// The chunk number
    unsigned int _chunk = 0;

    /// The name of a worker
    std::string _workerName;

    /// The name of a database
    std::string _databaseName;

    /// The name of a database family
    std::string _databaseFamilyName;

    /// The number of rows in the table of replicas (0 means no pages)
    size_t _pageSize = 20;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_DATABASETESTAPP_H */
