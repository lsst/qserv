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
#ifndef LSST_QSERV_REPLICA_WORKERALLAPP_H
#define LSST_QSERV_REPLICA_WORKERALLAPP_H

// System headers
#include <map>
#include <memory>

// Qserv headers
#include "replica/Application.h"

// LSST headers
#include "lsst/log/Log.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class WorkerRequestFactory;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class WorkerAllApp runs all worker servers within a single process.
 *
 * NOTE: a special single-node configuration is required by this test.
 * Also, each logical worker must get a unique path in a data file
 * system. The files must be read-write enabled for a user account
 * under which the test is run.
 */
class WorkerAllApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<WorkerAllApp> Ptr;

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

    WorkerAllApp() = delete;
    WorkerAllApp(WorkerAllApp const&) = delete;
    WorkerAllApp& operator=(WorkerAllApp const&) = delete;

    ~WorkerAllApp() final = default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see WorkerAllApp::create()
    WorkerAllApp(int argc, char* argv[]);

    /**
     * Launch all worker servers in dedicated detached threads. Also run
     * one extra thread per each worked for the 'hearbeat' monitoring.
     * 
     * @param workerRequestFactory
     *   A collection of the factories tuned for specific workers
     */
    void _runAllWorkers(std::map<std::string, std::shared_ptr<WorkerRequestFactory>>& workerRequestFactory);

    /// Logger stream
    LOG_LOGGER _log;

    /// Launch (if set to 'true') worker services for all known workers regardless
    /// of their configuration status (DISABLED or READ-ONLY).
    bool _allWorkers = false;

        /// Also run (if 'true') embedded file servers)
    bool _enableFileServer = false;
    
    /// A password for the MySQL account of the Qserv worker database.
    /// The account name is found in the Configuration.",
    std::string _qservDbPassword;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_WORKERALLAPP_H */
