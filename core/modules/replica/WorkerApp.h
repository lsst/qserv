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
#ifndef LSST_QSERV_REPLICA_WORKERAPP_H
#define LSST_QSERV_REPLICA_WORKERAPP_H

// System headers
#include <limits>
#include <string>

// Qserv headers
#include "replica/Application.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class WorkerApp implements represents a worker service.
 */
class WorkerApp : public Application {
public:
    typedef std::shared_ptr<WorkerApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc  The number of command-line arguments.
     * @param argv  A vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    WorkerApp() = delete;
    WorkerApp(WorkerApp const&) = delete;
    WorkerApp& operator=(WorkerApp const&) = delete;

    virtual ~WorkerApp() final = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see WorkerApp::create()
    WorkerApp(int argc, char* argv[]);

    /// Logger stream
    LOG_LOGGER _log;

    /// The name of a worker
    std::string _worker;
    
    /// A connection url for the MySQL service of the Qserv worker database.
    std::string _qservWorkerDbUrl;

    /// An authorization key for the catalog ingest operation.
    std::string _authKey;

    /// An "administrator"-level authorization key.
    std::string _adminAuthKey;

    /// The worker will create missing folders unless told not to do so by
    /// passing the corresponding command-line flag.
    bool _doNotCreateMissingFolders = false;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_WORKERAPP_H */
