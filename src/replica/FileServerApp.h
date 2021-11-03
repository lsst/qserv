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
#ifndef LSST_QSERV_REPLICA_FILESERVERAPP_H
#define LSST_QSERV_REPLICA_FILESERVERAPP_H

// System headers
#include <memory>
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
 * Class FileServerApp runs an instance of a file server on behalf of
 * the specified (via a command line parameter) worker.
 */
class FileServerApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FileServerApp> Ptr;

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

    FileServerApp()=delete;
    FileServerApp(FileServerApp const&)=delete;
    FileServerApp& operator=(FileServerApp const&)=delete;

    ~FileServerApp() override=default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see FileServerApp::create()
    FileServerApp(int argc, char* argv[]);

    /// The name of a worker
    std::string _workerName;

    /// The flag triggering (if 'true') the periodic "heartbeat" printouts
    bool _verbose = false;

    /// Message logger
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_FILESERVERAPP_H */
