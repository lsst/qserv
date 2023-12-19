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
#ifndef LSST_QSERV_REPLICA_QSERVWORKERPINGAPP_H
#define LSST_QSERV_REPLICA_QSERVWORKERPINGAPP_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/apps/Application.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class QservWorkerPingApp represents a command-line tool for testing a communication
 * path with Qserv workers. The application will be sending multiple requests containing
 * a string that is expected to be echoed back by a worker.
 */
class QservWorkerPingApp : public Application {
public:
    typedef std::shared_ptr<QservWorkerPingApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv The vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    QservWorkerPingApp() = delete;
    QservWorkerPingApp(QservWorkerPingApp const&) = delete;
    QservWorkerPingApp& operator=(QservWorkerPingApp const&) = delete;

    ~QservWorkerPingApp() override = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see QservWorkerPingApp::create()
    QservWorkerPingApp(int argc, char* argv[]);

    // Parameter value parsed from the command line.

    std::string _worker;  ///< The name of a worker.
    std::string _data;    ///< The data string to be sent to the worker.

    size_t _numRequests = 1;  ///< The total number of requests to be launched (>=1)
    size_t _maxRequests = 1;  ///< The maximum number of requests to be in flight at any moment (>=1).

    /// Requests will be cancelled if no response received before the specified timeout
    /// expires. Zero value of the parameter corresponds to the corresponding default set
    /// in the configuration.
    unsigned int _requestExpirationIvalSec = 0;

    bool _verbose = false;  ///< For reporting a progress of the testing.
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_QSERVWORKERPINGAPP_H */
