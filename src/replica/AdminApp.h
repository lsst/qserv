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
#ifndef LSST_QSERV_REPLICA_ADMINAPP_H
#define LSST_QSERV_REPLICA_ADMINAPP_H

// System headers
#include <string>
#include <memory>

// Qserv headers
#include "replica/Application.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class AdminApp implements a Controller application for launching
 * worker management requests.
 */
class AdminApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<AdminApp> Ptr;

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

    AdminApp() = delete;
    AdminApp(AdminApp const&) = delete;
    AdminApp& operator=(AdminApp const&) = delete;

    ~AdminApp() override = default;

protected:

    /// @see Application::runImpl()
    int runImpl() final;

private:

    /// @see AdminApp::create()
    AdminApp(int argc, char* argv[]);


    /// The name of an operation to execute
    std::string _operation;

    /// The flag which if set allows selecting all workers for the operation
    bool _allWorkers = false;

    /// The flag which if set will trigger detailed report on remote requests
    bool _dumpRequestInfo = false;

    /// The maximum timeout for the management requests
    unsigned int _requestExpirationIvalSec = 10;

    /// the flag triggering progress report when executing batches of requests
    bool _progressReport = false;

    /// the flag triggering detailed report on failed requests
    bool _errorReport = false;

    /// Print vertical separator in tables
    bool _verticalSeparator = false;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_ADMINAPP_H */
