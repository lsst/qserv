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
#ifndef LSST_QSERV_REPLICA_REDIRECTORHTTPAPP_H
#define LSST_QSERV_REPLICA_REDIRECTORHTTPAPP_H

// Qserv headers
#include "replica/Application.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class RedirectorHttpApp implements represents the worker registration (redirection)
 * service that's used by the workers to report themselves and by the controllers
 * to locate connection and configuration parameters of the workers. The service can be
 * used to obtain the run-time status of the workers for the system monitoring purposes.
 * 
 * The service is implemented as the REST/HTTP server.
 */
class RedirectorHttpApp: public Application {
public:
    typedef std::shared_ptr<RedirectorHttpApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc  The number of command-line arguments.
     * @param argv  A vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    RedirectorHttpApp() = delete;
    RedirectorHttpApp(RedirectorHttpApp const&) = delete;
    RedirectorHttpApp& operator=(RedirectorHttpApp const&) = delete;

    virtual ~RedirectorHttpApp() final = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see RedirectorHttpApp::create()
    RedirectorHttpApp(int argc, char* argv[]);
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_REDIRECTORHTTPAPP_H */
