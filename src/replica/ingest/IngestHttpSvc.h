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
#ifndef LSST_QSERV_REPLICA_INGESTHTTPSVC_H
#define LSST_QSERV_REPLICA_INGESTHTTPSVC_H

// System headers
#include <memory>
#include <string>
#include <thread>
#include <vector>

// Qserv headers
#include "replica/util/ChttpSvc.h"

// Forward declarations
namespace lsst::qserv::replica {
class IngestRequestMgr;
class ServiceProvider;
}  // namespace lsst::qserv::replica

namespace httplib {
class Server;
}  // namespace httplib

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestHttpSvc is used for handling incoming REST API requests for
 * the table contribution uploads. Each instance of this class will be running
 * in its own thread.
 *
 * @note The class's implementation starts its own collection of BOOST ASIO
 *   service threads as configured in Configuration.
 * @note The implementation of the class is not thread-safe.
 */
class IngestHttpSvc : public ChttpSvc {
public:
    /**
     * Create an instance of the service.
     *
     * @param serviceProvider For configuration, etc. services.
     * @param workerName The name of a worker this service is acting upon (used for
     *   checking consistency of the protocol).
     * @return A pointer to the created object.
     */
    static std::shared_ptr<IngestHttpSvc> create(std::shared_ptr<ServiceProvider> const& serviceProvider,
                                                 std::string const& workerName);

    IngestHttpSvc() = delete;
    IngestHttpSvc(IngestHttpSvc const&) = delete;
    IngestHttpSvc& operator=(IngestHttpSvc const&) = delete;

    virtual ~IngestHttpSvc() = default;

protected:
    /// @see HttpSvc::registerServices()
    virtual void registerServices(std::unique_ptr<httplib::Server> const& server) override;

private:
    /// @see IngestHttpSvc::create()
    IngestHttpSvc(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& workerName);

    // Input parameters
    std::string const _workerName;

    /// The manager maintains a collection of the ASYNC requests processed by
    /// the threads of the thread pool. The corresponding REST services interact
    /// with the manager to implement operations (submit, inspect, cancel, etc.)
    /// over requests on behalf of the user ingest workflows.
    std::shared_ptr<IngestRequestMgr> const _requestMgr;

    /// The thread pool for processing ASYNC requests.
    std::vector<std::unique_ptr<std::thread>> _threads;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTHTTPSVC_H
