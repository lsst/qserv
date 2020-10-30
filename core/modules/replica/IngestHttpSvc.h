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

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "qhttp/Server.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class IngestHttpSvc is used for handling incoming REST API requests for
  * the table contribution uploads. Each instance of this class will be running
  * in its own thread.
  * 
  * @note The class's implementation starts its own collection of BOOST ASIO
  *   service threads as configured in Configuration.
  * @note The implementation of the class is not thread-safe.
  */
class IngestHttpSvc: public std::enable_shared_from_this<IngestHttpSvc>  {
public:
    typedef std::shared_ptr<IngestHttpSvc> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider For configuration, etc. services.
     * @param workerName The name of a worker this service is acting upon (used for
     *   checking consistency of the protocol).
     * @param authKey An authorization key for the catalog ingest operation.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& workerName,
                      std::string const& authKey);

    IngestHttpSvc() = delete;
    IngestHttpSvc(IngestHttpSvc const&) = delete;
    IngestHttpSvc& operator=(IngestHttpSvc const&) = delete;

    /// Non-trivial destructor is required to stop the HTTP server.
    ~IngestHttpSvc();

    /**
     * Run the server in a thread pool (as per the Configuration).
     * @note This is the blocking operation. Please, run it within its own thread if needed.
     * @throw std::logic_error  If trying to call the method while the service is already running.
     */
    void run();

private:
    /// @see IngestHttpSvc::create()
    IngestHttpSvc(ServiceProvider::Ptr const& serviceProvider,
                  std::string const& workerName,
                  std::string const& authKey);

    /// @return The context string to be used for the message logging.
    std::string _context() const { return "INGEST-HTTP-SVC  "; }

   // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    std::string const _workerName;
    std::string const _authKey;

    boost::asio::io_service _io_service;
    qhttp::Server::Ptr _httpServer;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INGESTHTTPSVC_H
