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
#ifndef LSST_QSERV_REPLICA_INGESTSVC_H
#define LSST_QSERV_REPLICA_INGESTSVC_H

/**
 * This header declares class IngestSvc which is used as a worker-side
 * end point for ingesting catalog data into the Qserv worker's MySQL database.
 */

// System headers
#include <memory>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/IngestSvcConn.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class IngestSvc is used for handling incoming connections to
  * the file delivery service. Each instance of this class will be running
  * in its own thread.
  */
class IngestSvc: public std::enable_shared_from_this<IngestSvc>  {
public:

    typedef std::shared_ptr<IngestSvc> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  for configuration, etc. services.
     * @param workerName the name of a worker this service is acting upon (used for
     *   checking consistency of the protocol).
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& workerName);

    IngestSvc() = delete;
    IngestSvc(IngestSvc const&) = delete;
    IngestSvc& operator=(IngestSvc const&) = delete;

    ~IngestSvc() = default;

    /// @return the name of a worker this server runs for
    std::string const& worker() const { return _workerName; }

    /**
     * Run the server in a thread pool (as per the Configuration)
     *
     * @note This is the blocking operation. Please, run it within its own
     *   thread if needed.
     */
    void run();

private:
    /// @see IngestSvc::create()
    IngestSvc(ServiceProvider::Ptr const& serviceProvider,
              std::string const& workerName);

    /**
     * Begin (asynchronously) accepting connection requests.
     */
    void _beginAccept();
    
    /**
     * Handle a connection request once it's detected. The rest of
     * the communication will be forwarded to the connection object
     * specified as a parameter of the method.
     */
    void _handleAccept(IngestSvcConn::Ptr const& connection,
                       boost::system::error_code const& ec);

    /// @return the context string to be used for the message logging
    std::string _context() const { return "INGEST-SVC  "; }

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    std::string          const _workerName;

    boost::asio::io_service        _io_service;
    boost::asio::ip::tcp::acceptor _acceptor;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INGESTSVC_H
