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
#ifndef LSST_QSERV_REPLICA_WORKERSERVER_H
#define LSST_QSERV_REPLICA_WORKERSERVER_H

// System headers
#include <memory>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/services/ServiceProvider.h"
#include "replica/worker/WorkerProcessor.h"
#include "replica/worker/WorkerServerConnection.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerServer is used for handling incoming connections to
 * the worker replication service. Only one instance of this class is
 * allowed per a thread.
 */
class WorkerServer : public std::enable_shared_from_this<WorkerServer> {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<WorkerServer> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   provider is needed to access the Configuration of a setup
     *   and for validating the input parameters
     *
     * @param workerName
     *   the name of a worker this instance represents
     *
     * @return
     *   pointer to the new object created by the factory
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName);

    // Default construction and copy semantics are prohibited

    WorkerServer() = delete;
    WorkerServer(WorkerServer const&) = delete;
    WorkerServer& operator=(WorkerServer const&) = delete;

    ~WorkerServer() = default;

    /// @return the name of a worker this server runs for
    std::string const& worker() const { return _workerName; }

    /**
     * The processor object can be used for detailed monitoring of
     * the on-going activities and statistics collection if needed.
     *
     * @return
     *   reference to the request processor
     */
    WorkerProcessor::Ptr const& processor() const { return _processor; }

    /**
     * Begin listening for and processing incoming connections.
     *
     * @note
     *   This method is blocking, so it can be called just once from
     *   a thread. Calling it from different threads won't work because
     *   of a port conflict.
     */
    void run();

private:
    /// @see WorkerServer::create()
    WorkerServer(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName);

    /**
     * Begin (asynchronously) accepting connection requests.
     */
    void _beginAccept();

    /**
     * Handle a connection request once it's detected. The rest of
     * the communication will be forwarded to the connection object
     * specified as a parameter of the method.
     *
     * @param connection
     *   object responsible for communications with a client
     *
     * @param ec
     *   error condition to be checked for
     */
    void _handleAccept(WorkerServerConnection::Ptr const& connection, boost::system::error_code const& ec);

    /// @return the context string
    std::string context() const { return "SERVER  "; }

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    std::string const _workerName;

    /// This is pointer onto an object where the requests would
    /// get processed. This object gets created by the constructor of
    /// the class.
    WorkerProcessor::Ptr const _processor;

    boost::asio::io_service _io_service;
    boost::asio::ip::tcp::acceptor _acceptor;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERSERVER_H
