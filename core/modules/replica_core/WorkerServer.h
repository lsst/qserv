// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERSERVER_H
#define LSST_QSERV_REPLICA_CORE_WORKERSERVER_H

/// WorkerServer.h declares:
///
/// class WorkerServer
/// (see individual class documentation for more information)

// System headers

#include <boost/asio.hpp>

#include <memory>       // shared_ptr, enable_shared_from_this

// Qserv headers

#include "replica_core/WorkerProcessor.h"
#include "replica_core/WorkerServerConnection.h"

// Forward declarations


// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class ServiceProvider;
class WorkerInfo;
class WorkerRequestFactory;

/**
  * Class WorkerServer is used for handling incomming connections to
  * the worker replication service. Only one instance of this class is
  * allowed per a thread.
  */
class WorkerServer
    : public std::enable_shared_from_this<WorkerServer>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<WorkerServer> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, etc. services
     * @param requestFactory  - the factory of requests
     * @workerName            - the name of a worker this instance represents
     */
    static pointer create (ServiceProvider      &serviceProvider,
                           WorkerRequestFactory &requestFactory,
                           const std::string    &workerName);

    // Default construction and copy semantics are proxibited

    WorkerServer () = delete;
    WorkerServer (WorkerServer const&) = delete;
    WorkerServer & operator= (WorkerServer const&) = delete;

    /// Return the name of a worker this server runs for
    const std::string& worker () const { return _workerName; }

    /// The processor API can be used for detailed monitoring of
    /// the on-going activities and statistics collection if needed.
    const WorkerProcessor& processor () const { return _processor; }

    /**
     * Begin listening for and processing incoming connections
     */
    void run ();

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, etc. services
     * @param requestFactory  - the factory of requests
     * @workerName            - the name of a worker this instance represents
     */
    explicit WorkerServer (ServiceProvider      &serviceProvider,
                           WorkerRequestFactory &requestFactory,
                           const std::string    &workerName);

    /**
     * Begin (asynchrnonously) accepting connection requests.
     */
    void beginAccept ();
    
    /**
     * Handle a connection request once it's detected. The rest of
     * the comunication will be forewarded to the connection object
     * specified as a parameter of the method.
     */
    void handleAccept (const WorkerServerConnection::pointer &connection,
                       const boost::system::error_code       &ec);

    /// Return the context string
    std::string context () const { return "SERVER  "; }

private:

    // Parameters of the object

    ServiceProvider &_serviceProvider;
    std::string      _workerName;

    // Cached parameters of the worker

    WorkerProcessor _processor;

    const WorkerInfo &_workerInfo;

    // The mutable state of the object

    boost::asio::io_service        _io_service;
    boost::asio::ip::tcp::acceptor _acceptor;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERSERVER_H