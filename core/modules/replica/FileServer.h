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
#ifndef LSST_QSERV_REPLICA_FILESERVER_H
#define LSST_QSERV_REPLICA_FILESERVER_H

/// FileServer.h declares:
///
/// class FileServer
/// (see individual class documentation for more information)

// System headers
#include <memory>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/FileServerConnection.h"
#include "replica/ServiceProvider.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class WorkerInfo;

/**
  * Class FileServer is used for handling incomming connections to
  * the file delivery service. Each instance of this class will be runing
  * in its own thread.
  */
class FileServer
    : public std::enable_shared_from_this<FileServer>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FileServer> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, etc. services
     * @workerName            - the name of a worker this instance represents
     *
     * @reurn pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& workerName);

    // Default construction and copy semantics are prohibited

    FileServer() = delete;
    FileServer(FileServer const&) = delete;
    FileServer& operator=(FileServer const&) = delete;

    ~FileServer() = default;

    /// @return the name of a worker this server runs for
    std::string const& worker() const { return _workerName; }

    /**
     * Run the server in a thread pool (as per the Configuration)
     *
     * ATTENTION: this is the blovking operation. Please, run it
     * witin ots own thread if needed.
     */
    void run();

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, etc. services
     * @workerName            - the name of a worker this instance represents
     */
    FileServer(ServiceProvider::Ptr const& serviceProvider,
               std::string const& workerName);

    /**
     * Begin (asynchrnonously) accepting connection requests.
     */
    void beginAccept();
    
    /**
     * Handle a connection request once it's detected. The rest of
     * the comunication will be forewarded to the connection object
     * specified as a parameter of the method.
     */
    void handleAccept(FileServerConnection::Ptr const& connection,
                      boost::system::error_code const& ec);

    /// @return the context string
    std::string context() const { return "FILE-SERVER  "; }

private:

    ServiceProvider::Ptr _serviceProvider;
    std::string _workerName;

    /// Cached parameters of the worker
    WorkerInfo const& _workerInfo;

    boost::asio::io_service        _io_service;
    boost::asio::ip::tcp::acceptor _acceptor;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FILESERVER_H
