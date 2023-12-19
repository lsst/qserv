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
#ifndef LSST_QSERV_REPLICA_QHTTPTESTAPP_H
#define LSST_QSERV_REPLICA_QHTTPTESTAPP_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/apps/Application.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class QhttpTestApp is performance and scalability test for the embedded
 * HTTP server "qhttp".
 */
class QhttpTestApp : public Application {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<QhttpTestApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv The vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    QhttpTestApp() = delete;
    QhttpTestApp(QhttpTestApp const&) = delete;
    QhttpTestApp& operator=(QhttpTestApp const&) = delete;

    virtual ~QhttpTestApp() final = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see QhttpTestApp::create()
    QhttpTestApp(int argc, char* argv[]);

    /// A port number for listening for incoming connections
    uint16_t _port;

    /// The maximum length of the queue of pending connections to a socket open
    /// by the server.
    int _backlog = boost::asio::socket_base::max_listen_connections;

    /// The number of the BOOST ASIO threads to run the server
    size_t _numThreads = 1;

    /// An interval (milliseconds) for reporting the performance counters
    int _reportIntervalMs = 1000;

    /// The flag which would turn on detailed report on the incoming requests
    bool _verbose = false;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_QHTTPTESTAPP_H */
