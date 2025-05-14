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
#ifndef LSST_QSERV_WCOMMS_HTTPSVC_H
#define LSST_QSERV_WCOMMS_HTTPSVC_H

// System headers
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Third party headers
#include "boost/asio.hpp"

namespace lsst::qserv::qhttp {
class Server;
}  // namespace lsst::qserv::qhttp

namespace lsst::qserv::wcontrol {
class Foreman;
}  // namespace lsst::qserv::wcontrol

// This header declarations
namespace lsst::qserv::wcomms {

/**
 * Class HttpSvc is the HTTP server for processing worker management requests.
 *
 * The server creates and manages its own collection of BOOST ASIO service threads.
 * The number of threads is specified via the corresponding parameter of the class's
 * constructor.
 *
 * Typical usage of the class:
 * @code
 *   // Create the server. Note, it won't run yet until explicitly started.
 *   uint16_t const port = 0;           // The port will be dynamically allocated at start
 *   unsigned int const numThreads = 2; // The number of BOOST ASIO threads
 *   auto const svc = wcomms::HttpSvc::create(port, numThreads);
 *
 *   // Start the server and get the actual port number.
 *   uint16_t const actualPort = svc->start();
 *   std::cout << "HTTP server is running on port " << actualPort << std::endl;
 *
 *   // Stop the server to release resources.
 *   svc->stop();
 *   svc.reset();
 * @code
 * @note The class implementation is thread safe.
 */
class HttpSvc : public std::enable_shared_from_this<HttpSvc> {
public:
    /**
     * The factory will not initialize ASIO context and threads, or start
     * the server. This has to be done by calling method HttpSvc::start()
     *
     * @param port The number of a port to bind to.
     * @param numThreads The number of BOOST ASIO threads.
     * @return The shared pointer to the running server.
     */
    static std::shared_ptr<HttpSvc> create(std::shared_ptr<wcontrol::Foreman> const& foreman, uint16_t port,
                                           unsigned int numThreads);

    HttpSvc() = delete;
    HttpSvc(HttpSvc const&) = delete;
    HttpSvc& operator=(HttpSvc const&) = delete;

    ~HttpSvc() = default;

    /**
     * Initialize ASIO context and threads, and start the server.
     *
     * @note Once the server is started it has to be explicitly stopped
     * using the counterpart method stop() to allow releasing allocated
     * resources and letting the destructor to be executed. Note that
     * service threads started by the curent method and the HTTP server
     * incerement the reference counter on the shared pointer that is
     * returned by the class's factory method.
     *
     * @return The actual port number on which the server is run.
     * @throws std::logic_error If the server is already running.
     */
    uint16_t start();

    /**
     * Stop the server and threads, and release the relevant resources.
     * @throws std::logic_error If the server is not running.
     */
    void stop();

private:
    /**
     * The constructor will not initialize ASIO context and threads, or start
     * the server. This has to be done by calling method HttpSvc::start()
     * @param port The number of a port to bind to.
     * @param numThreads The number of BOOST ASIO threads.
     */
    HttpSvc(std::shared_ptr<wcontrol::Foreman> const& foreman, uint16_t port, unsigned int numThreads);

    // Input parameters

    std::shared_ptr<wcontrol::Foreman> const _foreman;

    uint16_t const _port;            ///< The input port number (could be 0 to allow autoallocation).
    unsigned int const _numThreads;  ///< The number of the BOOST ASIO service threads.

    /// This mutex protects the object state.
    mutable std::mutex _mtx;

    /// Worker management requests are processed by this server.
    std::shared_ptr<qhttp::Server> _httpServerPtr;

    /// The BOOST ASIO I/O services.
    boost::asio::io_service _io_service;

    /// The thread pool for running ASIO services.
    std::vector<std::unique_ptr<std::thread>> _threads;
};

}  // namespace lsst::qserv::wcomms

#endif  // LSST_QSERV_WCOMMS_HTTPSVC_H
