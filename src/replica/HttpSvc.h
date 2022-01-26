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
#ifndef LSST_QSERV_REPLICA_HTTPSVC_H
#define LSST_QSERV_REPLICA_HTTPSVC_H

// System headers
#include <memory>
#include <string>

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
  * Class HttpSvc is a base class for HTTP servers of various components of the system.
  * 
  * @note The class's implementation starts its own collection of BOOST ASIO
  *   service threads. The number of threads is specified via the corresponding
  *   parameter of the class's constructor.
  * @note The implementation of the class is not thread-safe.
  */
class HttpSvc: public std::enable_shared_from_this<HttpSvc> {
public:
    typedef std::shared_ptr<HttpSvc> Ptr;

    HttpSvc() = delete;
    HttpSvc(HttpSvc const&) = delete;
    HttpSvc& operator=(HttpSvc const&) = delete;

    /// Non-trivial destructor is required to stop the HTTP server.
    ~HttpSvc();

    /**
     * Register REST handlers, start threads and run the server in the thread pool.
     * @note This is the blocking operation. Please, run it within its own thread if needed.
     * @throw std::logic_error  If trying to call the method while the service is already running.
     */
    void run();

    /**
     * Stop the HTTP server.
     * @note This operation will also release the relevant BOOST ASIO threads and unblock
     *   an on-going call to HttpSvc::run() if any was made. Method "HttpSvc::run()" will
     *   get unlocked only after all REST requests that were being processed by the server
     *   would finish.
     * @throw std::logic_error  If trying to call the method while the service is not running.
     */
    void stop();

protected:
    /**
     * The constructor won't start any threads.
     *
     * @param serviceProvider For configuration, etc. services.
     * @param port The number of a port to bind to.
     * @param backlog The maximum length of the queue of pending connections to a socket
     *   open by the server.
     * @param numThreads The number of BOOST ASIO threads.
     */
    HttpSvc(ServiceProvider::Ptr const& serviceProvider,
            uint16_t port,
            unsigned int backlog,
            size_t numThreads);

    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }
    qhttp::Server::Ptr const& httpServer() const { return _httpServer; }
    std::shared_ptr<boost::asio::io_service> const& io_service_ptr() { return _io_service_ptr; }

    /// @return A shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /// @return The context string to be used for the message logging.
    virtual std::string  const& context() const=0;

    /// Register subclass-specific REST services.
    virtual void registerServices()=0;

private:
    ServiceProvider::Ptr const _serviceProvider;
    uint16_t const _port;
    unsigned int const _backlog;
    size_t const _numThreads;

    std::shared_ptr<boost::asio::io_service> const _io_service_ptr;
    qhttp::Server::Ptr _httpServer;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_HTTPSVC_H
