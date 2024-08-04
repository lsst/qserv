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
#ifndef LSST_QSERV_REPLICA_CHTTPSVC_H
#define LSST_QSERV_REPLICA_CHTTPSVC_H

// System headers
#include <memory>
#include <string>

// Forward declarations
namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

namespace httplib {
class Server;
}  // namespace httplib

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ChttpSvc is a base class for HTTP servers of various components of the system.
 *
 * @note The class's implementation runs the server witing its own collection
 *   of service threads. The number of threads is specified via the corresponding
 *   parameter of the class's constructor.
 * @note The implementation of the class is not thread-safe.
 */
class ChttpSvc : public std::enable_shared_from_this<ChttpSvc> {
public:
    ChttpSvc() = delete;
    ChttpSvc(ChttpSvc const&) = delete;
    ChttpSvc& operator=(ChttpSvc const&) = delete;

    ~ChttpSvc() = default;

    /// @return Return the port number the server is bound to.
    int port() const { return _port; }

    /**
     * Register REST handlers, start threads and run the server in the thread pool.
     * @note This is the blocking operation. Please, run it within its own thread if needed.
     * @throw std::runtime_error If the server can't be started.
     */
    void run();

protected:
    /**
     * The constructor won't start any threads.
     *
     * @param context The context string to be used for the message logging.
     * @param serviceProvider For configuration, etc. services.
     * @param port The number of a port to bind to (passing 0 would result in allocating the first available
     * port).
     * @param maxQueuedRequests The maximum number of queued requests (accepted()ed and waiting to be routed).
     * @param numThreads The number of BOOST ASIO threads.
     * @throws std::runtime_error If the server can't be created.
     */
    ChttpSvc(std::string const& context, std::shared_ptr<ServiceProvider> const& serviceProvider,
             std::uint16_t port, std::size_t maxQueuedRequests, std::size_t numThreads);

    std::shared_ptr<ServiceProvider> const& serviceProvider() const { return _serviceProvider; }

    /// @return A shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /// @return The context string to be used for the message logging.
    std::string const& context() const { return _context; }

    /// Register subclass-specific REST services.
    virtual void registerServices(std::unique_ptr<httplib::Server> const& server) = 0;

private:
    void _createAndConfigure();

    std::string const _context;
    std::shared_ptr<ServiceProvider> const _serviceProvider;
    int _port;
    std::size_t const _maxQueuedRequests = 0;  // 0 means unlimited
    std::size_t const _numThreads;
    std::string const _bindAddr = "0.0.0.0";
    std::unique_ptr<httplib::Server> _server;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CHTTPSVC_H
