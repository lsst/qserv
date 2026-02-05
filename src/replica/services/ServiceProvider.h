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
#ifndef LSST_QSERV_REPLICA_SERVICEPROVIDER_H
#define LSST_QSERV_REPLICA_SERVICEPROVIDER_H

// System headers
#include <memory>
#include <string>
#include <thread>
#include <vector>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "http/Auth.h"
#include "replica/services/ChunkLocker.h"
#include "replica/util/Mutex.h"
#include "replica/util/NamedMutexRegistry.h"

// Forward declarations
namespace lsst::qserv::replica {
class Configuration;
class DatabaseServices;
class Messenger;
class QservMgtServices;
class Registry;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ServiceProvider hosts various services used by both workers
 * and controllers.
 */
class ServiceProvider : public std::enable_shared_from_this<ServiceProvider> {
public:
    typedef std::shared_ptr<ServiceProvider> Ptr;

    ServiceProvider() = delete;
    ServiceProvider(ServiceProvider const&) = delete;
    ServiceProvider& operator=(ServiceProvider const&) = delete;

    /**
     * Static factory for creating objects of the class
     *
     * @param configUrl  A source of the application configuration parameters.
     * @param instanceId  A unique identifier of a Qserv instance served by
     *  the Replication System. Its value will be passed along various internal
     *  communication lines of the system to ensure that all services are related
     *  to the same instance. This mechanism also prevents 'cross-talks' between
     *  two (or many) Replication System's setups in case of an accidental
     *  mis-configuration.
     * @param httpAuthContext  An authorization context for operations affecting the state of
     *  Qserv or the Replication/Ingest system.
     */
    static ServiceProvider::Ptr create(std::string const& configUrl, std::string const& instanceId,
                                       http::AuthContext const& httpAuthContext);

    ~ServiceProvider() = default;

    /// @return reference to the I/O service for ASYNC requests
    boost::asio::io_service& io_service() { return _io_service; }

    /**
     * Run the services in a pool of threads unless it's already running.
     * It's safe to call this method multiple times from any thread.
     */
    void run();

    /// @return true if the service is running.
    bool isRunning() const;

    /**
     * Stop the services. This method will guarantee that all outstanding
     * operations will finish and not aborted.
     *
     * This operation will also result in stopping the internal threads
     * in which the server is being run and joining with these threads.
     */
    void stop();

    /// @return a reference to the configuration service
    std::shared_ptr<Configuration> const& config() const { return _configuration; }

    /// @return A unique identifier of a Qserv instance served by the Replication System
    std::string const& instanceId() const { return _instanceId; }

    /// @return the authorization context for operations affecting the state of Qserv or
    http::AuthContext httpAuthContext() const { return _httpAuthContext; }

    /// @return a reference to the local (process) chunk locking services
    ChunkLocker& chunkLocker() { return _chunkLocker; }

    /// @return a reference to the database services
    std::shared_ptr<DatabaseServices> const& databaseServices();

    /// @return a reference to the Qserv notification services (via the XRootD/SSI protocol)
    std::shared_ptr<QservMgtServices> const& qservMgtServices();

    /// @return a reference to worker messenger service (configured for controllers)
    std::shared_ptr<Messenger> const& messenger();

    /// @return a reference to worker registration service
    std::shared_ptr<Registry> const& registry();

    /**
     * Acquire (and register if none existed at a time of a call to the method) a mutex for
     * the given name.
     *
     * @note It's advised not to cache shared pointers returned by the method. Firstly, would
     *   have little or any benefits from the performance point of view. Secondly, it may complicate
     *   the "garbage collection" of unused mutexes, this (potentially) resulting in non-negligible
     *   memory consumption in class NamedMutexRegistry.
     * @see class NamedMutexRegistry
     *
     * Mutex objects returned by the method are expected to be used together with class replica::Lock
     * as it's shown below (both ways are the same):
     * @code
     *   // Okay
     *   auto mutex = serviceProvider->getNamedMutex("name");
     *   replica::Lock lock(mutex);
     *   // The better option
     *   replica::Lock lock(serviceProvider->getNamedMutex("name"));
     * @code
     * Class replica::Lock makes a copy of the shared pointer for a duration of the lock.
     *
     * If, for some reason, a code resorts to using low-level references/pointers to the stored mutex
     * object then, please, make sure the shared pointer outlives the lock. This comment relates
     * to the locking made like shown below:
     * @code
     *   auto mutex = serviceProvider->getNamedMutex("name");
     *   replica::Lock lock(*mutex);
     *   std::lock_guard<replica::Mutex> lock(*mutex);
     * @code
     * Though, in general this would work, the above shown dereferencing is not recommended.
     *
     * @param name The name of a named mutex.
     * @return A smart pointer to a mutex for the name.
     * @throw std::invalid_argument If the name is empty.
     */
    std::shared_ptr<replica::Mutex> getNamedMutex(std::string const& name);

private:
    /// @see ServiceProvider::create()
    explicit ServiceProvider(std::string const& configUrl, std::string const& instanceId,
                             http::AuthContext const& httpAuthContext);

    /// @return the context string for debugging and diagnostic printouts
    std::string _context() const;

    // The BOOST ASIO communication services & threads which run them

    boost::asio::io_service _io_service;
    std::unique_ptr<boost::asio::io_service::work> _work;
    std::vector<std::unique_ptr<std::thread>> _threads;

    /// Configuration manager (constructed from the Configuration specification
    /// URL passed into the constructor of the class).
    std::shared_ptr<Configuration> const _configuration;

    /// A unique identifier of a Qserv instance served by the Replication System
    std::string const _instanceId;

    /// Authorization context
    http::AuthContext const _httpAuthContext;

    /// For claiming exclusive ownership over chunks during replication
    /// operations to ensure consistency of the operations.
    ChunkLocker _chunkLocker;

    /// Database services (lazy instantiation on a first request)
    std::shared_ptr<DatabaseServices> _databaseServices;

    /// Qserv management services (lazy instantiation on a first request)
    std::shared_ptr<QservMgtServices> _qservMgtServices;

    /// Worker messenger service (lazy instantiation on a first request)
    std::shared_ptr<Messenger> _messenger;

    /// Worker registration service (lazy instantiation on a first request)
    std::shared_ptr<Registry> _registry;

    /// Registry of unique mutexes.
    NamedMutexRegistry _namedMutexRegistry;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable replica::Mutex _mtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SERVICEPROVIDER_H
