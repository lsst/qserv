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
#ifndef LSST_QSERV_REPLICA_CONTROLLER_H
#define LSST_QSERV_REPLICA_CONTROLLER_H

/**
 * This header defines the Replication Controller service for creating and
 * managing requests sent to the remote worker services.
 */

// System headers
#include <cstdint>
#include <map>
#include <memory>

// Qserv headers
#include "replica/util/Common.h"
#include "replica/util/Mutex.h"

// Forward declarations
namespace lsst::qserv::replica {
class Request;
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ControllerIdentity encapsulates various attributes which identify
 * each instance of the Controller class. This information is meant to
 * be used in the multi-Controller setups to coordinate operations
 * between multiple instances and to avoid/resolve conflicts.
 */
class ControllerIdentity {
public:
    std::string id;    ///< A unique identifier of the Controller
    std::string host;  ///< The name of a host where it runs
    pid_t pid;         ///< An identifier of a process
};

std::ostream& operator<<(std::ostream& os, ControllerIdentity const& identity);

/**
 * Class Controller is used for pushing replication (etc.) requests
 * to the worker replication services. Only one instance of this class is
 * allowed per a thread. Request-specific methods of the class will
 * instantiate and start the requests.
 *
 * All methods launching, stopping or checking status of requests
 * require that the server to be running. Otherwise it will throw
 * std::runtime_error. The current implementation of the server
 * doesn't support (yet?) an operation queuing mechanism.
 *
 * Methods which take worker names as parameters will throw exception
 * std::invalid_argument if the specified worker names are not found
 * in the configuration.
 */
class Controller : public std::enable_shared_from_this<Controller> {
public:
    friend class ControllerImpl;
    typedef std::shared_ptr<Controller> Ptr;

    static Ptr create(std::shared_ptr<ServiceProvider> const& serviceProvider);

    Controller() = delete;
    Controller(Controller const&) = delete;
    Controller& operator=(Controller const&) = delete;
    ~Controller() = default;

    ControllerIdentity const& identity() const { return _identity; }
    uint64_t startTime() const { return _startTime; }
    std::shared_ptr<ServiceProvider> const& serviceProvider() const { return _serviceProvider; }

    /**
     * Check if required folders exist and they're write-enabled for an effective user
     * of the current process. Create missing folders if needed.
     * @param createMissingFolders The optional flag telling the method to create missing folders.
     * @throw std::runtime_error If any folder can't be created, or if any folder is not
     *   write-enabled for the current user.
     */
    void verifyFolders(bool createMissingFolders = false) const;

private:
    explicit Controller(std::shared_ptr<ServiceProvider> const& serviceProvider);

    std::string _context(std::string const& func = std::string()) const;

    // Methods _add and _remove are used by the request classes to register and unregister
    // themselves in the Controller's registry.
    friend class Request;
    void _add(std::shared_ptr<Request> const& request);
    void _remove(std::shared_ptr<Request> const& request);

    // Input parameters
    std::shared_ptr<ServiceProvider> const _serviceProvider;

    /// The unique identity of the instance
    ControllerIdentity const _identity;

    /// The number of milliseconds since UNIX Epoch when an instance of
    /// the Controller was created.
    uint64_t const _startTime;

    /// For enforcing thread safety of the class's public API
    /// and internal operations.
    mutable replica::Mutex _mtx;

    /// The registry of the requests
    std::map<std::string, std::shared_ptr<Request>> _registry;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONTROLLER_H
