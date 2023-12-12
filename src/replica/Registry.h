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
#ifndef LSST_QSERV_REPLICA_REGISTRY_H
#define LSST_QSERV_REPLICA_REGISTRY_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Third-party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"
#include "replica/ConfigWorker.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class Registry is the client API for comunications with the service registration
 * server. The API provides an interface for inspecting and managing (adding/deleting)
 * serice entries at the server.
 *
 * @note The implementation of the class is thread-safe.
 */
class Registry : public std::enable_shared_from_this<Registry> {
public:
    typedef std::shared_ptr<Registry> Ptr;

    /**
     * Create an instance of the service.
     * @param serviceProvider For configuration, etc. services.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider);

    Registry() = delete;
    Registry(Registry const&) = delete;
    Registry& operator=(Registry const&) = delete;

    virtual ~Registry() = default;

    /// @return All workers
    /// @see method Registry::_request for other exceptions.
    std::vector<ConfigWorker> workers() const;

    /**
     * Add (or replace if exists) worker entry.
     * @param name The unique identifier of the worker
     * @see method Registry::_request for other exceptions.
     */
    void addWorker(std::string const& name) const;

    /**
     * Remove (if exists) a worker entry
     * @param name A unique identifier (the name) of the worker
     * @see method Registry::_request for other exceptions.
     */
    void removeWorker(std::string const& name) const;

private:
    /// @see Registry::create()
    Registry(ServiceProvider::Ptr const& serviceProvider);

    /**
     * Send a request to the server.
     * @note The current HTML standard doesn't allow sending any data in the GET
     *   request's body. The later require not using the default value of
     *   the parameter \param request.
     * @param method HTTP method for the request.
     * @param resource The relative path to the REST service.
     * @param request Optional parameters of the request (JSON object).
     * @return nlohmann::json A result (JSON object) reported by the server.
     * @throw http::Error for specific errors reported by the client library.
     * @throw std::runtime_error In case if a error was received from the server.
     */
    nlohmann::json _request(http::Method method, std::string const& resource,
                            nlohmann::json const& request = nlohmann::json()) const;

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;

    /// Base URL for communications with the Registry server
    std::string _baseUrl;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REGISTRY_H
