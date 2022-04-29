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
#ifndef LSST_QSERV_REGISTRYHTTPSVCMOD_H
#define LSST_QSERV_REGISTRYHTTPSVCMOD_H

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/HttpModuleBase.h"
#include "replica/ServiceProvider.h"

// Forward declarations
namespace lsst { namespace qserv { namespace replica {
class RegistryWorkers;
}}}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class RegistryHttpSvcMod processes worker registration requests made
 * over HTTP. The class is used by the HTTP server build into the Registry service.
 */
class RegistryHttpSvcMod : public HttpModuleBase {
public:
    RegistryHttpSvcMod() = delete;
    RegistryHttpSvcMod(RegistryHttpSvcMod const&) = delete;
    RegistryHttpSvcMod& operator=(RegistryHttpSvcMod const&) = delete;

    virtual ~RegistryHttpSvcMod() = default;

    /**
     * Process a request.
     *
     * Supported values for parameter 'subModuleName':
     *
     *   WORKERS        return a collection of known workers
     *   ADD-WORKER     worker registration request
     *   DELETE-WORKER  remove a worker from the collection
     *
     * @param serviceProvider The provider of services is needed to access
     *   the identity and the authorization keys of the instance.
     * @param workers The synchronized collection of workers.
     * @param req The HTTP request.
     * @param resp The HTTP response channel.
     * @param subModuleName The name of a submodule to be called.
     * @param authType The authorization requirements for the module
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(ServiceProvider::Ptr const& serviceProvider, RegistryWorkers& workers,
                        qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName,
                        HttpAuthType const authType = HttpAuthType::REQUIRED);

protected:
    /// @see HttpModuleBase::context()
    virtual std::string context() const final;

    /// @see HttpModuleBase::executeImpl()
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    /// @see method RegistryHttpSvcMod::create()
    RegistryHttpSvcMod(ServiceProvider::Ptr const& serviceProvider, RegistryWorkers& workers,
                       qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp);

    /**
     * @brief Check if the specified identifier of the Qserv instance that was received
     *   from a client matches the one of the current service. Throw an exception if not.
     *
     * @param context_ The calling context to be reported in the exception.
     * @param instanceId The instance identifier received from a client.
     * @throws std::invalid_argument If the identifier didn't match expectations.
     */
    void _enforceInstanceId(std::string const& context_, std::string const& instanceId) const;

    /// Return a collection of known workers.
    nlohmann::json _getWorkers() const;

    /// Register a worker in the collection.
    nlohmann::json _addWorker();

    /// Remove a worker from the collection.
    nlohmann::json _deleteWorker();

    // Input parameters
    ServiceProvider::Ptr const _serviceProvider;
    RegistryWorkers& _workers;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REGISTRYHTTPSVCMOD_H
