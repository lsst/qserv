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
#include "http/QhttpModule.h"
#include "replica/services/ServiceProvider.h"

// Forward declarations
namespace lsst::qserv::replica {
class RegistryServices;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class RegistryHttpSvcMod processes worker registration requests made
 * over HTTP. The class is used by the HTTP server build into the Registry service.
 * @note Each worker entry represents a collection of attributes merged from
 * from two sources - Replication System's worker and Qserv worker.
 */
class RegistryHttpSvcMod : public http::QhttpModule {
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
     *   SERVICES          return info on all known services
     *   ADD-WORKER        worker registration request (Replicaton System)
     *   ADD-QSERV-WORKER  worker registration request (Qserv)
     *   DELETE-WORKER     remove a worker from the collection
     *   ADD-CZAR          czar registration request (Qserv)
     *   DELETE-CZAR       remove a czar from the collection
     *   ADD-CONTROLLER    controller registration request (Replicaton System)
     *   DELETE-CONTROLLER remove a controller from the collection
     *
     * @param serviceProvider The provider of services is needed to access
     *   the identity and the authorization keys of the instance.
     * @param services The synchronized collection of services.
     * @param req The HTTP request.
     * @param resp The HTTP response channel.
     * @param subModuleName The name of a submodule to be called.
     * @param authType The authorization requirements for the module
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(ServiceProvider::Ptr const& serviceProvider, RegistryServices& services,
                        std::shared_ptr<qhttp::Request> const& req,
                        std::shared_ptr<qhttp::Response> const& resp, std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::REQUIRED);

protected:
    /// @see http::Module::context()
    virtual std::string context() const final;

    /// @see http::Module::executeImpl()
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    /// @see method RegistryHttpSvcMod::create()
    RegistryHttpSvcMod(ServiceProvider::Ptr const& serviceProvider, RegistryServices& services,
                       std::shared_ptr<qhttp::Request> const& req,
                       std::shared_ptr<qhttp::Response> const& resp);

    /// Return a collection of known services.
    nlohmann::json _getServices() const;

    /// Register/update a worker in the specified collection.
    /// @param kind A kind of the worker to be updated ("replicaton", "qserv").
    nlohmann::json _addWorker(std::string const& kind);

    /// Remove a worker from the collection.
    nlohmann::json _deleteWorker();

    /// Register/update a Czar.
    nlohmann::json _addCzar();

    /// Remove a Czar from the collection.
    nlohmann::json _deleteCzar();

    /// Register/update a Controller.
    nlohmann::json _addController();

    /// Remove a Controller from the collection.
    nlohmann::json _deleteController();

    // Input parameters
    ServiceProvider::Ptr const _serviceProvider;
    RegistryServices& _services;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REGISTRYHTTPSVCMOD_H
