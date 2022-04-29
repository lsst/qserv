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
#ifndef LSST_QSERV_REPLICA_REGISTRYHTTPSVC_H
#define LSST_QSERV_REPLICA_REGISTRYHTTPSVC_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/HttpSvc.h"
#include "replica/ServiceProvider.h"

// Forward declarations
namespace lsst::qserv::replica {
class RegistryWorkers;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class RegistryHttpSvc is used for handling incoming REST API requests to
 * the workers registration service. Each instance of this class
 * will be running in its own thread.
 *
 * @note The class's implementation starts its own collection of BOOST ASIO
 *   service threads as configured in Configuration.
 * @note The implementation of the class is not thread-safe.
 */
class RegistryHttpSvc : public HttpSvc {
public:
    typedef std::shared_ptr<RegistryHttpSvc> Ptr;

    /**
     * Create an instance of the service.
     *
     * @param serviceProvider For configuration, etc. services.
     * @param httpPort The TCP port number for running the service.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider);

    RegistryHttpSvc() = delete;
    RegistryHttpSvc(RegistryHttpSvc const&) = delete;
    RegistryHttpSvc& operator=(RegistryHttpSvc const&) = delete;

    virtual ~RegistryHttpSvc() = default;

protected:
    /// @see HttpSvc::context()
    virtual std::string const& context() const;

    /// @see HttpSvc::registerServices()
    virtual void registerServices();

private:
    /// @see RegistryHttpSvc::create()
    RegistryHttpSvc(ServiceProvider::Ptr const& serviceProvider);

    /// Synchronized collection of workers
    std::unique_ptr<RegistryWorkers> _workers;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REGISTRYHTTPSVC_H
