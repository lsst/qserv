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
#ifndef LSST_QSERV_REPLICA_INGESTHTTPSVC_H
#define LSST_QSERV_REPLICA_INGESTHTTPSVC_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/HttpSvc.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class IngestHttpSvc is used for handling incoming REST API requests for
 * the table contribution uploads. Each instance of this class will be running
 * in its own thread.
 * 
 * @note The class's implementation starts its own collection of BOOST ASIO
 *   service threads as configured in Configuration.
 * @note The implementation of the class is not thread-safe.
 */
class IngestHttpSvc: public HttpSvc {
public:
    typedef std::shared_ptr<IngestHttpSvc> Ptr;

    /**
     * Create an instance of the service.
     *
     * @param serviceProvider For configuration, etc. services.
     * @param workerName The name of a worker this service is acting upon (used for
     *   checking consistency of the protocol).
     * @param authKey An authorization key for the catalog ingest operation.
     * @param adminAuthKey  An administrator-level authorization key.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& workerName,
                      std::string const& authKey,
                      std::string const& adminAuthKey);

    IngestHttpSvc() = delete;
    IngestHttpSvc(IngestHttpSvc const&) = delete;
    IngestHttpSvc& operator=(IngestHttpSvc const&) = delete;

    virtual ~IngestHttpSvc() = default;

protected:
    /// @see HttpSvc::context()
    virtual std::string const& context() const;

    /// @see HttpSvc::registerServices()
    virtual void registerServices();

private:
    /// @see IngestHttpSvc::create()
    IngestHttpSvc(ServiceProvider::Ptr const& serviceProvider,
                  std::string const& workerName,
                  std::string const& authKey,
                  std::string const& adminAuthKey);

   // Input parameters

    std::string const _workerName;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INGESTHTTPSVC_H
