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
#ifndef LSST_QSERV_INGESTHTTPSVCMOD_H
#define LSST_QSERV_INGESTHTTPSVCMOD_H

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/HttpModuleBase.h"
#include "replica/IngestFileSvc.h"
#include "replica/ServiceProvider.h"


// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class IngestHttpSvcMod processes chunk/table contribution requests made over HTTP.
 * The class is used by the HTTP server build into the worker Ingest service.
 */
class IngestHttpSvcMod: public HttpModuleBase,
                        public IngestFileSvc {
public:
    IngestHttpSvcMod() = delete;
    IngestHttpSvcMod(IngestHttpSvcMod const&) = delete;
    IngestHttpSvcMod& operator=(IngestHttpSvcMod const&) = delete;

    virtual ~IngestHttpSvcMod() = default;

    /**
     * @note the only supported value of parameter 'subModuleName' is the empty string.
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(ServiceProvider::Ptr const& serviceProvider,
                        std::string const& workerName,
                        std::string const& authKey,
                        std::string const& adminAuthKey,
                        qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName=std::string(),
                        HttpModuleBase::AuthType const authType=HttpModuleBase::AUTH_REQUIRED);

protected:
    /// @see HttpModuleBase::context()
    virtual std::string context() const final;

    /// @see HttpModuleBase::executeImpl()
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    /**
     * @param serviceProvider  The provider of services is needed to access Configuration.
     * @param workerName  The name of a worker this service is acting upon (used to pull
     *   worker-specific configuration options for the service).
     * @param authKey  An authorization key for the catalog ingest operation.
     * @param adminAuthKey  An administrator-level authorization key.
     * @param req  The HTTP request.
     * @param resp  The HTTP response channel.
     */
    IngestHttpSvcMod(ServiceProvider::Ptr const& serviceProvider,
                     std::string const& workerName,
                     std::string const& authKey,
                     std::string const& adminAuthKey,
                     qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp);

    /**
     * Read a local file and preprocess it.
     * @param filename The name of a file to be ingested.
     * @return An object with statistics on the amount of data read from the file.
     */
    nlohmann::json _readLocal(std::string const& filename);

    /**
     * Pull an input file from a remote HTTP service and preprocess it.
     * @param method An HTTP method for a request.
     * @param url A location of a file to be ingested.
     * @param data Optional data to be sent with a  request (depends on the HTTP headers).
     * @param headers Optional HTTP headers to be send with a request.
     * @return An object with statistics on the amount of data read from the file.
     */
    nlohmann::json _readRemote(std::string const& method,
                               std::string const& url,
                               std::string const& data=std::string(),
                               std::vector<std::string> const& headers=std::vector<std::string>());
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_INGESTHTTPSVCMOD_H
