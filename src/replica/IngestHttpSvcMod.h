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
#include "replica/IngestRequest.h"
#include "replica/IngestRequestMgr.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestHttpSvcMod processes chunk/table contribution requests made over HTTP.
 * The class is used by the HTTP server build into the worker Ingest service.
 */
class IngestHttpSvcMod : public HttpModuleBase {
public:
    IngestHttpSvcMod() = delete;
    IngestHttpSvcMod(IngestHttpSvcMod const&) = delete;
    IngestHttpSvcMod& operator=(IngestHttpSvcMod const&) = delete;

    virtual ~IngestHttpSvcMod() = default;

    /**
     * Process a request.
     *
     * Supported values for parameter 'subModuleName':
     *
     *   SYNC-PROCESS  for synchronous execution of the table contribution requests
     *   ASYNC-SUBMIT  submit an asynchronous contribution request
     *   ASYNC-STATUS-BY-ID  return a status of a contribution request specified by its identifier
     *   ASYNC-CANCEL-BY-ID  cancel an outstanding contribution request specified by its identifier
     *   ASYNC-STATUS-BY-TRANS-ID  return a status of requests in a scope of the specified
     *                             transaction and the current worker
     *   ASYNC-CANCEL-BY-TRANS-ID  cancel all outstanding contribution requests in a scope of
     *                             the specified transaction and the current worker
     *
     * @param serviceProvider The provider of services is needed to access
     *   the configuration and the database services.
     * @param workerName The name of a worker this service is acting upon (used to pull
     *   worker-specific configuration options for the service).
     * @param ingestRequestMgr The manager for handling ASYNC requests.
     * @param req The HTTP request.
     * @param resp The HTTP response channel.
     * @param subModuleName The name of a submodule to be called.
     * @param authType The authorization requirements for the module
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(ServiceProvider::Ptr const& serviceProvider,
                        IngestRequestMgr::Ptr const& ingestRequestMgr, std::string const& workerName,
                        qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName,
                        HttpAuthType const authType = HttpAuthType::REQUIRED);

protected:
    /// @see HttpModuleBase::context()
    virtual std::string context() const final;

    /// @see HttpModuleBase::executeImpl()
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    /// @see method IngestHttpSvcMod::create()
    IngestHttpSvcMod(ServiceProvider::Ptr const& serviceProvider,
                     IngestRequestMgr::Ptr const& ingestRequestMgr, std::string const& workerName,
                     qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp);

    /// Process a table contribution request (SYNC).
    nlohmann::json _syncProcessRequest() const;

    /// Submit a table contribution request (ASYNC).
    nlohmann::json _asyncSubmitRequest() const;

    /// Return a status of an existing table contribution request (ASYNC).
    nlohmann::json _asyncRequest() const;

    /// Cancel an existing table contribution request (ASYNC).
    nlohmann::json _asyncCancelRequest() const;

    /// Return a status of existing table contribution requests in a scope of
    /// a transaction and the current worker (ASYNC).
    nlohmann::json _asyncTransRequests() const;

    /// Cancel all outstanding contribution requests in a scope of
    /// a transaction and the current worker (ASYNC).
    nlohmann::json _asyncTransCancelRequests() const;

    /**
     * Process request parameters and create table contribution request
     * of the specified type.
     * @note The method may throw exceptions in case of any problems with the parameters
     *   of the request, or other issues encountered during request creation (interaction
     *   with external services).
     * @param async The optional type of a request to be created.
     * @return A pointer to the created request.
     */
    IngestRequest::Ptr _createRequest(bool async = false) const;

    // Input parameters
    ServiceProvider::Ptr const _serviceProvider;
    IngestRequestMgr::Ptr const _ingestRequestMgr;
    std::string const _workerName;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_INGESTHTTPSVCMOD_H
