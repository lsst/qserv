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
#include "http/ChttpModule.h"

// Forward declarations
namespace lsst::qserv::replica {
class IngestRequest;
class IngestRequestMgr;
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestHttpSvcMod processes chunk/table contribution requests made over HTTP.
 * The class is used by the HTTP server built into the worker Ingest service.
 */
class IngestHttpSvcMod : public http::ChttpModule {
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
     *   SYNC-RETRY    for synchronous retry of a prior request that failed while reading
     *                 or preprocessing the input data
     *   ASYNC-SUBMIT  submit an asynchronous contribution request
     *   ASYNC-RETRY   submit an asynchronous retry of a prior request that failed while
     *                 reading or preprocessing the input data request
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
    static void process(std::shared_ptr<ServiceProvider> const& serviceProvider,
                        std::shared_ptr<IngestRequestMgr> const& ingestRequestMgr,
                        std::string const& workerName, httplib::Request const& req, httplib::Response& resp,
                        std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::REQUIRED);

protected:
    /// @see http::Module::context()
    virtual std::string context() const final;

    /// @see http::Module::executeImpl()
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    /// @see method IngestHttpSvcMod::create()
    IngestHttpSvcMod(std::shared_ptr<ServiceProvider> const& serviceProvider,
                     std::shared_ptr<IngestRequestMgr> const& ingestRequestMgr, std::string const& workerName,
                     httplib::Request const& req, httplib::Response& resp);

    /// Process a table contribution request (SYNC).
    nlohmann::json _syncProcessRequest() const;

    /// Make an attempt to retry a table contribution request that failed before (SYNC).
    nlohmann::json _syncProcessRetry() const;

    /// Submit a table contribution request (ASYNC).
    nlohmann::json _asyncSubmitRequest() const;

    /// Make an attempt to retry a table contribution request (ASYNC).
    nlohmann::json _asyncSubmitRetry() const;

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
     * @note The method may throw exceptions in case if there are problems
     *  with parameters request creation, or database services interactions.
     * @param async The optional type of a request to be created.
     * @return A pointer to the created request.
     */
    std::shared_ptr<IngestRequest> _createRequest(bool async = false) const;

    /**
     * Locate and evaluate the specified table contribution request, and if it's
     * eligible for retries then re-initialize it to allow submitting for processing.
     * @note The method may throw exceptions in case if there are problems
     *  with parameters request creation, or database services interactions.
     * @return A pointer to the prepared request.
     */
    std::shared_ptr<IngestRequest> _createRetry(bool async = false) const;

    // Input parameters
    std::shared_ptr<ServiceProvider> const _serviceProvider;
    std::shared_ptr<IngestRequestMgr> const _ingestRequestMgr;
    std::string const _workerName;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_INGESTHTTPSVCMOD_H
