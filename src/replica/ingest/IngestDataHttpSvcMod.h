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
#ifndef LSST_QSERV_INGESTDATAHTTPSVCMOD_H
#define LSST_QSERV_INGESTDATAHTTPSVCMOD_H

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/ModuleBase.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/ingest/IngestFileSvc.h"
#include "replica/ingest/TransactionContrib.h"
#include "replica/services/ServiceProvider.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestDataHttpSvcMod processes chunk/table contribution requests made over HTTP.
 * The class is used by the HTTP server built into the worker Ingest service.
 * Unlike class IngestHttpSvcMod, the current class is meant to be used for ingesting
 * payloads that are pushed directly into the service over the HTTP protocol.
 */
class IngestDataHttpSvcMod : public http::ModuleBase, public IngestFileSvc {
public:
    IngestDataHttpSvcMod() = delete;
    IngestDataHttpSvcMod(IngestDataHttpSvcMod const&) = delete;
    IngestDataHttpSvcMod& operator=(IngestDataHttpSvcMod const&) = delete;

    virtual ~IngestDataHttpSvcMod() = default;

    /**
     * Process a request.
     *
     * Supported values for parameter 'subModuleName':
     *
     *   SYNC-PROCESS-DATA  for synchronous execution of the table contribution requests
     *
     * @param serviceProvider The provider of services is needed to access
     *   the configuration and the database services.
     * @param workerName The name of a worker this service is acting upon (used to pull
     *   worker-specific configuration options for the service).
     * @param req The HTTP request.
     * @param resp The HTTP response channel.
     * @param subModuleName The name of a submodule to be called.
     * @param authType The authorization requirements for the module
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                        qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::REQUIRED);

protected:
    /// @see http::ModuleBase::context()
    virtual std::string context() const final;

    /// @see http::ModuleBase::executeImpl()
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    /// @see method IngestDataHttpSvcMod::create()
    IngestDataHttpSvcMod(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                         qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp);

    /// Process a table contribution request (SYNC).
    nlohmann::json _syncProcessData();

    /**
     * Close the temporary file if needed and post an error message.
     * @param context_ The caller's context.
     */
    void _failed(std::string const& context_);

    // The following three methods translate an input JSON object into a string.
    // Methods throw http::Error for any problems encountered during object proccesing
    // or translation.

    std::string _translateHexString(std::string const& context_, nlohmann::json const& jsonColumn,
                                    size_t rowIdx, size_t colIdx);
    std::string _translateBase64String(std::string const& context_, nlohmann::json const& jsonColumn,
                                       size_t rowIdx, size_t colIdx);
    std::u8string _translateByteArray(std::string const& context_, nlohmann::json const& jsonColumn,
                                      size_t rowIdx, size_t colIdx);
    std::string _translatePrimitiveType(std::string const& context_, nlohmann::json const& jsonColumn,
                                        size_t rowIdx, size_t colIdx);

    TransactionContribInfo _contrib;  ///< A state of the contribution processing
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_INGESTDATAHTTPSVCMOD_H
