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
#ifndef LSST_QSERV_INGESTFILEHTTPSVCMOD_H
#define LSST_QSERV_INGESTFILEHTTPSVCMOD_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/FileUploadModule.h"
#include "replica/ingest/IngestFileSvc.h"
#include "replica/ingest/TransactionContrib.h"

// Forward declarations
namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

namespace lsst::qserv::replica::csv {
class Parser;
}  // namespace lsst::qserv::replica::csv

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestFileHttpSvcMod processes chunk/table contribution requests made over HTTP.
 * The class is used by the HTTP server built into the worker Ingest service.
 * The current class is meant to be used for ingesting payloads that are pushed directly
 * into the service over the HTTP protocol in the "multipart/form-data" body of the request.
 */
class IngestFileHttpSvcMod : public http::FileUploadModule, public IngestFileSvc {
public:
    IngestFileHttpSvcMod() = delete;
    IngestFileHttpSvcMod(IngestFileHttpSvcMod const&) = delete;
    IngestFileHttpSvcMod& operator=(IngestFileHttpSvcMod const&) = delete;

    virtual ~IngestFileHttpSvcMod() = default;

    /**
     * Process a request.
     *
     * @param serviceProvider The provider of services is needed to access
     *   the configuration and the database services.
     * @param workerName The name of a worker this service is acting upon (used to pull
     *   worker-specific configuration options for the service).
     * @param req The HTTP request.
     * @param resp The HTTP response channel.
     * @param contentReader The content reader to be used for the file upload.
     * @param authType The authorization requirements for the module
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(std::shared_ptr<ServiceProvider> const& serviceProvider,
                        std::string const& workerName, httplib::Request const& req, httplib::Response& resp,
                        httplib::ContentReader const& contentReader,
                        http::AuthType const authType = http::AuthType::REQUIRED);

protected:
    virtual std::string context() const final;
    virtual void onStartOfFile(std::string const& name, std::string const& fileName,
                               std::string const& contentType) final;
    virtual void onFileData(char const* data, std::size_t length) final;
    virtual void onEndOfFile() final;
    virtual nlohmann::json onEndOfBody() final;

private:
    /// @see method IngestFileHttpSvcMod::create()
    IngestFileHttpSvcMod(std::shared_ptr<ServiceProvider> const& serviceProvider,
                         std::string const& workerName, httplib::Request const& req, httplib::Response& resp,
                         httplib::ContentReader const& contentReader);

    void _parseAndWriteData(char const* data, std::size_t length, bool flush);

    /**
     * Close the temporary file if needed and post an error message.
     * @param context_ The caller's context.
     */
    void _failed(std::string const& context_);

    TransactionContribInfo _contrib;  ///< A state of the contribution processing

    /// The parse of the input stream as configured for the CSV dialect reported
    /// by a client.
    std::unique_ptr<csv::Parser> _parser;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_INGESTFILEHTTPSVCMOD_H
