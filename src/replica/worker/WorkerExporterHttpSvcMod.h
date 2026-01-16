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
#ifndef LSST_QSERV_WORKEREXPORTERHTTPSVCMOD_H
#define LSST_QSERV_WORKERHTTPSVCMOD_H

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/ChttpModule.h"
#include "replica/util/Csv.h"

// Forward declarations

namespace lsst::qserv::replica::database::mysql {
class ConnectionPool;
}  // namespace lsst::qserv::replica::database::mysql

namespace lsst::qserv::replica {
class ServiceProvider;
class WorkerHttpProcessor;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerExporterHttpSvcMod processes the table and chunk exportation requests.
 * The class is used by the HTTP server built into the worker Replication service.
 */
class WorkerExporterHttpSvcMod : public http::ChttpModule {
public:
    WorkerExporterHttpSvcMod() = delete;
    WorkerExporterHttpSvcMod(WorkerExporterHttpSvcMod const&) = delete;
    WorkerExporterHttpSvcMod& operator=(WorkerExporterHttpSvcMod const&) = delete;

    /**
     * Process a request.
     *
     * Supported values for parameter 'subModuleName':
     *
     *  TABLE  for exporting fully replicated tables
     *  CHUNK  for exporting individual chunks of partitioned tables
     *
     * @note This module uses the custom response sending mechanism. The content type
     *  of the response is "text/csv" and the body contains the exported data. This differs
     *  from the standard response sending mechanism of the base class Module which
     *  sends JSON responses with the content type "application/json".
     *  See further details in the implementation of this module and the method sendResponse()
     *  of the base class ChttpModule.
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
    static void process(std::shared_ptr<ServiceProvider> const& serviceProvider,
                        std::string const& workerName,
                        std::shared_ptr<database::mysql::ConnectionPool> const& databaseConnectionPool,
                        httplib::Request const& req, httplib::Response& resp,
                        std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

protected:
    virtual std::string context() const final;
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    WorkerExporterHttpSvcMod(std::shared_ptr<ServiceProvider> const& serviceProvider,
                             std::string const& workerName,
                             std::shared_ptr<database::mysql::ConnectionPool> const& databaseConnectionPool,
                             httplib::Request const& req, httplib::Response& resp);

    /// Process the TABLE exportation request
    void _table();

    /// Process the CHUNK exportation request
    void _chunk();

    void _processRequest(std::string const& func);

    // Methods for parsing parameters of the request from the path and the query string
    void _parseParameters(std::string const& func);
    void _assertParamExists(std::string const& func, std::string const& name);
    void _parseCsvColumnFilters(std::string const& func);
    void _parseCsvDialect(std::string const& func);

    bool _keepAllCsvColumns() const {
        return _keepTransIdColumn && _keepChunkIdColumn && _keepSubChunkIdColumn;
    }

    // Request processing methods
    void _createTemporaryFile(std::string const& func);
    void _dumpTableIntoFile(std::string const& func);
    void _sendFileInResponse(std::string const& func);

    // Input parameters
    std::shared_ptr<ServiceProvider> const _serviceProvider;
    std::string const _workerName;
    std::shared_ptr<database::mysql::ConnectionPool> const _databaseConnectionPool;

    // Values of the parsed parameters defining a scope of the export operation
    std::string _databaseName;
    std::string _tableName;
    bool _isChunk = false;          ///< true for exporting chunks of partitioned tables
    unsigned int _chunkNumber = 0;  ///< The chunk number to be exported
    bool _isOverlap = false;        ///< Export overlapping chunks only

    /// Export format
    std::string _format = "CSV";

    // Parameters related to the CSV formatted output
    bool _keepTransIdColumn = false;    ///< Keep the 'qserv_trans_id' column
    bool _keepChunkIdColumn = true;     ///< Keep the 'chunkId' column
    bool _keepSubChunkIdColumn = true;  ///< Keep the 'subChunkId' column
    csv::Dialect _csvDialect;           ///< The CSV dialect tuned by request parameters

    // Request processing context
    std::string _filePath;  ///< The name of the temporary file holding exported data
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_WORKEREXPORTERHTTPSVCMOD_H
