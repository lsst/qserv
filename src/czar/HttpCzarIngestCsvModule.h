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
#ifndef LSST_QSERV_CZAR_HTTPCZARINGESTCSVMODULE_H
#define LSST_QSERV_CZAR_HTTPCZARINGESTCSVMODULE_H

// System headers
#include <fstream>
#include <list>
#include <map>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "czar/HttpCzarIngestModuleBase.h"
#include "czar/WorkerIngestProcessor.h"
#include "http/FileUploadModule.h"
#include "qmeta/UserTableIngestRequest.h"

// Forward declarations

namespace lsst::qserv::http {
class ClientConnPool;
class ClientMimeEntry;
}  // namespace lsst::qserv::http

namespace lsst::qserv::qmeta {
class UserTables;
}  // namespace lsst::qserv::qmeta

namespace lsst::qserv::sql {
class SqlConnection;
}  // namespace lsst::qserv::sql

namespace httplib {
class ContentReader;
class Request;
class Response;
}  // namespace httplib

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpCzarIngestCsvModule implements a handler for processing requests for ingesting
 * user-generated data products via the HTTP-based frontend. The requests are expected to
 * contain CSV data, JSON schema and the relevant parameters in the multipart/form-data body of
 * the request.
 */
class HttpCzarIngestCsvModule : public http::FileUploadModule, public HttpCzarIngestModuleBase {
public:
    static void process(boost::asio::io_service& io_service, std::string const& context,
                        std::string const& tmpDir, httplib::Request const& req, httplib::Response& resp,
                        httplib::ContentReader const& contentReader,
                        std::shared_ptr<http::ClientConnPool> const& clientConnPool,
                        std::shared_ptr<ingest::Processor> const& workerIngestProcessor,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpCzarIngestCsvModule() = delete;
    HttpCzarIngestCsvModule(HttpCzarIngestCsvModule const&) = delete;
    HttpCzarIngestCsvModule& operator=(HttpCzarIngestCsvModule const&) = delete;

    /// Destructor is responsible for cleaning up the temporary files.
    virtual ~HttpCzarIngestCsvModule();

protected:
    virtual std::string context() const final;
    virtual void onStartOfFile(std::string const& name, std::string const& fileName,
                               std::string const& contentType) final;
    virtual void onFileData(char const* data, std::size_t length) final;
    virtual void onEndOfFile() final;
    virtual nlohmann::json onEndOfBody() final;

private:
    HttpCzarIngestCsvModule(boost::asio::io_service& io_service, std::string const& context,
                            std::string const& tmpDir, httplib::Request const& req, httplib::Response& resp,
                            httplib::ContentReader const& contentReader,
                            std::shared_ptr<http::ClientConnPool> const& clientConnPool,
                            std::shared_ptr<ingest::Processor> const& workerIngestProcessor);

    nlohmann::json _ingestDirectorTable();
    nlohmann::json _ingestChildTable();

    void _makeDirectorIndexFile();

    /**
     * Scan the input file and get a collection of the object identifiers.
     * @return The key of the map is the object identifier and the value is the pair
     *  of (chunkId, subChunkId) values. Values of (chunkId, subChunkId) will be
     *  initialized with (-1, -1).
     */
    std::map<std::string, std::pair<int, int>> _getObjectIdsFromInputFile();

    /**
     * Get the maximum allowed length of a query to be sent to the MySQL server. The method is used
     * to determine the maximum number of identifiers that can be packed into a single query when
     * pulling the (objectId, chunkId, subChunkId) mapping for the referenced director table.
     * The implementation will submit the "SHOW VARIABLES LIKE 'max_allowed_packet'" query
     * to the MySQL server and parse the results.
     * @see https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet
     * @throw http::Error In case of a communication error or an error reported by the MySQL server.
     */
    size_t _getMaxQueryLength(std::shared_ptr<sql::SqlConnection> const& conn);

    void _injectIdColValues();
    void _createChunksDir();
    void _getFileSize();

    /**
     * Partition the input data file into the chunk files.
     * @param configBase The table type-specific configuration for the partitioning.
     *   The configuration will be extended with the common parameters.
     */
    void _partitionTableData(nlohmann::json const& configBase);

    void _pushChunksToWorkersDriver();
    std::map<std::string, std::string> _pushChunksToWorkers(
            std::uint32_t transactionId,
            std::map<std::int32_t, std::vector<std::string>> const& chunk2workerIds);

    nlohmann::json _ingestFullyReplicatedTable();
    std::map<std::string, std::string> _pushDataToWorkers(std::uint32_t transactionId);

    void _pushFileToWorker(std::string const& chunkFilePath, std::string const& workerId,
                           std::int32_t chunkId = 0, bool overlap = false);

    std::list<http::ClientMimeEntry> _createMimeData(std::string const& filePath, std::int32_t chunkId,
                                                     bool overlap) const;
    void _reportCompletedRequest(std::string const& func);
    nlohmann::json _reportFailedRequest(std::string const& func, std::string const& operation,
                                        std::string const& errorMessage,
                                        nlohmann::json const& errorExt = nlohmann::json());

    // Input parameters
    std::string const _context;  ///< The context for posting messages into the logging stream.
    std::string const _tmpDir;   ///< The temporary directory for storing the uploaded files.

    /// The HTTP connection pool for communications with workers.
    std::shared_ptr<http::ClientConnPool> const _clientConnPool;  ///< Provided via c-tor parameter.

    // The que-based ingest processor for uploading data to workers and a queue for
    // accumulating the results.
    std::shared_ptr<ingest::Processor> const _workerIngestProcessor;  ///< Provided via c-tor parameter.
    std::shared_ptr<ingest::ResultQueue> const _resultQueue;          ///< Constructed locally.

    // The following parameters are used to store the uploaded files found the body of a request.
    std::string _name;            ///< The name of a file entry that is open ("rows", "schema" or "indexes").
    std::string _csvFilePath;     ///< The absolute path of the CSV file in the temporary directory.
    std::string _csvExtFilePath;  ///< The absolute path of the extended CSV file (if any).
    std::string _csvDirIndexFilePath;  ///< The absolute path of the director index file for partitioned
                                       ///< tables (if any).
    std::ofstream _csvFile;            ///< The output stream for the CSV file.
    std::string _schema;               ///< The schema payload before parsing it into the JSON object.
    std::string _indexes;              ///< The indexes payload before parsing it into the JSON object.

    // The following parameters are parsed from the request body.
    std::string _databaseName;
    std::string _tableName;
    bool _isPartitioned = false;
    bool _isDirector = false;
    bool _injectIdCol = false;      ///< A flag indicating whether to inject an ID (PK) column.
    std::string _idColName;         ///< The name of the ID (PK) column for partitioned tables.
    std::string _longitudeColName;  ///< Right ascension column name for partitioned tables.
    std::string _latitudeColName;   ///< Declination column name for partitioned tables.
    std::string
            _refDirectorDatabase;   ///< The name of the database for the director table of a dependent table.
    std::string _refDirectorTable;  ///< The name of the director table of a dependent table.
    std::string
            _refDirectorIdColName;  ///< The name of the ID column in the director table of a dependent table.
    std::string _charsetName;
    std::string _collationName;
    std::string _fieldsTerminatedBy;
    std::string _fieldsEnclosedBy;
    std::string _fieldsEscapedBy;
    std::string _linesTerminatedBy;

    // These parameters are used to store and update the state of the ingest request in the QMeta database.
    std::shared_ptr<qmeta::UserTables> _userTables;  ///< The reference to the UserTables database interface.
    qmeta::UserTableIngestRequest _request;          ///< The ingest request registered in the QMeta database.

    // These parameters are used when the input file is partitioned.
    std::string _chunksDirName;                     ///< The name of the directory for the chunk files.
    std::set<int32_t> _chunkIds;                    ///< The unique identifiers of the chunks.
    std::map<int32_t, std::string> _chunkTables;    ///< The names of the chunk tables.
    std::map<int32_t, std::string> _overlapTables;  ///< The names of the overlap tables.

    // Ingest statistics
    std::uint32_t _transactionId = 0;  ///< The transaction ID assigned by the Replication/Ingest system.
    std::uint64_t _numBytes = 0;       ///< The number of bytes in the input data file/stream.
    std::uint32_t _numChunks = 0;      ///< The total number of chunks ingested.
    std::atomic<std::uint64_t> _numRows{0};  ///< The total number of rows in the input data file/stream.
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARINGESTCSVMODULE_H
