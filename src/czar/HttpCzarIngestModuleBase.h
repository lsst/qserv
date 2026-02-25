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
#ifndef LSST_QSERV_CZAR_HTTPCZARINGESTMODULEBASE_H
#define LSST_QSERV_CZAR_HTTPCZARINGESTMODULEBASE_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"

// Forward declarations

namespace lsst::qserv::http {
class AsyncReq;
class Client;
class ClientConnPool;
class ClientMimeEntry;
}  // namespace lsst::qserv::http

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpCzarIngestModuleBase is a base class for a family of the Czar ingest modules.
 * A purpose of the class is to provide subclasses with common services and data, and avoid code
 * duplication should each subclass had its own implementation of the services.
 */
class HttpCzarIngestModuleBase {
public:
    HttpCzarIngestModuleBase() = delete;
    HttpCzarIngestModuleBase(HttpCzarIngestModuleBase const&) = delete;
    HttpCzarIngestModuleBase& operator=(HttpCzarIngestModuleBase const&) = delete;

    virtual ~HttpCzarIngestModuleBase() = default;

protected:
    // Common partitioning parameters.
    static unsigned int defaultNumStripes;
    static unsigned int defaultNumSubStripes;
    static float defaultOverlap;

    HttpCzarIngestModuleBase(boost::asio::io_service& io_service);

    // HTTP timeout management methods.

    void setTimeoutSec(unsigned int timeoutSec) { _timeoutSec = timeoutSec; }
    unsigned int timeoutSec() const { return _timeoutSec; }

    /**
     * Ingest the table into the Qserv.
     * @param databaseName The name of the database to ingest the data into.
     * @param tableName The name of the table to ingest the data into.
     * @param isPartitioned A flag indicating whether the table is partitioned.
     * @param isDirector A flag indicating whether the table is a director table.
     * @param directorIdColName The name of the column to be used as a director key.
     * @param directorLongitudeColName The name of the column to be used as a director longitude.
     * @param directorLatitudeColName The name of the column to be used as a director latitude.
     * @param refDirectorDatabaseName The name of the reference director database.
     * @param refDirectorTableName The name of the reference director table.
     * @param charsetName The name of the character set for the table.
     * @param collationName The name of the collation for the table.
     * @param schema The schema of the table.
     * @param indexes The indexes to be created for the table.
     * @param chunkIds The set of chunk IDs to be allocated (allowed to be empty)
     * @param submitRequestsToWorkers A function to submit requests to the workers. The function is
     *   expected to return a map of worker identifiers and error messages reported by the workers.
     *   The first input parameter of the function is the transaction identifier.
     *   The second input parameter of the function is the mapping of chunk IDs to the corresponding
     * collections of worker IDs (the map will match a collection of the chunk IDs in the parameter chunkIds).
     * @note The current implementation of the function allows creating two types of tables:
     *   - Fully replicated ables that are not partitioned and have no director columns. In this case,
     *     the set of chunk IDs and the director-specific parameters are ignored.
     *   - Partitioned tables that have director-specific columns. In this case, the set of chunk IDs
     *     and the director-specific parameters are expected to be non-empty.
     * @return A collection of warnings reported by the ingest process, where each entry is represented by
     *   a pair of a scope (function) and a message of the warning.
     * @throw http::Error In case of a communication error or an error reported by the server.
     */
    std::list<std::pair<std::string, std::string>> ingestData(
            std::string const& databaseName, std::string const& tableName, bool isPartitioned,
            bool isDirector, std::string directorIdColName, std::string directorLongitudeColName,
            std::string directorLatitudeColName, std::string const& refDirectorDatabaseName,
            std::string const& refDirectorTableName, std::string const& charsetName,
            std::string const& collationName, nlohmann::json const& schema, nlohmann::json const& indexes,
            std::set<std::int32_t> const& chunkIds,
            std::function<std::map<std::string, std::string>(
                    std::uint32_t, std::map<std::int32_t, std::vector<std::string>> const&)> const&
                    submitRequestsToWorkers);

    /**
     * Verify the user-provided database name to ensure the name starts with the reserved
     * prefix "user_".
     * @param func The name of the calling function.
     * @param databaseName The name of the database to be verified.
     * @throw http::Error if the name is too short, or it doesn't start with the required prefix.
     */
    static void verifyUserDatabaseName(std::string const& func, std::string const& databaseName);

    /**
     * Verify the user-provided table name to ensure the name doesn't start with the reserved
     * prefix "qserv_".
     * @param func The name of the calling function.
     * @param tableName The name of the table to be verified.
     * @throw http::Error if the name is too short, or it starts with the reserved prefix.
     */
    static void verifyUserTableName(std::string const& func, std::string const& tableName);

    /**
     * Delete the specified database in Qserv.
     * @param databaseName The name of the database to be deleted.
     * @throw http::Error In case of a communication error or an error reported by the server.
     */
    void deleteDatabase(std::string const& databaseName);

    /**
     * Delete the specified table in Qserv.
     * @param databaseName The name of the database were the table is residing.
     * @param tableName The name of the table to be deleted.
     * @throw http::Error In case of a communication error or an error reported by the server.
     */
    void deleteTable(std::string const& databaseName, std::string const& tableName);

    /**
     * Get the list of worker identifiers.
     * @return A vector of worker identifiers.
     * @throw http::Error In case of a communication error or an error reported by the server.
     */
    std::vector<std::string> getWorkerIds();

    /**
     * Create an asynchronous POST request to the specified Replication Worker.
     * @note The request won't be started. It's up to a caller to do so.
     * @param workerId The worker's identifier.
     * @param method HTTP method for the request.
     * @param data Serialized JSON object to be sent with the request.
     * @return std::shared_ptr<http::AsyncReq> A pointer to the request object.
     */
    std::shared_ptr<http::AsyncReq> asyncRequestWorker(std::string const& workerId, std::string const& data) {
        return _asyncPostRequest(_worker(workerId) + "/ingest/data", data);
    }

    /**
     * Create a synchronous MIMEPOST request to the specified Replication Worker.
     * @note The request won't be started. It's up to a caller to do so. The duration of
     * the request is limited by the optional timeout attribute set by calling the method
     * setTimeoutSec().
     * @param workerId The worker's identifier.
     * @param mimeData The collection of the mime descriptors to be sent with the request.
     * @param connPool The optional connection pool.
     * @return std::shared_ptr<http::Client> A pointer to the request object.
     */
    std::shared_ptr<http::Client> syncCsvRequestWorker(
            std::string const& workerId, std::list<http::ClientMimeEntry> const& mimeData,
            std::shared_ptr<http::ClientConnPool> const& connPool = nullptr) {
        return _syncMimePostRequest(_worker(workerId) + "/ingest/csv", mimeData, connPool);
    }

    /**
     * Set the protocol fields in the JSON object.
     * @param data The JSON object to be updated.
     */
    void setProtocolFields(nlohmann::json& data) const;

    /**
     * Set the protocol fields in a collection of the mime descriptors.
     * @param mimeData The collection of the descriptors to be updated.
     */
    void setProtocolFields(std::list<http::ClientMimeEntry>& mimeData) const;

    /**
     * The database family descriptor.
     */
    struct DatabaseFamily {
        std::string familyName;
        unsigned int numStripes = 0;
        unsigned int numSubStripes = 0;
        double overlap = 0.0;
    };

    /**
     * Get parameters of the database family for the specified database.
     * @param databaseName The name of the database to get the family for.
     * @return DatabaseFamily The family descriptor.
     * @throw std::invalid_argument If the input parameter is empty.
     * @throw std::logic_error If no such database or family exists.
     * @throw http::Error In case of a communication errors or any errors reported by the Controller.
     */
    DatabaseFamily getDatabaseFamily(const std::string& databaseName);

private:
    // The following methods are used to interact with the Replication Controller.
    // The methods throw http::Error or other exceptions in case of communication
    // errors or errors reported by the server.

    void _unpublishOrCreateDatabase(const std::string& databaseName, bool createDefaultDirectorTable);
    void _createDatabase(std::string const& databaseName);
    void _unpublishDatabase(std::string const& databaseName);
    void _publishDatabase(std::string const& databaseName);

    void _createTable(std::string const& databaseName, std::string const& tableName, bool isPartitioned,
                      bool isDirector, std::string directorIdColName, std::string directorLongitudeColName,
                      std::string directorLatitudeColName, std::string const& refDirectorDatabaseName,
                      std::string const& refDirectorTableName, std::string const& charsetName,
                      std::string const& collationName, nlohmann::json const& schema);
    void _createDefaultDirectorTable(std::string const& databaseName);

    std::uint32_t _startTransaction(std::string const& databaseName);
    void _abortTransaction(std::uint32_t id) { _abortOrCommitTransaction(id, true); }
    void _commitTransaction(std::uint32_t id) { _abortOrCommitTransaction(id, false); }
    void _abortOrCommitTransaction(std::uint32_t id, bool abort);

    /**
     * Allocate a chunk for the specified chunk ID.
     * @param databaseName The name of the database to allocate the chunk for.
     * @param chunkId The chunk ID to be allocated.
     * @return A worker identifier where the chunk is allocated.
     * @throw http::Error In case of a communication error or an error reported by the server.
     */
    std::string _allocateChunk(std::string const& databaseName, std::int32_t chunkId);

    /**
     * Allocate chunks for the specified chunks.
     * @param chunkIds The set of chunk identifiers.
     * @param databaseName The name of the database to allocate the chunks for.
     * @return A map of chunks to the corresponding collections of worker identifiers.
     * @throw http::Error In case of a communication error or an error reported by the server.
     */
    std::map<std::int32_t, std::vector<std::string>> _allocateChunks(std::set<std::int32_t> const& chunkIds,
                                                                     std::string const& databaseName);

    void _createIndexes(std::string const& func, std::string const& databaseName,
                        std::string const& tableName, nlohmann::json const& indexes,
                        std::list<std::pair<std::string, std::string>>& warnings);

    void _countRows(std::string const& func, std::string const& databaseName, std::string const& tableName,
                    std::list<std::pair<std::string, std::string>>& warnings);

    /**
     * Pull connection parameters of the Master Replication Controller from Registry
     * and build the base path of the Controller's service. The result will be cached.
     * The method will return the cached value if the one is already available.
     * @return The base URL for the Controller.
     */
    std::string _controller();

    /**
     * Pull connection parameters of the specified worker from the Master Replication Controller
     * and build the base path of the service. The result will be cached.
     * The method will return the cached value if the one is already available.
     * @param workerId The worker's identifier.
     * @return The base URL for the worker.
     */
    std::string _worker(std::string const& workerId);

    /**
     * Send a request to the Master Replication Controller.
     * @param method HTTP method for the request.
     * @param service The REST service to be called (not a complete URL).
     * @param data Data to be sent with the request.
     * @return nlohmann::json A result (JSON object) reported by the server.
     */
    nlohmann::json _requestController(http::Method method, std::string const& service, nlohmann::json& data) {
        return _request(method, _controller() + service, data);
    }

    nlohmann::json _requestController(http::Method method, std::string const& service) {
        nlohmann::json data;
        return _requestController(method, service, data);
    }

    /**
     * Send a request to the Registry.
     * @param method HTTP method for the request.
     * @param service The REST service to be called (not a complete URL).
     * @return nlohmann::json A result (JSON object) reported by the server.
     */
    nlohmann::json _requestRegistry(http::Method method, std::string const& service) {
        return _request(method, _registryBaseUrl + service);
    }

    /**
     * Send a request to a server, wait for its completion and extract a result).
     * @note The data object may be extended by the method to include additional
     *  attrubutes required for the request, including the version number of
     *  the REST API and the authorization keys.
     * @param method HTTP method for the request.
     * @param url A complete URL for the REST service to be called.
     * @param data Data to be sent with the request.
     * @return nlohmann::json A result (JSON object) reported by the server.
     * @throw http::Error for specific errors reported by the client library.
     * @throw std::runtime_error In case if a error was received from the server.
     */
    nlohmann::json _request(http::Method method, std::string const& url, nlohmann::json& data);

    nlohmann::json _request(http::Method method, std::string const& url) {
        nlohmann::json data;
        return _request(method, url, data);
    }

    /**
     * Create an asynchronous request to the server.
     * @note The data object may be extended by the method to include additional
     *  attrubutes required for the request, including the version number of
     *  the REST API and the authorization keys.
     * @note The request won't be started. It's up to a caller to do so.
     * @param method HTTP method for the request.
     * @param url A complete URL for the REST service to be called.
     * @param data Data to be sent with the request.
     * @return std::shared_ptr<http::AsyncReq> A pointer to the request object.
     * @throw http::Error for specific errors reported by the client library.
     */
    std::shared_ptr<http::AsyncReq> _asyncRequest(http::Method method, std::string const& url,
                                                  nlohmann::json& data);

    /**
     * Create an asynchronous POST request to the server.
     * @note The request won't be started. It's up to a caller to do so.
     * @param url A complete URL for the REST service to be called.
     * @param data Serialized JSON object to be sent with the request.
     * @return std::shared_ptr<http::AsyncReq> A pointer to the request object.
     * @throw http::Error for specific errors reported by the client library.
     */
    std::shared_ptr<http::AsyncReq> _asyncPostRequest(std::string const& url, std::string const& data);

    /**
     * Create a synchronous MIMEPOST request to the server.
     * @note The request won't be started. It's up to a caller to do so.
     * @param url A complete URL for the REST service to be called.
     * @param mimeData The collection of the mime descriptors to be sent with the request.
     * @param connPool The optional connection pool.
     * @return std::shared_ptr<http::Client> A pointer to the request object.
     * @throw http::Error for specific errors reported by the client library.
     */
    std::shared_ptr<http::Client> _syncMimePostRequest(
            std::string const& url, std::list<http::ClientMimeEntry> const& mimeData,
            std::shared_ptr<http::ClientConnPool> const& connPool = nullptr);

    /// I/O service for async TCP communications.
    boost::asio::io_service& _io_service;

    /// Base URL for communications with the Registry server.
    std::string const _registryBaseUrl;

    /// The default timeout for processing requests can be changed by a subclass
    /// before sending requests to servers.
    unsigned int _timeoutSec = 300;

    /// The cached values of the base URLs for the Controller and workers.
    /// The values are filled on demand by the methods _controller() and _worker()
    /// the first time they are called.
    std::string _controllerBaseUrl;                      ///< The URL for the Controller's REST service.
    std::map<std::string, std::string> _workerBaseUrls;  ///< The URLs for workers' REST services.
    std::mutex _mtx;                                     ///< Mutex to protect access to the cached URLs.
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARINGESTMODULEBASE_H
