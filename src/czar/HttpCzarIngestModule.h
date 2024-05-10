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
#ifndef LSST_QSERV_CZAR_HTTPCZARINGESTMODULE_H
#define LSST_QSERV_CZAR_HTTPCZARINGESTMODULE_H

// System headers
#include <map>
#include <memory>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"
#include "http/ModuleBase.h"

// Forward declarations

namespace lsst::qserv::http {
class AsyncReq;
}  // namespace lsst::qserv::http

namespace lsst::qserv::qhttp {
class Request;
class Response;
}  // namespace lsst::qserv::qhttp

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpCzarIngestModule implements a handler for processing requests for ingesting
 * user-generated data prodicts via the HTTP-based frontend.
 */
class HttpCzarIngestModule : public http::ModuleBase {
public:
    /**
     * @note supported values for parameter 'subModuleName' are:
     *   'INGEST-DATA'     - create a table and load it with data (sync)
     *   'DELETE-DATABASE' - delete an existing database (sync)
     *   'DELETE-TABLE'    - delete an existing table (sync)
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(boost::asio::io_service& io_service, std::string const& context,
                        std::shared_ptr<qhttp::Request> const& req,
                        std::shared_ptr<qhttp::Response> const& resp, std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpCzarIngestModule() = delete;
    HttpCzarIngestModule(HttpCzarIngestModule const&) = delete;
    HttpCzarIngestModule& operator=(HttpCzarIngestModule const&) = delete;

    virtual ~HttpCzarIngestModule() = default;

protected:
    virtual std::string context() const final;
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpCzarIngestModule(boost::asio::io_service& io_service, std::string const& context,
                         std::shared_ptr<qhttp::Request> const& req,
                         std::shared_ptr<qhttp::Response> const& resp);

    nlohmann::json _ingestData();
    nlohmann::json _deleteDatabase();
    nlohmann::json _deleteTable();

    // The following methods are used to interact with the Replication Controller.
    // The methods throw http::Error or other exceptions in case of communication
    // errors or errors reported by the server.

    void _unpublishOrCreateDatabase(const std::string& databaseName);
    void _createDatabase(std::string const& databaseName);
    void _deleteDatabase(std::string const& databaseName);
    void _unpublishDatabase(std::string const& databaseName);
    void _publishDatabase(std::string const& databaseName);

    void _createTable(std::string const& databaseName, std::string const& tableName,
                      nlohmann::json const& schema);
    void _createDirectorTable(std::string const& databaseName);
    void _deleteTable(std::string const& databaseName, std::string const& tableName);

    std::uint32_t _startTransaction(std::string const& databaseName);
    void _abortTransaction(std::uint32_t id) { _abortOrCommitTransaction(id, true); }
    void _commitTransaction(std::uint32_t id) { _abortOrCommitTransaction(id, false); }
    void _abortOrCommitTransaction(std::uint32_t id, bool abort);

    nlohmann::json _allocateChunk(std::string const& databaseName, unsigned int chunkId);

    void _createIndexes(std::string const& func, std::string const& databaseName,
                        std::string const& tableName, nlohmann::json const& indexes);
    void _countRows(std::string const& func, std::string const& databaseName, std::string const& tableName);

    std::vector<std::string> _getWorkerIds();

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
     * Create an asynchronous POST request to the specified Replication Worker.
     * @note The request won't be started. It's up to a caller to do so.
     * @param workerId The worker's identifier.
     * @param method HTTP method for the request.
     * @param data Serialized JSON object to be sent with the request.
     * @return std::shared_ptr<http::AsyncReq> A pointer to the request object.
     */
    std::shared_ptr<http::AsyncReq> _asyncRequestWorker(std::string const& workerId,
                                                        std::string const& data) {
        return _asyncPostRequest(_worker(workerId) + "/ingest/data", data);
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

    /// I/O service for async TCP communications.
    boost::asio::io_service& _io_service;

    /// The context string for posting messages into the logging stream.
    std::string const _context;

    /// Base URL for communications with the Registry server.
    std::string const _registryBaseUrl;

    // Parameters set upon the request processing.

    unsigned int _timeoutSec = 300;  ///< The default timeout for requests.
    std::string _controllerBaseUrl;  ///< The cached URL for the Controller's REST service.
    std::map<std::string, std::string> _workerBaseUrls;  ///< The cached URLs for workers' REST services.
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARINGESTMODULE_H
