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
#ifndef LSST_QSERV_REPLICA_HTTPSQLREQUEST_H
#define LSST_QSERV_REPLICA_HTTPSQLREQUEST_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <tuple>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/BinaryEncoding.h"
#include "replica/proto/Protocol.h"
#include "replica/requests/http/HttpRequest.h"
#include "replica/requests/SqlResultSet.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::http {
class AsyncReq;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Abstract class HttpSqlRequest is a base class for a family of the Controller-side
 * requests launching queries against a MySQL/MariaDB database of Qserv workers
 * via the corresponding Replication workers. The requests are sent over
 * the controller-worker protocol and they are executed by the the worker-side
 * framework.
 *
 * In case of a successful completion of a request an object of this request class
 * will receive a result set (if any) of the query.
 */
class HttpSqlRequest : public HttpRequest {
public:
    /**
     * Make an extended print of the request which would include a result set.
     * The method will also make a call to RequestI::defaultPrinter().
     * @param ptr An object to be printed.
     */
    static void extendedPrinter(std::shared_ptr<HttpSqlRequest> const& ptr);

    HttpSqlRequest() = delete;
    HttpSqlRequest(HttpSqlRequest const&) = delete;
    HttpSqlRequest& operator=(HttpSqlRequest const&) = delete;

    virtual ~HttpSqlRequest() override = default;

    std::uint64_t maxRows() const { return _maxRows; }

    /**
     * @note This operation will return a sensible result only if the operation
     *  finishes with status FINISHED::SUCCESS
     * @return a reference to a result obtained from a remote service.
     */
    SqlResultSet const& responseData() const;

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    /**
     * Create a new request with specified parameters.
     *
     * Class-specific parameters are documented below:
     * @param type The type of the SQL request.
     * @param databaseName The name of the database to query.
     * @param batchMode Whether to execute the request in batch mode.
     * @param data The additional JSON-encoded data to be sent with the request.
     * @param maxRows The (optional) limit for the maximum number of rows to be returned with the request.
     *  Leaving the default value of the parameter to 0 will result in not imposing any
     *  explicit restrictions on a size of the result set. Note that other, resource-defined
     *  restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *  objects, the amount of available memory, etc.
     * @param binaryEncodingMode The optional binary encoding for the server responses.
     * @see The very base class for the description of the common parameters of all subclasses.
     */
    HttpSqlRequest(std::shared_ptr<Controller> const& controller, std::string const& requestName,
                   protocol::SqlRequestType type, std::string const& databaseName, bool batchMode,
                   nlohmann::json const& params, std::string const& workerName, std::uint64_t maxRows,
                   int priority, bool keepTracking,
                   http::BinaryEncodingMode binaryEncodingMode = http::BinaryEncodingMode::HEX);

    virtual void startImpl(replica::Lock const& lock) final;
    virtual void savePersistentState(replica::Lock const& lock) final;
    virtual void analyze(std::shared_ptr<http::AsyncReq> req, std::string error) final;

private:
    nlohmann::json _params;        ///< The final JSON-encoded parameters to be sent with the request.
    std::uint64_t const _maxRows;  ///< The maximum number of rows to be returned with the request.
    http::BinaryEncodingMode const _binaryEncodingMode;
    SqlResultSet _responseData;  ///< The results reported by a worker service
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPSQLREQUEST_H
