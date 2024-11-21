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
#ifndef LSST_QSERV_REPLICA_SQLQUERYREQUEST_H
#define LSST_QSERV_REPLICA_SQLQUERYREQUEST_H

// System headers
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/requests/SqlRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlQueryRequest represents Controller-side requests for initiating
 * arbitrary database queries at a remote worker nodes.
 */
class SqlQueryRequest : public SqlRequest {
public:
    typedef std::shared_ptr<SqlQueryRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    SqlQueryRequest() = delete;
    SqlQueryRequest(SqlQueryRequest const&) = delete;
    SqlQueryRequest& operator=(SqlQueryRequest const&) = delete;

    ~SqlQueryRequest() final = default;

    std::string const& query() const { return requestBody.query(); }
    std::string const& user() const { return requestBody.user(); }
    std::string const& password() const { return requestBody.password(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * Class-specific parameters are documented below:
     * @param query The query to be executed.
     * @param user The name of a database account for connecting to the database service.
     * @param password The database account password for connecting to the database service.
     * @param maxRows  The limit for the maximum number of rows to be returned with
     *   the request. Setting a value of the parameter to 0 will result in not imposing
     *   any explicit restrictions on a size of the result set. Note that other, resource-defined
     *   restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *   objects, the amount of available memory, etc.
     *
     * @see The very base class Request for the description of the common parameters
     *   of all subclasses.
     *
     * @return A pointer to the created object.
     */
    static Ptr createAndStart(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                              std::string const& query, std::string const& user, std::string const& password,
                              uint64_t maxRows, CallbackType const& onFinish = nullptr,
                              int priority = PRIORITY_NORMAL, bool keepTracking = true,
                              std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

protected:
    void notify(replica::Lock const& lock) final;

private:
    SqlQueryRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                    std::string const& query, std::string const& user, std::string const& password,
                    uint64_t maxRows, CallbackType const& onFinish, int priority, bool keepTracking);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLQUERYREQUEST_H