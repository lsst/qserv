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
#include "replica/SqlRequest.h"

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
     * @param serviceProvider Is needed to access the Configuration and
     *   the Controller for communicating with the worker.
     * @param io_service The BOOST ASIO communication end-point.
     * @param worker An identifier of a worker node.
     * @param query The query to be executed.
     * @param user  The name of a database account for connecting to the database service.
     * @param password The database account password for connecting to the database service.
     * @param maxRows  The (optional) limit for the maximum number of rows to be returned with
     *   the request. Leaving the default value of the parameter to 0 will result in not imposing
     *   any explicit restrictions on a size of the result set. Note that other, resource-defined
     *   restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *   objects, the amount of available memory, etc.
     * @param onFinish  The (optional) callback function to call upon completion of
     *   the request.
     * @param priority  The priority level of the request.
     * @param keepTracking  Keep tracking the request before it finishes or fails.
     * @param messenger An interface for communicating with workers.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& worker, std::string const& query, std::string const& user,
                      std::string const& password, uint64_t maxRows, CallbackType const& onFinish,
                      int priority, bool keepTracking, std::shared_ptr<Messenger> const& messenger);

protected:
    /// @see Request::notify()
    void notify(replica::Lock const& lock) final;

private:
    /// @see SqlQueryRequest::create()
    SqlQueryRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                    std::string const& worker, std::string const& query, std::string const& user,
                    std::string const& password, uint64_t maxRows, CallbackType const& onFinish, int priority,
                    bool keepTracking, std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLQUERYREQUEST_H