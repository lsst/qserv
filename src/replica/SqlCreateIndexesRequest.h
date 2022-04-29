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
#ifndef LSST_QSERV_REPLICA_SQLCREATEINDEXESREQUEST_H
#define LSST_QSERV_REPLICA_SQLCREATEINDEXESREQUEST_H

// System headers
#include <functional>
#include <list>
#include <memory>
#include <tuple>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/SqlRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlCreateIndexesRequest represents Controller-side requests for initiating
 * queries for creating table indexes at remote worker nodes.
 *
 * @note all tables created by the operation will have exactly the same index.
 */
class SqlCreateIndexesRequest : public SqlRequest {
public:
    typedef std::shared_ptr<SqlCreateIndexesRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    SqlCreateIndexesRequest() = delete;
    SqlCreateIndexesRequest(SqlCreateIndexesRequest const&) = delete;
    SqlCreateIndexesRequest& operator=(SqlCreateIndexesRequest const&) = delete;

    ~SqlCreateIndexesRequest() final = default;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider Is needed to access the Configuration and
     *   the Controller for communicating with the worker.
     * @param io_service The BOOST ASIO communication end-point.
     * @param worker An identifier of a worker node.
     * @param database The name of an existing database where the tables are residing.
     * @param tables The names of tables affected by the operation.
     * @param indexSpec The type specification of the index.
     * @param indexName The name of the index.
     * @param indexComment An arbitrary comment string documenting the index.
     * @param indexColumns Column definitions (name,length,ordering) for the index.
     * @param onFinish (optional) A callback function to call upon completion of
     *   the request.
     * @param priority A priority level of the request.
     * @param keepTracking Keep tracking the request before it finishes or fails.
     * @param messenger An interface for communicating with workers.
     *
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& worker, std::string const& database,
                      std::vector<std::string> const& tables, SqlRequestParams::IndexSpec const& indexSpec,
                      std::string const& indexName, std::string const& indexComment,
                      std::vector<SqlIndexColumn> const& indexColumns, CallbackType const& onFinish,
                      int priority, bool keepTracking, std::shared_ptr<Messenger> const& messenger);

protected:
    void notify(util::Lock const& lock) final;

private:
    SqlCreateIndexesRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                            std::string const& worker, std::string const& database,
                            std::vector<std::string> const& tables,
                            SqlRequestParams::IndexSpec const& indexSpec, std::string const& indexName,
                            std::string const& indexComment, std::vector<SqlIndexColumn> const& indexColumns,
                            CallbackType const& onFinish, int priority, bool keepTracking,
                            std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLCREATEINDEXESREQUEST_H
