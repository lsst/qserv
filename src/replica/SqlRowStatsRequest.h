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
#ifndef LSST_QSERV_REPLICA_SQLROWSTATSREQUEST_H
#define LSST_QSERV_REPLICA_SQLROWSTATSREQUEST_H

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
 * Class SqlRowStatsRequest represents Controller-side requests for initiating
 * queries for obtaining row counters from existing tables at remote worker nodes.
 */
class SqlRowStatsRequest : public SqlRequest {
public:
    typedef std::shared_ptr<SqlRowStatsRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    SqlRowStatsRequest() = delete;
    SqlRowStatsRequest(SqlRowStatsRequest const&) = delete;
    SqlRowStatsRequest& operator=(SqlRowStatsRequest const&) = delete;

    ~SqlRowStatsRequest() final = default;

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
                      std::vector<std::string> const& tables, CallbackType const& onFinish, int priority,
                      bool keepTracking, std::shared_ptr<Messenger> const& messenger);

protected:
    void notify(replica::Lock const& lock) final;

private:
    SqlRowStatsRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                       std::string const& worker, std::string const& database,
                       std::vector<std::string> const& tables, CallbackType const& onFinish, int priority,
                       bool keepTracking, std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLROWSTATSREQUEST_H
