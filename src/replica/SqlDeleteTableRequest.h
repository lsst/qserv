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
#ifndef LSST_QSERV_REPLICA_SQLDELETETABLEREQUEST_H
#define LSST_QSERV_REPLICA_SQLDELETETABLEREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/SqlRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlDeleteTableRequest represents Controller-side requests for initiating
 * queries for deleting tables at a remote worker nodes.
 */
class SqlDeleteTableRequest : public SqlRequest {
public:
    typedef std::shared_ptr<SqlDeleteTableRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    SqlDeleteTableRequest() = delete;
    SqlDeleteTableRequest(SqlDeleteTableRequest const&) = delete;
    SqlDeleteTableRequest& operator=(SqlDeleteTableRequest const&) = delete;

    ~SqlDeleteTableRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider is needed to access the Configuration and the Controller
     *   for communicating with the worker
     * @param io_service a communication end-point
     * @param worker identifier of a worker node
     * @param database the name of an existing database where the table is residing
     * @param tables the names of tables to be deleted
     * @param onFinish (optional) callback function to call upon completion of the request
     * @param priority priority level of the request
     * @param keepTracking keep tracking the request before it finishes or fails
     * @param messenger interface for communicating with workers
     * @return a pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                      std::string const& worker, std::string const& database,
                      std::vector<std::string> const& tables, CallbackType const& onFinish, int priority,
                      bool keepTracking, std::shared_ptr<Messenger> const& messenger);

protected:
    void notify(replica::Lock const& lock) final;

private:
    SqlDeleteTableRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                          std::string const& worker, std::string const& database,
                          std::vector<std::string> const& tables, CallbackType const& onFinish, int priority,
                          bool keepTracking, std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLDELETETABLEREQUEST_H
