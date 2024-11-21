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
#include "replica/requests/SqlRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

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
     * Class-specific parameters are documented below:
     * @param database The name of an existing database where the table is residing.
     * @param tables The names of tables to be deleted.
     *
     * @see The very base class Request for the description of the common parameters
     *   of all subclasses.
     *
     * @return a pointer to the created object
     */
    static Ptr createAndStart(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                              std::string const& database, std::vector<std::string> const& tables,
                              CallbackType const& onFinish = nullptr, int priority = PRIORITY_NORMAL,
                              bool keepTracking = true, std::string const& jobId = "",
                              unsigned int requestExpirationIvalSec = 0);

protected:
    void notify(replica::Lock const& lock) final;

private:
    SqlDeleteTableRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                          std::string const& database, std::vector<std::string> const& tables,
                          CallbackType const& onFinish, int priority, bool keepTracking);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLDELETETABLEREQUEST_H
