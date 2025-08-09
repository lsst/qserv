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
#ifndef LSST_QSERV_REPLICA_SQLCREATETABLEREQUEST_H
#define LSST_QSERV_REPLICA_SQLCREATETABLEREQUEST_H

// System headers
#include <functional>
#include <list>
#include <memory>
#include <tuple>
#include <string>

// Qserv headers
#include "replica/requests/SqlRequest.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlCreateTableRequest represents Controller-side requests for initiating
 * queries for creating tables at a remote worker nodes.
 */
class SqlCreateTableRequest : public SqlRequest {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlCreateTableRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlCreateTableRequest() = delete;
    SqlCreateTableRequest(SqlCreateTableRequest const&) = delete;
    SqlCreateTableRequest& operator=(SqlCreateTableRequest const&) = delete;

    ~SqlCreateTableRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * Class-specific parameters are documented below:
     * @param database The name of an existing database where the new table will be created.
     * @param table The name of a table to be created.
     * @param engine The name of the MySQL engine for the new table.
     * @param partitionByColumn The (optional, if not empty) name of a column which
     *   will be used as a key to configure MySQL partitions for the new table.
     *   This variation of table schema will be used for the super-transaction-based
     *   ingest into the table.
     * @param columns Column definitions (name,type) of the table.
     * @param charsetName (optional, if not empty) the name of a character set for the new table
     * @param collationName (optional, if not empty) the name of a collation for the new table
     *
     * @see The very base class Request for the description of the common parameters
     *   of all subclasses.
     *
     * @return A pointer to the created object.
     */
    static Ptr createAndStart(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                              std::string const& database, std::string const& table,
                              std::string const& engine, std::string const& partitionByColumn,
                              std::list<SqlColDef> const& columns, std::string const& charsetName,
                              std::string const& collationName, CallbackType const& onFinish = nullptr,
                              int priority = PRIORITY_NORMAL, bool keepTracking = true,
                              std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

protected:
    void notify(replica::Lock const& lock) final;

private:
    SqlCreateTableRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                          std::string const& database, std::string const& table, std::string const& engine,
                          std::string const& partitionByColumn, std::list<SqlColDef> const& columns,
                          std::string const& charsetName, std::string const& collationName,
                          CallbackType const& onFinish, int priority, bool keepTracking);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLCREATETABLEREQUEST_H
