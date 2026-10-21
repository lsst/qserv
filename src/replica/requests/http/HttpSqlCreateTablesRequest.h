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
#ifndef LSST_QSERV_REPLICA_HTTPSQLCREATETABLESREQUEST_H
#define LSST_QSERV_REPLICA_HTTPSQLCREATETABLESREQUEST_H

// System headers
#include <functional>
#include <list>
#include <memory>
#include <tuple>
#include <string>
#include <vector>

// Qserv headers
#include "replica/requests/http/HttpSqlRequest.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpSqlCreateTablesRequest represents Controller-side requests for initiating
 * queries for creating tables at the worker nodes.
 */
class HttpSqlCreateTablesRequest : public HttpSqlRequest {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<HttpSqlCreateTablesRequest>)> CallbackType;

    /**
     * Create a new request with specified parameters.
     *
     * Class-specific parameters are documented below:
     * @param database The name of an existing database where the new table will be created.
     * @param tables The names of tables to be created.
     * @param engine The name of the MySQL engine for the new table.
     * @param partitionByColumn The (optional, if not empty) name of a column which
     *   will be used as a key to configure MySQL partitions for the new table.
     *   This variation of table schema will be used for the super-transaction-based
     *   ingest into the table.
     * @param columns Column definitions (name,type) of the table.
     * @param charsetName (optional, if not empty) the name of a character set for the new table
     * @param collationName (optional, if not empty) the name of a collation for the new table
     * @see The very base class for the description of the common parameters of all subclasses.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<HttpSqlCreateTablesRequest> createAndStart(
            std::shared_ptr<Controller> const& controller, std::string const& workerName,
            std::string const& database, std::vector<std::string> const& tables, std::string const& engine,
            std::string const& partitionByColumn, std::list<SqlColDef> const& columns,
            std::string const& charsetName, std::string const& collationName,
            CallbackType const& onFinish = nullptr, int priority = PRIORITY_NORMAL, bool keepTracking = true,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    HttpSqlCreateTablesRequest() = delete;
    HttpSqlCreateTablesRequest(HttpSqlCreateTablesRequest const&) = delete;
    HttpSqlCreateTablesRequest& operator=(HttpSqlCreateTablesRequest const&) = delete;

    virtual ~HttpSqlCreateTablesRequest() final = default;

protected:
    virtual void notify(replica::Lock const& lock) final;

private:
    HttpSqlCreateTablesRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                               std::string const& database, std::vector<std::string> const& tables,
                               std::string const& engine, std::string const& partitionByColumn,
                               std::list<SqlColDef> const& columns, std::string const& charsetName,
                               std::string const& collationName, CallbackType const& onFinish, int priority,
                               bool keepTracking);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPSQLCREATETABLESREQUEST_H
