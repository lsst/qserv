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

// Class header
#include "replica/requests/SqlCreateTablesRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateTablesRequest");
const uint64_t unlimitedMaxRows = 0;
}  // namespace

namespace lsst::qserv::replica {

SqlCreateTablesRequest::Ptr SqlCreateTablesRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, std::string const& database,
        std::vector<std::string> const& tables, std::string const& engine, string const& partitionByColumn,
        std::list<SqlColDef> const& columns, CallbackType const& onFinish, int priority, bool keepTracking,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = Ptr(new SqlCreateTablesRequest(controller, workerName, database, tables, engine,
                                              partitionByColumn, columns, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

SqlCreateTablesRequest::SqlCreateTablesRequest(shared_ptr<Controller> const& controller,
                                               string const& workerName, std::string const& database,
                                               std::vector<std::string> const& tables,
                                               std::string const& engine, string const& partitionByColumn,
                                               std::list<SqlColDef> const& columns,
                                               CallbackType const& onFinish, int priority, bool keepTracking)
        : SqlRequest(controller, "SQL_CREATE_TABLES", workerName, ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {
    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::CREATE_TABLE);
    requestBody.set_database(database);
    requestBody.clear_tables();
    for (auto&& table : tables) {
        requestBody.add_tables(table);
    }
    requestBody.set_engine(engine);
    requestBody.set_partition_by_column(partitionByColumn);
    for (auto&& column : columns) {
        auto out = requestBody.add_columns();
        out->set_name(column.name);
        out->set_type(column.type);
    }
    requestBody.set_batch_mode(true);
}

void SqlCreateTablesRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");
    notifyDefaultImpl<SqlCreateTablesRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
