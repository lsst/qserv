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
#include "replica/requests/SqlCreateTableRequest.h"

// Qserv headers
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateTableRequest");

}  // namespace

namespace lsst::qserv::replica {

SqlCreateTableRequest::Ptr SqlCreateTableRequest::create(
        ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
        string const& worker, std::string const& database, std::string const& table,
        std::string const& engine, string const& partitionByColumn, std::list<SqlColDef> const& columns,
        CallbackType const& onFinish, int priority, bool keepTracking,
        shared_ptr<Messenger> const& messenger) {
    return Ptr(new SqlCreateTableRequest(serviceProvider, io_service, worker, database, table, engine,
                                         partitionByColumn, columns, onFinish, priority, keepTracking,
                                         messenger));
}

SqlCreateTableRequest::SqlCreateTableRequest(ServiceProvider::Ptr const& serviceProvider,
                                             boost::asio::io_service& io_service, string const& worker,
                                             std::string const& database, std::string const& table,
                                             std::string const& engine, string const& partitionByColumn,
                                             std::list<SqlColDef> const& columns,
                                             CallbackType const& onFinish, int priority, bool keepTracking,
                                             shared_ptr<Messenger> const& messenger)
        : SqlRequest(serviceProvider, io_service, "SQL_CREATE_TABLE", worker, 0, /* maxRows */
                     priority, keepTracking, messenger),
          _onFinish(onFinish) {
    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::CREATE_TABLE);
    requestBody.set_database(database);
    requestBody.set_table(table);
    requestBody.set_engine(engine);
    requestBody.set_partition_by_column(partitionByColumn);
    for (auto&& column : columns) {
        auto out = requestBody.add_columns();
        out->set_name(column.name);
        out->set_type(column.type);
    }
}

void SqlCreateTableRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlCreateTableRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
