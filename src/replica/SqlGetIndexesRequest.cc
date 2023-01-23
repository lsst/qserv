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
#include "replica/SqlGetIndexesRequest.h"

// Qserv headers
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlGetIndexesRequest");

}  // namespace

namespace lsst::qserv::replica {

SqlGetIndexesRequest::Ptr SqlGetIndexesRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                                       boost::asio::io_service& io_service,
                                                       string const& worker, string const& database,
                                                       vector<string> const& tables,
                                                       CallbackType const& onFinish, int priority,
                                                       bool keepTracking,
                                                       shared_ptr<Messenger> const& messenger) {
    return Ptr(new SqlGetIndexesRequest(serviceProvider, io_service, worker, database, tables, onFinish,
                                        priority, keepTracking, messenger));
}

SqlGetIndexesRequest::SqlGetIndexesRequest(ServiceProvider::Ptr const& serviceProvider,
                                           boost::asio::io_service& io_service, string const& worker,
                                           string const& database, vector<string> const& tables,
                                           CallbackType const& onFinish, int priority, bool keepTracking,
                                           shared_ptr<Messenger> const& messenger)
        : SqlRequest(serviceProvider, io_service, "SQL_GET_TABLE_INDEXES", worker, 0, /* maxRows */
                     priority, keepTracking, messenger),
          _onFinish(onFinish) {
    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::GET_TABLE_INDEX);
    requestBody.set_database(database);
    requestBody.clear_tables();
    for (auto&& table : tables) {
        requestBody.add_tables(table);
    }
    requestBody.set_batch_mode(true);
}

void SqlGetIndexesRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlGetIndexesRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
