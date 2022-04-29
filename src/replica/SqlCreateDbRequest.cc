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
#include "replica/SqlCreateDbRequest.h"

// Qserv headers
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateDbRequest");

}  // namespace

namespace lsst { namespace qserv { namespace replica {

SqlCreateDbRequest::Ptr SqlCreateDbRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                                   boost::asio::io_service& io_service, string const& worker,
                                                   std::string const& database, CallbackType const& onFinish,
                                                   int priority, bool keepTracking,
                                                   shared_ptr<Messenger> const& messenger) {
    return Ptr(new SqlCreateDbRequest(serviceProvider, io_service, worker, database, onFinish, priority,
                                      keepTracking, messenger));
}

SqlCreateDbRequest::SqlCreateDbRequest(ServiceProvider::Ptr const& serviceProvider,
                                       boost::asio::io_service& io_service, string const& worker,
                                       std::string const& database, CallbackType const& onFinish,
                                       int priority, bool keepTracking,
                                       shared_ptr<Messenger> const& messenger)
        : SqlRequest(serviceProvider, io_service, "SQL_CREATE_DATABASE", worker, 0 /* maxRows */, priority,
                     keepTracking, messenger),
          _onFinish(onFinish) {
    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::CREATE_DATABASE);
    requestBody.set_database(database);
}

void SqlCreateDbRequest::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlCreateDbRequest>(lock, _onFinish);
}

}}}  // namespace lsst::qserv::replica
