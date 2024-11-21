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
#include "replica/requests/SqlQueryRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlQueryRequest");

}  // namespace

namespace lsst::qserv::replica {

SqlQueryRequest::Ptr SqlQueryRequest::createAndStart(shared_ptr<Controller> const& controller,
                                                     string const& workerName, std::string const& query,
                                                     std::string const& user, std::string const& password,
                                                     uint64_t maxRows, CallbackType const& onFinish,
                                                     int priority, bool keepTracking, string const& jobId,
                                                     unsigned int requestExpirationIvalSec) {
    auto ptr = Ptr(new SqlQueryRequest(controller, workerName, query, user, password, maxRows, onFinish,
                                       priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

SqlQueryRequest::SqlQueryRequest(shared_ptr<Controller> const& controller, string const& workerName,
                                 std::string const& query, std::string const& user,
                                 std::string const& password, uint64_t maxRows, CallbackType const& onFinish,
                                 int priority, bool keepTracking)
        : SqlRequest(controller, "SQL_QUERY", workerName, maxRows, priority, keepTracking),
          _onFinish(onFinish) {
    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::QUERY);
    requestBody.set_query(query);
    requestBody.set_user(user);
    requestBody.set_password(password);
}

void SqlQueryRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");
    notifyDefaultImpl<SqlQueryRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
