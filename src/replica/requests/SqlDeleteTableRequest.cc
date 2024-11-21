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
#include "replica/requests/SqlDeleteTableRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlDeleteTableRequest");
const uint64_t unlimitedMaxRows = 0;
}  // namespace

namespace lsst::qserv::replica {

SqlDeleteTableRequest::Ptr SqlDeleteTableRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        vector<string> const& tables, CallbackType const& onFinish, int priority, bool keepTracking,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = Ptr(new SqlDeleteTableRequest(controller, workerName, database, tables, onFinish, priority,
                                             keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

SqlDeleteTableRequest::SqlDeleteTableRequest(shared_ptr<Controller> const& controller,
                                             string const& workerName, string const& database,
                                             vector<string> const& tables, CallbackType const& onFinish,
                                             int priority, bool keepTracking)
        : SqlRequest(controller, "SQL_DROP_TABLE", workerName, ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {
    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::DROP_TABLE);
    requestBody.set_database(database);
    for (auto&& table : tables) {
        requestBody.add_tables(table);
    }
    requestBody.set_batch_mode(true);
}

void SqlDeleteTableRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");
    notifyDefaultImpl<SqlDeleteTableRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
