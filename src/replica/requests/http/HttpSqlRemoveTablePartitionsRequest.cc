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
#include "replica/requests/http/HttpSqlRemoveTablePartitionsRequest.h"

// Qserv headers
#include "replica/proto/Protocol.h"

using namespace std;

namespace {
bool const batchMode = true;
uint64_t const unlimitedMaxRows = 0;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpSqlRemoveTablePartitionsRequest> HttpSqlRemoveTablePartitionsRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        vector<string> const& tables, CallbackType const& onFinish, int priority, bool keepTracking,
        string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpSqlRemoveTablePartitionsRequest>(new HttpSqlRemoveTablePartitionsRequest(
            controller, workerName, database, tables, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpSqlRemoveTablePartitionsRequest::HttpSqlRemoveTablePartitionsRequest(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        vector<string> const& tables, CallbackType const& onFinish, int priority, bool keepTracking)
        : HttpSqlRequest(controller, "SQL_REMOVE_TABLE_PARTITIONING",
                         protocol::SqlRequestType::REMOVE_TABLE_PARTITIONING, database, ::batchMode,
                         {{"tables", tables}}, workerName, ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {}

void HttpSqlRemoveTablePartitionsRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpSqlRemoveTablePartitionsRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
