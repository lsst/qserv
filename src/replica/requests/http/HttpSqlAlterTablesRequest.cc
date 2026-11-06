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
#include "replica/requests/http/HttpSqlAlterTablesRequest.h"

// Qserv headers
#include "replica/proto/Protocol.h"

using namespace std;

namespace {
bool const batchMode = true;
uint64_t const unlimitedMaxRows = 0;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpSqlAlterTablesRequest> HttpSqlAlterTablesRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        vector<string> const& tables, string const& alterSpec, CallbackType const& onFinish, int priority,
        bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpSqlAlterTablesRequest>(new HttpSqlAlterTablesRequest(
            controller, workerName, database, tables, alterSpec, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpSqlAlterTablesRequest::HttpSqlAlterTablesRequest(shared_ptr<Controller> const& controller,
                                                     string const& workerName, string const& database,
                                                     vector<string> const& tables, string const& alterSpec,
                                                     CallbackType const& onFinish, int priority,
                                                     bool keepTracking)
        : HttpSqlRequest(controller, "SQL_ALTER_TABLES", protocol::SqlRequestType::ALTER_TABLE, database,
                         ::batchMode, {{"tables", tables}, {"alter_spec", alterSpec}}, workerName,
                         ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {}

void HttpSqlAlterTablesRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpSqlAlterTablesRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
