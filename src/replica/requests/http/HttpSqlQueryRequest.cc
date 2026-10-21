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
#include "replica/requests/http/HttpSqlQueryRequest.h"

// Qserv headers
#include "replica/proto/Protocol.h"

using namespace std;

namespace {
string const noDatabase = "";
bool const batchMode = false;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpSqlQueryRequest> HttpSqlQueryRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, std::string const& query,
        std::string const& user, std::string const& password, uint64_t maxRows, CallbackType const& onFinish,
        int priority, bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpSqlQueryRequest>(new HttpSqlQueryRequest(
            controller, workerName, query, user, password, maxRows, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpSqlQueryRequest::HttpSqlQueryRequest(shared_ptr<Controller> const& controller, string const& workerName,
                                         std::string const& query, std::string const& user,
                                         std::string const& password, uint64_t maxRows,
                                         CallbackType const& onFinish, int priority, bool keepTracking)
        : HttpSqlRequest(controller, "SQL_QUERY", protocol::SqlRequestType::QUERY, ::noDatabase, ::batchMode,
                         {{"query", query}, {"user", user}, {"password", password}}, workerName, maxRows,
                         priority, keepTracking),
          _onFinish(onFinish) {}

void HttpSqlQueryRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpSqlQueryRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
