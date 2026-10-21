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
#include "replica/requests/http/HttpSqlDisableDbRequest.h"

// Qserv headers
#include "replica/proto/Protocol.h"

using namespace std;

namespace {
bool const batchMode = false;
uint64_t const unlimitedMaxRows = 0;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpSqlDisableDbRequest> HttpSqlDisableDbRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, std::string const& database,
        CallbackType const& onFinish, int priority, bool keepTracking, string const& jobId,
        unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpSqlDisableDbRequest>(
            new HttpSqlDisableDbRequest(controller, workerName, database, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpSqlDisableDbRequest::HttpSqlDisableDbRequest(shared_ptr<Controller> const& controller,
                                                 string const& workerName, std::string const& database,
                                                 CallbackType const& onFinish, int priority,
                                                 bool keepTracking)
        : HttpSqlRequest(controller, "SQL_DISABLE_DATABASE", protocol::SqlRequestType::DISABLE_DATABASE,
                         database, ::batchMode, {{}}, workerName, ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {}

void HttpSqlDisableDbRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpSqlDisableDbRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica