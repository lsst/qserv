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
#include "replica/jobs/SqlQueryJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
#include "replica/requests/http/HttpSqlQueryRequest.h"
#else
#include "replica/requests/SqlQueryRequest.h"
#endif
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlQueryJob");
}  // namespace

namespace lsst::qserv::replica {

string SqlQueryJob::typeName() { return "SqlQueryJob"; }

shared_ptr<SqlQueryJob> SqlQueryJob::create(string const& query, string const& user, string const& password,
                                            uint64_t maxRows, bool allWorkers,
                                            shared_ptr<Controller> const& controller,
                                            string const& parentJobId, CallbackType const& onFinish,
                                            int priority) {
    return shared_ptr<SqlQueryJob>(new SqlQueryJob(query, user, password, maxRows, allWorkers, controller,
                                                   parentJobId, onFinish, priority));
}

SqlQueryJob::SqlQueryJob(string const& query, string const& user, string const& password, uint64_t maxRows,
                         bool allWorkers, shared_ptr<Controller> const& controller, string const& parentJobId,
                         CallbackType const& onFinish, int priority)
        : SqlJob(maxRows, allWorkers, controller, parentJobId, "SQL_QUERY", priority),
          _query(query),
          _user(user),
          _password(password),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlQueryJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("query", query());
    result.emplace_back("user", user());
    result.emplace_back("max_rows", to_string(maxRows()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

#ifdef QSERV_REPLICA_PROTOCOL_HTTP
list<shared_ptr<HttpSqlRequest>> SqlQueryJob::launchRequests(replica::Lock const& lock, string const& worker,
                                                             size_t maxRequestsPerWorker) {
    list<shared_ptr<HttpSqlRequest>> requests;
#else
list<shared_ptr<SqlRequest>> SqlQueryJob::launchRequests(replica::Lock const& lock, string const& worker,
                                                         size_t maxRequestsPerWorker) {
    list<shared_ptr<SqlRequest>> requests;
#endif
    // Launch exactly one request per worker unless it was already launched earlier
    if (!_workers.count(worker) && maxRequestsPerWorker != 0) {
        bool const keepTracking = true;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
        requests.push_back(HttpSqlQueryRequest::createAndStart(
                controller(), worker, query(), user(), password(), maxRows(),
                [self = shared_from_base<SqlQueryJob>()](shared_ptr<HttpSqlQueryRequest> const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
#else
        requests.push_back(SqlQueryRequest::createAndStart(
                controller(), worker, query(), user(), password(), maxRows(),
                [self = shared_from_base<SqlQueryJob>()](shared_ptr<SqlQueryRequest> const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
#endif
        _workers.insert(worker);
    }
    return requests;
}

void SqlQueryJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlQueryJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
