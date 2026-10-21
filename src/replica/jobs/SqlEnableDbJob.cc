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
#include "replica/jobs/SqlEnableDbJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
#include "replica/requests/http/HttpSqlEnableDbRequest.h"
#else
#include "replica/requests/SqlEnableDbRequest.h"
#endif
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlEnableDbJob");
uint64_t const MAX_ROWS_UNLIMITED = 0;
}  // namespace

namespace lsst::qserv::replica {

string SqlEnableDbJob::typeName() { return "SqlEnableDbJob"; }

shared_ptr<SqlEnableDbJob> SqlEnableDbJob::create(string const& database, bool allWorkers,
                                                  shared_ptr<Controller> const& controller,
                                                  string const& parentJobId, CallbackType const& onFinish,
                                                  int priority) {
    return shared_ptr<SqlEnableDbJob>(
            new SqlEnableDbJob(database, allWorkers, controller, parentJobId, onFinish, priority));
}

SqlEnableDbJob::SqlEnableDbJob(string const& database, bool allWorkers,
                               shared_ptr<Controller> const& controller, string const& parentJobId,
                               CallbackType const& onFinish, int priority)
        : SqlJob(::MAX_ROWS_UNLIMITED, allWorkers, controller, parentJobId, "SQL_ENABLE_DATABASE", priority),
          _database(database),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlEnableDbJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

#ifdef QSERV_REPLICA_PROTOCOL_HTTP
list<shared_ptr<HttpSqlRequest>> SqlEnableDbJob::launchRequests(replica::Lock const& lock,
                                                                string const& worker,
                                                                size_t maxRequestsPerWorker) {
    list<shared_ptr<HttpSqlRequest>> requests;
#else
list<shared_ptr<SqlRequest>> SqlEnableDbJob::launchRequests(replica::Lock const& lock, string const& worker,
                                                            size_t maxRequestsPerWorker) {
    list<shared_ptr<SqlRequest>> requests;
#endif
    // Launch exactly one request per worker unless it was already launched earlier
    if (!_workers.count(worker) && maxRequestsPerWorker != 0) {
        bool const keepTracking = true;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
        requests.push_back(HttpSqlEnableDbRequest::createAndStart(
                controller(), worker, database(),
                [self = shared_from_base<SqlEnableDbJob>()](
                        shared_ptr<HttpSqlEnableDbRequest> const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
#else
        requests.push_back(SqlEnableDbRequest::createAndStart(
                controller(), worker, database(),
                [self = shared_from_base<SqlEnableDbJob>()](shared_ptr<SqlEnableDbRequest> const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
#endif
        _workers.insert(worker);
    }
    return requests;
}

void SqlEnableDbJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlEnableDbJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
