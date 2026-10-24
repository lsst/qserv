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
#include "replica/jobs/SqlCreateDbJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/requests/SqlCreateDbRequest.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateDbJob");
uint64_t const MAX_ROWS_UNLIMITED = 0;
}  // namespace

namespace lsst::qserv::replica {

string SqlCreateDbJob::typeName() { return "SqlCreateDbJob"; }

shared_ptr<SqlCreateDbJob> SqlCreateDbJob::create(string const& database, bool allWorkers,
                                                  shared_ptr<Controller> const& controller,
                                                  string const& parentJobId, CallbackType const& onFinish,
                                                  int priority) {
    return shared_ptr<SqlCreateDbJob>(
            new SqlCreateDbJob(database, allWorkers, controller, parentJobId, onFinish, priority));
}

SqlCreateDbJob::SqlCreateDbJob(string const& database, bool allWorkers,
                               shared_ptr<Controller> const& controller, string const& parentJobId,
                               CallbackType const& onFinish, int priority)
        : SqlJob(::MAX_ROWS_UNLIMITED, allWorkers, controller, parentJobId, "SQL_CREATE_DATABASE", priority),
          _database(database),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlCreateDbJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<shared_ptr<SqlRequest>> SqlCreateDbJob::launchRequests(replica::Lock const& lock, string const& worker,
                                                            size_t maxRequestsPerWorker) {
    // Launch exactly one request per worker unless it was already launched earlier
    list<shared_ptr<SqlRequest>> requests;
    if (!_workers.count(worker) && maxRequestsPerWorker != 0) {
        bool const keepTracking = true;
        requests.push_back(SqlCreateDbRequest::createAndStart(
                controller(), worker, database(),
                [self = shared_from_base<SqlCreateDbJob>()](shared_ptr<SqlCreateDbRequest> const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
        _workers.insert(worker);
    }
    return requests;
}

void SqlCreateDbJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlCreateDbJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
