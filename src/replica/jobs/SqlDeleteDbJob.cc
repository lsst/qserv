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
#include "replica/jobs/SqlDeleteDbJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/requests/SqlDeleteDbRequest.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlDeleteDbJob");

}  // namespace

namespace lsst::qserv::replica {

string SqlDeleteDbJob::typeName() { return "SqlDeleteDbJob"; }

SqlDeleteDbJob::Ptr SqlDeleteDbJob::create(string const& database, bool allWorkers,
                                           Controller::Ptr const& controller, string const& parentJobId,
                                           CallbackType const& onFinish, int priority) {
    return Ptr(new SqlDeleteDbJob(database, allWorkers, controller, parentJobId, onFinish, priority));
}

SqlDeleteDbJob::SqlDeleteDbJob(string const& database, bool allWorkers, Controller::Ptr const& controller,
                               string const& parentJobId, CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_DROP_DATABASE", priority),
          _database(database),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlDeleteDbJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<SqlRequest::Ptr> SqlDeleteDbJob::launchRequests(replica::Lock const& lock, string const& worker,
                                                     size_t maxRequestsPerWorker) {
    // Launch exactly one request per worker unless it was already
    // launched earlier
    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequestsPerWorker != 0) {
        bool const keepTracking = true;
        requests.push_back(SqlDeleteDbRequest::createAndStart(
                controller(), worker, database(),
                [self = shared_from_base<SqlDeleteDbJob>()](SqlDeleteDbRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
        _workers.insert(worker);
    }
    return requests;
}

void SqlDeleteDbJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDeleteDbJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
