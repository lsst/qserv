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
#include "replica/SqlDisableDbJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlDisableDbRequest.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlDisableDbJob");

}  // namespace

namespace lsst { namespace qserv { namespace replica {

string SqlDisableDbJob::typeName() { return "SqlDisableDbJob"; }

SqlDisableDbJob::Ptr SqlDisableDbJob::create(string const& database, bool allWorkers,
                                             Controller::Ptr const& controller, string const& parentJobId,
                                             CallbackType const& onFinish, int priority) {
    return Ptr(new SqlDisableDbJob(database, allWorkers, controller, parentJobId, onFinish, priority));
}

SqlDisableDbJob::SqlDisableDbJob(string const& database, bool allWorkers, Controller::Ptr const& controller,
                                 string const& parentJobId, CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_DISABLE_DATABASE", priority),
          _database(database),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlDisableDbJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<SqlRequest::Ptr> SqlDisableDbJob::launchRequests(util::Lock const& lock, string const& worker,
                                                      size_t maxRequestsPerWorker) {
    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequestsPerWorker != 0) {
        auto const self = shared_from_base<SqlDisableDbJob>();
        requests.push_back(controller()->sqlDisableDb(
                worker, database(),
                [self](SqlDisableDbRequest::Ptr const& request) { self->onRequestFinish(request); },
                priority(), true, /* keepTracking*/
                id()              /* jobId */
                ));
        _workers.insert(worker);
    }
    return requests;
}

void SqlDisableDbJob::stopRequest(util::Lock const& lock, SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlDisableDbRequest>(lock, request);
}

void SqlDisableDbJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDisableDbJob>(lock, _onFinish);
}

}}}  // namespace lsst::qserv::replica
