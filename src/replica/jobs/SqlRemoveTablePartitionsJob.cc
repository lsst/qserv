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
#include "replica/jobs/SqlRemoveTablePartitionsJob.h"

// Qserv headers
#include "replica/requests/SqlRemoveTablePartitionsRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <vector>

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlRemoveTablePartitionsJob");

}  // namespace

namespace lsst::qserv::replica {

string SqlRemoveTablePartitionsJob::typeName() { return "SqlRemoveTablePartitionsJob"; }

SqlRemoveTablePartitionsJob::Ptr SqlRemoveTablePartitionsJob::create(
        string const& database, string const& table, bool allWorkers, bool ignoreNonPartitioned,
        Controller::Ptr const& controller, string const& parentJobId, CallbackType const& onFinish,
        int priority) {
    return Ptr(new SqlRemoveTablePartitionsJob(database, table, allWorkers, ignoreNonPartitioned, controller,
                                               parentJobId, onFinish, priority));
}

SqlRemoveTablePartitionsJob::SqlRemoveTablePartitionsJob(string const& database, string const& table,
                                                         bool allWorkers, bool ignoreNonPartitioned,
                                                         Controller::Ptr const& controller,
                                                         string const& parentJobId,
                                                         CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_REMOVE_TABLE_PARTITIONING", priority,
                 ignoreNonPartitioned),
          _database(database),
          _table(table),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlRemoveTablePartitionsJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    result.emplace_back("ignore_non_partitioned", bool2str(ignoreNonPartitioned()));
    return result;
}

list<SqlRequest::Ptr> SqlRemoveTablePartitionsJob::launchRequests(replica::Lock const& lock,
                                                                  string const& worker,
                                                                  size_t maxRequestsPerWorker) {
    list<SqlRequest::Ptr> requests;
    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers.count(worker) != 0) return requests;
    _workers.insert(worker);

    // All tables which are going to be processed at the worker
    vector<string> const allTables = workerTables(worker, database(), table());

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    for (auto&& tables : distributeTables(allTables, maxRequestsPerWorker)) {
        bool const keepTracking = true;
        requests.push_back(SqlRemoveTablePartitionsRequest::createAndStart(
                controller(), worker, database(), tables,
                [self = shared_from_base<SqlRemoveTablePartitionsJob>()](
                        SqlRemoveTablePartitionsRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
    }
    return requests;
}

void SqlRemoveTablePartitionsJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlRemoveTablePartitionsJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
