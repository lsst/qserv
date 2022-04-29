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
#include "replica/SqlGetIndexesJob.h"

// Qserv headers
#include "replica/SqlGetIndexesRequest.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <vector>

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlGetIndexesJob");

}  // namespace

namespace lsst { namespace qserv { namespace replica {

string SqlGetIndexesJob::typeName() { return "SqlGetIndexesJob"; }

SqlGetIndexesJob::Ptr SqlGetIndexesJob::create(string const& database, string const& table, bool overlap,
                                               bool allWorkers, Controller::Ptr const& controller,
                                               string const& parentJobId, CallbackType const& onFinish,
                                               int priority) {
    return Ptr(new SqlGetIndexesJob(database, table, overlap, allWorkers, controller, parentJobId, onFinish,
                                    priority));
}

SqlGetIndexesJob::SqlGetIndexesJob(string const& database, string const& table, bool overlap, bool allWorkers,
                                   Controller::Ptr const& controller, string const& parentJobId,
                                   CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_GET_TABLE_INDEXES", priority),
          _database(database),
          _table(table),
          _overlap(overlap),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlGetIndexesJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("overlap", bool2str(overlap()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<SqlRequest::Ptr> SqlGetIndexesJob::launchRequests(util::Lock const& lock, string const& worker,
                                                       size_t maxRequestsPerWorker) {
    list<SqlRequest::Ptr> requests;

    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers.count(worker) != 0) return requests;
    _workers.insert(worker);

    // Only the requested subset of tables is going to be processed at the worker.
    bool const allTables = false;
    vector<string> const tables2process = workerTables(worker, database(), table(), allTables, overlap());

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    auto const self = shared_from_base<SqlGetIndexesJob>();
    for (auto&& tables : distributeTables(tables2process, maxRequestsPerWorker)) {
        requests.push_back(controller()->sqlGetTableIndexes(
                worker, database(), tables,
                [self](SqlGetIndexesRequest::Ptr const& request) { self->onRequestFinish(request); },
                priority(), true, /* keepTracking*/
                id()              /* jobId */
                ));
    }
    return requests;
}

void SqlGetIndexesJob::stopRequest(util::Lock const& lock, SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlGetIndexesRequest>(lock, request);
}

void SqlGetIndexesJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlGetIndexesJob>(lock, _onFinish);
}

}}}  // namespace lsst::qserv::replica
