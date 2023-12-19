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
#include "replica/jobs/SqlCreateTablesJob.h"

// Qserv headers
#include "replica/requests/SqlCreateTablesRequest.h"
#include "replica/requests/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <vector>

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateTablesJob");

}  // namespace

namespace lsst::qserv::replica {

string SqlCreateTablesJob::typeName() { return "SqlCreateTablesJob"; }

SqlCreateTablesJob::Ptr SqlCreateTablesJob::create(string const& database, string const& table,
                                                   string const& engine, string const& partitionByColumn,
                                                   list<SqlColDef> const& columns, bool allWorkers,
                                                   Controller::Ptr const& controller,
                                                   string const& parentJobId, CallbackType const& onFinish,
                                                   int priority) {
    return Ptr(new SqlCreateTablesJob(database, table, engine, partitionByColumn, columns, allWorkers,
                                      controller, parentJobId, onFinish, priority));
}

SqlCreateTablesJob::SqlCreateTablesJob(string const& database, string const& table, string const& engine,
                                       string const& partitionByColumn, list<SqlColDef> const& columns,
                                       bool allWorkers, Controller::Ptr const& controller,
                                       string const& parentJobId, CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_CREATE_TABLES", priority),
          _database(database),
          _table(table),
          _engine(engine),
          _partitionByColumn(partitionByColumn),
          _columns(columns),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlCreateTablesJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("engine", engine());
    result.emplace_back("partition_by_column", partitionByColumn());
    result.emplace_back("num_columns", to_string(columns().size()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<SqlRequest::Ptr> SqlCreateTablesJob::launchRequests(replica::Lock const& lock, string const& worker,
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
    auto const self = shared_from_base<SqlCreateTablesJob>();
    for (auto&& tables : distributeTables(allTables, maxRequestsPerWorker)) {
        requests.push_back(controller()->sqlCreateTables(
                worker, database(), tables, engine(), partitionByColumn(), columns(),
                [self](SqlCreateTablesRequest::Ptr const& request) { self->onRequestFinish(request); },
                priority(), true, /* keepTracking*/
                id()              /* jobId */
                ));
    }
    return requests;
}

void SqlCreateTablesJob::stopRequest(replica::Lock const& lock, SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlCreateTablesRequest>(lock, request);
}

void SqlCreateTablesJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlCreateTablesJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
