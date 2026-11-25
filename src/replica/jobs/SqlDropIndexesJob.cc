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
#include "replica/jobs/SqlDropIndexesJob.h"

// Qserv headers
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
#include "replica/requests/http/HttpSqlDropIndexesRequest.h"
#else
#include "replica/requests/SqlDropIndexesRequest.h"
#endif

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <vector>

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlDropIndexesJob");
uint64_t const MAX_ROWS_UNLIMITED = 0;
}  // namespace

namespace lsst::qserv::replica {

string SqlDropIndexesJob::typeName() { return "SqlDropIndexesJob"; }

shared_ptr<SqlDropIndexesJob> SqlDropIndexesJob::create(string const& database, string const& table,
                                                        bool overlap, string const& indexName,
                                                        bool allWorkers,
                                                        shared_ptr<Controller> const& controller,
                                                        string const& parentJobId,
                                                        CallbackType const& onFinish, int priority) {
    return shared_ptr<SqlDropIndexesJob>(new SqlDropIndexesJob(
            database, table, overlap, indexName, allWorkers, controller, parentJobId, onFinish, priority));
}

SqlDropIndexesJob::SqlDropIndexesJob(string const& database, string const& table, bool overlap,
                                     string const& indexName, bool allWorkers,
                                     shared_ptr<Controller> const& controller, string const& parentJobId,
                                     CallbackType const& onFinish, int priority)
        : SqlJob(::MAX_ROWS_UNLIMITED, allWorkers, controller, parentJobId, "SQL_DROP_TABLE_INDEXES",
                 priority),
          _database(database),
          _table(table),
          _overlap(overlap),
          _indexName(indexName),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlDropIndexesJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("overlap", bool2str(overlap()));
    result.emplace_back("index_name", indexName());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

#ifdef QSERV_REPLICA_PROTOCOL_HTTP
list<shared_ptr<HttpSqlRequest>> SqlDropIndexesJob::launchRequests(replica::Lock const& lock,
                                                                   string const& worker,
                                                                   size_t maxRequestsPerWorker) {
    list<shared_ptr<HttpSqlRequest>> requests;
#else
list<shared_ptr<SqlRequest>> SqlDropIndexesJob::launchRequests(replica::Lock const& lock,
                                                               string const& worker,
                                                               size_t maxRequestsPerWorker) {
    list<shared_ptr<SqlRequest>> requests;
#endif
    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers.count(worker) != 0) return requests;
    _workers.insert(worker);

    // Only the requested subset of tables is going to be processed at the worker.
    bool const allTables = false;
    vector<string> const tables2process = workerTables(worker, database(), table(), allTables, overlap());

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    for (auto const& tables : distributeTables(tables2process, maxRequestsPerWorker)) {
        bool const keepTracking = true;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
        requests.push_back(HttpSqlDropIndexesRequest::createAndStart(
                controller(), worker, database(), tables, indexName(),
                [self = shared_from_base<SqlDropIndexesJob>()](
                        shared_ptr<HttpSqlDropIndexesRequest> const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
#else
        requests.push_back(SqlDropIndexesRequest::createAndStart(
                controller(), worker, database(), tables, indexName(),
                [self = shared_from_base<SqlDropIndexesJob>()](
                        shared_ptr<SqlDropIndexesRequest> const& request) { self->onRequestFinish(request); },
                priority(), keepTracking, id()));
#endif
    }
    return requests;
}

void SqlDropIndexesJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDropIndexesJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
