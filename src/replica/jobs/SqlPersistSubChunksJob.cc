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
#include "replica/jobs/SqlPersistSubChunksJob.h"

// Qserv headers
#include "replica/requests/http/HttpSqlPersistSubChunksRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <vector>

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlPersistSubChunksJob");
uint64_t const MAX_ROWS_UNLIMITED = 0;
}  // namespace

namespace lsst::qserv::replica {

string SqlPersistSubChunksJob::typeName() { return "SqlPersistSubChunksJob"; }

shared_ptr<SqlPersistSubChunksJob> SqlPersistSubChunksJob::create(string const& database, string const& table,
                                                                  bool allWorkers,
                                                                  shared_ptr<Controller> const& controller,
                                                                  string const& parentJobId,
                                                                  CallbackType const& onFinish,
                                                                  int priority) {
    return shared_ptr<SqlPersistSubChunksJob>(new SqlPersistSubChunksJob(
            database, table, allWorkers, controller, parentJobId, onFinish, priority));
}

SqlPersistSubChunksJob::SqlPersistSubChunksJob(string const& database, string const& table, bool allWorkers,
                                               shared_ptr<Controller> const& controller,
                                               string const& parentJobId, CallbackType const& onFinish,
                                               int priority)
        : SqlJob(::MAX_ROWS_UNLIMITED, allWorkers, controller, parentJobId, "SQL_PERSIST_SUBCHUNKS",
                 priority),
          _database(database),
          _table(table),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlPersistSubChunksJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<shared_ptr<HttpSqlRequest>> SqlPersistSubChunksJob::launchRequests(replica::Lock const& lock,
                                                                        string const& worker,
                                                                        size_t maxRequestsPerWorker) {
    list<shared_ptr<HttpSqlRequest>> requests;
    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers.count(worker) != 0) return requests;
    _workers.insert(worker);

    // Only the requested subset of tables is going to be processed at the worker.
    bool const allTables = true;
    vector<string> const tables2process = workerTables(worker, database(), table(), allTables);

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    for (auto const& tables : distributeTables(tables2process, maxRequestsPerWorker)) {
        bool const keepTracking = true;
        requests.push_back(HttpSqlPersistSubChunksRequest::createAndStart(
                controller(), worker, database(), tables,
                [self = shared_from_base<SqlPersistSubChunksJob>()](
                        shared_ptr<HttpSqlPersistSubChunksRequest> const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
    }
    return requests;
}

void SqlPersistSubChunksJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlPersistSubChunksJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
