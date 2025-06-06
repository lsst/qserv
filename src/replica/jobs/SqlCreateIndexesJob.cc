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
#include "replica/jobs/SqlCreateIndexesJob.h"

// Qserv headers
#include "replica/requests/SqlCreateIndexesRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <vector>

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateIndexesJob");

}  // namespace

namespace lsst::qserv::replica {

string SqlCreateIndexesJob::typeName() { return "SqlCreateIndexesJob"; }

SqlCreateIndexesJob::Ptr SqlCreateIndexesJob::create(
        string const& database, string const& table, bool overlap,
        SqlRequestParams::IndexSpec const& indexSpec, string const& indexName, string const& indexComment,
        vector<SqlIndexColumn> const& indexColumns, bool allWorkers, bool ignoreDuplicateKey,
        Controller::Ptr const& controller, string const& parentJobId, CallbackType const& onFinish,
        int priority) {
    return Ptr(new SqlCreateIndexesJob(database, table, overlap, indexSpec, indexName, indexComment,
                                       indexColumns, allWorkers, ignoreDuplicateKey, controller, parentJobId,
                                       onFinish, priority));
}

SqlCreateIndexesJob::SqlCreateIndexesJob(string const& database, string const& table, bool overlap,
                                         SqlRequestParams::IndexSpec const& indexSpec,
                                         string const& indexName, string const& indexComment,
                                         vector<SqlIndexColumn> const& indexColumns, bool allWorkers,
                                         bool ignoreDuplicateKey, Controller::Ptr const& controller,
                                         string const& parentJobId, CallbackType const& onFinish,
                                         int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_CREATE_TABLE_INDEXES", priority,
                 false, /* ignoreNonPartitioned */
                 ignoreDuplicateKey),
          _database(database),
          _table(table),
          _overlap(overlap),
          _indexSpec(indexSpec),
          _indexName(indexName),
          _indexComment(indexComment),
          _indexColumns(indexColumns),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlCreateIndexesJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("overlap", bool2str(overlap()));
    result.emplace_back("index_spec", indexSpec().str());
    result.emplace_back("index_name", indexName());
    result.emplace_back("index_comment", indexComment());
    result.emplace_back("index_num_columns", to_string(indexColumns().size()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    result.emplace_back("ignore_duplicate_key", bool2str(ignoreDuplicateKey()));
    return result;
}

list<SqlRequest::Ptr> SqlCreateIndexesJob::launchRequests(replica::Lock const& lock, string const& worker,
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
    for (auto&& tables : distributeTables(tables2process, maxRequestsPerWorker)) {
        bool const keepTracking = true;
        requests.push_back(SqlCreateIndexesRequest::createAndStart(
                controller(), worker, database(), tables, indexSpec(), indexName(), indexComment(),
                indexColumns(),
                [self = shared_from_base<SqlCreateIndexesJob>()](
                        SqlCreateIndexesRequest::Ptr const& request) { self->onRequestFinish(request); },
                priority(), keepTracking, id()));
    }
    return requests;
}

void SqlCreateIndexesJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlCreateIndexesJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
