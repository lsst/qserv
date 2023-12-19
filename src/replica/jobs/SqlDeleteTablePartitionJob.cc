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
#include "replica/jobs/SqlDeleteTablePartitionJob.h"

// Qserv headers
#include "replica/contr/Controller.h"
#include "replica/requests/SqlDeleteTablePartitionRequest.h"
#include "replica/requests/StopRequest.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <algorithm>
#include <vector>

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlDeleteTablePartitionJob");

}  // namespace

namespace lsst::qserv::replica {

string SqlDeleteTablePartitionJob::typeName() { return "SqlDeleteTablePartitionJob"; }

SqlDeleteTablePartitionJob::Ptr SqlDeleteTablePartitionJob::create(
        TransactionId transactionId, string const& table, bool allWorkers, Controller::Ptr const& controller,
        string const& parentJobId, CallbackType const& onFinish, int priority) {
    return Ptr(new SqlDeleteTablePartitionJob(transactionId, table, allWorkers, controller, parentJobId,
                                              onFinish, priority));
}

SqlDeleteTablePartitionJob::SqlDeleteTablePartitionJob(TransactionId transactionId, string const& table,
                                                       bool allWorkers, Controller::Ptr const& controller,
                                                       string const& parentJobId,
                                                       CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_DROP_TABLE_PARTITION", priority),
          _transactionId(transactionId),
          _table(table),
          _onFinish(onFinish) {
    // Get the name of a database associated with the transaction.
    // Verify input parameters.
    try {
        auto const serviceProvider = controller->serviceProvider();
        auto const transactionInfo = serviceProvider->databaseServices()->transaction(transactionId);
        _database = transactionInfo.database;

        auto const databaseInfo = serviceProvider->config()->databaseInfo(_database);
        auto const tables = databaseInfo.tables();
        if (tables.end() == find(tables.cbegin(), tables.cend(), _table)) {
            throw invalid_argument(context() + string(__func__) + " the table '" + _table +
                                   "' was not found in database '" + _database + "'.");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, ex.what());
        throw;
    }
}

list<pair<string, string>> SqlDeleteTablePartitionJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("transaction_id", to_string(transactionId()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<SqlRequest::Ptr> SqlDeleteTablePartitionJob::launchRequests(replica::Lock const& lock,
                                                                 string const& worker,
                                                                 size_t maxRequestsPerWorker) {
    list<SqlRequest::Ptr> requests;

    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers.count(worker) != 0) return requests;
    _workers.insert(worker);

    // All tables modified during the transaction will be selected
    vector<string> const allTables = workerTables(worker, transactionId(), table());

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    bool const keepTracking = true;
    string const jobId = id();
    auto const self = shared_from_base<SqlDeleteTablePartitionJob>();
    for (auto&& tables : distributeTables(allTables, maxRequestsPerWorker)) {
        requests.push_back(controller()->sqlDeleteTablePartition(
                worker, database(), tables, transactionId(),
                [self](SqlDeleteTablePartitionRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, jobId));
    }
    return requests;
}

void SqlDeleteTablePartitionJob::stopRequest(replica::Lock const& lock, SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlDeleteTablePartitionRequest>(lock, request);
}

void SqlDeleteTablePartitionJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDeleteTablePartitionJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
