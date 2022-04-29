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
#include "replica/SqlCreateTableJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlCreateTableRequest.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateTableJob");

}  // namespace

namespace lsst::qserv::replica {

string SqlCreateTableJob::typeName() { return "SqlCreateTableJob"; }

SqlCreateTableJob::Ptr SqlCreateTableJob::create(string const& database, string const& table,
                                                 string const& engine, string const& partitionByColumn,
                                                 list<SqlColDef> const& columns, bool allWorkers,
                                                 Controller::Ptr const& controller, string const& parentJobId,
                                                 CallbackType const& onFinish, int priority) {
    return Ptr(new SqlCreateTableJob(database, table, engine, partitionByColumn, columns, allWorkers,
                                     controller, parentJobId, onFinish, priority));
}

SqlCreateTableJob::SqlCreateTableJob(string const& database, string const& table, string const& engine,
                                     string const& partitionByColumn, list<SqlColDef> const& columns,
                                     bool allWorkers, Controller::Ptr const& controller,
                                     string const& parentJobId, CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_CREATE_TABLE", priority),
          _database(database),
          _table(table),
          _engine(engine),
          _partitionByColumn(partitionByColumn),
          _columns(columns),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlCreateTableJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("engine", engine());
    result.emplace_back("partition_by_column", partitionByColumn());
    result.emplace_back("num_columns", to_string(columns().size()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<SqlRequest::Ptr> SqlCreateTableJob::launchRequests(util::Lock const& lock, string const& worker,
                                                        size_t maxRequestsPerWorker) {
    // Launch exactly one request per worker unless it was already
    // launched earlier

    list<SqlRequest::Ptr> requests;
    if (not _workers.count(worker) and maxRequestsPerWorker != 0) {
        auto const self = shared_from_base<SqlCreateTableJob>();
        requests.push_back(controller()->sqlCreateTable(
                worker, database(), table(), engine(), partitionByColumn(), columns(),
                [self](SqlCreateTableRequest::Ptr const& request) { self->onRequestFinish(request); },
                priority(), true, /* keepTracking*/
                id()              /* jobId */
                ));
        _workers.insert(worker);
    }
    return requests;
}

void SqlCreateTableJob::stopRequest(util::Lock const& lock, SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlCreateTableRequest>(lock, request);
}

void SqlCreateTableJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlCreateTableJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
