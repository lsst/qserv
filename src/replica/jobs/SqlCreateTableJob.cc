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
#include "replica/jobs/SqlCreateTableJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/requests/SqlCreateTableRequest.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlCreateTableJob");
uint64_t const MAX_ROWS_UNLIMITED = 0;
}  // namespace

namespace lsst::qserv::replica {

string SqlCreateTableJob::typeName() { return "SqlCreateTableJob"; }

shared_ptr<SqlCreateTableJob> SqlCreateTableJob::create(
        string const& database, string const& table, string const& engine, string const& partitionByColumn,
        list<SqlColDef> const& columns, string const& charsetName, string const& collationName,
        bool allWorkers, shared_ptr<Controller> const& controller, string const& parentJobId,
        CallbackType const& onFinish, int priority) {
    return shared_ptr<SqlCreateTableJob>(
            new SqlCreateTableJob(database, table, engine, partitionByColumn, columns, charsetName,
                                  collationName, allWorkers, controller, parentJobId, onFinish, priority));
}

SqlCreateTableJob::SqlCreateTableJob(string const& database, string const& table, string const& engine,
                                     string const& partitionByColumn, list<SqlColDef> const& columns,
                                     string const& charsetName, string const& collationName, bool allWorkers,
                                     shared_ptr<Controller> const& controller, string const& parentJobId,
                                     CallbackType const& onFinish, int priority)
        : SqlJob(::MAX_ROWS_UNLIMITED, allWorkers, controller, parentJobId, "SQL_CREATE_TABLE", priority),
          _database(database),
          _table(table),
          _engine(engine),
          _partitionByColumn(partitionByColumn),
          _columns(columns),
          _charsetName(charsetName),
          _collationName(collationName),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlCreateTableJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("engine", engine());
    result.emplace_back("partition_by_column", partitionByColumn());
    result.emplace_back("num_columns", to_string(columns().size()));
    result.emplace_back("charset_name", charsetName());
    result.emplace_back("collation_name", collationName());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<shared_ptr<SqlRequest>> SqlCreateTableJob::launchRequests(replica::Lock const& lock,
                                                               string const& worker,
                                                               size_t maxRequestsPerWorker) {
    // Launch exactly one request per worker unless it was already launched earlier
    list<shared_ptr<SqlRequest>> requests;
    if (!_workers.count(worker) && maxRequestsPerWorker != 0) {
        bool const keepTracking = true;
        requests.push_back(SqlCreateTableRequest::createAndStart(
                controller(), worker, database(), table(), engine(), partitionByColumn(), columns(),
                charsetName(), collationName(),
                [self = shared_from_base<SqlCreateTableJob>()](
                        shared_ptr<SqlCreateTableRequest> const& request) { self->onRequestFinish(request); },
                priority(), keepTracking, id()));
        _workers.insert(worker);
    }
    return requests;
}

void SqlCreateTableJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlCreateTableJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
