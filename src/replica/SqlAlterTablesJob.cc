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
#include "replica/SqlAlterTablesJob.h"

// Qserv headers
#include "replica/SqlAlterTablesRequest.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <vector>

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlAlterTablesJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

string SqlAlterTablesJob::typeName() { return "SqlAlterTablesJob"; }


SqlAlterTablesJob::Ptr SqlAlterTablesJob::create(
        string const& database,
        string const& table,
        string const& alterSpec,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {
    return Ptr(new SqlAlterTablesJob(
        database,
        table,
        alterSpec,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlAlterTablesJob::SqlAlterTablesJob(
        string const& database,
        string const& table,
        string const& alterSpec,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_ALTER_TABLES",
               options),
        _database(database),
        _table(table),
        _alterSpec(alterSpec),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlAlterTablesJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("alter_spec", alterSpec());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}


list<SqlRequest::Ptr> SqlAlterTablesJob::launchRequests(util::Lock const& lock,
                                                        string const& worker,
                                                        size_t maxRequestsPerWorker) {
    list<SqlRequest::Ptr> requests;

    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers.count(worker) != 0) return requests;
    _workers.insert(worker);

    // Only the requested subset of tables is going to be processed at the worker.
    bool const allTables = true;
    vector<string> const tables2process = workerTables(worker, database(), table(), allTables);

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    auto const self = shared_from_base<SqlAlterTablesJob>();
    for (auto&& tables: distributeTables(tables2process, maxRequestsPerWorker)) {
        requests.push_back(
            controller()->sqlAlterTables(
                worker,
                database(),
                tables,
                alterSpec(),
                [self] (SqlAlterTablesRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
    }
    return requests;
}


void SqlAlterTablesJob::stopRequest(util::Lock const& lock,
                                    SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlAlterTablesRequest>(lock, request);
}


void SqlAlterTablesJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlAlterTablesJob>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
