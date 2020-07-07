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
#include "replica/SqlDeleteTableJob.h"

// System headers
#include <vector>

// Qserv headers
#include "replica/SqlDeleteTableRequest.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlDeleteTableJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

string SqlDeleteTableJob::typeName() { return "SqlDeleteTableJob"; }


SqlDeleteTableJob::Ptr SqlDeleteTableJob::create(
        string const& database,
        string const& table,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlDeleteTableJob(
        database,
        table,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlDeleteTableJob::SqlDeleteTableJob(
        string const& database,
        string const& table,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options)
    :   SqlJob(0,
               allWorkers,
               controller,
               parentJobId,
               "SQL_DROP_TABLE",
               options),
        _database(database),
        _table(table),
        _onFinish(onFinish) {
}


list<pair<string,string>> SqlDeleteTableJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}


list<SqlRequest::Ptr> SqlDeleteTableJob::launchRequests(util::Lock const& lock,
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
    auto const self = shared_from_base<SqlDeleteTableJob>();
    for (auto&& tables: distributeTables(allTables, maxRequestsPerWorker)) {
        requests.push_back(
            controller()->sqlDeleteTable(
                worker,
                database(),
                tables,
                [self] (SqlDeleteTableRequest::Ptr const& request) {
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


void SqlDeleteTableJob::stopRequest(util::Lock const& lock,
                                    SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlDeleteTableRequest>(lock, request);
}


void SqlDeleteTableJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlDeleteTableJob>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
