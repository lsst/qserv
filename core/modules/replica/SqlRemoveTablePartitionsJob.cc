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
#include "replica/SqlRemoveTablePartitionsJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlRemoveTablePartitionsRequest.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlRemoveTablePartitionsJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

string SqlRemoveTablePartitionsJob::typeName() { return "SqlRemoveTablePartitionsJob"; }


SqlRemoveTablePartitionsJob::Ptr SqlRemoveTablePartitionsJob::create(
        string const& database,
        string const& table,
        bool allWorkers,
        Controller::Ptr const& controller,
        string const& parentJobId,
        CallbackType const& onFinish,
        Job::Options const& options) {

    return Ptr(new SqlRemoveTablePartitionsJob(
        database,
        table,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


SqlRemoveTablePartitionsJob::SqlRemoveTablePartitionsJob(
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
               "SQL_REMOVE_TABLE_PARTITIONING",
               options),
        _database(database),
        _table(table),
        _onFinish(onFinish) {

    // Determine the type of the table
    auto const info = controller->serviceProvider()->config()->databaseInfo(database);
    if (find(info.partitionedTables.begin(),
             info.partitionedTables.end(), table) != info.partitionedTables.end()) {
        _isPartitioned = true;
        return;
    }

    // And the following test is just to ensure the table name is valid
    if (find(info.regularTables.begin(),
             info.regularTables.end(), table) != info.regularTables.end()) return;

    throw invalid_argument(
            context() + string(__func__) + "  unknown <database>.<table> '" + database +
            "'.'" + table + "'");
}


list<pair<string,string>> SqlRemoveTablePartitionsJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<SqlRequest::Ptr> SqlRemoveTablePartitionsJob::launchRequests(util::Lock const& lock,
                                                                  string const& worker,
                                                                  size_t maxRequests) {
    list<SqlRequest::Ptr> requests;

    // Initialize worker's sub-collection if the first time seeing
    // this worker.
    if (not _workers2tables.count(worker)) {

        // This table must exist in both versions
        _workers2tables[worker].push_back(table());

        // Add chunk specific tables
        if (_isPartitioned) {

            // Locate all chunks registered on the worker. These chunks will be used
            // to build names of the corresponding chunk-specific partitioned tables.

            vector<ReplicaInfo> replicas;
            controller()->serviceProvider()->databaseServices()->findWorkerReplicas(
                replicas,
                worker,
                database()
            );
            for (auto&& replica: replicas) {
                auto const chunk = replica.chunk();
                _workers2tables[worker].push_back(table() + "_" + to_string(chunk));
                _workers2tables[worker].push_back(table() + "FullOverlap_" + to_string(chunk));
            }
        }
    }

    // Launch up to (not to exceed) the specified number of requests for tables
    // by pulling table names from the worker's sub-collection. NOte that used
    // tables will get removed from the sub-collections.

    auto const self = shared_from_base<SqlRemoveTablePartitionsJob>();
    while (not _workers2tables[worker].empty() and requests.size() < maxRequests) {
        requests.push_back(
            controller()->sqlRemoveTablePartitions(
                worker,
                database(),
                _workers2tables[worker].front(),
                [self] (SqlRemoveTablePartitionsRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
        _workers2tables[worker].pop_front();
    }
    return requests;
}


void SqlRemoveTablePartitionsJob::stopRequest(util::Lock const& lock,
                                              SqlRequest::Ptr const& request) {
    controller()->stopById<StopSqlRemoveTablePartitionsRequest>(
        request->worker(),
        request->id(),
        nullptr,    /* onFinish */
        options(lock).priority,
        true,       /* keepTracking */
        id()        /* jobId */
    );
}


void SqlRemoveTablePartitionsJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlRemoveTablePartitionsJob>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
