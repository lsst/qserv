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
#include "replica/SqlRowStatsJob.h"

// Qserv headers
#include "global/constants.h"
#include "replica/ChunkedTable.h"
#include "replica/Performance.h"
#include "replica/SqlJobResult.h"
#include "replica/SqlRowStatsRequest.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlRowStatsJob");
uint64_t const unlimitedMaxRows = 0;
}  // namespace

namespace lsst::qserv::replica {

string SqlRowStatsJob::typeName() { return "SqlRowStatsJob"; }

string SqlRowStatsJob::policy2str(SqlRowStatsJob::StateUpdatePolicy policy) {
    switch (policy) {
        case StateUpdatePolicy::DISABLED:
            return "DISABLED";
        case StateUpdatePolicy::ENABLED:
            return "ENABLED";
        case StateUpdatePolicy::FORCED:
            return "FORCED";
    }
    throw invalid_argument("SqlRowStatsJob::" + string(__func__) + ": unsupported policy.");
}

SqlRowStatsJob::StateUpdatePolicy SqlRowStatsJob::str2policy(string const& str) {
    if (str == "DISABLED")
        return StateUpdatePolicy::DISABLED;
    else if (str == "ENABLED")
        return StateUpdatePolicy::ENABLED;
    else if (str == "FORCED")
        return StateUpdatePolicy::FORCED;
    throw invalid_argument("SqlRowStatsJob::" + string(__func__) + ": '" + str + "' is not a valid policy.");
}

SqlRowStatsJob::Ptr SqlRowStatsJob::create(string const& database, string const& table,
                                           ChunkOverlapSelector overlapSelector,
                                           SqlRowStatsJob::StateUpdatePolicy stateUpdatePolicy,
                                           bool allWorkers, Controller::Ptr const& controller,
                                           string const& parentJobId, CallbackType const& onFinish,
                                           int priority) {
    return Ptr(new SqlRowStatsJob(database, table, overlapSelector, stateUpdatePolicy, allWorkers, controller,
                                  parentJobId, onFinish, priority));
}

SqlRowStatsJob::SqlRowStatsJob(string const& database, string const& table,
                               ChunkOverlapSelector overlapSelector,
                               SqlRowStatsJob::StateUpdatePolicy stateUpdatePolicy, bool allWorkers,
                               Controller::Ptr const& controller, string const& parentJobId,
                               CallbackType const& onFinish, int priority)
        : SqlJob(::unlimitedMaxRows, allWorkers, controller, parentJobId, "SQL_TABLE_ROW_STATS", priority),
          _database(database),
          _table(table),
          _overlapSelector(overlapSelector),
          _stateUpdatePolicy(stateUpdatePolicy),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlRowStatsJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("overlap_selector", overlapSelector2str(overlapSelector()));
    result.emplace_back("state_update_policy", policy2str(stateUpdatePolicy()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<SqlRequest::Ptr> SqlRowStatsJob::launchRequests(replica::Lock const& lock, string const& worker,
                                                     size_t maxRequestsPerWorker) {
    list<SqlRequest::Ptr> requests;
    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers.count(worker) != 0) return requests;
    _workers.insert(worker);

    // Only the requested subset of tables is going to be processed at the worker.
    bool const allTables = false;
    vector<string> tables2process;
    switch (overlapSelector()) {
        case ChunkOverlapSelector::CHUNK:
            tables2process = workerTables(worker, database(), table(), allTables, false);
            break;
        case ChunkOverlapSelector::OVERLAP:
            tables2process = workerTables(worker, database(), table(), allTables, true);
            break;
        case ChunkOverlapSelector::CHUNK_AND_OVERLAP: {
            tables2process = workerTables(worker, database(), table(), allTables, false);
            auto overlaps = workerTables(worker, database(), table(), allTables, true);
            tables2process.insert(tables2process.end(), std::make_move_iterator(overlaps.begin()),
                                  std::make_move_iterator(overlaps.end()));
            break;
        }
    }

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    bool const keepTracking = true;
    auto const self = shared_from_base<SqlRowStatsJob>();
    for (auto&& tables : distributeTables(tables2process, maxRequestsPerWorker)) {
        requests.push_back(controller()->sqlRowStats(
                worker, database(), tables,
                [self](SqlRowStatsRequest::Ptr const& request) { self->onRequestFinish(request); },
                priority(), keepTracking, id()));
    }
    return requests;
}

void SqlRowStatsJob::stopRequest(replica::Lock const& lock, SqlRequest::Ptr const& request) {
    stopRequestDefaultImpl<StopSqlGetIndexesRequest>(lock, request);
}

void SqlRowStatsJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlRowStatsJob>(lock, _onFinish);
}

void SqlRowStatsJob::processResultAndFinish(replica::Lock const& lock, ExtendedState extendedState) {
    string const context_ = context() + string(__func__) + " ";

    bool const isForced = stateUpdatePolicy() == StateUpdatePolicy::FORCED;
    if (isForced ||
        (stateUpdatePolicy() == StateUpdatePolicy::ENABLED && extendedState == ExtendedState::SUCCESS)) {
        // Scan results (ignore failed requests) and store them in the intermediate
        // collection of the counters defined below. Each table replica encountered
        // by the scanner in a context of a table and a transaction will be
        // represented by a number of rows.
        map<string, map<TransactionId, vector<size_t>>> counters;

        // This flag will be used when scanning the partitioned tables to exclude special
        // tables like the prototype table (w/o a chunk number attached), or another
        // special case of table that has the dummy chunk.
        bool const isPartitioned = controller()
                                           ->serviceProvider()
                                           ->config()
                                           ->databaseInfo(database())
                                           .findTable(table())
                                           .isPartitioned;

        bool dataError = false;
        getResultData(lock).iterate([&](SqlJobResult::Worker const& worker,
                                        SqlJobResult::Scope const& internalTable,
                                        SqlResultSet::ResultSet const& resultSet) {
            bool const succeeded =
                    _process(context_, isPartitioned, worker, internalTable, resultSet, counters);
            dataError = dataError || !succeeded;
        });
        if (dataError) {
            finish(lock, ExtendedState::BAD_RESULT);
            return;
        }

        // Analyze counters to ensure that in a scope of each table:
        // - transactions have the same number of replicas
        // - row counters within each transaction match
        // Report errors and refuse updating the state if these conditions
        // aren't met.
        bool counterMismatchDetected = false;
        uint64_t const updateTime = PerformanceUtils::now();
        TableRowStats stats(database(), table());
        for (auto const& tableItr : counters) {
            string const& internalTable = tableItr.first;
            auto const& transactions = tableItr.second;
            size_t numReplicas = numeric_limits<size_t>::max();
            for (auto const& transItr : transactions) {
                TransactionId const transactionId = transItr.first;
                auto const& replicas = transItr.second;
                if (numReplicas == numeric_limits<size_t>::max()) {
                    numReplicas = replicas.size();
                } else if (numReplicas != replicas.size()) {
                    LOGS(_log, LOG_LVL_ERROR,
                         context_ << "replicas don't match in table: " << internalTable
                                  << " for transactionId: " << transactionId);
                    counterMismatchDetected = true;
                    break;
                }
                size_t numRows = numeric_limits<size_t>::max();
                for (size_t counter : replicas) {
                    if (numRows == numeric_limits<size_t>::max()) {
                        numRows = counter;
                    } else if (numRows != counter) {
                        LOGS(_log, LOG_LVL_ERROR,
                             context_ << "row counts don't match in table: " << internalTable
                                      << " for transactionId: " << transactionId);
                        counterMismatchDetected = true;
                        break;
                    }
                    if (isPartitioned) {
                        ChunkedTable const chunkedTable(internalTable);
                        stats.entries.emplace_back(TableRowStatsEntry(transactionId, chunkedTable.chunk(),
                                                                      chunkedTable.overlap(), numRows,
                                                                      updateTime));
                    } else {
                        stats.entries.emplace_back(
                                TableRowStatsEntry(transactionId, 0, false, numRows, updateTime));
                    }
                }
            }
        }
        if (counterMismatchDetected) {
            finish(lock, ExtendedState::BAD_RESULT);
            return;
        }

        // Update row numbers in the persistent state.
        try {
            controller()->serviceProvider()->databaseServices()->saveTableRowStats(stats);
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR,
                 context_ << "failed to update row counts in a scope of database: '" << database()
                          << ", table: " << table() << ", ex: " << ex.what());
            finish(lock, ExtendedState::FAILED);
            return;
        }
    }
    finish(lock, extendedState);
}

bool SqlRowStatsJob::_process(string const& context_, bool isPartitioned, SqlJobResult::Worker const& worker,
                              SqlJobResult::Scope const& internalTable,
                              SqlResultSet::ResultSet const& resultSet,
                              map<string, map<TransactionId, vector<size_t>>>& counters) const {
    bool const succeeded = true;
    auto const reportResultThatHas = [&](string const& problem) {
        LOGS(_log, LOG_LVL_ERROR,
             context_ << "result set received from worker '" << worker << "' for table '" << internalTable
                      << "' has " << problem);
    };

    // This scenario is possible in the FORCED mode. And we're guaranteed
    // not to seen it in the ENABLED mode.
    if (resultSet.extendedStatus != ProtocolStatusExt::NONE) return succeeded;

    // Skip special tables.
    if (isPartitioned) {
        if (internalTable == table()) return succeeded;
        try {
            ChunkedTable const chunkedTable(internalTable);
            if (chunkedTable.chunk() == lsst::qserv::DUMMY_CHUNK) return succeeded;
            if (chunkedTable.baseName() != table()) {
                reportResultThatHas("incorrect base name of the partitioned table");
                return !succeeded;
            }
        } catch (...) {
            reportResultThatHas("incorrect name of the partitioned table");
            return !succeeded;
        }
    }

    // Expecting a result set that have least one row and exactly two columns:
    //
    //   'qserv_trans_id' | 'num_rows'
    //  ------------------+------------
    //
    if (resultSet.fields.size() != 2 || resultSet.fields[0].name != "qserv_trans_id" ||
        resultSet.fields[1].name != "num_rows") {
        reportResultThatHas("unexpected format");
        LOGS(_log, LOG_LVL_ERROR,
             context_ << "rows.size(): " << resultSet.rows.size()
                      << " fields.size(): " << resultSet.fields.size() << " fields[0].name: "
                      << (resultSet.fields.size() > 0 ? resultSet.fields[0].name : "") << " fields[1].name: "
                      << (resultSet.fields.size() > 1 ? resultSet.fields[1].name : ""));
        return !succeeded;
    }
    if (resultSet.rows.empty()) {
        // IMPORTANT: Each tables is required to have a representation in the statistics
        // even if it has 0 rows. The default transaction 0 will be used in this case.
        counters[internalTable][0].push_back(0);
    } else {
        for (auto&& row : resultSet.rows) {
            if (row.nulls[0] || row.nulls[1]) {
                reportResultThatHas("unexpected NULL values");
                return !succeeded;
            }
            try {
                TransactionId const transactionId = lsst::qserv::replica::stoui(row.cells[0]);
                size_t const numRows = std::stoul(row.cells[1]);
                counters[internalTable][transactionId].push_back(numRows);
            } catch (out_of_range const&) {
                reportResultThatHas("values that can't be interpreted as unsigned numbers");
                return !succeeded;
            }
        }
    }
    return succeeded;
}

}  // namespace lsst::qserv::replica
