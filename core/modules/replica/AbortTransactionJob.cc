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
#include "replica/AbortTransactionJob.h"

// System headers
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.AbortTransactionJob");

/**
 * The algorithm will distribute tables between the specified number of
 * bins. The resulting collection will be empty if the input collection
 * of tables is empty or if the number of bins is 0, and the result will
 * not have empty bins.
 *
 * @param allTables all known tables 
 * @param numBins the total number of bins for distributing tables
 * @return tables distributed between the bins
 */
vector<vector<string>> distributeTables(vector<string> const& allTables,
                                        size_t numBins) {

    // If the total number of tables if less than the number of bins
    // then we won't be constructing empty bins.
    vector<vector<string>> tablesPerBin(min(numBins,allTables.size()));

    // The trivial 'round-robin' 
    for (size_t i=0; i<allTables.size(); ++i) {
        auto const bin = i % tablesPerBin.size();
        tablesPerBin[bin].push_back(allTables[i]);
    }
    return tablesPerBin;
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

void AbortTransactionJobResult::iterate(
        AbortTransactionJobResult::OnTableVisitCallback const& onTableVisit) const {

    for (auto&& workerItr: resultSets) {
        auto&& worker = workerItr.first;
        for (auto&& requestResultSets: workerItr.second) {
            for (auto&& tableItr: requestResultSets.queryResultSet) {
                auto&& table = tableItr.first;
                auto&& resultSet = tableItr.second;
                onTableVisit(worker, table, resultSet);
            }
        }
    }
}

json AbortTransactionJobResult::toJson() const {
    json result = json::object();
    iterate([&result](WorkerName const& worker,
                      TableName const& table,
                      SqlResultSet::ResultSet const& resultSet) {
        result["completed"][worker][table] = resultSet.extendedStatus == ExtendedCompletionStatus::EXT_STATUS_NONE ? 1 : 0;
        result["error"    ][worker][table] = resultSet.error;
    });
    return result;
}


util::ColumnTablePrinter AbortTransactionJobResult::toColumnTable(
        string const& caption,
        string const& indent,
        bool verticalSeparator,
        bool reportAll) const {

    vector<string> workers;
    vector<string> tables;
    vector<string> statuses;
    vector<string> errors;

    iterate([&](WorkerName const& worker,
                TableName const& table,
                SqlResultSet::ResultSet const& resultSet) {
        if (reportAll or resultSet.extendedStatus != ExtendedCompletionStatus::EXT_STATUS_NONE) {
            workers.push_back(worker);
            tables.push_back(table);
            statuses.push_back(status2string(resultSet.extendedStatus));
            errors.push_back(resultSet.error);
        }
    });

    util::ColumnTablePrinter table(caption, indent, verticalSeparator);
    table.addColumn("worker", workers,  util::ColumnTablePrinter::LEFT);
    table.addColumn("table",  tables,   util::ColumnTablePrinter::LEFT);
    table.addColumn("status", statuses, util::ColumnTablePrinter::LEFT);
    table.addColumn("error",  errors,   util::ColumnTablePrinter::LEFT);

    return table;
}


string AbortTransactionJob::typeName() { return "AbortTransactionJob"; }


Job::Options const& AbortTransactionJob::defaultOptions() {
    static Job::Options const options{
        2,      /* priority */
        false,  /* exclusive */
        true    /* preemptable */
    };
    return options;
}


AbortTransactionJob::Ptr AbortTransactionJob::create(TransactionId transactionId,
                                                     bool allWorkers,
                                                     Controller::Ptr const& controller,
                                                     string const& parentJobId,
                                                     CallbackType const& onFinish,
                                                     Job::Options const& options) {
    return AbortTransactionJob::Ptr(
            new AbortTransactionJob(
                transactionId, allWorkers, controller, parentJobId,
                onFinish, options
            ));
}


AbortTransactionJob::AbortTransactionJob(TransactionId transactionId,
                                         bool allWorkers,
                                         Controller::Ptr const& controller,
                                         string const& parentJobId,
                                         CallbackType const& onFinish,
                                         Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "ABORT_TRANSACTION",
            options),
        _transactionId(transactionId),
        _allWorkers(allWorkers),
        _onFinish(onFinish) {

    auto const serviceProvider = controller->serviceProvider();
    auto const config = serviceProvider->config();

    // Exception will be thrown if any of those objects couldn't be found
    _transactionInfo = serviceProvider->databaseServices()->transaction(_transactionId);
    _databaseInfo = config->databaseInfo(_transactionInfo.database);

    // The names of workers are cached for the duration of the job
    _workers = _allWorkers ? config->allWorkers() : config->workers();
}


AbortTransactionJobResult const& AbortTransactionJob::getResultData() const {

    LOGS(_log, LOG_LVL_TRACE, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error(
            "AbortTransactionJob::" + string(__func__) +
            "  the method can't be called until the job hasn't finished");
}


list<pair<string,string>> AbortTransactionJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("transaction_id", to_string(transactionId()));
    result.emplace_back("all_workers", allWorkers() ? "1" : "0");
    return result;
}


list<pair<string,string>> AbortTransactionJob::persistentLogData() const {
    list<pair<string,string>> result;
    _resultData.iterate([&result](AbortTransactionJobResult::WorkerName const& worker,
                                  AbortTransactionJobResult::TableName const& table,
                                  SqlResultSet::ResultSet const& resultSet) {
        result.emplace_back(
            "status",
            "worker=" + worker +
            " table=" + table +
            " completed=" + string(resultSet.extendedStatus == ExtendedCompletionStatus::EXT_STATUS_NONE ? "1" : "0") +
            " error=" + resultSet.error
        );
    });
    return result;
}


void AbortTransactionJob::startImpl(util::Lock const& lock) {

    string const context_ =
        context() + string(__func__) + " transactionId=" + to_string(_transactionId) + " ";

    LOGS(_log, LOG_LVL_TRACE, context_);

    // Verify the current state of the transaction

    if (_transactionInfo.state != TransactionInfo::ABORTED) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "transaction is not ABORTED");
        finish(lock, ExtendedState::FAILED);
        return;
    }
    if (_databaseInfo.isPublished) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "database " << _databaseInfo.name
             << " is already PUBLISHED");
        finish(lock, ExtendedState::FAILED);
        return;
    }

    // Submit requests to process tables on each workers. For each worker,
    // The algorithm will identify all tables to be processed on the worker.
    // Then it will create (up to) as many requests as many processing threads are
    // known to exist on each worker. Each request would get its 'fair share'
    // of tables to be processed sequentially by the request. This algorithm has
    // the following benefits:
    //
    // - it limits the number of requests submitted to the workers by
    //   the number of workers multiplied by the number of the processing
    //   threads configured for each worker.
    // - it will ensure each thread will get enough work to absorb any
    //   latencies incurred by the request handling protocol.
    // - altogether, this will result in a more efficient utilization of
    //   various resources, at both the Controller and workers sides.

    auto const threadsPerWorker =
        controller()->serviceProvider()->config()->workerNumProcessingThreads();

    auto self = shared_from_base<AbortTransactionJob>();

    for (auto const& worker: _workers) {

        // All tables which are going to be processed at the worker
        vector<string> allTables;

        for (auto&& table: _databaseInfo.regularTables) {
            allTables.push_back(table);
        }
        
        // Locate all chunks registered on the worker. These chunks will be used
        // to build names of the corresponding chunk-specific partitioned tables.

        vector<ReplicaInfo> replicas;
        controller()->serviceProvider()->databaseServices()->findWorkerReplicas(
            replicas,
            worker,
            _databaseInfo.name
        );
        for (auto&& replica: replicas) {
            auto const chunk = replica.chunk();
            for (auto&& table: _databaseInfo.partitionedTables) {
                allTables.push_back(table + "FullOverlap_" + to_string(chunk));
                allTables.push_back(table + "_" + to_string(chunk));
            }
        }
        
        // Divide tables between requests. Then launch the requests
        // for the current worker.

        auto const tablesPerThread = ::distributeTables(allTables, threadsPerWorker);
        for (auto&& tables: tablesPerThread) {
            _requests.push_back(
                controller()->sqlDeleteTablePartition(
                    worker,
                    _databaseInfo.name,
                    tables,
                    _transactionId,
                    [self] (SqlDeleteTablePartitionRequest::Ptr const& request) {
                        self->_onRequestFinish(request);
                    },
                    options(lock).priority,
                    true,   /* keepTracking */
                    id()    /* parent Job ID */
                )
            );
        }
    }
    if (0 == _requests.size()) {
        finish(lock, ExtendedState::SUCCESS);
    }
}


void AbortTransactionJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    for (auto&& request: _requests) {
        request->cancel();
    }
}


void AbortTransactionJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<AbortTransactionJob>(lock, _onFinish);
}


void AbortTransactionJob::_onRequestFinish(SqlDeleteTablePartitionRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_TRACE, context() << __func__
         << "  worker=" << request->worker() << " id=" << request->id());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + string(__func__));

    if (state() == State::FINISHED) return;

    ++_numFinished;
    if (request->extendedState() == Request::SUCCESS) ++_numSuccess;

    if (_numFinished == _requests.size()) {

        // Harvest results from all jobs regardless of their completion status.
        // Requests declared as failed might be partially successful. In order
        // to determine which tables have not been processed one has to look
        // at the corresponding result set reported in the response data
        // object of the request.

        for (auto&& ptr: _requests) {
            _resultData.resultSets[ptr->worker()].push_back(ptr->responseData());
        }
        finish(lock, _numSuccess == _numFinished ? ExtendedState::SUCCESS
                                                 : ExtendedState::FAILED);
    }
}

}}} // namespace lsst::qserv::replica
