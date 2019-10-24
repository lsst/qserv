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
#include <limits>
#include <stdexcept>

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

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

json AbortTransactionJobResult::toJson() const {
    string const context = "AbortTransactionJobResult::" + string(__func__) + "  ";
    json result = json::object();
    result["completed"] = json::object();
    result["error"    ] = json::object();
    for (auto&& workerEntry: resultSets) {
        auto&& worker = workerEntry.first;
        result["completed"][worker] = json::object();
        result["error"    ][worker] = json::object();
        for (auto&& tableEntry: workerEntry.second) {
            auto&& table = tableEntry.first;
            auto&& resultSet = tableEntry.second;
            result["completed"][worker][table] = completed.at(worker).at(table) ? 1 : 0;
            result["error"    ][worker][table] = resultSet.error;
        }
    }
    return result;
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


AbortTransactionJob::Ptr AbortTransactionJob::create(uint32_t transactionId,
                                                     bool allWorkers,
                                                     Controller::Ptr const& controller,
                                                     string const& parentJobId,
                                                     CallbackType const& onFinish,
                                                     Job::Options const& options) {
    return AbortTransactionJob::Ptr(
        new AbortTransactionJob(transactionId,
                                allWorkers,
                                controller,
                                parentJobId,
                                onFinish,
                                options));
}


AbortTransactionJob::AbortTransactionJob(uint32_t transactionId,
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

    for (auto&& workerEntry: _resultData.resultSets) {
        auto&& worker = workerEntry.first;
        for (auto&& tableEntry: workerEntry.second) {
            auto&& table = tableEntry.first;
            result.emplace_back(
                "status",
                "worker=" + worker +
                " table=" + table +
                " completed=" + string(_resultData.completed.at(worker).at(table) ? "1" : "0") +
                " error=" + _resultData.resultSets.at(worker).at(table).error
            );
        }
    }
    return result;
}


void AbortTransactionJob::startImpl(util::Lock const& lock) {

    string const context_ =
        context() + string(__func__) + " transactionId=" + to_string(_transactionId) + " ";

    LOGS(_log, LOG_LVL_TRACE, context_);

    // Verify the current state of the transaction

    if (_transactionInfo.state != "ABORTED") {
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

    // Prepare a collection of tables to be processed. Group them by
    // worker names.

    for (auto const& worker: _workers) {
        _worker2tables[worker] = list<string>();

        for (auto&& table: _databaseInfo.regularTables) {
            _worker2tables[worker].push_back(table);
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
                _worker2tables[worker].push_back(table + "FullOverlap_" + to_string(chunk));
                _worker2tables[worker].push_back(table + "_" + to_string(chunk));
            }
        }
    }

    // Submit the first batch of requests. The number of the requests not to exceed
    // the processing capacity of all workers.

    size_t const maxRequests =
        _workers.size() * controller()->serviceProvider()->config()->workerNumProcessingThreads();

    if (0 == _submitNextBatch(lock, maxRequests)) {
        // No tables to be processed
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

    // Replace the finished job with a new one unless the input set is empty
    if (0 == _submitNextBatch(lock, 1)) {
        if (_numFinished == _numLaunched) {

            // Harvest results from all jobs (regardless of their completion status)
            // and be done.

            for (auto&& ptr: _requests) {
                auto const table = ptr->targetRequestParams().table;
                _resultData.completed [ptr->worker()][table] = ptr->extendedState() == Request::SUCCESS;
                _resultData.resultSets[ptr->worker()][table] = ptr->responseData();
            }
            finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS
                                                     : ExtendedState::FAILED);
        }
    }
}


size_t AbortTransactionJob::_submitNextBatch(util::Lock const& lock,
                                             size_t const maxRequests) {

    if (0 == maxRequests) return maxRequests;

    auto self = shared_from_base<AbortTransactionJob>();

    size_t batchSize = 0;
    for (size_t i = 0; i < maxRequests; ++i) {

        // Scan the input queue and find a worker which has the longest list
        // of tables to be processed. Then submit a request for any table
        // of that worker.
        
        size_t maxNumTables = 0; 
        string candidateWorker;
        for (auto&& entry: _worker2tables) {
            string const& worker = entry.first;
            size_t const numTables = entry.second.size();
            if (numTables > maxNumTables) {
                maxNumTables = numTables;
                candidateWorker = worker;
            }
        }
        if (candidateWorker.empty()) return batchSize;  // no more entries in the input queue
        
        string const table = _worker2tables[candidateWorker].front();
        _worker2tables[candidateWorker].pop_front();

        _requests.push_back(
            controller()->sqlDeleteTablePartition(
                candidateWorker,
                _databaseInfo.name,
                table,
                _transactionId,
                [self] (SqlDeleteTablePartitionRequest::Ptr const& request) {
                    self->_onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking */
                id()    /* parent Job ID */
            )
        );
        ++batchSize;
        ++_numLaunched;
    }
    return batchSize;
}

}}} // namespace lsst::qserv::replica
