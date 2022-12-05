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
#include "replica/DirectorIndexJob.h"

// System headers
#include <algorithm>
#include <iostream>
#include <limits>
#include <stdexcept>

// Qserv headers
#include "global/constants.h"
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ServiceProvider.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DirectorIndexJob");

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

json DirectorIndexJob::Result::toJson() const {
    json result;
    for (auto&& workerItr : error) {
        string const& worker = workerItr.first;
        result[worker] = json::object();
        json& workerJson = result[worker];
        for (auto&& chunksItr : workerItr.second) {
            unsigned int chunk = chunksItr.first;
            string const& errorMessage = chunksItr.second;
            workerJson[chunk] = errorMessage;
        }
    }
    return result;
}

string DirectorIndexJob::typeName() { return "DirectorIndexJob"; }

DirectorIndexJob::Ptr DirectorIndexJob::create(string const& databaseName, string const& directorTableName,
                                               bool hasTransactions, TransactionId transactionId,
                                               bool allWorkers, bool localFile,
                                               Controller::Ptr const& controller, string const& parentJobId,
                                               CallbackType const& onFinish, int priority) {
    return Ptr(new DirectorIndexJob(databaseName, directorTableName, hasTransactions, transactionId,
                                    allWorkers, localFile, controller, parentJobId, onFinish, priority));
}

DirectorIndexJob::DirectorIndexJob(string const& databaseName, string const& directorTableName,
                                   bool hasTransactions, TransactionId transactionId, bool allWorkers,
                                   bool localFile, Controller::Ptr const& controller,
                                   string const& parentJobId, CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "INDEX", priority),
          _directorTableName(directorTableName),
          _hasTransactions(hasTransactions),
          _transactionId(transactionId),
          _allWorkers(allWorkers),
          _localFile(localFile),
          _onFinish(onFinish),
          _connPool(ConnectionPool::create(Configuration::qservCzarDbParams(lsst::qserv::SEC_INDEX_DB),
                                           controller->serviceProvider()->config()->get<size_t>(
                                                   "controller", "num-director-index-connections"))) {
    try {
        _database = controller->serviceProvider()->config()->databaseInfo(databaseName);
        if (!_database.findTable(directorTableName).isDirector) {
            throw runtime_error(context() + "::" + string(__func__) + " no such director table '" +
                                directorTableName + "' in the database: '" + _database.name + "'.");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, ex.what());
        throw;
    }
}

Job::Progress DirectorIndexJob::progress() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    util::Lock lock(_mtx, context() + __func__);
    return Progress{_completeChunks, _totalChunks};
}

DirectorIndexJob::Result const& DirectorIndexJob::getResultData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error(typeName() + "::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

list<std::pair<string, string>> DirectorIndexJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("directorTable", directorTable());
    result.emplace_back("has_transactions", bool2str(hasTransactions()));
    result.emplace_back("transaction_id", to_string(transactionId()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    result.emplace_back("local_file", bool2str(localFile()));
    return result;
}

list<pair<string, string>> DirectorIndexJob::persistentLogData() const {
    // Report failed chunks only

    list<pair<string, string>> result;
    for (auto&& workerItr : getResultData().error) {
        auto&& worker = workerItr.first;
        for (auto&& chunkItr : workerItr.second) {
            auto&& chunk = chunkItr.first;
            auto&& error = chunkItr.second;
            if (!error.empty()) {
                result.emplace_back("worker=" + worker + " chunk=" + to_string(chunk), "error=" + error);
            }
        }
    }
    return result;
}

void DirectorIndexJob::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // ------------------------
    // Stage I: replica scanner
    // ------------------------

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const workerNames = allWorkers() ? controller()->serviceProvider()->config()->allWorkers()
                                          : controller()->serviceProvider()->config()->workers();

    // Initialize a collection of chunks grouped by workers, in a way which
    // would make an attempt to keep requests equally (as much as that's possible)
    // balanced between the workers.
    //
    // Note, that the algorithm considers a possibility that chunks may have
    // multiple (more than 1) replicas. In this scenario a choice which replica
    // of a particular chunk to use will be based on the number of the chunk's
    // replicas as well as on the total number of chunks per each worker.

    // The first step is to find workers which store replicas of each
    // chunk to be processed by the job.

    bool const unusedAllDatabases = false;  // is still required by the method's contract
    bool const unusedIsPublished = true;    // is still required by the method's contract
    bool const includeFileInfo = false;     // to speed up the query as we don't need file info

    map<unsigned int, list<string>> chunk2workers;
    for (auto&& worker : workerNames) {
        // Scan for chunk replicas at the worker. The algorithm fills the data structure
        // used by the planner algorithm. The scanner has two flavors that depend on the input
        // parameters to the class.
        //   - If a specific transaction was requested then the algorithm will look at
        //     the actual chunk contributions made into the 'director' table at the worker.
        //     in a context of the given transaction. This scenario is more efficient during
        //     ingests since only a few chunks may get populated during a transaction.
        //   - Otherwise, the scanner relies upon the replica info records. This is a typical
        //     scenario for building the index after publishing a catalog.

        if (hasTransactions()) {
            // The unique combinations of the pairs (chunk,worker) represent replicas.
            // This intermediate data structure is needed to reduce individual chunk contributions
            // into replicas, in order to ensure the results of this version of the chunk screening
            // algorithm will be compatible with expectations of the planner.
            set<pair<unsigned int, string>> chunkAndWorker;

            // Locate all contributions into the table made at the given worker.
            vector<TransactionContribInfo> const contribs =
                    databaseServices->transactionContribs(transactionId(), directorTable(), worker);
            for (auto&& contrib : contribs) {
                chunkAndWorker.insert(make_pair(contrib.chunk, contrib.worker));
            }

            // Transform findings into the input data structure used by the planner.
            for (auto const& elem : chunkAndWorker) {
                chunk2workers[elem.first].push_back(elem.second);
            }
        } else {
            vector<ReplicaInfo> replicas;
            databaseServices->findWorkerReplicas(replicas, worker, database(), unusedAllDatabases,
                                                 unusedIsPublished, includeFileInfo);
            for (auto&& replica : replicas) {
                chunk2workers[replica.chunk()].push_back(replica.worker());
            }
        }
    }

    // ---------------------
    // Stage II: the planner
    // ---------------------

    // Now build the plan for each worker based on the above harvested
    // distribution of chunk replicas across workers.
    //
    // TODO: this single-pass algorithm may be biased to an order
    // in which chunks are being processed by the algorithm. Consider
    // a more sophisticated implementation which would be bias-free.

    for (auto&& itr : chunk2workers) {
        unsigned int const chunk = itr.first;
        auto&& workers = itr.second;

        // Find the least loaded worker from those where chunk replicas
        // are residing.
        string worker;
        size_t minNumChunks = numeric_limits<size_t>::max();
        for (auto&& candidateWorker : workers) {
            auto const numChunksAtCandidate = _chunks[candidateWorker].size();
            if (numChunksAtCandidate < minNumChunks) {
                worker = candidateWorker;
                minNumChunks = numChunksAtCandidate;
            }
        }
        if (worker.empty()) {
            throw logic_error(context() + string(__func__) + ":  internal bug");
        }
        _chunks[worker].push(chunk);
        _totalChunks++;
    }

    // --------------------------------------------------
    // Stage III: launching the initial batch of requests
    // --------------------------------------------------

    // Launch the initial batch of requests in the number which won't exceed
    // the number of the service processing threads at each worker multiplied
    // by the number of workers involved into the operation and by the "magic"
    // number 8. The later is needed to absorb the latency of the network
    // communications so that the worker threads would be able to work on
    // another batch of the data extraction requests while results of the
    // previous batch were being sent back to the Controller.

    size_t const maxRequestsPerWorker = 8 * controller()->serviceProvider()->config()->get<size_t>(
                                                    "worker", "num-svc-processing-threads");

    for (auto&& worker : workerNames) {
        for (auto&& ptr : _launchRequests(lock, worker, maxRequestsPerWorker)) {
            _requests[ptr->id()] = ptr;
        }
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
}

void DirectorIndexJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    _chunks.clear();

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& itr : _requests) {
        auto&& ptr = itr.second;
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            controller()->stopById<StopDirectorIndexRequest>(ptr->worker(), ptr->id(), nullptr, /* onFinish */
                                                             priority(), true, /* keepTracking */
                                                             id()              /* jobId */
            );
        }
    }
    _requests.clear();
}

void DirectorIndexJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<DirectorIndexJob>(lock, _onFinish);
}

void DirectorIndexJob::_onRequestFinish(DirectorIndexRequest::Ptr const& request) {
    // NOTE: this algorithm assumes "zero tolerance" to failures - any failure
    // in executing requests or processing data of the requests would result in
    // the job termination. The only exception from this rule is a scenario
    // when a target chunk table won't have a partition. This may be expected
    // for some chunk tables because they may not have contributions in a context
    // of the given super-transaction.

    string const context_ = context() + string(__func__) + " worker=" + request->worker() + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);

    // This synchronized block performs the light-weight operations that are meant
    // to evaluate the completion status of the request, update the internal data
    // structures and decide if the algorithm should proceed with ingesitng
    // the request's data into the "director" index table.
    bool hasData = true;
    {
        if (state() == State::FINISHED) return;
        util::Lock lock(_mtx, context_);
        if (state() == State::FINISHED) return;

        _completeChunks++;

        if (request->extendedState() != Request::SUCCESS) {
            if (request->extendedServerStatus() == ProtocolStatusExt::NO_SUCH_PARTITION) {
                // OK to proceed. We just don't have any contribution into the
                // partition.
                hasData = false;
            } else {
                _resultData.error[request->worker()][request->chunk()] = request->responseData().error;
                finish(lock, ExtendedState::FAILED);
                return;
            }
        }

        // Submit a replacement request for the same worker BEFORE processing
        // results of the current one. This little optimization is meant to keep
        // workers busy in case of a non-negligible latency in processing data of
        // requests.
        for (auto&& ptr : _launchRequests(lock, request->worker())) {
            _requests[ptr->id()] = ptr;
        }

        // Removing request from the list before processing its data is fine as
        // we still have a shared pointer passed into this method. Note that
        // we need to erase completed requests from memory since they may carry
        // a significant amount of data.
        // Erasing completed requests is also needed for evaluating the completion
        // condition of the job.
        _requests.erase(request->id());
    }

    // The next step performs the actual data loading within the lock-free (by not locking
    // the job's mutex guarding the job's internal state) context.
    // The loading is done by a thread that invoked the current handler. Note that
    // Loading data within the lock-free context allows the parellel processing of multiple
    // requests. Problems (if any) will be reported into the variable 'error' that will be
    // evaluated later to abort the processing should it be not empty.
    string error;
    if (hasData) {
        try {
            _processRequestData(request);
        } catch (exception const& ex) {
            error = context_ + "request data processing failed, ex: " + string(ex.what());
            LOGS(_log, LOG_LVL_ERROR, error);
        }
    }

    // The rest of the algorithm needs to be performed in the synchronized context.
    {
        if (state() == State::FINISHED) return;
        util::Lock lock(_mtx, context_);
        if (state() == State::FINISHED) return;

        if (!error.empty()) {
            _resultData.error[request->worker()][request->chunk()] = error;
            finish(lock, ExtendedState::FAILED);
            return;
        }

        // Evaluate for the completion condition of the job.
        if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
    }
}

void DirectorIndexJob::_processRequestData(DirectorIndexRequest::Ptr const& request) const {
    // Allocate a database connection using the RAII style handler that would automatically
    // deallocate the connection and abort the transaction should any problem occured
    // when loading data into the table.
    ConnectionHandler h(_connPool);
    QueryGenerator const g(h.conn);
    string const query = g.loadDataInfile(
            request->responseData().fileName, directorIndexTableName(database(), directorTable()),
            controller()->serviceProvider()->config()->get<string>("worker", "ingest-charset-name"),
            localFile());
    h.conn->executeInOwnTransaction([&](auto conn) {
        conn->execute(query);
        // Loading operations based on this mechanism won't result in throwing exceptions in
        // case of certain types of problems encountered during the loading, such as
        // out-of-range data, duplicate keys, etc. These errors are reported as warnings
        // which need to be retrieved using a special call to the database API.
        if (localFile()) {
            auto const warnings = conn->warnings();
            if (!warnings.empty()) {
                auto const& w = warnings.front();
                throw database::mysql::Error("query: " + query + " failed with total number of problems: " +
                                             to_string(warnings.size()) +
                                             ", first problem (Level,Code,Message) was: " + w.level + "," +
                                             to_string(w.code) + "," + w.message);
            }
        }
    });
}

list<DirectorIndexRequest::Ptr> DirectorIndexJob::_launchRequests(util::Lock const& lock,
                                                                  string const& worker, size_t maxRequests) {
    // Create as many requests as specified by the corresponding parameter of
    // the method or as many as are still available for the specified
    // worker (not to exceed the limit) by popping chunk numbers from the worker's
    // queue.
    list<DirectorIndexRequest::Ptr> requests;
    auto const self = shared_from_base<DirectorIndexJob>();
    while (_chunks[worker].size() > 0 && requests.size() < maxRequests) {
        auto const chunk = _chunks[worker].front();
        _chunks[worker].pop();

        requests.push_back(controller()->directorIndex(
                worker, database(), directorTable(), chunk, hasTransactions(), transactionId(),
                [self](DirectorIndexRequest::Ptr const& request) { self->_onRequestFinish(request); },
                priority(), true, /* keepTracking*/
                id()              /* jobId */
                ));
    }
    return requests;
}

}  // namespace lsst::qserv::replica
