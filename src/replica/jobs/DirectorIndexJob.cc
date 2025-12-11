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
#include "replica/jobs/DirectorIndexJob.h"

// System headers
#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>

// Qserv headers
#include "global/constants.h"
#include "replica/config/Configuration.h"
#include "replica/ingest/TransactionContrib.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/requests/StopRequest.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"

// LSST headers
#include "lsst/log/Log.h"
#include "DirectorIndexJob.h"

using namespace std;
using namespace std::chrono_literals;
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
                                               bool allWorkers, Controller::Ptr const& controller,
                                               string const& parentJobId, CallbackType const& onFinish,
                                               int priority) {
    return Ptr(new DirectorIndexJob(databaseName, directorTableName, hasTransactions, transactionId,
                                    allWorkers, controller, parentJobId, onFinish, priority));
}

DirectorIndexJob::DirectorIndexJob(string const& databaseName, string const& directorTableName,
                                   bool hasTransactions, TransactionId transactionId, bool allWorkers,
                                   Controller::Ptr const& controller, string const& parentJobId,
                                   CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "INDEX", priority),
          _directorTableName(directorTableName),
          _hasTransactions(hasTransactions),
          _transactionId(transactionId),
          _allWorkers(allWorkers),
          _onFinish(onFinish) {
    try {
        _database = controller->serviceProvider()->config()->databaseInfo(databaseName);
        if (!_database.findTable(directorTableName).isDirector()) {
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
    replica::Lock lock(_mtx, context() + __func__);
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
    return result;
}

list<pair<string, string>> DirectorIndexJob::persistentLogData() const {
    // Report failed chunks only
    list<pair<string, string>> result;
    for (auto&& workerItr : getResultData().error) {
        auto&& workerName = workerItr.first;
        for (auto&& chunkItr : workerItr.second) {
            auto&& chunk = chunkItr.first;
            auto&& error = chunkItr.second;
            if (!error.empty()) {
                result.emplace_back("worker=" + workerName + " chunk=" + to_string(chunk), "error=" + error);
            }
        }
    }
    return result;
}

void DirectorIndexJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // ------------------------
    // Stage I: replica scanner
    // ------------------------

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const workerNames = allWorkers() ? config->allWorkers() : config->workers();

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
    for (auto&& workerName : workerNames) {
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
            vector<TransactionContribInfo> contribs;
            try {
                contribs =
                        databaseServices->transactionContribs(transactionId(), directorTable(), workerName);
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR,
                     context() << __func__ << "  failed to fetch transaction contributions for "
                               << " transactionId: " << transactionId()
                               << " directorTable: " << directorTable() << " workerName: " << workerName
                               << "  exception: " << ex.what());
                finish(lock, ExtendedState::FAILED);
                return;
            }
            for (auto&& contrib : contribs) {
                chunkAndWorker.insert(make_pair(contrib.chunk, contrib.worker));
            }

            // Transform findings into the input data structure used by the planner.
            for (auto const& elem : chunkAndWorker) {
                chunk2workers[elem.first].push_back(elem.second);
            }
        } else {
            vector<ReplicaInfo> replicas;
            databaseServices->findWorkerReplicas(replicas, workerName, database(), unusedAllDatabases,
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
        string workerName;
        size_t minNumChunks = numeric_limits<size_t>::max();
        for (auto&& candidateWorker : workers) {
            auto const numChunksAtCandidate = _chunks[candidateWorker].size();
            if (numChunksAtCandidate < minNumChunks) {
                workerName = candidateWorker;
                minNumChunks = numChunksAtCandidate;
            }
        }
        if (workerName.empty()) {
            throw logic_error(context() + string(__func__) + ":  internal bug");
        }
        _chunks[workerName].push(chunk);
        _totalChunks++;
    }

    // --------------------------------------------------
    // Stage III: launching the initial batch of requests
    // --------------------------------------------------

    // Launch the initial batch of requests in the number which won't exceed
    // the number of the service processing threads at each worker multiplied
    // by the number of workers involved into the operation. This is needed
    // to absorb the latency of the network and disk I/O, so that worker threads
    // would be able to work on another batch of the data extraction requests while
    // results of the previous batch were being sent back to the Controller.
    size_t const maxRequestsPerWorker = config->get<size_t>("worker", "num-svc-processing-threads");
    for (auto&& workerName : workerNames) {
        for (auto&& ptr : _launchRequests(lock, workerName, maxRequestsPerWorker)) {
            _inFlightRequests[ptr->id()] = ptr;
        }
    }

    // In case if no workers or database are present in the Configuration
    // at this time.
    if (_inFlightRequests.size() == 0) finish(lock, ExtendedState::SUCCESS);

    // Start a pool of threads for ingesting the "director" index data into MySQL.
    // Note that threads are getting a copy of a shared pointer. This guarantees that
    // the job will outlive the threads.
    auto const self = shared_from_base<DirectorIndexJob>();
    size_t const numThreads = config->get<size_t>("controller", "num-director-index-connections");
    for (size_t i = 0; i < numThreads; ++i) {
        std::thread t([self]() { self->_loadDataIntoTable(); });
        t.detach();
    }
}

void DirectorIndexJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.
    _chunks.clear();

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.
    auto const noCallbackOnFinish = nullptr;
    bool const keepTracking = true;
    for (auto&& itr : _inFlightRequests) {
        auto&& ptr = itr.second;
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            StopRequest::createAndStart(controller(), ptr->workerName(), ptr->id(), noCallbackOnFinish,
                                        priority(), keepTracking, id());
        }
    }
    _inFlightRequests.clear();
}

void DirectorIndexJob::notify(replica::Lock const& lock) {
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

    string const context_ = context() + string(__func__) + " worker=" + request->workerName() + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context_);
    if (state() == State::FINISHED) return;

    if (request->extendedState() == Request::SUCCESS) {
        // This request has data that will be loaded into the table.
        _completedRequests.push_back(request);
        _cv.notify_one();
    } else if (request->extendedServerStatus() == ProtocolStatusExt::NO_SUCH_PARTITION) {
        // No contribution is expected into the non-existing (in this chunk) partition.
        // This is would be the normal situaton for many chunks. However, we still
        // need to increment the counter of the fully completed chunks.
        _completeChunks++;
    } else {
        _resultData.error[request->workerName()][request->chunk()] = request->responseData().error;
        finish(lock, ExtendedState::FAILED);
        return;
    }

    // Submit a replacement request for the same worker BEFORE processing
    // results of the current one. This little optimization is meant to keep
    // workers busy as a compensatory mechanism for the non-negligible latencies
    // in processing data of the completed requests.
    for (auto&& ptr : _launchRequests(lock, request->workerName())) {
        _inFlightRequests[ptr->id()] = ptr;
    }

    // Removing request from the list before processing its data is fine as
    // we still have a shared pointer passed into this method. Note that
    // we need to erase completed requests from memory since they may carry
    // a significant amount of data.
    _inFlightRequests.erase(request->id());

    // Evaluate the completion condition of the job.
    if (_completeChunks == _totalChunks) {
        finish(lock, ExtendedState::SUCCESS);
    }
}

void DirectorIndexJob::_loadDataIntoTable() {
    string const context_ = context() + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);

    // Open MySQL connection using the RAII-style handler that would automatically
    // abort the transaction should any problem occured when loading data into the table.
    Connection::Ptr conn;
    try {
        conn = Connection::open(Configuration::qservCzarDbParams(lsst::qserv::SEC_INDEX_DB));
    } catch (exception const& ex) {
        string const error =
                context_ + "failed to connect to the czar's database server, ex: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, error);
        if (state() == State::FINISHED) return;
        replica::Lock lock(_mtx, context_);
        if (state() == State::FINISHED) return;
        finish(lock, ExtendedState::FAILED);
        return;
    }
    ConnectionHandler h(conn);
    QueryGenerator const g(h.conn);

    // Pool completed requests from the queue and process them.
    while (true) {
        // The method will return the null pointer if the job has finished.
        auto const request = _nextRequest();
        if (request == nullptr) break;

        // Load request's data into the destination table.
        bool const localFile = true;
        try {
            string const query = g.loadDataInfile(
                    request->responseData().fileName, directorIndexTableName(database(), directorTable()),
                    controller()->serviceProvider()->config()->get<string>("worker", "ingest-charset-name"),
                    localFile);
            h.conn->executeInOwnTransaction([&](auto conn) {
                conn->execute(query);
                // Loading operations based on this mechanism won't result in throwing exceptions in
                // case of certain types of problems encountered during the loading, such as
                // out-of-range data, duplicate keys, etc. These errors are reported as warnings
                // which need to be retrieved using a special call to the database API.
                auto const warnings = conn->warnings();
                if (!warnings.empty()) {
                    auto const& w = warnings.front();
                    throw database::mysql::Error(
                            "query: " + query +
                            " failed with total number of problems: " + to_string(warnings.size()) +
                            ", first problem (Level,Code,Message) was: " + w.level + "," + to_string(w.code) +
                            "," + w.message);
                }
            });

            // Decrement the counter for the number of the on-going loading operations.
            replica::Lock lock(_mtx, context_);
            _numLoadingRequests--;

            // The counter of the fully completed chunks gets incremented only after
            // succesfully finishing the ingest into the destination table.
            _completeChunks++;

        } catch (exception const& ex) {
            string const error = context_ + "failed to load data into the 'director' index table, ex: " +
                                 string(ex.what());
            LOGS(_log, LOG_LVL_ERROR, error);
            if (state() == State::FINISHED) return;
            replica::Lock lock(_mtx, context_);
            if (state() == State::FINISHED) return;
            _resultData.error[request->workerName()][request->chunk()] = error;
            finish(lock, ExtendedState::FAILED);
            return;
        }
    }
}

DirectorIndexRequest::Ptr DirectorIndexJob::_nextRequest() {
    string const context_ = context() + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);

    // This method has to use std::condition_variable::wait_for()
    // instead of std::condition_variable::wait() in order to allow rechecking
    // changes in a status of the job, not just notifications on new entries
    // in the queue _completedRequests.
    //
    // The re-check interval of 1s should be reasonable here since this type
    // of jobs is designed for processing large amounts of data, which may
    // take many minutes or even many hours.
    chrono::milliseconds const jobStatusCheckIvalMsec{1s};

    // Pull the next request from the queue or wait.
    DirectorIndexRequest::Ptr request;
    while (request == nullptr) {
        if (state() == State::FINISHED) return nullptr;
        unique_lock<mutex> lock(_mtx);
        if (state() == State::FINISHED) return nullptr;

        if (!_completedRequests.empty()) {
            request = _completedRequests.front();
            _completedRequests.pop_front();
            _numLoadingRequests++;
        } else {
            _cv.wait_for(lock, jobStatusCheckIvalMsec, [&] {
                if (state() == State::FINISHED) return true;
                if (_completeChunks == _totalChunks) {
                    // We got here because the main condition for completing the job
                    // has been met. It means the job needs to be finished right away.
                    // Unfortunately, the desired state transition can't be done under
                    // the currently held lock of thr type std::unique_lock. The API for
                    // the operation require acquering a different type of locks
                    // represented by the class replica::Lock.
                    return true;
                }
                if (!_completedRequests.empty()) {
                    request = _completedRequests.front();
                    _completedRequests.pop_front();
                    _numLoadingRequests++;
                    return true;
                }
                // Keep waiting before the next wakeup.
                return false;
            });
            if (_completeChunks == _totalChunks) break;
        }
    }

    // Re-evaluate the current state of the job under a different type
    // of lock that's suitable for making state transitions of the job
    // if needed.
    if (state() == State::FINISHED) return nullptr;
    replica::Lock lock(_mtx, context_);
    if (state() == State::FINISHED) return nullptr;

    if (_completeChunks == _totalChunks) {
        finish(lock, ExtendedState::SUCCESS);
        return nullptr;
    }
    LOGS(_log, LOG_LVL_DEBUG,
         context_ << "request: " << request->id() << " _inFlightRequests.size(): " << _inFlightRequests.size()
                  << " _completedRequests.size(): " << _completedRequests.size()
                  << " _numLoadingRequests: " << _numLoadingRequests
                  << " _completeChunks: " << _completeChunks << " _totalChunks: " << _totalChunks);
    return request;
}

list<DirectorIndexRequest::Ptr> DirectorIndexJob::_launchRequests(replica::Lock const& lock,
                                                                  string const& workerName,
                                                                  size_t maxRequests) {
    // Create as many requests as specified by the corresponding parameter of
    // the method or as many as are still available for the specified
    // worker (not to exceed the limit) by popping chunk numbers from the worker's
    // queue.
    list<DirectorIndexRequest::Ptr> requests;
    auto const self = shared_from_base<DirectorIndexJob>();
    bool const keepTracking = true;
    while (_chunks[workerName].size() > 0 && requests.size() < maxRequests) {
        auto const chunk = _chunks[workerName].front();
        _chunks[workerName].pop();

        requests.push_back(DirectorIndexRequest::createAndStart(
                controller(), workerName, database(), directorTable(), chunk, hasTransactions(),
                transactionId(),
                [self](DirectorIndexRequest::Ptr const& request) { self->_onRequestFinish(request); },
                priority(), keepTracking, id()));
    }
    return requests;
}

}  // namespace lsst::qserv::replica
