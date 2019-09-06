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
#include "replica/IndexJob.h"

// System headers
#include <algorithm>
#include <iostream>
#include <limits>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IndexJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

string IndexJob::typeName() { return "IndexJob"; }

Job::Options const& IndexJob::defaultOptions() {
    static Job::Options const options{
        2,      /* priority */
        false,  /* exclusive */
        true    /* preemptable */
    };
    return options;
}


IndexJob::Ptr IndexJob::create(string const& database,
                               bool hasTransactions,
                               uint32_t transactionId,
                               bool allWorkers,
                               Controller::Ptr const& controller,
                               string const& parentJobId,
                               CallbackType const& onFinish,
                               Job::Options const& options) {
    return Ptr(new IndexJob(
        database,
        hasTransactions,
        transactionId,
        allWorkers,
        controller,
        parentJobId,
        onFinish,
        options
    ));
}


IndexJob::IndexJob(string const& database,
                   bool hasTransactions,
                   uint32_t transactionId,
                   bool allWorkers,
                   Controller::Ptr const& controller,
                   string const& parentJobId,
                   CallbackType const& onFinish,
                   Job::Options const& options)
    :   Job(controller, parentJobId, "INDEX", options),
        _database(database),
        _hasTransactions(hasTransactions),
        _transactionId(transactionId),
        _allWorkers(allWorkers),
        _onFinish(onFinish) {
}


IndexJobResult const& IndexJob::getResultData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error(
            "IndexJob::" + string(__func__) +
            "  the method can't be called while the job hasn't finished");
}


list<std::pair<string,string>> IndexJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database", database());
    result.emplace_back("has_transactions", string(hasTransactions() ? "1" : "0"));
    result.emplace_back("transaction_id", to_string(transactionId()));
    result.emplace_back("all_workers", string(allWorkers() ? "1" : "0"));
    return result;
}


list<pair<string,string>> IndexJob::persistentLogData() const {

    // Report failed chunks only

    list<pair<string,string>> result;
    for (auto&& workerItr: getResultData().error) {
        auto&& worker = workerItr.first;
        for (auto&& chunkItr: workerItr.second) {
            auto&& chunk = chunkItr.first;
            auto&& error = chunkItr.second;
            if (not error.empty()) {
                result.emplace_back(
                    "worker=" + worker + " chunk=" + to_string(chunk),
                    "error=" + error
                );
            }
        }
    }
    return result;
}


void IndexJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto const workerNames = allWorkers() ?
        controller()->serviceProvider()->config()->allWorkers() :
        controller()->serviceProvider()->config()->workers();

    // Initialize a collection of chunks grouped by workers, in a way which
    // would make a best attempt to keep requests equally (as much as that's possible)
    // balanced between the workers.
    //
    // Note, that the algorithm considers a possibility that chunks may have
    // multiple (more than 1) replicas. In this scenario a choice which replica
    // of a particular chunk to use will be based on the number of the chunk's
    // replicas as well as on the total number of chunks per each worker.

    // The first step is to find workers which store replicas of each
    // chunk to be processed by the job.

    map<unsigned int, list<string>> chunk2workers;
    for (auto&& worker: workerNames) {
        vector<ReplicaInfo> replicas;
        controller()->serviceProvider()->databaseServices()->findWorkerReplicas(
            replicas,
            worker,
            database()
        );
        for (auto&& replica: replicas) {
            chunk2workers[replica.chunk()].push_back(replica.worker());
        }
    }
    
    // Now build the plan for each worker based on the above harvested
    // distribution of chunk replicas across workers.
    //
    // TODO: this single-pass algorithm may be biased to an order
    // in which chunks are being processed by the algorithm. Consider
    // a more sophisticated implementation which would be bias-free.

    for (auto&& itr: chunk2workers) {

        unsigned int const chunk = itr.first;
        auto&& workers = itr.second;

        // Find the least loaded worker from those where chunk replicas
        // are residing.
        string worker;
        size_t minNumChunks = numeric_limits<size_t>::max();
        for (auto&& candidateWorker: workers) {
            auto const numChunksAtCandidate = _chunks[candidateWorker].size();
            if (numChunksAtCandidate < minNumChunks) {
                worker = candidateWorker;
                minNumChunks = numChunksAtCandidate;
            }
        }
        if (worker.empty()) {
            throw logic_error(context() + string(__func__) + ":  internal bug");
        }
        _chunks[worker].push_back(chunk);
    }

    // Launch the initial batch of requests in the number which won't exceed
    // the number of the service processing threads at each worker multiplied
    // by the number of workers involved into the operation.

    size_t const maxRequestsPerWorker =
        controller()->serviceProvider()->config()->workerNumProcessingThreads();

    for (auto&& worker: workerNames) {
        for (auto&& ptr: _launchRequests(lock, worker, maxRequestsPerWorker)) {
            _requests[ptr->id()] = ptr;
        }
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
}


void IndexJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    _chunks.clear();

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& itr: _requests) {
        auto&& ptr = itr.second;
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            controller()->stopById<StopIndexRequest>(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                options(lock).priority,
                true,       /* keepTracking */
                id()        /* jobId */
            );
        }
    }
    _requests.clear();
    _numFinished = 0;
    _numSuccess  = 0;
}


void IndexJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<IndexJob>(lock, _onFinish);
}


void IndexJob::_onRequestFinish(IndexRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  worker=" << request->worker());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    _numFinished++;
    if (request->extendedState() == Request::SUCCESS) _numSuccess++;

    // Update stats and save result at the specified destination before discarding
    // the request object. Note that we HAVE TO erase completed requests since they
    // may carry a significant amount of data.

    _processRequestData(lock, request);
    _requests.erase(request->id());
    
    // Try submitting a replacement request for the same worker. If none
    // would be launched then evaluate for the completion condition of the job.

    for (auto&& ptr: _launchRequests(lock, request->worker())) {
        _requests[ptr->id()] = ptr;
    }
    if (_requests.size() == 0) {
        finish(lock, _numSuccess == _numFinished ? ExtendedState::SUCCESS :
                                                   ExtendedState::FAILED);
    }
}


void IndexJob::_processRequestData(util::Lock const& lock,
                                   IndexRequest::Ptr const& request) {

    // Unconditionally update stats which may include the MySQL-specific errors
    // reported by the failed queries.
    _resultData.error[request->worker()][request->chunk()] = request->responseData().error;

    if (request->extendedState() != Request::SUCCESS) return;

    // TODO: this is just a test to be replaced this with a meaningful code.
    cout << request->responseData().data;
}


list<IndexRequest::Ptr> IndexJob::_launchRequests(util::Lock const& lock,
                                                  string const& worker,
                                                  size_t maxRequests) {
    list<IndexRequest::Ptr> requests;
    
    // Create as many requests as specified by the corresponding parameter of
    // the method or as many as are still available for the specified
    // worker (not to exceed the limit) by popping chunk numbers from the worker's
    // queue.

    auto const self = shared_from_base<IndexJob>();

    while (_chunks[worker].size() > 0 and requests.size() < maxRequests) {

        auto const chunk = _chunks[worker].front();
        _chunks[worker].pop_front();

        requests.push_back(
            controller()->index(
                worker,
                database(),
                chunk,
                hasTransactions(),
                transactionId(),
                [self] (IndexRequest::Ptr const& request) {
                    self->_onRequestFinish(request);
                },
                options(lock).priority,
                true,   /* keepTracking*/
                id()    /* jobId */
            )
        );
    }
    return requests;
}

}}} // namespace lsst::qserv::replica
