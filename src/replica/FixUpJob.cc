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
#include "replica/FixUpJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FixUpJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& FixUpJob::defaultOptions() {
    static Job::Options const options{
        1,      /* priority */
        true,   /* exclusive */
        true    /* exclusive */
    };
    return options;
}


string FixUpJob::typeName() { return "FixUpJob"; }


FixUpJob::Ptr FixUpJob::create(string const& databaseFamily,
                               Controller::Ptr const& controller,
                               string const& parentJobId,
                               CallbackType const& onFinish,
                               Job::Options const& options) {
    return FixUpJob::Ptr(
        new FixUpJob(databaseFamily,
                     controller,
                     parentJobId,
                     onFinish,
                     options));
}


FixUpJob::FixUpJob(string const& databaseFamily,
                   Controller::Ptr const& controller,
                   string const& parentJobId,
                   CallbackType const& onFinish,
                   Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "FIXUP",
            options),
        _databaseFamily(databaseFamily),
        _onFinish(onFinish) {
}


FixUpJobResult const& FixUpJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error(
            typeName() + "::" + string(__func__) +
            "  the method can't be called while the job hasn't finished");
}


list<pair<string,string>> FixUpJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database_family", databaseFamily());
    return result;
}


list<pair<string,string>> FixUpJob::persistentLogData() const {

    list<pair<string,string>> result;

    auto&& replicaData = getReplicaData();

    // Report workers failed to respond to the requests

    for (auto&& workerInfo: replicaData.workers) {
        auto&& worker = workerInfo.first;
        auto const numFailedRequests = workerInfo.second;
        if (numFailedRequests != 0) {
            result.emplace_back(
                "failed-worker", "worker=" + worker
                + " num-failed-requests=" + to_string(numFailedRequests));
        }
    }

    // Per-worker counters for the following categories:
    //
    //   created-chunks:
    //     the total number of chunks created on the workers as a result
    //     of the operation

    map<string,
        map<string,
            size_t>> workerCategoryCounter;

    for (auto&& info: replicaData.replicas) {
        workerCategoryCounter[info.worker()]["created-chunks"]++;
    }
    for (auto&& workerItr: workerCategoryCounter) {
        auto&& worker = workerItr.first;
        string val = "worker=" + worker;

        for (auto&& categoryItr: workerItr.second) {
            auto&& category = categoryItr.first;
            size_t const counter = categoryItr.second;
            val += " " + category + "=" + to_string(counter);
        }
        result.emplace_back("worker-stats", val);
    }
    return result;
}


void FixUpJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<FixUpJob>();
    bool const saveReplicInfo = true;           // always save the replica info in a database because
                                                // the algorithm depends on it.
    bool const allWorkers = false;              // only consider enabled workers
    _findAllJob = FindAllJob::create(
        databaseFamily(),
        saveReplicInfo,
        allWorkers,
        controller(),
        id(),
        [self] (FindAllJob::Ptr job) {
            self->_onPrecursorJobFinish();
        }
    );
    _findAllJob->start();
}


void FixUpJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            controller()->stopById<StopReplicationRequest>(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                options(lock).priority,
                true,       /* keepTracking */
                id()        /* jobId */);
    }
    _destinationWorker2tasks.clear();
    _requests.clear();
}


void FixUpJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<FixUpJob>(lock, _onFinish);
}


void FixUpJob::_onPrecursorJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // Proceed with the replication effort only if the precursor job
    // has succeeded.

    if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
        finish(lock, ExtendedState::FAILED);
        return;
    }

    // Analyze results and prepare a replication plan to fix chunk
    // co-location for under-represented chunks

    FindAllJobResult const& replicaData = _findAllJob->getReplicaData();
    for (auto&& chunk2workers: replicaData.isColocated) {
        unsigned int chunk = chunk2workers.first;

        for (auto&& worker2colocated: chunk2workers.second) {
            string const& destinationWorker = worker2colocated.first;
            bool   const  isColocated       = worker2colocated.second;

            if (isColocated) continue;

            // Iterate over all participating databases, find the ones which aren't
            // represented on the worker, find a suitable source worker which has
            // a complete chunk for the database and which (the worker) is not the same
            // as the current one and submit the replication request.

            for (auto&& database: replicaData.databases.at(chunk)) {

                if (not replicaData.chunks.chunk(chunk)
                                          .database(database)
                                          .workerExists(destinationWorker)) {

                    // Finding a source worker first
                    string sourceWorker;
                    for (auto&& worker: replicaData.complete.at(chunk).at(database)) {
                        if (worker != destinationWorker) {
                            sourceWorker = worker;
                            break;
                        }
                    }
                    if (sourceWorker.empty()) {

                        LOGS(_log, LOG_LVL_ERROR, context()
                             << __func__ << "  failed to find a source worker for chunk: "
                             << chunk << " and database: " << database);

                        finish(lock, ExtendedState::FAILED);
                        return;
                    }

                    // Register the replica creation task which will turn into a job.
                    _destinationWorker2tasks[destinationWorker].emplace(ReplicationTask{
                        destinationWorker,
                        sourceWorker,
                        database,
                        chunk
                    });
                }
            }
        }
    }
    
    // Launch the initial batch of requests in the number which won't exceed
    // the number of the service processing threads at each worker multiplied
    // by the number of workers involved into the operation.
    size_t const maxRequestsPerWorker =
        controller()->serviceProvider()->config()->workerNumProcessingThreads();

    for (auto&& itr: _destinationWorker2tasks) {
        auto&& destinationWorker = itr.first;
        _launchNext(lock, destinationWorker, maxRequestsPerWorker);
    }

    // In case if everything is alright, and no fix-ups were needed.
    if (_requests.empty()) {
        finish(lock, ExtendedState::SUCCESS);
    }
}


void FixUpJob::_onRequestFinish(ReplicationRequest::Ptr const& request) {

    string       const database = request->database();
    string       const worker   = request->worker();
    unsigned int const chunk    = request->chunk();

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << " "
         << " database=" << database << " worker=" << worker << " chunk=" << chunk);

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    _numFinished++;
    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        _numSuccess++;
        _replicaData.replicas.push_back(request->responseData());
        _replicaData.chunks[chunk][database][worker] = request->responseData();
    } else {
        _replicaData.workers[worker]++;
    }

    // Launch a replacement request for the worker and check if this was the very
    // last request in flight and no more are ready to be lunched.
    if (0 == _launchNext(lock, worker, 1)) {
        if (_numFinished == _requests.size()) {
            finish(lock, _numSuccess == _numFinished ? ExtendedState::SUCCESS :
                                                       ExtendedState::FAILED);
        }
    }
}


size_t FixUpJob::_launchNext(util::Lock const& lock,
                             string const& destinationWorker,
                             size_t maxRequests) {

    if (maxRequests == 0) return 0;

    auto const self = shared_from_base<FixUpJob>();
    auto&& tasks = _destinationWorker2tasks[destinationWorker];

    size_t numLaunched = 0;
    for (size_t i = 0; i < maxRequests; ++i) {

        if (tasks.size() == 0) break;

        // Launch the replication request and register it for further
        // tracking (or cancellation, should the one be requested)

        ReplicationTask const& task = tasks.front();

        _requests.push_back(controller()->replicate(
            task.destinationWorker,
            task.sourceWorker,
            task.database,
            task.chunk,
            [self] (ReplicationRequest::Ptr const& ptr) {
                self->_onRequestFinish(ptr);
            },
            0,      /* priority */
            true,   /* keepTracking */
            true,   /* allowDuplicate */
            id()    /* jobId */
        ));
        tasks.pop();
        numLaunched++;
    }
    return numLaunched;
}

}}} // namespace lsst::qserv::replica
