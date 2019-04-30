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
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"

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
        _onFinish(onFinish),
        _numIterations(0),
        _numFailedLocks(0),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}


FixUpJob::~FixUpJob() {
    // Make sure all chunks locked by this job are released
    controller()->serviceProvider()->chunkLocker().release(id());
}


FixUpJobResult const& FixUpJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error(
            "FixUpJob::" + string(__func__) +
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

        bool const responded = workerInfo.second;
        if (not responded) {
            result.emplace_back("failed-worker", worker);
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

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  _numIterations=" << _numIterations);

    ++_numIterations;

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

    setState(lock, State::IN_PROGRESS);
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
            controller()->stopReplication(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                id()        /* jobId */);
    }

    _chunk2requests.clear();
    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}


void FixUpJob::_restart(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (_findAllJob or (_numLaunched != _numFinished)) {
        throw logic_error("FixUpJob::" + string(__func__) + "  not allowed in this object state");
    }
    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
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

    /////////////////////////////////////////////////////////////////
    // Proceed with the replication effort only if the precursor job
    // has succeeded.

    if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
        finish(lock, ExtendedState::FAILED);
        return;
    }

    /////////////////////////////////////////////////////////////////
    // Analyze results and prepare a replication plan to fix chunk
    // co-location for under-represented chunks

    FindAllJobResult const& replicaData = _findAllJob->getReplicaData();

    auto self = shared_from_base<FixUpJob>();

    for (auto&& chunk2workers: replicaData.isColocated) {
        unsigned int chunk = chunk2workers.first;

        for (auto&& worker2colocated: chunk2workers.second) {
            string const& destinationWorker = worker2colocated.first;
            bool   const  isColocated       = worker2colocated.second;

            if (isColocated) continue;

            // Chunk locking is mandatory. If it's not possible to do this now then
            // the job will need to make another attempt later.

            if (not controller()->serviceProvider()->chunkLocker().lock({databaseFamily(), chunk}, id())) {
                ++_numFailedLocks;
                continue;
            }

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

                        _release(chunk);
                        finish(lock, ExtendedState::FAILED);

                        break;
                    }

                    // Finally, launch the replication request and register it for further
                    // tracking (or cancellation, should the one be requested)

                    ReplicationRequest::Ptr ptr =
                        controller()->replicate(
                            destinationWorker,
                            sourceWorker,
                            database,
                            chunk,
                            [self] (ReplicationRequest::Ptr ptr) {
                                self->_onRequestFinish(ptr);
                            },
                            0,      /* priority */
                            true,   /* keepTracking */
                            true,   /* allowDuplicate */
                            id()    /* jobId */
                        );

                    _chunk2requests[chunk][destinationWorker][database] = ptr;
                    _requests.push_back(ptr);
                    _numLaunched++;
                }
            }
            if (state() == State::FINISHED) break;
        }
        if (state() == State::FINISHED) break;
    }
    if (state() != State::FINISHED) {

        // ATTENTION: We need to evaluate reasons why no single request was
        // launched while the job is still in the unfinished state and take
        // proper actions. Otherwise (if this isn't done here) the object will
        // get into a "zombie" state.

        if (not _requests.size()) {

            // Finish right away if no problematic chunks found

            if (not _numFailedLocks) {
                finish(lock, ExtendedState::SUCCESS);
            } else {

                // Some of the chunks were locked and yet, no single request was
                // lunched. Hence we should start another iteration by requesting
                // the fresh state of the chunks within the family.

                _restart(lock);
            }
        }
    }
}


void FixUpJob::_onRequestFinish(ReplicationRequest::Ptr const& request) {

    string       const database = request->database();
    string       const worker   = request->worker();
    unsigned int const chunk    = request->chunk();

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << " "
         << " database=" << database
         << " worker="   << worker
         << " chunk="    << chunk);

    if (state() == State::FINISHED)  {
        _release(chunk);
        return;
    }

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) {
        _release(chunk);
        return;
    }

    // Update counters and object state if needed.
    _numFinished++;
    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        _numSuccess++;
        _replicaData.replicas.push_back(request->responseData());
        _replicaData.chunks[chunk][database][worker] = request->responseData();
        _replicaData.workers[worker] = true;
    } else {
        _replicaData.workers[worker] = false;
    }

    // Make sure the chunk is released if this was the last
    // request in its scope.

    _chunk2requests.at(chunk).at(worker).erase(database);
    if (_chunk2requests.at(chunk).at(worker).empty()) {
        _chunk2requests.at(chunk).erase(worker);
        if (_chunk2requests.at(chunk).empty()) {
            _chunk2requests.erase(chunk);
            _release(chunk);
        }
    }

    // Evaluate the status of on-going operations to see if the job
    // has finished.

    if (_numFinished == _numLaunched) {
        if (_numSuccess == _numLaunched) {
            if (_numFailedLocks) {
                
                // Make another iteration (and another one, etc. as many as needed)
                // before it succeeds or fails.

                _restart(lock);
                return;

            } else {
                finish(lock,
                       ExtendedState::SUCCESS);
            }
        } else {
            finish(lock,
                   ExtendedState::FAILED);
        }
    }
}


void FixUpJob::_release(unsigned int chunk) {

    // THREAD-SAFETY NOTE: This method is thread-agnostic because it's trading
    // a static context of the request with an external service which is guaranteed
    // to be thread-safe.

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  chunk=" << chunk);

    Chunk chunkObj {databaseFamily(), chunk};
    controller()->serviceProvider()->chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica
