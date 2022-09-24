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
#include "replica/PurgeJob.h"

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.PurgeJob");

}  // namespace

namespace lsst::qserv::replica {

string PurgeJob::typeName() { return "PurgeJob"; }

PurgeJob::Ptr PurgeJob::create(string const& databaseFamily, unsigned int numReplicas,
                               Controller::Ptr const& controller, string const& parentJobId,
                               CallbackType const& onFinish, int priority) {
    return PurgeJob::Ptr(
            new PurgeJob(databaseFamily, numReplicas, controller, parentJobId, onFinish, priority));
}

PurgeJob::PurgeJob(string const& databaseFamily, unsigned int numReplicas, Controller::Ptr const& controller,
                   string const& parentJobId, CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "PURGE", priority),
          _databaseFamily(databaseFamily),
          _numReplicas(controller->serviceProvider()->config()->effectiveReplicationLevel(databaseFamily,
                                                                                          numReplicas)),
          _onFinish(onFinish) {
    if (0 == _numReplicas) {
        throw invalid_argument(typeName() + "::" + string(__func__) + "  0 replicas is not allowed");
    }
}

PurgeJobResult const& PurgeJob::getReplicaData() const {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error(typeName() + "::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

list<pair<string, string>> PurgeJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("num_replicas", to_string(numReplicas()));
    return result;
}

list<pair<string, string>> PurgeJob::persistentLogData() const {
    list<pair<string, string>> result;

    auto&& replicaData = getReplicaData();

    // Report workers failed to respond to the jobs

    for (auto&& workerInfo : replicaData.workers) {
        auto&& worker = workerInfo.first;
        auto const numFailedJobs = workerInfo.second;
        if (numFailedJobs != 0) {
            result.emplace_back("failed-worker",
                                "worker=" + worker + " num-failed-jobs=" + to_string(numFailedJobs));
        }
    }

    // Per-worker counters for the following categories:
    //
    //   deleted-chunks:
    //     the total number of chunks deleted from the workers as a result
    //     of the operation

    map<string, map<string, size_t>> workerCategoryCounter;

    for (auto&& info : replicaData.replicas) {
        workerCategoryCounter[info.worker()]["deleted-chunks"]++;
    }
    for (auto&& workerItr : workerCategoryCounter) {
        auto&& worker = workerItr.first;
        string val = "worker=" + worker;

        for (auto&& categoryItr : workerItr.second) {
            auto&& category = categoryItr.first;
            size_t const counter = categoryItr.second;
            val += " " + category + "=" + to_string(counter);
        }
        result.emplace_back("worker-stats", val);
    }
    return result;
}

void PurgeJob::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<PurgeJob>();

    bool const saveReplicInfo = true;  // always save the replica info in a database because
                                       // the algorithm depends on it.
    bool const allWorkers = false;     // only consider enabled workers
    _findAllJob = FindAllJob::create(
            databaseFamily(), saveReplicInfo, allWorkers, controller(), id(),
            [self](FindAllJob::Ptr job) { self->_onPrecursorJobFinish(); }, priority());
    _findAllJob->start();
}

void PurgeJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    for (auto&& ptr : _jobs) {
        if (ptr->state() != Job::State::FINISHED) {
            ptr->cancel();
        }
    }
    _targetWorker2tasks.clear();
    _jobs.clear();
}

void PurgeJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<PurgeJob>(lock, _onFinish);
}

void PurgeJob::_onPrecursorJobFinish() {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
        finish(lock, ExtendedState::FAILED);
        return;
    }

    // Analyze results and prepare a deletion plan to remove extra
    // replicas for over-represented chunks
    //
    // IMPORTANT:
    //
    // - chunks which were found locked by some other job will not be deleted
    //
    // - when deciding on a number of replicas to be deleted the algorithm
    //   will only consider 'good' chunks (the ones which meet the 'colocation'
    //   requirement and which has good chunks only.
    //
    // - at a presence of more than one candidate for deletion, a worker with
    //   more chunks will be chosen.
    //
    // - the statistics for the number of chunks on each worker will be
    //   updated as deletion jobs targeting the corresponding
    //   workers were issued.
    //
    // ATTENTION: the read-only workers will not be considered by
    //            the algorithm. Those workers are used by different kinds
    //            of jobs.

    FindAllJobResult const& replicaData = _findAllJob->getReplicaData();

    // The number of replicas to be deleted for eligible chunks
    map<unsigned int, int> chunk2numReplicas2delete;

    for (auto&& chunk2workers : replicaData.isGood) {
        unsigned int const chunk = chunk2workers.first;
        auto const& replicas = chunk2workers.second;

        // skip the special chunk which must be present on all workers
        if (chunk == replica::overflowChunkNumber) continue;

        size_t const numReplicasFound = replicas.size();
        if (numReplicasFound > numReplicas()) {
            LOGS(_log, LOG_LVL_DEBUG,
                 context() << __func__ << "  chunk=" << chunk << ", numReplicasFound=" << numReplicasFound
                           << ", numReplicas()=" << numReplicas());
            chunk2numReplicas2delete[chunk] = numReplicasFound - numReplicas();
        }
    }

    // The 'occupancy' map or workers which will be used by the replica
    // removal algorithm later. The map is initialized below is based on
    // results reported by the precursor job and it will also be dynamically
    // updated by the algorithm as new replica removal jobs for workers will
    // be issued.
    //
    // Note, this map includes chunks in any state.

    map<string, size_t> worker2occupancy;

    for (auto chunk : replicaData.chunks.chunkNumbers()) {
        auto chunkMap = replicaData.chunks.chunk(chunk);

        for (auto&& database : chunkMap.databaseNames()) {
            auto databaseMap = chunkMap.database(database);

            for (auto&& worker : databaseMap.workerNames()) {
                worker2occupancy[worker]++;
            }
        }
    }
    for (auto&& entry : worker2occupancy) {
        LOGS(_log, LOG_LVL_DEBUG,
             context() << __func__ << "  worker=" << entry.first << ", occupancy=" << entry.second);
    }

    // Check which chunks are over-represented. Then find a least loaded
    // worker and register a replica removal task.

    for (auto&& chunk2replicas : chunk2numReplicas2delete) {
        unsigned int const chunk = chunk2replicas.first;
        int numReplicas2delete = chunk2replicas.second;

        LOGS(_log, LOG_LVL_DEBUG,
             context() << __func__ << "  chunk=" << chunk << ", numReplicas2delete=" << numReplicas2delete);

        // This list of workers will be reduced as the replica will get deleted

        list<string> goodWorkersOfThisChunk;
        for (auto&& entry : replicaData.isGood.at(chunk)) {
            string const& worker = entry.first;
            goodWorkersOfThisChunk.push_back(worker);
            LOGS(_log, LOG_LVL_DEBUG,
                 context() << __func__ << "  chunk=" << chunk
                           << ", goodWorkersOfThisChunk : worker=" << worker);
        }
        LOGS(_log, LOG_LVL_DEBUG,
             context() << __func__ << "  chunk=" << chunk
                       << ", goodWorkersOfThisChunk.size()=" << goodWorkersOfThisChunk.size());

        // Begin shaving extra 'good' replicas of the chunk

        for (int i = 0; i < numReplicas2delete; ++i) {
            // Find the most populated worker among the good ones of this chunk,
            // which are still available.

            size_t maxNumChunks = 0;  // will get updated within the next loop
            string targetWorker;      // will be set to the best worker when the loop is over

            for (auto&& worker : goodWorkersOfThisChunk) {
                if (targetWorker.empty() or (worker2occupancy[worker] > maxNumChunks)) {
                    maxNumChunks = worker2occupancy[worker];
                    targetWorker = worker;
                }
            }
            if (targetWorker.empty() or not maxNumChunks) {
                LOGS(_log, LOG_LVL_ERROR,
                     context() << __func__ << "  failed to find a target worker for chunk: " << chunk);
                finish(lock, ExtendedState::FAILED);
                return;
            }

            // Remove the select worker from the list, so that the next iteration (if the one
            // will happen) will be not considering this worker for deletion.
            goodWorkersOfThisChunk.remove(targetWorker);

            // Register the replica deletion task which will turn into
            // a job affecting all participating databases.
            _targetWorker2tasks[targetWorker].emplace(ReplicaPurgeTask{chunk, targetWorker});

            // Reduce the worker occupancy count by the number of databases participating
            // in the replica of the chunk, so that it will be taken into
            // consideration when creating next replicas.
            worker2occupancy[targetWorker] -= replicaData.databases.at(chunk).size();
        }
    }

    // Launch the initial batch of jobs in the number which won't exceed
    // the number of the service processing threads at each worker multiplied
    // by the number of workers involved into the operation.
    size_t const maxJobsPerWorker =
            controller()->serviceProvider()->config()->get<size_t>("worker", "num-svc-processing-threads");

    for (auto&& itr : _targetWorker2tasks) {
        auto&& targetWorker = itr.first;
        _launchNext(lock, targetWorker, maxJobsPerWorker);
    }

    // In case if everything is alright, and no replica removals were needed.
    if (_jobs.empty()) {
        finish(lock, ExtendedState::SUCCESS);
    }
}

void PurgeJob::_onDeleteJobFinish(DeleteReplicaJob::Ptr const& job) {
    string const worker = job->worker();
    unsigned int const chunk = job->chunk();

    LOGS(_log, LOG_LVL_TRACE,
         context() << __func__ << "  databaseFamily=" << databaseFamily() << "  worker=" << worker
                   << "  chunk=" << chunk);

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // Update counters and merge results of the finished job into the current
    // job's stats if the replica deletion has been a success.

    _numFinished++;
    if (job->extendedState() == Job::ExtendedState::SUCCESS) {
        _numSuccess++;

        DeleteReplicaJobResult const& jobReplicaData = job->getReplicaData();

        // Append replicas info by the end of the list
        _replicaData.replicas.insert(_replicaData.replicas.end(), jobReplicaData.replicas.begin(),
                                     jobReplicaData.replicas.end());

        // Merge the replica info into the dictionary
        for (auto&& databaseEntry : jobReplicaData.chunks.at(chunk)) {
            string const& database = databaseEntry.first;
            _replicaData.chunks[chunk][database][worker] =
                    jobReplicaData.chunks.at(chunk).at(database).at(worker);
        }
    } else {
        _replicaData.workers[worker]++;
    }

    // Launch a replacement job for the worker and check if this was the very
    // last job in flight and no more are ready to be lunched.
    if (0 == _launchNext(lock, worker, 1)) {
        if (_numFinished == _jobs.size()) {
            finish(lock, _numSuccess == _numFinished ? ExtendedState::SUCCESS : ExtendedState::FAILED);
        }
    }
}

size_t PurgeJob::_launchNext(util::Lock const& lock, string const& targetWorker, size_t maxJobs) {
    if (maxJobs == 0) return 0;

    auto const self = shared_from_base<PurgeJob>();
    auto&& tasks = _targetWorker2tasks[targetWorker];

    size_t numLaunched = 0;
    for (size_t i = 0; i < maxJobs; ++i) {
        if (tasks.size() == 0) break;

        // Launch the replica removal job and register it for further
        // tracking (or cancellation, should the one be requested)

        ReplicaPurgeTask const& task = tasks.front();

        auto const job = DeleteReplicaJob::create(
                databaseFamily(), task.chunk, task.targetWorker, controller(), id(),
                [self](DeleteReplicaJob::Ptr const& job) { self->_onDeleteJobFinish(job); },
                priority()  // inherit from the current job
        );
        job->start();
        _jobs.push_back(job);
        tasks.pop();
        numLaunched++;
    }
    return numLaunched;
}
}  // namespace lsst::qserv::replica
