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
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.PurgeJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& PurgeJob::defaultOptions() {
    static Job::Options const options{
        -1,     /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}


string PurgeJob::typeName() { return "PurgeJob"; }


PurgeJob::Ptr PurgeJob::create(
                        string const& databaseFamily,
                        unsigned int numReplicas,
                        Controller::Ptr const& controller,
                        string const& parentJobId,
                        CallbackType const& onFinish,
                        Job::Options const& options) {
    return PurgeJob::Ptr(
        new PurgeJob(databaseFamily,
                     numReplicas,
                     controller,
                     parentJobId,
                     onFinish,
                     options));
}


PurgeJob::PurgeJob(string const& databaseFamily,
                   unsigned int numReplicas,
                   Controller::Ptr const& controller,
                   string const& parentJobId,
                   CallbackType const& onFinish,
                   Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "PURGE",
            options),
        _databaseFamily(databaseFamily),
        _numReplicas(numReplicas ?
                     numReplicas :
                     controller->serviceProvider()->config()->replicationLevel(databaseFamily)),
        _onFinish(onFinish),
        _numIterations(0),
        _numFailedLocks(0),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess (0) {

    if (not _numReplicas) {
        throw invalid_argument(
                "PurgeJob::" + string(__func__) + "  0 replicas is not allowed");
    }
}


PurgeJob::~PurgeJob() {
    // Make sure all chunks locked by this job are released
    controller()->serviceProvider()->chunkLocker().release(id());
}


PurgeJobResult const& PurgeJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error (
            "PurgeJob::" + string(__func__) +
            "  the method can't be called while the job hasn't finished");
}


list<pair<string,string>> PurgeJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("num_replicas",    to_string(numReplicas()));
    return result;
}


list<pair<string,string>> PurgeJob::persistentLogData() const {

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
    //   deleted-chunks:
    //     the total number of chunks deleted from the workers as a result
    //     of the operation

    map<string,
        map<string,
            size_t>> workerCategoryCounter;

    for (auto&& info: replicaData.replicas) {
        workerCategoryCounter[info.worker()]["deleted-chunks"]++;
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


void PurgeJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<PurgeJob>();

    bool const saveReplicInfo = true;           // always save the replica info in a database because
                                                // the algorithm depends on it.
    bool const allWorkers = false;              // only consider enabled workers
    _findAllJob = FindAllJob::create(
        _databaseFamily,
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


void PurgeJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    for (auto&& ptr: _jobs) {
        if (ptr->state() != Job::State::FINISHED) {
            ptr->cancel();
        }
    }
    _chunk2jobs.clear();
    _jobs.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}


void PurgeJob::_restart(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (_findAllJob or (_numLaunched != _numFinished)) {
        throw logic_error("PurgeJob::" + string(__func__) + "  not allowed in this object state");
    }
    _jobs.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
}


void PurgeJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<PurgeJob>(lock, _onFinish);
}


void PurgeJob::_onPrecursorJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    ////////////////////////////////////////////////////////////////
    // Only proceed with the replication effort if the precursor job
    // has succeeded.

    if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
        finish(lock, ExtendedState::FAILED);
        return;
    }

    /////////////////////////////////////////////////////////////////
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
    //
    map<unsigned int,int> chunk2numReplicas2delete;

    for (auto&& chunk2workers: replicaData.isGood) {
        unsigned int const  chunk    = chunk2workers.first;
        auto         const& replicas = chunk2workers.second;

        // skip the special chunk which must be present on all workers
        if (chunk == replica::overflowChunkNumber) continue;

        size_t const numReplicasFound = replicas.size();
        if (numReplicasFound > numReplicas()) {
            LOGS(_log, LOG_LVL_DEBUG, context() << __func__
                 << "  chunk="            << chunk
                 << ", numReplicasFound=" << numReplicasFound
                 << ", numReplicas()="    << numReplicas());
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

    for (auto chunk: replicaData.chunks.chunkNumbers()) {
        auto chunkMap = replicaData.chunks.chunk(chunk);

        for (auto&& database: chunkMap.databaseNames()) {
            auto databaseMap = chunkMap.database(database);

            for (auto&& worker: databaseMap.workerNames()) {
                worker2occupancy[worker]++;
            }
        }
    }
    for (auto&& entry: worker2occupancy) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__
             << "  worker=" << entry.first
             << ", occupancy=" << entry.second);
    }

    /////////////////////////////////////////////////////////////////////
    // Check which chunks are over-represented. Then find a least loaded
    // worker and launch a replica removal jobs.

    auto self = shared_from_base<PurgeJob>();

    for (auto&& chunk2replicas: chunk2numReplicas2delete) {

        unsigned int const chunk              = chunk2replicas.first;
        int                numReplicas2delete = chunk2replicas.second;

        LOGS(_log, LOG_LVL_DEBUG, context() << __func__
             << "  chunk=" << chunk
             << ", numReplicas2delete=" << numReplicas2delete);

        // Chunk locking is mandatory. If it's not possible to do this now then
        // the job will need to make another attempt later.

        if (not controller()->serviceProvider()->chunkLocker().lock({_databaseFamily, chunk}, id())) {
            ++_numFailedLocks;
            LOGS(_log, LOG_LVL_DEBUG, context() << __func__
                 << "  chunk=" << chunk
                 << ", _numFailedLocks=" << _numFailedLocks);
            continue;
        }

        // This list of workers will be reduced as the replica will get deleted

        list<string> goodWorkersOfThisChunk;
        for (auto&& entry: replicaData.isGood.at(chunk)) {
            string const& worker = entry.first;
            goodWorkersOfThisChunk.push_back(worker);
            LOGS(_log, LOG_LVL_DEBUG, context() << __func__
                 << "  chunk=" << chunk
                 << ", goodWorkersOfThisChunk : worker=" << worker);
        }
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__
             << "  chunk=" << chunk
             << ", goodWorkersOfThisChunk.size()=" << goodWorkersOfThisChunk.size());

        // Begin shaving extra 'good' replicas of the chunk

        for (int i = 0; i < numReplicas2delete; ++i) {

            // Find the most populated worker among the good ones of this chunk,
            // which are still available.

            size_t maxNumChunks = 0;    // will get updated within the next loop
            string targetWorker;        // will be set to the best worker when the loop is over

            for (auto&& worker: goodWorkersOfThisChunk) {
                if (targetWorker.empty() or (worker2occupancy[worker] > maxNumChunks)) {
                    maxNumChunks = worker2occupancy[worker];
                    targetWorker = worker;
                }
            }
            if (targetWorker.empty() or not maxNumChunks) {

                LOGS(_log, LOG_LVL_ERROR, context() << __func__
                     << "  failed to find a target worker for chunk: " << chunk);

                finish(lock, ExtendedState::FAILED);
                break;
            }

            // Remove the select worker from the list, so that the next iteration (if the one
            // will happen) will be not considering this worker for deletion.

            goodWorkersOfThisChunk.remove(targetWorker);

            // Finally, launch and register for further tracking deletion
            // a job which will affect all participating databases

            auto ptr = DeleteReplicaJob::create(
                databaseFamily(),
                chunk,
                targetWorker,
                controller(),
                id(),
                [self] (DeleteReplicaJob::Ptr const& job) {
                    self->_onDeleteJobFinish(job);
                },
                options(lock)   // inherit from the current job
            );
            ptr->start();

            _chunk2jobs[chunk][targetWorker] = ptr;
            _jobs.push_back(ptr);

            _numLaunched++;

            // Reduce the worker occupancy count by the number of databases participating
            // in the replica of the chunk, so that it will be taken into
            // consideration when creating next replicas.

            worker2occupancy[targetWorker] -= replicaData.databases.at(chunk).size();
        }
        if (state() == State::FINISHED) break;
    }
    if (state() != State::FINISHED) {

        // ATTENTION: if the job submission algorithm didn't launch any
        // child jobs while leaving this object in the unfinished state
        // then we must evaluate reasons and take proper actions. Otherwise
        // the object will get into a 'zombie' state.

        if (not _jobs.size()) {
            if (not _numFailedLocks) {

                // Finish right away if no problematic chunks found
                finish(lock, ExtendedState::SUCCESS);

            } else {

                // Some of the chunks were locked and yet, no single job was
                // lunched. Hence we should start another iteration by requesting
                // the fresh state of the chunks within the family.

                _restart(lock);
                return;
            }
        }
    }
}


void PurgeJob::_onDeleteJobFinish(DeleteReplicaJob::Ptr const& job) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__
         << "  databaseFamily=" << job->databaseFamily()
         << "  worker="         << job->worker()
         << "  chunk="          << job->chunk());

    if (state() == State::FINISHED) {
        _release(job->chunk());
        return;
    }

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) {
        _release(job->chunk());
        return;
    }

    // Update counters and merge results of the finished job into the current
    // job's stats if the replica deletion has been a success.

    _numFinished++;

    if (job->extendedState() == Job::ExtendedState::SUCCESS) {

        _numSuccess++;

        DeleteReplicaJobResult const& jobReplicaData = job->getReplicaData();

        // Append replicas info by the end of the list

        _replicaData.replicas.insert(_replicaData.replicas.end(),
                                     jobReplicaData.replicas.begin(),
                                     jobReplicaData.replicas.end());

        // Merge the replica info into the dictionary

        for (auto&& databaseEntry: jobReplicaData.chunks.at(job->chunk())) {
            string const& database = databaseEntry.first;
            _replicaData.chunks[job->chunk()][database][job->worker()] =
                jobReplicaData.chunks.at(job->chunk()).at(database).at(job->worker());
        }
        _replicaData.workers[job->worker()] = true;
    } else {
        _replicaData.workers[job->worker()] = false;
    }

    // Make sure the chunk is released if this was the last
    // job in its scope.

    _chunk2jobs.at(job->chunk()).erase(job->worker());
    if (_chunk2jobs.at(job->chunk()).empty()) {
        _chunk2jobs.erase(job->chunk());
        _release(job->chunk());
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
                finish(lock, ExtendedState::SUCCESS);
            }
        } else {
            finish(lock, ExtendedState::FAILED);
        }
    }
}


void PurgeJob::_release(unsigned int chunk) {

    // THREAD-SAFETY NOTE: This method is thread-agnostic because it's trading
    // a static context of the request with an external service which is guaranteed
    // to be thread-safe.

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  chunk=" << chunk);

    Chunk chunkObj {databaseFamily(), chunk};
    controller()->serviceProvider()->chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica
