/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

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

PurgeJob::Ptr PurgeJob::create(
                        std::string const& databaseFamily,
                        unsigned int numReplicas,
                        Controller::Ptr const& controller,
                        std::string const& parentJobId,
                        CallbackType onFinish,
                        Job::Options const& options) {
    return PurgeJob::Ptr(
        new PurgeJob(databaseFamily,
                     numReplicas,
                     controller,
                     parentJobId,
                     onFinish,
                     options));
}

PurgeJob::PurgeJob(std::string const& databaseFamily,
                   unsigned int numReplicas,
                   Controller::Ptr const& controller,
                   std::string const& parentJobId,
                   CallbackType onFinish,
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
        throw std::invalid_argument(
                        "PurgeJob::PurgeJob ()  0 is not allowed for the number of replias");
    }
}

PurgeJob::~PurgeJob() {
    // Make sure all chuks locked by this job are released
    controller()->serviceProvider()->chunkLocker().release(id());
}

PurgeJobResult const& PurgeJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (state() == State::FINISHED) return _replicaData;

    throw std::logic_error (
        "PurgeJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::string PurgeJob::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily(),
                              numReplicas());
}

void PurgeJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<PurgeJob>();

    bool const saveReplicInfo = true;           // always save the replica info in a database because
                                                // the algorithm depends on it.
    _findAllJob = FindAllJob::create(
        _databaseFamily,
        saveReplicInfo,
        controller(),
        id(),
        [self] (FindAllJob::Ptr job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();

    setState(lock, State::IN_PROGRESS);
}

void PurgeJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

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

void PurgeJob::restart(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    if (_findAllJob or (_numLaunched != _numFinished)) {
        throw std::logic_error ("PurgeJob::restart ()  not allowed in this object state");
    }
    _jobs.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
}

void PurgeJob::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish) {
        _onFinish(shared_from_base<PurgeJob>());
    }
}

void PurgeJob::onPrecursorJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onPrecursorJobFinish");

    if (state() == State::FINISHED) return;

    ////////////////////////////////////////////////////////////////
    // Only proceed with the replication effort if the precursor job
    // has succeeded.

    if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
        finish(lock, ExtendedState::FAILED);
        return;
    }

    /////////////////////////////////////////////////////////////////
    // Analyse results and prepare a deletion plan to remove extra
    // replocas for over-represented chunks
    //
    // IMPORTANT:
    //
    // - chunks which were found locked by some other job will not be deleted
    //
    // - when deciding on a number of replicas to be deleted the algorithm
    //   will only consider 'good' chunks (the ones which meet the 'colocation'
    //   requirement and which has good chunks only.
    //
    // - at a presense of more than one candidate for deletion, a worker with
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
    std::map<unsigned int,int> chunk2numReplicas2delete;

    for (auto&& chunk2workers: replicaData.isGood) {
        unsigned int const  chunk    = chunk2workers.first;
        auto         const& replicas = chunk2workers.second;

        // skip the special chunk which must be present on all workers
        if (chunk == 1234567890) continue;

        size_t const numReplicasFound = replicas.size();
        if (numReplicasFound > numReplicas()) {
            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
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

    std::map<std::string, size_t> worker2occupancy;

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
        LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
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

        LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
             << "  chunk=" << chunk
             << ", numReplicas2delete=" << numReplicas2delete);

        // Chunk locking is mandatory. If it's not possible to do this now then
        // the job will need to make another attempt later.

        if (not controller()->serviceProvider()->chunkLocker().lock({_databaseFamily, chunk}, id())) {
            ++_numFailedLocks;
            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
                 << "  chunk=" << chunk
                 << ", _numFailedLocks=" << _numFailedLocks);
            continue;
        }

        // This list of workers will be reduced as the replica will get deleted

        std::list<std::string> goodWorkersOfThisChunk;
        for (auto&& entry: replicaData.isGood.at(chunk)) {
            std::string const& worker = entry.first;
            goodWorkersOfThisChunk.push_back(worker);
            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
                 << "  chunk=" << chunk
                 << ", goodWorkersOfThisChunk : worker=" << worker);
        }
        LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
             << "  chunk=" << chunk
             << ", goodWorkersOfThisChunk.size()=" << goodWorkersOfThisChunk.size());

        // Begin shaving extra 'good' replicas of the chunk

        for (int i = 0; i < numReplicas2delete; ++i) {

            // Find the most populated worker among the good ones of this chunk,
            // which are still available.

            size_t      maxNumChunks = 0;   // will get updated within the next loop
            std::string targetWorker;       // will be set to the best worker inwhen the loop is over

            for (auto&& worker: goodWorkersOfThisChunk) {
                if (targetWorker.empty() or (worker2occupancy[worker] > maxNumChunks)) {
                    maxNumChunks = worker2occupancy[worker];
                    targetWorker = worker;
                }
            }
            if (targetWorker.empty() or not maxNumChunks) {

                LOGS(_log, LOG_LVL_ERROR, context() << "onPrecursorJobFinish  "
                     << "failed to find a target worker for chunk: " << chunk);

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
                    self->onDeleteJobFinish(job);
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

                // Some of the chuks were locked and yet, no sigle job was
                // lunched. Hence we should start another iteration by requesting
                // the fresh state of the chunks within the family.

                restart(lock);
                return;
            }
        }
    }
}

void PurgeJob::onDeleteJobFinish(DeleteReplicaJob::Ptr const& job) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onDeleteJobFinish"
         << "  databaseFamily=" << job->databaseFamily()
         << "  worker="         << job->worker()
         << "  chunk="          << job->chunk());

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (state() == State::FINISHED) {
        release(job->chunk());
        return;
    }

    util::Lock lock(_mtx, context() + "onDeleteJobFinish");

    if (state() == State::FINISHED) {
        release(job->chunk());
        return;
    }

    // Update counters and merge results of the finished job into the current
    // job's stats if the replica deletion has been a success.

    _numFinished++;

    if (job->extendedState() == Job::ExtendedState::SUCCESS) {

        _numSuccess++;

        DeleteReplicaJobResult const& jobReplicaData = job->getReplicaData();

        // Append replicas infos by the end of the list

        _replicaData.replicas.insert(_replicaData.replicas.end(),
                                     jobReplicaData.replicas.begin(),
                                     jobReplicaData.replicas.end());

        // Merge the replica infos into the dictionary

        for (auto&& databaseEntry: jobReplicaData.chunks.at(job->chunk())) {
            std::string const& database = databaseEntry.first;
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
        release(job->chunk());
    }

    // Evaluate the status of on-going operations to see if the job
    // has finished.

    if (_numFinished == _numLaunched) {
        if (_numSuccess == _numLaunched) {
            if (_numFailedLocks) {

                // Make another iteration (and another one, etc. as many as needed)
                // before it succeeds or fails.

                restart(lock);
                return;

            } else {
                finish(lock, ExtendedState::SUCCESS);
            }
        } else {
            finish(lock, ExtendedState::FAILED);
        }
    }
}

void PurgeJob::release(unsigned int chunk) {

    // THREAD-SAFETY NOTE: This method is thread-agnostic because it's trading
    // a static context of the request with an external service which is guaranteed
    // to be thread-safe.

    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);

    Chunk chunkObj {databaseFamily(), chunk};
    controller()->serviceProvider()->chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica
