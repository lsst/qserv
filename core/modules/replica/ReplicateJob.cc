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
#include "replica/ReplicateJob.h"

// System headers
#include <algorithm>
#include <future>
#include <set>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK(MUTEX) std::lock_guard<util::Mutex> lock(MUTEX)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ReplicateJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& ReplicateJob::defaultOptions() {
    static Job::Options const options{
        1,      /* priority */
        true,   /* exclusive */
        true    /* exclusive */
    };
    return options;
}

ReplicateJob::Ptr ReplicateJob::create(
                            std::string const& databaseFamily,
                            unsigned int numReplicas,
                            Controller::Ptr const& controller,
                            std::string const& parentJobId,
                            CallbackType onFinish,
                            Job::Options const& options) {
    return ReplicateJob::Ptr(
        new ReplicateJob(databaseFamily,
                         numReplicas,
                         controller,
                         parentJobId,
                         onFinish,
                         options));
}

ReplicateJob::ReplicateJob(std::string const& databaseFamily,
                           unsigned int numReplicas,
                           Controller::Ptr const& controller,
                           std::string const& parentJobId,
                           CallbackType onFinish,
                           Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "REPLICATE",
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
        _numSuccess(0) {
}

ReplicateJob::~ReplicateJob() {
    // Make sure all chuks locked by this job are released
    _controller->serviceProvider()->chunkLocker().release(_id);
}

ReplicateJobResult const& ReplicateJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "ReplicateJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::string ReplicateJob::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily(),
                              numReplicas());
}

void ReplicateJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<ReplicateJob>();

    _findAllJob = FindAllJob::create(
        _databaseFamily,
        _controller,
        _id,
        [self] (FindAllJob::Ptr job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void ReplicateJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    for (auto&& ptr: _jobs) {
        ptr->cancel();
    }
    _chunk2jobs.clear();
    _jobs.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void ReplicateJob::restart() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    if (_findAllJob or (_numLaunched != _numFinished)) {
        throw std::logic_error("ReplicateJob::restart()  not allowed in this object state");
    }
    _jobs.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void ReplicateJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<ReplicateJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void ReplicateJob::onPrecursorJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    LOCK(_mtx);

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) return;

    // IMPLEMENTATION NOTE: using a single-iteration loop in order to bail
    // out of it at any moment. When this happens object state will get
    // re-evaluated and a client will get notified if needed.

    do {

        //////////////////////////////////////////////////////////////////////
        // Do not proceed with the replication effort if there was any problem
        // with the precursor job.

        if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
            finish(ExtendedState::FAILED);
            break;
        }

        /////////////////////////////////////////////////////////////////
        // Analyse results and prepare a replication plan to create extra
        // replicas for under-represented chunks
        //
        // IMPORTANT:
        //
        // - chunks which were found locked by some other job will not be replicated
        //
        // - when deciding on a number of extra replicas to be created the algorithm
        //   will only consider 'good' chunks (the ones which meet the 'colocation'
        //   requirement and which has good chunks only.
        //
        // - the algorithm will create only 'good' chunks
        //
        // - when looking for workers on which sources of the replicated chunks
        //   are found any worker which has a 'complete' chunk will be assumed.
        //
        // - when deciding on a destination worker for a new replica of a chunk
        //   the following rules will apply:
        //     a) workers which found as 'FAILED' by the precursor job will be excluded
        //     b) workers which already have the chunk replica in any state ('good',
        //        'incomplete', etc.) will be excluded
        //     c) a worker which has a fewer number of chunks will be assumed.
        //     d) the statistics for the number of chunks on each worker will be
        //        updated as new replica creation jobs targeting the corresponding
        //        workers were issued.

        FindAllJobResult const& replicaData = _findAllJob->getReplicaData();

        // The number of replicas to be created for eligible chunks
        //
        std::map<unsigned int,int> chunk2numReplicas2create;

        for (auto&& chunk2workers: replicaData.isGood) {
            unsigned int const  chunk    = chunk2workers.first;
            auto         const& replicas = chunk2workers.second;

            size_t const numReplicas = replicas.size();
            if (numReplicas < _numReplicas) {
                chunk2numReplicas2create[chunk] = _numReplicas - numReplicas;
            }
        }

        // The 'occupancy' map or workers which will be used by the replica
        // placement algorithm later. The map is initialized below is based on
        // results reported by the precursor job and it will also be dynamically
        // updated by the algorithm as new replication requests for workers will
        // be issued.
        //
        // NOTE: this map includes chunks in 'good' standing only

        std::map<std::string, size_t> worker2occupancy;

        for (auto&& chunkEntry: replicaData.isGood) {
            for (auto&& workerEntry: chunkEntry.second) {
                auto worker = workerEntry.first;
                bool const isGood = workerEntry.second;
                if (isGood) {
                    if (not worker2occupancy.count(worker)) { worker2occupancy[worker] = 0; }
                    worker2occupancy[worker]++;
                }
            }
        }

        // The 'black list' of workers to be avoided as new replica destinations
        // for specific chunks because they already have a replica (regardless of
        // its status) of that chunk for any database of the family
        //
        // This is done in order to avoid conflicts when attempting to place new chunk
        // replicas on that node.
        //
        std::map<std::string, std::set<unsigned int>> worker2chunks;

        for (auto chunk: replicaData.chunks.chunkNumbers()) {
            auto chunkMap = replicaData.chunks.chunk(chunk);

            for (auto&& database: chunkMap.databaseNames()) {
                auto databaseMap = chunkMap.database(database);

                for (auto&& worker: databaseMap.workerNames()) {
                    worker2chunks[worker].insert(chunk);
                }
            }
        }

        // The 'white list of workers which haven't been reported as FAILED
        // by the precursor job. These workers will be considered as destinations
        // for the new replicas.

        std::vector<std::string> workers;
        for (auto&& worker: _controller->serviceProvider()->config()->workers()) {
            if (replicaData.workers.at(worker)) {
                workers.push_back(worker);
            }
        }
        if (not workers.size()) {

            LOGS(_log, LOG_LVL_ERROR, context()
                 << "onPrecursorJobFinish  not workers are available for new replicas");

            finish(ExtendedState::FAILED);
            break;
        }

        /////////////////////////////////////////////////////////////////////
        // Check which chunks are under-represented. Then find a least loaded
        // worker and launch a replica creation job.

        auto self = shared_from_base<ReplicateJob>();

        for (auto&& chunk2replicas: chunk2numReplicas2create) {

            unsigned int const chunk              = chunk2replicas.first;
            int          const numReplicas2create = chunk2replicas.second;

            // Chunk locking is mandatory. If it's not possible to do this now then
            // the job will need to make another attempt later.

            Chunk const chunkObj{_databaseFamily, chunk};
            if (not _controller->serviceProvider()->chunkLocker().lock(chunkObj, _id)) {
                ++_numFailedLocks;
                continue;
            }

            // Find the first available source worker which has a 'good'
            // chunk

            std::string sourceWorker;
            for (auto&& workerEntry: replicaData.isGood.at(chunk)) {
                std::string const& worker = workerEntry.first;
                bool const isGood = workerEntry.second;
                if (isGood) {
                    sourceWorker = worker;
                    break;
                }
            }
            if (sourceWorker.empty()) {
                LOGS(_log, LOG_LVL_ERROR, context()
                     << "onPrecursorJobFinish  no suitable soure worker found for chunk: "
                     << chunk);

                release(chunk);
                finish(ExtendedState::FAILED);
                break;
            }

            // Iterate over the number of replicas to be created and create
            // a new one on each step.
            //
            // NOTE: the worker ocupancy map worker2occupancy will get
            // updated on ech successful iteration of the loop, so that
            // the corresponidng destination worker will also be accounted
            // for when deciding on a placement of other replicas.

            for (int i=0; i < numReplicas2create; ++i) {

                // Find a suitable destination worker based on the worker load
                // and chunk-specific exclusions.

                std::string destinationWorker;

                size_t minNumChunks = (size_t) -1;  // this will be decreased witin the loop to find
                                                    // the absolute minimum among the eligible workers
                for (const auto& worker: workers) {

                    // Skip if this worker already has any replica of the chunk
                    if (worker2chunks[worker].count(chunk)) {
                        continue;
                    }

                    // Evaluate the occupancy
                    if (destinationWorker.empty() or worker2occupancy[worker] < minNumChunks) {
                        destinationWorker = worker;
                        minNumChunks = worker2occupancy[worker];
                    }
                }
                if (destinationWorker.empty()) {
                    LOGS(_log, LOG_LVL_ERROR, context()
                         << "onPrecursorJobFinish  no suitable destination worker found for chunk: "
                         << chunk);

                    release(chunk);
                    finish(ExtendedState::FAILED);
                    break;
                }

                // Finally, launch and register for further tracking a replica
                // creation job

                auto ptr = CreateReplicaJob::create(
                    _databaseFamily,
                    chunk,
                    sourceWorker,
                    destinationWorker,
                    _controller,
                    _id,
                    [self] (CreateReplicaJob::Ptr const& job) {
                        self->onCreateJobFinish(job);
                    },
                    options()   // inherit from the current job
                );
                ptr->start();

                _chunk2jobs[chunk][destinationWorker] = ptr;
                _jobs.push_back(ptr);

                _numLaunched++;

                // Bump the worker occupancy, so that it will be taken into
                // consideration when deciding on destinations of other replicas.

                worker2occupancy[destinationWorker]++;
            }
            if (state() == State::FINISHED) break;
        }

        if (state() != State::FINISHED) {

            // ATTENTION: this condition needs to be evaluated to prevent
            // getting into the 'zombie' state.

            if (not _jobs.size()) {

                // Finish right away if no problematic chunks found
                if (not _numFailedLocks) {
                    finish(ExtendedState::SUCCESS);
                    break;
                } else {

                    // Some of the chuks were locked and yet, no single replica creation
                    // job was lunched. Hence we should start another iteration by requesting
                    // the fresh state of the chunks within the family.
                    restart();
                    return;
                }
            }
        }

    } while (false);

    if (_state == State::FINISHED) notify();
}

void ReplicateJob::onCreateJobFinish(CreateReplicaJob::Ptr const& job) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onCreateJobFinish"
         << "  chunk="             << job->chunk()
         << "  databaseFamily="    << job->databaseFamily()
         << "  sourceWorker="      << job->sourceWorker()
         << "  destinationWorker=" << job->destinationWorker());

    LOCK(_mtx);

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) {
        release(job->chunk());
        return;
    }

    // Make sure the chunk is released regardless of the completion
    // status of the replica creation job.

    _chunk2jobs.at(job->chunk()).erase(job->destinationWorker());
    if (_chunk2jobs.at(job->chunk()).empty()) {
        _chunk2jobs.erase(job->chunk());
        release(job->chunk());
    }

    // Update counters and object state if needed

    _numFinished++;
    if (job->extendedState() == Job::ExtendedState::SUCCESS) {
        _numSuccess++;
        auto replicaData = job->getReplicaData();
        for (auto&& replica: replicaData.replicas) {
            _replicaData.replicas.push_back(replica);
        }
        for (auto&& chunkEntry: replicaData.chunks) {
            auto chunk = chunkEntry.first;

            for (auto&& databaseEntry: chunkEntry.second) {
                auto database = databaseEntry.first;

                for (auto&& workerEntry: databaseEntry.second) {
                    auto worker  = workerEntry.first;
                    auto replica = workerEntry.second;

                    _replicaData.chunks[chunk][database][worker] = replica;
                }
            }
        }
        _replicaData.workers[job->destinationWorker()] = true;
    } else {
        _replicaData.workers[job->destinationWorker()] = false;
    }

    // Evaluate the status of on-going operations to see if the job
    // has finished.
    //
    if (_numFinished == _numLaunched) {
        if (_numSuccess == _numLaunched) {
            if (_numFailedLocks) {
                // Make another iteration (and another one, etc. as many as needed)
                // before it succeeds or fails.
                restart();
                return;
            } else {
                finish(ExtendedState::SUCCESS);
            }
        } else {
            finish(ExtendedState::FAILED);
        }
    }

    if (_state == State::FINISHED) notify();
}

void ReplicateJob::release(unsigned int chunk) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);

    Chunk chunkObj {_databaseFamily, chunk};
    _controller->serviceProvider()->chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica
