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
#include "replica/RebalanceJob.h"

// System headers
#include <algorithm>
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK(MUTEX) std::lock_guard<util::Mutex> lock(MUTEX)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.RebalanceJob");

template <class COLLECTION>
void countJobStates(size_t& numLaunched,
                    size_t& numFinished,
                    size_t&  numSuccess,
                    COLLECTION const& collection) {

    using namespace lsst::qserv::replica;

    numLaunched = collection.size();
    numFinished = 0;
    numSuccess  = 0;

    for (auto&& ptr: collection) {
        if (ptr->state() == Job::State::FINISHED) {
            numFinished++;
            if (ptr->extendedState() == Job::ExtendedState::SUCCESS) {
                numSuccess++;
            }
        }
    }
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& RebalanceJob::defaultOptions() {
    static Job::Options const options{
        -2,     /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}

RebalanceJob::Ptr RebalanceJob::create(
                            std::string const& databaseFamily,
                            bool estimateOnly,
                            Controller::Ptr const& controller,
                            std::string const& parentJobId,
                            CallbackType onFinish,
                            Job::Options const& options) {
    return RebalanceJob::Ptr(
        new RebalanceJob(databaseFamily,
                         estimateOnly,
                         controller,
                         parentJobId,
                         onFinish,
                         options));
}

RebalanceJob::RebalanceJob(std::string const& databaseFamily,
                           bool estimateOnly,
                           Controller::Ptr const& controller,
                           std::string const& parentJobId,
                           CallbackType onFinish,
                           Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "REBALANCE",
            options),
        _databaseFamily(databaseFamily),
        _estimateOnly(estimateOnly),
        _onFinish(onFinish) {
}

RebalanceJob::~RebalanceJob() {
    // Make sure all chuks locked by this job are released
    _controller->serviceProvider()->chunkLocker().release(_id);
}

RebalanceJobResult const& RebalanceJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED)  return _replicaData;

    throw std::logic_error(
        "RebalanceJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::string RebalanceJob::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily());
}

void RebalanceJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  numIterations=" << _replicaData.numIterations);

    _replicaData.numIterations++;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<RebalanceJob>();

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

void RebalanceJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    for (auto&& ptr: _moveReplicaJobs) {
        ptr->cancel();
    }
    _moveReplicaJobs.clear();
}

void RebalanceJob::restart() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    size_t numLaunched;
    size_t numFinished;
    size_t numSuccess;

    ::countJobStates(numLaunched,
                     numFinished,
                     numSuccess,
                     _moveReplicaJobs);

    if ((_findAllJob and (_findAllJob->state() != State::FINISHED)) or
        (numLaunched != numFinished)) {

        throw std::logic_error(
                        "RebalanceJob::restart ()  not allowed in this object state");
    }

    _moveReplicaJobs.clear();

    // Take a fresh snapshot of chunk disposition within the cluster
    // to see what else can be rebalanced. Note that this is going to be
    // a lengthy operation allowing other on-going activities locking chunks
    // to be finished before the current job will get another chance
    // to rebalance (if needed).

    auto self = shared_from_base<RebalanceJob>();

    _findAllJob = FindAllJob::create(
        _databaseFamily,
        _controller,
        _id,
        [self] (FindAllJob::Ptr job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();
}

void RebalanceJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<RebalanceJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void RebalanceJob::onPrecursorJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    LOCK(_mtx);

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) return;

    // IMPLEMENTATION NOTE: using a single-iteration loop in order to bail
    // out of it at any moment. When this happens object state will get
    // re-evaluated and a client will get notified if needed.

    do {

        ////////////////////////////////////////////////////////////////////
        // Do not proceed with the replication effort unless running the job
        // under relaxed condition.

        if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {

            LOGS(_log, LOG_LVL_ERROR, context()
                 << "onPrecursorJobFinish  failed due to the precurson job failure");

            finish(ExtendedState::FAILED);
            break;
        }

        ///////////////////////////////////////////////
        // Analyse results and prepare a rebalance plan

        FindAllJobResult const& replicaData = _findAllJob->getReplicaData();

        // Compute key parameters of the algorithm by counting the number of 'useful'
        // workers and 'good' chunks.

        _replicaData.totalWorkers    = 0;     // not counting workers which failed to report chunks
        _replicaData.totalGoodChunks = 0;     // good chunks reported by the precursor job

        for (auto&& entry: replicaData.workers) {
            bool const  reported = entry.second;
            if (reported) {
                _replicaData.totalWorkers++;
            }
        }
        for (auto&& chunkEntry: replicaData.isGood) {
            for (auto&& workerEntry: chunkEntry.second) {
                bool const  isGood = workerEntry.second;
                if (isGood) {
                    _replicaData.totalGoodChunks++;
                }
            }
        }
        if (not _replicaData.totalWorkers or not _replicaData.totalGoodChunks) {

            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish:  "
                 << "no eligible 'good' chunks found");

            finish(ExtendedState::SUCCESS);
            break;
        }

        _replicaData.avgChunks = _replicaData.totalGoodChunks / _replicaData.totalWorkers;
        if (not _replicaData.avgChunks) {

            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish:  "
                 << "the average number of 'good' chunks per worker is 0. "
                 << "This won't trigger the operation");

            finish(ExtendedState::SUCCESS);
            break;
        }

        // This map is prepopulated with all workers which have responded to the FindAll
        // requests. It's meant to tell the planner which workers to avoid when looking for
        // a new home for a chunk to be moved elswhere from an overpopulated
        // worker.
        //
        // IMPORTANT: the map be updated by the planner as it will be deciding
        // on new destinations for the moved chunks.

        std::map<std::string,                   // worker
                 std::map<unsigned int,         // chunk
                          bool>> worker2chunks;

        for (auto&& entry: replicaData.workers) {
            std::string const& worker   = entry.first;
            bool        const  reported = entry.second;
            if (reported) {
                worker2chunks[worker] = std::map<unsigned int,bool>();
            }
        }
        for (auto chunk: replicaData.chunks.chunkNumbers()) {
            auto chunkMap = replicaData.chunks.chunk(chunk);

            for (auto&& database: chunkMap.databaseNames()) {
                auto databaseMap = chunkMap.database(database);

                for (auto&& worker: databaseMap.workerNames()) {
                    worker2chunks[worker][chunk] = true;
                }
            }
        }

        // Get a disposition of good chunks accross workers. This map will be used
        // on the next step as a foundation for two collections: overpopulated ('source')
        // and underpopulated ('destination') workers.
        //
        // NOTE: this algorithm will also create entries for workers which don't
        // have any good (or any) chunks. We need to included those later into
        // a collection of the underpopulated workers.

        std::map<std::string,
                 std::vector<unsigned int>> worker2goodChunks;

        for (auto&& entry: replicaData.workers) {
            std::string const& worker   = entry.first;
            bool        const  reported = entry.second;
            if (reported) {
                worker2goodChunks[worker] = std::vector<unsigned int>();
            }
        }
        for (auto&& chunkEntry: replicaData.isGood) {
            unsigned int const chunk = chunkEntry.first;
            for (auto&& workerEntry: chunkEntry.second) {
                std::string const& worker = workerEntry.first;
                bool        const  isGood = workerEntry.second;
                if (isGood) {
                    worker2goodChunks[worker].push_back(chunk);
                }
            }
        }

        // Get a disposition of the source workers along with chunks located
        // on the workers. The candidate worker must be strictly
        // above the previously computed average.
        //
        // NOTE: this collection will be sorted (descending order) based on
        // the total number of chunks per each worker entry.

        std::vector<std::pair<std::string,
                              std::vector<unsigned int>>> sourceWorkers;

        for (auto&& entry: worker2goodChunks) {
            size_t const numChunks = entry.second.size();
            if (numChunks > _replicaData.avgChunks) {
                sourceWorkers.push_back(entry);
            }
        }
        if (not sourceWorkers.size()) {

            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish:  "
                 << "no overloaded 'source' workers found");

            finish(ExtendedState::SUCCESS);
            break;
        }
        std::sort(
            sourceWorkers.begin(),
            sourceWorkers.end(),
            [] (std::pair<std::string, std::vector<unsigned int>> const& a,
                std::pair<std::string, std::vector<unsigned int>> const& b) {
                return b.second.size() < a.second.size();
            }
        );

        // Get a disposition of the destination workers along with the number
        // of available slots for chunks which can be hosted by the workers
        // before they'll hist the average. The number of good chunks on each
        // such (candidate) worker must be strictly below the previously computed
        // average.

        std::vector<std::pair<std::string,
                              size_t>> destinationWorkers;

        for (auto&& entry: worker2goodChunks) {
            std::string const worker    = entry.first;
            size_t      const numChunks = entry.second.size();

            if (numChunks < _replicaData.avgChunks) {
                destinationWorkers.push_back(
                    std::make_pair(worker,
                                   _replicaData.avgChunks - numChunks));
            }
        }
        if (not destinationWorkers.size()) {

            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish:  "
                 << "no underloaded 'destination' workers found");

            finish(ExtendedState::SUCCESS);
            break;
        }

        // Prepare the rebalance plan based on the following considerations:
        //
        // - use the above formed map 'worker2chunks' to avoid chunk collisions
        //   and to record claimed destination workers
        //
        // - use and update the above formed map 'destinationWorkers'
        //   to find a candidate worker with fewer number of chunks
        //
        // - the algorithim will go over all chunks of each eligible (source) worker
        //   to see if it's possible to find a new home for a chunk until
        //   the number of extra chunks parameter is exausted. Note. it's okay
        //   if it won't be possible to solve this problem for any chunk
        //   of the source worker - this will be just reported into the log
        //   stream before moving to the next worker. This problem will be
        //   resolved on the next iteration of the job after taking a fresh
        //   snapshot of chunk disposition. Possible infinite loops (over job
        //   iterations) can be resolved by setting some reasonable limit onto
        //   the total number of iterations before this job will be supposed
        //   to 'succced' in one way or another. Perhaps a special status
        //   flag for this job could be introduced to let a caller know about
        //   this situation.
        //
        // ATTENTION: this algorithm may need to be optimized for performance

        _replicaData.plan.clear();

        for (auto&& sourceWorkerEntry: sourceWorkers) {

            std::string               const& sourceWorker   = sourceWorkerEntry.first;
            std::vector<unsigned int> const& chunks         = sourceWorkerEntry.second;

            // Will get decremented in the chunks loop later when looking for chunks
            // to be moved elswhere.
            size_t numExtraChunks = chunks.size() - _replicaData.avgChunks;

            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish: "
                 << " sourceWorker: " << sourceWorker
                 << " numExtraChunks: " << numExtraChunks);

            for (unsigned int chunk: chunks) {

                if (not numExtraChunks) break;

                // Always sort the collection in the descending order to make sure
                // least populated workers are considered first
                std::sort(
                    destinationWorkers.begin(),
                    destinationWorkers.end(),
                    [] (std::pair<std::string, size_t> const& a,
                        std::pair<std::string, size_t> const& b) {
                        return b.second < a.second;
                    }
                );

                // Search for a candidate worker where to move this chunk to
                //
                // IMPLEMENTTION NOTES: using non-const references in the loop to allow
                // updates to the number of slots

                for (auto&& destinationWorkerEntry: destinationWorkers) {
                    std::string const& destinationWorker = destinationWorkerEntry.first;
                    size_t&            numSlots          = destinationWorkerEntry.second;

                    // Are there any awailable slots on the worker?
                    if (not numSlots) continue;

                    // Skip this worker if it already has this chunk
                    if (worker2chunks[destinationWorker].count(chunk)) continue;

                    // Found the one. Update

                    _replicaData.plan[chunk][sourceWorker] = destinationWorker;
                    worker2chunks[destinationWorker][chunk] = true;
                    numSlots--;

                    --numExtraChunks;
                    break;
                }
            }
        }

        // Finish right away if the 'estimate' mode requested.
        if (_estimateOnly) {
            finish(ExtendedState::SUCCESS);
            break;
        }

        // Finish right away if no badly unbalanced workers found to trigger the operation
        if (_replicaData.plan.empty()) {
            finish(ExtendedState::SUCCESS);
            break;
        }

        // Now submit chunk movement requests for chunks which could be
        // locked.
        //
        // TODO: Limit the number of migrated chunks to avoid overloading
        // the cluster with too many simultaneous requests. The chunk migration
        // limit should be specifid via the configuration.

        auto self = shared_from_base<RebalanceJob>();

        size_t numFailedLocks = 0;

        for (auto&& chunkEntry: _replicaData.plan) {
            unsigned int const chunk = chunkEntry.first;
            if (not _controller->serviceProvider()->chunkLocker().lock({_databaseFamily, chunk}, _id)) {
                ++_numFailedLocks;
                continue;
            }
            for (auto&& sourceWorkerEntry: chunkEntry.second) {
                std::string const& sourceWorker      = sourceWorkerEntry.first;
                std::string const& destinationWorker = sourceWorkerEntry.second;

                auto job = MoveReplicaJob::create(
                    _databaseFamily,
                    chunk,
                    sourceWorker,
                    destinationWorker,
                    true,   /* purge */
                    _controller,
                    _id,
                    [self](MoveReplicaJob::Ptr job) {
                        self->onJobFinish(job);
                    }
                );
                _moveReplicaJobs.push_back(job);
                _chunk2jobs[chunk][sourceWorker] = job;
                job->start();
            }
        }

        // Finish right away if no jobs were submitted and no failed attempts
        // to lock chunks were encountered.

        if (not _moveReplicaJobs.size()) {
            if (not numFailedLocks) {
                finish(ExtendedState::SUCCESS);
            } else {
                // Start another iteration by requesting the fresh state of
                // chunks within the family or until it all fails.
                restart();
                return;
            }
        }

    } while (false);

    if (_state == State::FINISHED) notify();
}

void RebalanceJob::onJobFinish(MoveReplicaJob::Ptr const& job) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onJobFinish"
         << "  databaseFamily="    << databaseFamily()
         << "  chunk="             << job->chunk()
         << "  sourceWorker="      << job->sourceWorker()
         << "  destinationWorker=" << job->destinationWorker());

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) {
        release(job->chunk());
        return;
    }
    do {
        // This lock will be automatically release beyond this scope
        // to allow client notifications (see the end of the method)
        LOCK(_mtx);

        // Make sure the chunk is released if this was the last job in
        // its scope regardless of the completion status of the job.

        _chunk2jobs.at(job->chunk()).erase(job->sourceWorker());
        if (_chunk2jobs.at(job->chunk()).empty()) {
            _chunk2jobs.erase(job->chunk());
            release(job->chunk());
        }

        // Update counters and object state if needed.

        if (job->extendedState() == Job::ExtendedState::SUCCESS) {

            // Copy over data from the job

            MoveReplicaJobResult const& replicaData = job->getReplicaData();

            for (auto&& replica: replicaData.createdReplicas) {
                _replicaData.createdReplicas.emplace_back(replica);
            }
            for (auto&& databaseEntry: replicaData.createdChunks.at(job->chunk())) {
                std::string const& database = databaseEntry.first;
                ReplicaInfo const& replica  = databaseEntry.second.at(job->destinationWorker());

                _replicaData.createdChunks[job->chunk()][database][job->destinationWorker()] = replica;
            }
            for (auto&& replica: replicaData.deletedReplicas) {
                _replicaData.deletedReplicas.emplace_back(replica);
            }
            for (auto&& databaseEntry: replicaData.deletedChunks.at(job->chunk())) {
                std::string const& database = databaseEntry.first;
                ReplicaInfo const& replica  = databaseEntry.second.at(job->sourceWorker());

                _replicaData.deletedChunks[job->chunk()][database][job->sourceWorker()] = replica;
            }
        }

        // Evaluate the status of on-going operations to see if the job
        // has finished.

        size_t numLaunched, numFinished, numSuccess;
        ::countJobStates(numLaunched, numFinished, numSuccess, _moveReplicaJobs);

        if (numFinished == numLaunched) {
            if (numSuccess == numLaunched) {
                // Make another iteration (and another one, etc. as many as needed)
                // before it succeeds or fails.
                //
                // NOTE: a condition for this jobs to succeed is evaluated in
                //       the precursor job completion code.
                restart();
            } else {
                finish(ExtendedState::FAILED);
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify (); }
}

void RebalanceJob::release(unsigned int chunk) {
    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);
    Chunk chunkObj {databaseFamily(), chunk};
    _controller->serviceProvider()->chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica
