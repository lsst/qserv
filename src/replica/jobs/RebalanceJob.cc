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
#include "replica/jobs/RebalanceJob.h"

// System headers
#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>

// Qserv headers
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"
#include "replica/util/ErrorReporting.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.RebalanceJob");

}  // namespace

namespace lsst::qserv::replica {

string RebalanceJob::typeName() { return "RebalanceJob"; }

RebalanceJob::Ptr RebalanceJob::create(string const& databaseFamily, bool estimateOnly,
                                       Controller::Ptr const& controller, string const& parentJobId,
                                       CallbackType const& onFinish, int priority) {
    return RebalanceJob::Ptr(
            new RebalanceJob(databaseFamily, estimateOnly, controller, parentJobId, onFinish, priority));
}

RebalanceJob::RebalanceJob(string const& databaseFamily, bool estimateOnly, Controller::Ptr const& controller,
                           string const& parentJobId, CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "REBALANCE", priority),
          _databaseFamily(databaseFamily),
          _estimateOnly(estimateOnly),
          _onFinish(onFinish) {}

RebalanceJobResult const& RebalanceJob::getReplicaData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error("RebalanceJob::" + string(__func__) +
                      "  the method can't be called while "
                      " the job hasn't finished");
}

list<pair<string, string>> RebalanceJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    return result;
}

list<pair<string, string>> RebalanceJob::persistentLogData() const {
    list<pair<string, string>> result;

    auto&& replicaData = getReplicaData();

    // Report workers failed to respond to the requests

    for (auto&& workerInfo : replicaData.workers) {
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
    //
    //   deleted-chunks:
    //     the total number of chunks deleted from the workers as a result
    //     of the operation

    map<string, map<string, size_t>> workerCategoryCounter;

    for (auto&& info : replicaData.createdReplicas) {
        workerCategoryCounter[info.worker()]["created-chunks"]++;
    }
    for (auto&& info : replicaData.deletedReplicas) {
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

void RebalanceJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<RebalanceJob>();

    bool const saveReplicInfo = true;  // always save the replica info in a database because
                                       // the algorithm depends on it.
    bool const allWorkers = false;     // only consider enabled workers
    _findAllJob = FindAllJob::create(
            databaseFamily(), saveReplicInfo, allWorkers, controller(), id(),
            [self](FindAllJob::Ptr job) { self->_onPrecursorJobFinish(); }, priority());
    _findAllJob->start();
}

void RebalanceJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if ((nullptr != _findAllJob) and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    _jobs.clear();

    for (auto&& ptr : _activeJobs) ptr->cancel();
    _activeJobs.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess = 0;
}

void RebalanceJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<RebalanceJob>(lock, _onFinish);
}

void RebalanceJob::_onPrecursorJobFinish() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;

    replica::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    ////////////////////////////////////////////////////////////////////
    // Do not proceed with the replication effort unless running the job
    // under relaxed condition.

    if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  failed due to the precursor job failure");

        finish(lock, ExtendedState::FAILED);
        return;
    }

    ////////////////////////////////////////////////
    // Analyze results and prepare a re-balance plan

    FindAllJobResult const& replicaData = _findAllJob->getReplicaData();

    // Compute key parameters of the algorithm by counting the number of 'useful'
    // workers and 'good' chunks.

    _replicaData.totalWorkers = 0;     // not counting workers which failed to report chunks
    _replicaData.totalGoodChunks = 0;  // good chunks reported by the precursor job

    for (auto&& entry : replicaData.workers) {
        bool const reported = entry.second;
        if (reported) {
            _replicaData.totalWorkers++;
        }
    }
    for (auto&& chunkEntry : replicaData.isGood) {
        unsigned int const chunk = chunkEntry.first;

        // skip the special chunk which must be present on all workers
        if (chunk == replica::overflowChunkNumber) continue;

        for (auto&& workerEntry : chunkEntry.second) {
            bool const isGood = workerEntry.second;
            if (isGood) {
                _replicaData.totalGoodChunks++;
            }
        }
    }
    if (not _replicaData.totalWorkers or not _replicaData.totalGoodChunks) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  no eligible 'good' chunks found");

        finish(lock, ExtendedState::SUCCESS);
        return;
    }

    _replicaData.avgChunks = _replicaData.totalGoodChunks / _replicaData.totalWorkers;
    if (not _replicaData.avgChunks) {
        LOGS(_log, LOG_LVL_DEBUG,
             context() << __func__ << "  the average number of 'good' chunks per worker is 0. "
                       << "This won't trigger the operation");

        finish(lock, ExtendedState::SUCCESS);
        return;
    }

    // This map is populated with all workers which have responded to the FindAll
    // requests. It's meant to tell the planner which workers to avoid when looking for
    // a new home for a chunk to be moved elsewhere from an overpopulated
    // worker.
    //
    // IMPORTANT: the map be updated by the planner as it will be deciding
    // on new destinations for the moved chunks.

    map<string,            // worker
        map<unsigned int,  // chunk
            bool>>
            worker2chunks;

    for (auto&& entry : replicaData.workers) {
        string const& worker = entry.first;
        bool const reported = entry.second;
        if (reported) {
            worker2chunks[worker] = map<unsigned int, bool>();
        }
    }
    for (auto chunk : replicaData.chunks.chunkNumbers()) {
        // skip the special chunk which must be present on all workers
        if (chunk == replica::overflowChunkNumber) continue;

        auto chunkMap = replicaData.chunks.chunk(chunk);

        for (auto&& database : chunkMap.databaseNames()) {
            auto databaseMap = chunkMap.database(database);

            for (auto&& worker : databaseMap.workerNames()) {
                worker2chunks[worker][chunk] = true;
            }
        }
    }

    // Get a disposition of good chunks across workers. This map will be used
    // on the next step as a foundation for two collections: overpopulated ('source')
    // and underpopulated ('destination') workers.
    //
    // NOTE: this algorithm will also create entries for workers which don't
    // have any good (or any) chunks. We need to included those later into
    // a collection of the underpopulated workers.

    map<string, vector<unsigned int>> worker2goodChunks;

    for (auto&& entry : replicaData.workers) {
        string const& worker = entry.first;
        bool const reported = entry.second;
        if (reported) {
            worker2goodChunks[worker] = vector<unsigned int>();
        }
    }
    for (auto&& chunkEntry : replicaData.isGood) {
        unsigned int const chunk = chunkEntry.first;

        // skip the special chunk which must be present on all workers
        if (chunk == replica::overflowChunkNumber) continue;

        for (auto&& workerEntry : chunkEntry.second) {
            string const& worker = workerEntry.first;
            bool const isGood = workerEntry.second;
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

    vector<pair<string, vector<unsigned int>>> sourceWorkers;

    for (auto&& entry : worker2goodChunks) {
        size_t const numChunks = entry.second.size();
        if (numChunks > _replicaData.avgChunks) {
            sourceWorkers.push_back(entry);
        }
    }
    if (not sourceWorkers.size()) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  no overloaded 'source' workers found");

        finish(lock, ExtendedState::SUCCESS);
        return;
    }
    sort(sourceWorkers.begin(), sourceWorkers.end(),
         [](pair<string, vector<unsigned int>> const& a, pair<string, vector<unsigned int>> const& b) {
             return b.second.size() < a.second.size();
         });

    // Get a disposition of the destination workers along with the number
    // of available slots for chunks which can be hosted by the workers
    // before they'll hist the average. The number of good chunks on each
    // such (candidate) worker must be strictly below the previously computed
    // average.

    vector<pair<string, size_t>> destinationWorkers;

    for (auto&& entry : worker2goodChunks) {
        string const worker = entry.first;
        size_t const numChunks = entry.second.size();

        if (numChunks < _replicaData.avgChunks) {
            destinationWorkers.push_back(make_pair(worker, _replicaData.avgChunks - numChunks));
        }
    }
    if (not destinationWorkers.size()) {
        LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  no under-loaded 'destination' workers found");

        finish(lock, ExtendedState::SUCCESS);
        return;
    }

    // Prepare the re-balance plan based on the following considerations:
    //
    // - use the above formed map 'worker2chunks' to avoid chunk collisions
    //   and to record claimed destination workers
    //
    // - use and update the above formed map 'destinationWorkers'
    //   to find a candidate worker with fewer number of chunks
    //
    // - the algorithm will go over all chunks of each eligible (source) worker
    //   to see if it's possible to find a new home for a chunk until
    //   the number of extra chunks parameter is exhausted. Note. it's okay
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

    for (auto&& sourceWorkerEntry : sourceWorkers) {
        string const& sourceWorker = sourceWorkerEntry.first;
        vector<unsigned int> const& chunks = sourceWorkerEntry.second;

        // This number (below) will get decremented in the chunks loop later when
        // looking for chunks to be moved elsewhere.
        size_t numExtraChunks = chunks.size() - _replicaData.avgChunks;

        LOGS(_log, LOG_LVL_DEBUG,
             context() << __func__ << "  sourceWorker: " << sourceWorker
                       << " numExtraChunks: " << numExtraChunks);

        for (unsigned int chunk : chunks) {
            if (not numExtraChunks) break;

            // Always sort the collection in the descending order to make sure
            // least populated workers are considered first
            sort(destinationWorkers.begin(), destinationWorkers.end(),
                 [](pair<string, size_t> const& a, pair<string, size_t> const& b) {
                     return b.second < a.second;
                 });

            // Search for a candidate worker where to move this chunk to
            //
            // IMPLEMENTTION NOTES: using non-constant references in the loop to allow
            // updates to the number of slots

            for (auto&& destinationWorkerEntry : destinationWorkers) {
                string const& destinationWorker = destinationWorkerEntry.first;
                size_t& numSlots = destinationWorkerEntry.second;

                // Are there any available slots on the worker?
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
    if (estimateOnly()) {
        finish(lock, ExtendedState::SUCCESS);
        return;
    }

    // Finish right away if no badly unbalanced workers found to trigger
    // the operation

    if (_replicaData.plan.empty()) {
        finish(lock, ExtendedState::SUCCESS);
        return;
    }

    // Pre-create chunk movement jobs according to the migration
    // plan.

    auto self = shared_from_base<RebalanceJob>();

    for (auto&& chunkEntry : _replicaData.plan) {
        unsigned int const chunk = chunkEntry.first;
        for (auto&& sourceWorkerEntry : chunkEntry.second) {
            string const& sourceWorker = sourceWorkerEntry.first;
            string const& destinationWorker = sourceWorkerEntry.second;

            auto job = MoveReplicaJob::create(
                    databaseFamily(), chunk, sourceWorker, destinationWorker, true, /* purge */
                    controller(), id(), [self](MoveReplicaJob::Ptr job) { self->_onJobFinish(job); },
                    priority());
            _jobs.push_back(job);
        }
    }

    // ATTENTION: this condition needs to be evaluated to prevent
    // getting into the 'zombie' state.

    if (not _jobs.size()) {
        finish(lock, ExtendedState::SUCCESS);
        return;
    }

    // Otherwise start the first batch of jobs. The number of jobs in
    // the batch is determined by the number of source workers in
    // the above prepared plan multiplied by the number of worker-side
    // processing threads.

    set<string> uniqueDestinationWorkers;
    for (auto&& ptr : _jobs) {
        uniqueDestinationWorkers.insert(ptr->destinationWorker());
    }
    size_t const numJobs =
            uniqueDestinationWorkers.size() *
            controller()->serviceProvider()->config()->get<size_t>("worker", "num-svc-processing-threads");

    size_t const numJobsLaunched = _launchNextJobs(lock, numJobs);
    if (0 != numJobsLaunched) {
        _numLaunched += numJobsLaunched;
    } else {
        LOGS(_log, LOG_LVL_ERROR,
             context() << __func__ << "  unexpected failure when launching " << numJobs
                       << " replica migration jobs");
        _jobs.clear();
        finish(lock, ExtendedState::FAILED);
    }
}

void RebalanceJob::_onJobFinish(MoveReplicaJob::Ptr const& job) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  databaseFamily=" << databaseFamily() << "  chunk=" << job->chunk()
                   << "  sourceWorker=" << job->sourceWorker()
                   << "  destinationWorker=" << job->destinationWorker());

    if (state() == State::FINISHED) {
        _activeJobs.remove(job);
        return;
    }

    replica::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) {
        _activeJobs.remove(job);
        return;
    }

    // The job needs to be removed from this list so that the next job schedule
    // would operate on the actual state of the active job disposition.

    _activeJobs.remove(job);

    // Update counters and object state if needed.

    _numFinished++;
    if (job->extendedState() == Job::ExtendedState::SUCCESS) {
        _numSuccess++;

        // Copy over data from the job

        MoveReplicaJobResult const& replicaData = job->getReplicaData();

        for (auto&& replica : replicaData.createdReplicas) {
            _replicaData.createdReplicas.emplace_back(replica);
        }
        for (auto&& databaseEntry : replicaData.createdChunks.at(job->chunk())) {
            string const& database = databaseEntry.first;
            ReplicaInfo const& replica = databaseEntry.second.at(job->destinationWorker());
            _replicaData.createdChunks[job->chunk()][database][job->destinationWorker()] = replica;
        }
        for (auto&& replica : replicaData.deletedReplicas) {
            _replicaData.deletedReplicas.emplace_back(replica);
        }
        for (auto&& databaseEntry : replicaData.deletedChunks.at(job->chunk())) {
            string const& database = databaseEntry.first;
            ReplicaInfo const& replica = databaseEntry.second.at(job->sourceWorker());
            _replicaData.deletedChunks[job->chunk()][database][job->sourceWorker()] = replica;
        }
    }

    // Try to submit one more job

    size_t const numJobsLaunched = _launchNextJobs(lock, 1);
    if (numJobsLaunched != 0) {
        _numLaunched += numJobsLaunched;
    } else {
        // Evaluate the status of on-going operations to see if the job
        // has finished.

        if (_numFinished == _numLaunched) {
            finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS : ExtendedState::FAILED);
        }
    }
}

size_t RebalanceJob::_launchNextJobs(replica::Lock const& lock, size_t numJobs) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  numJobs=" << numJobs);

    // Compute the number of jobs which are already active at both ends
    // (destination and source workers).

    map<string, size_t> numAtDest;
    map<string, size_t> numAtSrc;

    for (auto&& ptr : _activeJobs) {
        numAtDest[ptr->destinationWorker()]++;
        numAtSrc[ptr->sourceWorker()]++;
    }

    // Try to fulfill the request (to submit the given number of jobs)
    // by evaluating best candidates using an algorithm explained
    // within the loop below.

    size_t numJobsLaunched = 0;
    for (size_t i = 0; i < numJobs; ++i) {
        // THE LOAD BALANCING ALGORITHM:
        //
        //   The algorithms evaluates candidates (pairs of (dstWorker,srcWorker))
        //   to find the one which allows more even spread of load among the destination
        //   and source workers. For each pair of the workers the algorithm computes
        //   a 'load' which is just a sum of the on-going activities at both ends of
        //   the proposed transfer:
        //
        //     load := numAtDest[destWorker] + numAtSrc[srcWorker]
        //
        //   A part which has the lowest number will be selected.

        size_t minLoad = numeric_limits<unsigned long long>::max();
        MoveReplicaJob::Ptr job;

        for (auto&& ptr : _jobs) {
            size_t const load = numAtDest[ptr->destinationWorker()] + numAtSrc[ptr->sourceWorker()];
            if (load <= minLoad) {
                minLoad = load;
                job = ptr;
            }
        }
        if (nullptr != job) {
            // Update occupancy of the worker nodes at both ends
            numAtDest[job->destinationWorker()]++;
            numAtSrc[job->sourceWorker()]++;

            // Move the job into another queue
            _activeJobs.push_back(job);
            _jobs.remove(job);

            // Let it run
            job->start();
            numJobsLaunched++;
        }
    }
    return numJobsLaunched;
}

}  // namespace lsst::qserv::replica
