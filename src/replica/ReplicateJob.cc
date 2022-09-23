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
#include "replica/ReplicateJob.h"

// System headers
#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ReplicateJob");

}  // namespace

namespace lsst::qserv::replica {

string ReplicateJob::typeName() { return "ReplicateJob"; }

ReplicateJob::Ptr ReplicateJob::create(string const& databaseFamily, unsigned int numReplicas,
                                       Controller::Ptr const& controller, string const& parentJobId,
                                       CallbackType const& onFinish, int priority) {
    return ReplicateJob::Ptr(
            new ReplicateJob(databaseFamily, numReplicas, controller, parentJobId, onFinish, priority));
}

ReplicateJob::ReplicateJob(string const& databaseFamily, unsigned int numReplicas,
                           Controller::Ptr const& controller, string const& parentJobId,
                           CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "REPLICATE", priority),
          _databaseFamily(databaseFamily),
          _numReplicas(controller->serviceProvider()->config()->effectiveReplicationLevel(databaseFamily,
                                                                                          numReplicas)),
          _onFinish(onFinish) {
    if (0 == _numReplicas) {
        throw invalid_argument(typeName() + "::" + string(__func__) + "  0 replicas is not allowed");
    }
}

ReplicateJobResult const& ReplicateJob::getReplicaData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error("ReplicateJob::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

list<pair<string, string>> ReplicateJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("num_replicas", to_string(numReplicas()));
    return result;
}

list<pair<string, string>> ReplicateJob::persistentLogData() const {
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

    map<string, map<string, size_t>> workerCategoryCounter;

    for (auto&& info : replicaData.replicas) {
        workerCategoryCounter[info.worker()]["created-chunks"]++;
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

void ReplicateJob::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<ReplicateJob>();

    bool const saveReplicInfo = true;  // always save the replica info in a database because
                                       // the algorithm depends on it.
    bool const allWorkers = false;     // only consider enabled workers
    _findAllJob = FindAllJob::create(
            databaseFamily(), saveReplicInfo, allWorkers, controller(), id(),
            [self](FindAllJob::Ptr job) { self->_onPrecursorJobFinish(); }, priority());
    _findAllJob->start();
}

void ReplicateJob::cancelImpl(util::Lock const& lock) {
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

void ReplicateJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<ReplicateJob>(lock, _onFinish);
}

void ReplicateJob::_onPrecursorJobFinish() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    //////////////////////////////////////////////////////////////////////
    // Do not proceed with the replication effort if there was any problem
    // with the precursor job.

    if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
        finish(lock, ExtendedState::FAILED);
        return;
    }

    /////////////////////////////////////////////////////////////////
    // Analyze results and prepare a replication plan to create extra
    // replicas for under-represented chunks
    //
    // IMPORTANT:
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
    map<unsigned int, int> chunk2numReplicas2create;

    for (auto&& chunk2workers : replicaData.isGood) {
        unsigned int const chunk = chunk2workers.first;
        auto const& replicas = chunk2workers.second;

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

    map<string, size_t> worker2occupancy;

    for (auto&& chunkEntry : replicaData.isGood) {
        for (auto&& workerEntry : chunkEntry.second) {
            auto worker = workerEntry.first;
            bool const isGood = workerEntry.second;
            if (isGood) {
                if (not worker2occupancy.count(worker)) worker2occupancy[worker] = 0;
                worker2occupancy[worker]++;
            }
        }
    }

    // The "black list" of workers to be avoided as new replica destinations
    // for specific chunks because they already have a replica (regardless of
    // its status) of that chunk for any database of the family
    //
    // This is done in order to avoid conflicts when attempting to place new chunk
    // replicas on that node.
    //
    map<string, set<unsigned int>> worker2chunks;

    for (auto chunk : replicaData.chunks.chunkNumbers()) {
        auto chunkMap = replicaData.chunks.chunk(chunk);

        for (auto&& database : chunkMap.databaseNames()) {
            auto databaseMap = chunkMap.database(database);

            for (auto&& worker : databaseMap.workerNames()) {
                worker2chunks[worker].insert(chunk);
            }
        }
    }

    // The 'white list of workers which haven't been reported as FAILED
    // by the precursor job. These workers will be considered as destinations
    // for the new replicas.

    vector<string> workers;
    for (auto&& worker : controller()->serviceProvider()->config()->workers()) {
        if (replicaData.workers.at(worker)) {
            workers.push_back(worker);
        }
    }
    if (not workers.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  not workers are available for new replicas");

        finish(lock, ExtendedState::FAILED);
        return;
    }

    /////////////////////////////////////////////////////////////////////
    // Check which chunks are under-represented. Then find a least loaded
    // worker and launch a replica creation job.

    // The number of times each source worker is allocated is computed and used
    // by the replication planner in order to spread the load across as many
    // source workers as possible.
    map<string, size_t> sourceWorkerAllocations;
    for (auto&& worker : controller()->serviceProvider()->config()->workers()) {
        sourceWorkerAllocations[worker] = 0;
    }

    auto self = shared_from_base<ReplicateJob>();

    for (auto&& chunk2replicas : chunk2numReplicas2create) {
        unsigned int const chunk = chunk2replicas.first;
        int const numReplicas2create = chunk2replicas.second;

        // Find the least used (as a source) worker which has a 'good'
        // chunk

        string sourceWorker;
        size_t minAllocations = numeric_limits<unsigned long long>::max();

        for (auto&& workerEntry : replicaData.isGood.at(chunk)) {
            string const& worker = workerEntry.first;
            bool const isGood = workerEntry.second;
            if (isGood) {
                size_t const allocations = sourceWorkerAllocations[worker];
                if (allocations < minAllocations) {
                    sourceWorker = worker;
                    minAllocations = allocations;
                }
            }
        }
        if (sourceWorker.empty()) {
            LOGS(_log, LOG_LVL_ERROR,
                 context() << __func__ << "  no suitable source worker found for chunk: " << chunk);
            finish(lock, ExtendedState::FAILED);
            return;
        }

        // Iterate over the number of replicas to be created and create
        // a new one on each step.
        //
        // NOTE: the worker occupancy map worker2occupancy will get
        // updated on each successful iteration of the loop, so that
        // the corresponding destination worker will also be accounted
        // for when deciding on a placement of other replicas.

        for (int i = 0; i < numReplicas2create; ++i) {
            // Find a suitable destination worker based on the worker load
            // and chunk-specific exclusions.

            string destinationWorker;

            size_t minNumChunks = (size_t)-1;  // this will be decreased within the loop to find
                                               // the absolute minimum among the eligible workers
            for (const auto& worker : workers) {
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
                LOGS(_log, LOG_LVL_ERROR,
                     context() << __func__ << "  no suitable destination worker found for chunk: " << chunk);
                finish(lock, ExtendedState::FAILED);
                return;
            }

            // Finally, create, but DO NOT START the replica creation job.

            auto ptr = CreateReplicaJob::create(
                    databaseFamily(), chunk, sourceWorker, destinationWorker, controller(), id(),
                    [self](CreateReplicaJob::Ptr const& job) { self->_onCreateJobFinish(job); },
                    priority()  // inherit from the current job
            );
            _jobs.push_back(ptr);

            // Bump the occupancy of workers on both ends of the operations, so that it
            // will be taken into consideration when deciding on sources and destinations
            // of other replicas.

            worker2occupancy[destinationWorker]++;
            sourceWorkerAllocations[sourceWorker]++;
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

    set<string> destinationWorkers;
    for (auto&& ptr : _jobs) {
        destinationWorkers.insert(ptr->destinationWorker());
    }
    size_t const numJobs = destinationWorkers.size() * controller()->serviceProvider()->config()->get<size_t>(
                                                               "worker", "num-svc-processing-threads");

    size_t const numJobsLaunched = _launchNextJobs(lock, numJobs);
    if (0 != numJobsLaunched) {
        _numLaunched += numJobsLaunched;
    } else {
        LOGS(_log, LOG_LVL_ERROR,
             context() << __func__ << "  unexpected failure when launching " << numJobs
                       << " replication jobs");
        _jobs.clear();
        finish(lock, ExtendedState::FAILED);
    }
}

void ReplicateJob::_onCreateJobFinish(CreateReplicaJob::Ptr const& job) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  chunk=" << job->chunk() << "  databaseFamily=" << job->databaseFamily()
                   << "  sourceWorker=" << job->sourceWorker()
                   << "  destinationWorker=" << job->destinationWorker());

    if (state() == State::FINISHED) {
        _activeJobs.remove(job);
        return;
    }

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) {
        _activeJobs.remove(job);
        return;
    }

    // The job needs to be removed from this list so that the next job schedule
    // would operate on the actual state of the active job disposition.

    _activeJobs.remove(job);

    // Update counters and object state if needed

    _numFinished++;
    if (job->extendedState() == Job::ExtendedState::SUCCESS) {
        _numSuccess++;
        auto replicaData = job->getReplicaData();
        for (auto&& replica : replicaData.replicas) {
            _replicaData.replicas.push_back(replica);
        }
        for (auto&& chunkEntry : replicaData.chunks) {
            auto chunk = chunkEntry.first;

            for (auto&& databaseEntry : chunkEntry.second) {
                auto database = databaseEntry.first;

                for (auto&& workerEntry : databaseEntry.second) {
                    auto worker = workerEntry.first;
                    auto replica = workerEntry.second;

                    _replicaData.chunks[chunk][database][worker] = replica;
                }
            }
        }
        _replicaData.workers[job->destinationWorker()] = true;
    } else {
        _replicaData.workers[job->destinationWorker()] = false;
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

size_t ReplicateJob::_launchNextJobs(util::Lock const& lock, size_t numJobs) {
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
        CreateReplicaJob::Ptr job;

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
