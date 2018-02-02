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
#include "replica_core/RebalanceJob.h"

// System headers

#include <algorithm>
#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/BlockPost.h"
#include "replica_core/ErrorReporting.h"
#include "replica_core/ServiceProvider.h"


// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.RebalanceJob");

template <class COLLECTION>
void countJobStates (size_t&           numLaunched,
                     size_t&           numFinished,
                     size_t&           numSuccess,
                     COLLECTION const& collection) {

    using namespace lsst::qserv::replica_core;

    numLaunched = collection.size();
    numFinished = 0;
    numSuccess  = 0;

    for (auto const& ptr: collection) {
        if (ptr->state() == Job::State::FINISHED) {
            numFinished++;
            if (ptr->extendedState() == Job::ExtendedState::SUCCESS)
                numSuccess++;
        }
    }
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

RebalanceJob::pointer
RebalanceJob::create (std::string const&         databaseFamily,
                      unsigned int               startPercent,
                      unsigned int               stopPercent,
                      bool                       estimateOnly,
                      Controller::pointer const& controller,
                      callback_type              onFinish,
                      bool                       bestEffort,
                      int                        priority,
                      bool                       exclusive,
                      bool                       preemptable) {
    return RebalanceJob::pointer (
        new RebalanceJob (databaseFamily,
                          startPercent,
                          stopPercent,
                          estimateOnly,
                          controller,
                          onFinish,
                          bestEffort,
                          priority,
                          exclusive,
                          preemptable));
}

RebalanceJob::RebalanceJob (std::string const&         databaseFamily,
                            unsigned int               startPercent,
                            unsigned int               stopPercent,
                            bool                       estimateOnly,
                            Controller::pointer const& controller,
                            callback_type              onFinish,
                            bool                       bestEffort,
                            int                        priority,
                            bool                       exclusive,
                            bool                       preemptable)

    :   Job (controller,
             "REBALANCE",
             priority,
             exclusive,
             preemptable),

        _databaseFamily (databaseFamily),
        _startPercent   (startPercent),
        _stopPercent    (stopPercent),
        _estimateOnly   (estimateOnly),

        _onFinish   (onFinish),
        _bestEffort (bestEffort) {

    // Neither limit should be outside a range of [10,50], and the difference shouldn't
    // be less than 5%.

    if ((_startPercent < 10 or _startPercent > 50) or
        (_stopPercent  <  5 or _stopPercent  > 45) or
        (_stopPercent  > _startPercent) or (_stopPercent  - _startPercent < 5))
        throw std::invalid_argument (
                "RebalanceJob::RebalanceJob ()  invalid values of parameters 'startPercent' or 'stopPercent'");
}

RebalanceJob::~RebalanceJob () {
    // Make sure all chuks locked by this job are released
    _controller->serviceProvider().chunkLocker().release(_id);
}

RebalanceJobResult const&
RebalanceJob::getReplicaData () const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED)  return _replicaData;

    throw std::logic_error (
        "RebalanceJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void
RebalanceJob::track (bool          progressReport,
                     bool          errorReport,
                     bool          chunkLocksReport,
                     std::ostream& os) const {

    BlockPost blockPost (1000, 2000);

    while (_state != State::FINISHED) {

        if (_findAllJob and _findAllJob->state() != State::FINISHED)
            _findAllJob->track (progressReport,
                                errorReport,
                                chunkLocksReport,
                                os);

        if (progressReport) {

            // Need this to guarantee a consisent view onto the collection of
            // the jobs.
            LOCK_GUARD;

            size_t numLaunched, numFinished, numSuccess;
            ::countJobStates (numLaunched, numFinished, numSuccess, _moveReplicaJobs);

            os  << "RebalanceJob::track() "
                << " iters:"   << _replicaData.numIterations
                << " workers:" << _replicaData.totalWorkers
                << " chunks:"  << _replicaData.totalGoodChunks
                << " avg:"     << _replicaData.avgChunksPerWorker
                << " start:"   << _replicaData.startChunksPerWorker
                << " stop:"    << _replicaData.stopChunksPerWorker
                << " jobs:"    << numLaunched
                << " done:"    << numFinished
                << " ok:"      << numSuccess
                << std::endl;
        }
        if (chunkLocksReport)
            os  << "RebalanceJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
                << _controller->serviceProvider().chunkLocker().locked (_id);

         blockPost.wait();
   }
}

void
RebalanceJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  numIterations=" << _replicaData.numIterations);

    _replicaData.numIterations++;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<RebalanceJob>();

    _findAllJob = FindAllJob::create (
        _databaseFamily,
        _controller,
        [self] (FindAllJob::pointer job) { self->onPrecursorJobFinish(); }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void
RebalanceJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob && (_findAllJob->state() != State::FINISHED))
        _findAllJob->cancel();

    _findAllJob = nullptr;

    for (auto const& ptr: _moveReplicaJobs) {
        ptr->cancel();
    }
    _moveReplicaJobs.clear();

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void
RebalanceJob::restart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    size_t numLaunched;
    size_t numFinished;
    size_t numSuccess;

    ::countJobStates (numLaunched, numFinished, numSuccess,
                      _moveReplicaJobs);

    if ((_findAllJob && (_findAllJob->state() != State::FINISHED)) or (numLaunched != numFinished))
        throw std::logic_error ("RebalanceJob::restart ()  not allowed in this object state");

    _moveReplicaJobs.clear();

    // Take a fresh snapshot opf chunk disposition within the cluster
    // to see what else can be rebalanced. Note that this is going to be
    // a lengthy operation allowing other on-going activities locking chunks
    // to be finished before the current job will get another chance
    // to rebalance (if needed).

    auto self = shared_from_base<RebalanceJob>();

    _findAllJob = FindAllJob::create (
        _databaseFamily,
        _controller,
        [self] (FindAllJob::pointer job) { self->onPrecursorJobFinish(); }
    );
    _findAllJob->start();
}

void
RebalanceJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<RebalanceJob>();
        _onFinish(self);
    }
}

void
RebalanceJob::onPrecursorJobFinish () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    do {
        // This lock will be automatically release beyon this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;
    
        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) return;
    
        ////////////////////////////////////////////////////////////////////
        // Do not proceed with the replication effort unless running the job
        // under relaxed condition.
    
        if (not _bestEffort && (_findAllJob->extendedState() != ExtendedState::SUCCESS)) {
            setState(State::FINISHED, ExtendedState::FAILED);
            break;
        }

        ///////////////////////////////////////////////
        // Analyse results and prepare a rebalance plan

        FindAllJobResult const& replicaData = _findAllJob->getReplicaData ();

        // Count the number of 'good' chunks (if any) per each worker as well as
        // the total number of good chunks.

        _replicaData.totalWorkers    = 0;     // not counting workers which failed to report chunks
        _replicaData.totalGoodChunks = 0;     // good chunks reported by the precursor job

        std::map<std::string, size_t> worker2numGoodChunks;
        for (std::string const& worker: _controller->serviceProvider().config()->workers()) {
            if (replicaData.workers.count(worker) and replicaData.workers.at(worker)) {
                _replicaData.totalWorkers++;
                worker2numGoodChunks[worker] = 0;
            }
        }
        for (auto const& chunkEntry: replicaData.isGood) {
            for (auto const& workerEntry: chunkEntry.second) {
                std::string const& worker = workerEntry.first;
                bool        const  isGood = workerEntry.second;
                if (isGood) {
                    _replicaData.totalGoodChunks++;
                    worker2numGoodChunks[worker]++;
                }
            }
        }       
        if (not _replicaData.totalWorkers or not _replicaData.totalGoodChunks) {
            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish:  "
                 << "no eligible 'good' chunks found");
            setState (State::FINISHED, ExtendedState::SUCCESS);
            break;
        }

        // Find candidate workers which are above the 'startPercent' threshold
        // and count the number of chunks to be shaved off.

        _replicaData.avgChunksPerWorker   = _replicaData.totalGoodChunks / _replicaData.totalWorkers;
        _replicaData.startChunksPerWorker = (size_t)(_replicaData.avgChunksPerWorker + _startPercent / 100. * _replicaData.avgChunksPerWorker);
        _replicaData.stopChunksPerWorker  = (size_t)(_replicaData.avgChunksPerWorker + _stopPercent  / 100. * _replicaData.avgChunksPerWorker);

        if (_replicaData.startChunksPerWorker == _replicaData.stopChunksPerWorker) {
            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish:  "
                 << "too few 'good' chunks per worker to trigger the operation");
            setState (State::FINISHED, ExtendedState::SUCCESS);
            break;
        }
        std::map<std::string, size_t> sourceWorker2numExtraChunks;      // overpopulated workers
        std::map<std::string, size_t> destinationWorker2numChunks;      // underpopulated workers (will be updated by the algorithm)

        for (auto const& workerEntry: worker2numGoodChunks) {
            std::string const& worker    = workerEntry.first;
            size_t      const  numChunks = workerEntry.second;

            // Consider workers which are overpopulated above the upper bound
            //
            // ATTENTION: using '>' in the comparision instead of '>=' is meant to dumpen
            //            a possibility of the jitrerring effect when 'startChunksPerWorker'
            //            and 'stopChunksPerWorker' are off just by one.
            if (numChunks > _replicaData.startChunksPerWorker) {

                // shave chunks down to the lower bound
                sourceWorker2numExtraChunks[worker] = numChunks - _replicaData.stopChunksPerWorker;
            } else {
                destinationWorker2numChunks[worker] = numChunks;
            }
        }
        if (not sourceWorker2numExtraChunks.size()) {
            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish:  "
                 << "no badly unbalanced workers found to trigger the operation");
            setState (State::FINISHED, ExtendedState::SUCCESS);
            break;
        }

        // This map will be playing two rolels when forming the rebalancing plan
        // later in this block of mode:
        //
        // - it will tell the planner which workers to avoid when looking for
        //   a new home for a chunk to be moved elswhere from an overpopulated
        //   worker
        //
        // - it will be updated by the planner as it will be deciding on new
        //   destinations for the moved chunks

        std::map<std::string,                   // worker
                 std::map<unsigned int,         // chunk
                          bool>> worker2chunks;

        // - prepopulate the map with all workers which have responded
        //   to the FindAll requests.
        for (auto const& workerEntry: replicaData.workers) {
            std::string const& worker = workerEntry.first;
            worker2chunks[worker] = std::map<unsigned int,bool>();
        }
        // - fill in chunk nubers for those workers which have at least one of those
        for (auto const& chunkEntry: replicaData.chunks) {
            unsigned int const chunk = chunkEntry.first;
            for (auto const& databaseEntry: chunkEntry.second) {
                for (auto const& workerEntry: databaseEntry.second) {
                    std::string const& worker = workerEntry.first;
                    worker2chunks[worker][chunk] = true;
                }
            }
        }

        // Prepare the rebalance plan based on the following considerations:
        //
        // - use the above formed map 'worker2chunks' to avoid chunk collisions
        //   and to record claimed destination workers
        //
        // - use and update the above formed map 'destinationWorker2numChunks'
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
        //   iterations) can be resolved by setting som ereasonable limit onto
        //   the total number of iterations before this job will be supposed
        //   to 'succced' in one way or another. Perhaps a special status
        //   flag for this job could be introduced to let a caller know about
        //   this situation.
        //
        // ATTENTION: this algorithm may need to be optimized for performance

        _replicaData.plan.clear();

        for (auto const& sourceWorkerEntry: sourceWorker2numExtraChunks) {
            std::string const& sourceWorker   = sourceWorkerEntry.first;
            size_t             numExtraChunks = sourceWorkerEntry.second;

            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish: "
                 << " sourceWorker: " << sourceWorker
                 << " numExtraChunks: " << numExtraChunks);

            if (numExtraChunks) {

                LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish: "
                     << " sourceWorker: " << sourceWorker
                     << " worker2chunks.count(sourceWorker): " << worker2chunks.count(sourceWorker));

                for (auto const& chunkEntry: worker2chunks.at(sourceWorker)) {
                    unsigned int chunk = chunkEntry.first;

                    // Build a collection of destination workers sorted in the ascenting
                    // order by the number of chunks. This step is VERY IMPORTANT in order
                    // to prevent the algorithm from depending on the static iteration
                    // order of map 'destinationWorker2numChunks' which will favour
                    // the first entries.
                    //
                    // Entries of the collection are pairs of: (destinationWorker,numChunks)
 
                    std::vector<std::pair<std::string, size_t>> destinationWorker2numChunksSorted;
                    for (auto const& destinationWorkerEntry: destinationWorker2numChunks)
                        destinationWorker2numChunksSorted.push_back(destinationWorkerEntry);

                    std::sort (
                        destinationWorker2numChunksSorted.begin(),
                        destinationWorker2numChunksSorted.end  (),
                        [] (std::pair<std::string, size_t> const& a,
                            std::pair<std::string, size_t> const& b) {
                            return a.second < b.second;
                        }
                    );

                    // Find a least populated destination worker which doesn't have
                    // any replcas of this chunk.

                    std::string destinationWorker;
                    size_t      minNumChunks = std::numeric_limits<size_t>::max();

                    for (auto const& destinationWorkerEntry: destinationWorker2numChunksSorted) {
                        std::string const& worker    = destinationWorkerEntry.first;
                        size_t      const  numChunks = destinationWorkerEntry.second;

                        LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish: "
                             << " worker: " << worker
                             << " worker2chunks.count(sourceWorker): " << worker2chunks.count(worker));

                        if (not worker2chunks.at(worker).count(chunk)) {
                            if (numChunks < minNumChunks) {
                                destinationWorker = worker;
                                minNumChunks      = numChunks;
                            }
                        }
                    }

                    // Found a chunk which can be ptentially moved at a suitable destination.
                    // Record this in the plan. Update chunk disposition for the next
                    // iteration (if any) over the number of extra chunks of the worker.
                    //
                    // NOTE: it's perfectly safe to update the maps because the iterations
                    //       over the maps will get restarted from the updated states of
                    //       the maps after the 'break' statement below.
                    //
                    // TODO: consider geting rid of 'destinationWorker2numChunks' because
                    //       'worker2chunks' already allows to count the number of chunks
                    //        per each worker. The only trick would be to exclude workers
                    //        which are registered in 'sourceWorkerEntry'.

                    if (not destinationWorker.empty()) {
                        _replicaData.plan[chunk][sourceWorker] = destinationWorker;
                        destinationWorker2numChunks[destinationWorker]++;
                        worker2chunks              [destinationWorker][chunk] = true;

                        // Done, found enough candidate chunks to move elswhere
                        if (not --numExtraChunks) break;
                    }
                }
            }
        }

        // Finish right away if the 'estimate' mode requested.        
        if (_estimateOnly) {
            setState (State::FINISHED, ExtendedState::SUCCESS);
            break;
        }

        // Now submit chunk movement requests for chunks which could be
        // locked. Limit the number of migrated chunks to avoid overloading
        // the cluster with too many simultaneous requests.
        //
        // TODO: the chunk migration limit should be specifid via the configuration.

        auto self = shared_from_base<RebalanceJob>();

        size_t numFailedLocks = 0;

        for (auto const& chunkEntry: _replicaData.plan) {
            unsigned int const chunk = chunkEntry.first;
            if (not _controller->serviceProvider().chunkLocker().lock({_databaseFamily, chunk}, _id)) {
                ++_numFailedLocks;
                continue;
            }
            for (auto const& sourceWorkerEntry: chunkEntry.second) {
                std::string const& sourceWorker      = sourceWorkerEntry.first;
                std::string const& destinationWorker = sourceWorkerEntry.second;
                
                auto job = MoveReplicaJob::create (
                    _databaseFamily,
                    chunk,
                    sourceWorker,
                    destinationWorker,
                    true,   /* purge */
                    _controller,
                    [self](MoveReplicaJob::pointer job) { self->onJobFinish(job); }
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
                setState (State::FINISHED, ExtendedState::SUCCESS);
            } else {
                // Start another iteration by requesting the fresh state of
                // chunks within the family or until it all fails.
                restart ();
            }
        }

    } while (false);
    
    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED)
        notify();
}

void
RebalanceJob::onJobFinish (MoveReplicaJob::pointer job) {

    std::string  const databaseFamily    = job->databaseFamily(); 
    unsigned int const chunk             = job->chunk();
    std::string  const sourceWorker      = job->sourceWorker(); 
    std::string  const destinationWorker = job->destinationWorker(); 

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onJobFinish"
         << "  databaseFamily="    << databaseFamily
         << "  chunk="             << chunk
         << "  sourceWorker="      << sourceWorker
         << "  destinationWorker=" << destinationWorker);

    do {
        // This lock will be automatically release beyon this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Make sure the chunk is released if this was the last job in
        // its scope regardless of the completion status of the job.

        _chunk2jobs.at(chunk).erase(sourceWorker);
        if (_chunk2jobs.at(chunk).empty()) {
            _chunk2jobs.erase(chunk);
            Chunk chunkObj {_databaseFamily, chunk};
            _controller->serviceProvider().chunkLocker().release(chunkObj);
        }

        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) return;

        // Update counters and object state if needed.

        if (job->extendedState() == Job::ExtendedState::SUCCESS) {
            
            // Copy over data from the job

            MoveReplicaJobResult const& replicaData = job->getReplicaData();

            for (auto const& replica: replicaData.createdReplicas) {
                _replicaData.createdReplicas.emplace_back(replica);
            }
            for (auto const& databaseEntry: replicaData.createdChunks.at(chunk)) {
                std::string const& database = databaseEntry.first;
                ReplicaInfo const& replica  = databaseEntry.second.at(destinationWorker);

                _replicaData.createdChunks[chunk][database][destinationWorker] = replica;
            }
            for (auto const& replica: replicaData.deletedReplicas) {
                _replicaData.deletedReplicas.emplace_back(replica);
            }
            for (auto const& databaseEntry: replicaData.deletedChunks.at(chunk)) {
                std::string const& database = databaseEntry.first;
                ReplicaInfo const& replica  = databaseEntry.second.at(sourceWorker);

                _replicaData.deletedChunks[chunk][database][sourceWorker] = replica;
            }
        }
        
        // Evaluate the status of on-going operations to see if the job
        // has finished.

        size_t numLaunched;
        size_t numFinished;
        size_t numSuccess;
    
        ::countJobStates (numLaunched, numFinished, numSuccess,
                          _moveReplicaJobs);

        if (numFinished == numLaunched) {
            if (numSuccess == numLaunched) {
                // Make another iteration (and another one, etc. as many as needed)
                // before it succeeds or fails.
                //
                // NOTE: a condition for this jobs is to succeed is evaluated in
                //       the precursor job completion code.
                restart ();
            } else {
                setState (State::FINISHED, ExtendedState::FAILED);
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED)
        notify ();
}

}}} // namespace lsst::qserv::replica_core