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
#include <set>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/BlockPost.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"


// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ReplicateJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

ReplicateJob::pointer ReplicateJob::create (
                            std::string const&         databaseFamily,
                            unsigned int               numReplicas,
                            Controller::pointer const& controller,
                            callback_type              onFinish,
                            bool                       bestEffort,
                            int                        priority,
                            bool                       exclusive,
                            bool                       preemptable) {
    return ReplicateJob::pointer (
        new ReplicateJob (databaseFamily,
                          numReplicas,
                          controller,
                          onFinish,
                          bestEffort,
                          priority,
                          exclusive,
                          preemptable));
}

ReplicateJob::ReplicateJob (std::string const&         databaseFamily,
                            unsigned int               numReplicas,
                            Controller::pointer const& controller,
                            callback_type              onFinish,
                            bool                       bestEffort,
                            int                        priority,
                            bool                       exclusive,
                            bool                       preemptable)

    :   Job (controller,
             "REPLICATE",
             priority,
             exclusive,
             preemptable),
        _databaseFamily (databaseFamily),
        _numReplicas (numReplicas ?
                      numReplicas :
                      controller->serviceProvider().config()->replicationLevel(databaseFamily)),
        _onFinish    (onFinish),
        _bestEffort  (bestEffort),
        _numIterations  (0),
        _numFailedLocks (0),
        _numLaunched (0),
        _numFinished (0),
        _numSuccess  (0) {
}

ReplicateJob::~ReplicateJob () {
    // Make sure all chuks locked by this job are released
    _controller->serviceProvider().chunkLocker().release(_id);
}

ReplicateJobResult const& ReplicateJob::getReplicaData () const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) {
        return _replicaData;
    }
    throw std::logic_error (
        "ReplicateJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void ReplicateJob::track (bool progressReport,
                          bool errorReport,
                          bool chunkLocksReport,
                          std::ostream& os) const {

    if (_state == State::FINISHED) {
        return;
    }
    if (_findAllJob) {
        _findAllJob->track (progressReport,
                            errorReport,
                            chunkLocksReport,
                            os);
    }
    BlockPost blockPost (1000, 2000);

    while (_numFinished < _numLaunched) {

        blockPost.wait();

        if (progressReport) {
            os  << "ReplicateJob::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
        }
        if (chunkLocksReport) {
            os  << "ReplicateJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
                << _controller->serviceProvider().chunkLocker().locked (_id);
        }
    }
    if (progressReport) {
        os  << "ReplicateJob::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;
    }
    if (chunkLocksReport) {
        os  << "ReplicateJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
            << _controller->serviceProvider().chunkLocker().locked (_id);
    }
    if (errorReport && (_numLaunched - _numSuccess)) {
        replica::reportRequestState (_requests, os);
    }
}

void ReplicateJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<ReplicateJob>();

    _findAllJob = FindAllJob::create (
        _databaseFamily,
        _controller,
        [self] (FindAllJob::pointer job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void ReplicateJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob && (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto const& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            _controller->stopReplication (
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
        }
    }
    _chunk2requests.clear();
    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void ReplicateJob::restart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");
    
    if (_findAllJob or (_numLaunched != _numFinished)) {
        throw std::logic_error ("ReplicateJob::restart ()  not allowed in this object state");
    }
    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void ReplicateJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<ReplicateJob>();
        _onFinish(self);
    }
}

void ReplicateJob::onPrecursorJobFinish () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    do {

        LOCK_GUARD;

        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) {
            return;
        }

        ////////////////////////////////////////////////////////////////////
        // Do not proceed with the replication effort unless running the job
        // under relaxed condition.
    
        if (not _bestEffort and (_findAllJob->extendedState() != ExtendedState::SUCCESS)) {
            setState(State::FINISHED, ExtendedState::FAILED);
            break;
        }

        /////////////////////////////////////////////////////////////////
        // Analyse results and prepare a replication plan to create extra
        // replocas for under-represented chunks
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
        //        updated as new replication requests targeting the corresponding
        //        workers were issued.
        //
        // ATTENTION: the read-only workers will not be considered by
        //            the algorithm. Those workers are used by different kinds
        //            of jobs.
    
        FindAllJobResult const& replicaData = _findAllJob->getReplicaData ();
    
        // The number of replicas to be created for eligible chunks
        //
        std::map<unsigned int,int> chunk2numReplicas2create;
    
        for (auto const& chunk2workers: replicaData.isGood) {
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
        // Note, this map includes chunks in any state.
        //
        std::map<std::string, size_t> worker2occupancy;
    
        // The 'black list' of workers to be avoided as new replica destinations
        // for specific chunks because they already have a replica (regardless of
        // its status) of that chunk for any database of the family
        //
        // This is done in order to avoid conflicts when attempting to place new chunk
        // replicas on that node.
        //
        std::map<std::string, std::set<unsigned int>> worker2chunks;
    
        for (auto const& chunk2databases     : replicaData.chunks) {
            for (auto const& database2workers: chunk2databases.second) {
                for (auto const& worker2info : database2workers.second) {
    
                    unsigned int const   chunk = chunk2databases.first;
                    std::string  const& worker = worker2info.first;
    
                    worker2occupancy[worker]++;
                    worker2chunks   [worker].insert(chunk);
                }
            }
        }

        // The 'white list of workers which haven't been reported as FAILED
        // by the precursor job. These workers will be considered as destinations
        // for the new replicas.
    
        std::vector<std::string> workers;
        for (auto const& worker: _controller->serviceProvider().config()->workers()) {
            if (replicaData.workers.at(worker)) {
                workers.push_back(worker);
            }
        }
        if (not workers.size()) {

            LOGS(_log, LOG_LVL_ERROR, context()
                 << "onPrecursorJobFinish  not workers are available for new replicas");

            setState(State::FINISHED, ExtendedState::FAILED);
            break;
        }

        /////////////////////////////////////////////////////////////////////
        // Check which chunks are under-represented. Then find a least loaded
        // worker and launch a replication request.
    
        auto self = shared_from_base<ReplicateJob>();
    
        for (auto const& chunk2replicas: chunk2numReplicas2create) {
    
            unsigned int const chunk              = chunk2replicas.first;
            int          const numReplicas2create = chunk2replicas.second;
    
            // Chunk locking is mandatory. If it's not possible to do this now then
            // the job will need to make another attempt later.
    
            Chunk const chunkObj{_databaseFamily, chunk};
            if (not _controller->serviceProvider().chunkLocker().lock(chunkObj, _id)) {
                ++_numFailedLocks;
                continue;
            }
    
            // Find the first available source worker which has a 'complete'
            // chunk for each participating database.
            //
            std::map<std::string, std::string> database2sourceWorker;
    
            for (auto const& database: replicaData.databases.at(chunk)) {
                for (const auto& worker: replicaData.complete.at(chunk).at(database)) {
                    database2sourceWorker[database] = worker;
                    break;
                }
                if (not database2sourceWorker.count(database)) {
                    LOGS(_log, LOG_LVL_ERROR, context()
                         << "onPrecursorJobFinish  no suitable soure worker found for chunk: "
                         << chunk);
    
                    release (chunk);
                    setState(State::FINISHED, ExtendedState::FAILED);
                    break;
                }
            }
            if (state() == State::FINISHED) {
                break;
            }

            // Iterate over the number of replicas to be created and create
            // a new one (for all participating databases) on each step
    
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
    
                    release (chunk);
                    setState(State::FINISHED, ExtendedState::FAILED);
                    break;
                }
    
                // Finally, launch and register for further tracking and replication
                // request for all participating databases
                //
                // NOTE: sources may vary from one database to another one, depending
                //       on the availability of good chunks. Meanwhile the destination
                //       is always stays the same in order to preserve the chunk colocation.
    
                for (auto const& database2worker: database2sourceWorker) {
                    std::string const& database     = database2worker.first;
                    std::string const& sourceWorker = database2worker.second;
    
                    ReplicationRequest::pointer ptr =
                        _controller->replicate (
                            destinationWorker,
                            sourceWorker,
                            database,
                            chunk,
                            [self] (ReplicationRequest::pointer ptr) {
                                self->onRequestFinish(ptr);
                            },
                            0,      /* priority */
                            true,   /* keepTracking */
                            true,   /* allowDuplicate */
                            _id     /* jobId */
                        );
    
                    _chunk2requests[chunk][destinationWorker][database] = ptr;
                    _requests.push_back (ptr);
    
                    _numLaunched++;
    
                    // Bump the worker occupancy, so that it will be taken into consideration
                    // when creating next replicas.
    
                    worker2occupancy[destinationWorker]++;
                }
            }
            if (state() == State::FINISHED) {
                break;
            }
        }
        if (state() == State::FINISHED) {
            break;
        }

        // Finish right away if no problematic chunks found
        if (not _requests.size()) {
            if (not _numFailedLocks) {
                setState (State::FINISHED, ExtendedState::SUCCESS);
                break;
            } else {
                // Some of the chuks were locked and yet, no sigle request was
                // lunched. Hence we should start another iteration by requesting
                // the fresh state of the chunks within the family.
                restart ();
                return;
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) {
        notify ();
    }
}

void ReplicateJob::onRequestFinish (ReplicationRequest::pointer request) {

    std::string  const database = request->database(); 
    std::string  const worker   = request->worker(); 
    unsigned int const chunk    = request->chunk();

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish"
         << "  database=" << database
         << "  worker="   << worker
         << "  chunk="    << chunk);


    do {
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) {
            release (chunk);
            return;
        }

        // Update counters and object state if needed.
        _numFinished++;
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _numSuccess++;
            _replicaData.replicas.emplace_back(request->responseData());
            _replicaData.chunks[chunk][database][worker] = request->responseData();
            _replicaData.workers[worker] = true;
        } else {
            _replicaData.workers[worker] = false;
        }
        
        // Make sure the chunk is released if this was the last
        // request in its scope.
        //
        _chunk2requests.at(chunk).at(worker).erase(database);
        if (_chunk2requests.at(chunk).at(worker).empty()) {
            _chunk2requests.at(chunk).erase(worker);
            if (_chunk2requests.at(chunk).empty()) {
                _chunk2requests.erase(chunk);
                release(chunk);
            }
        }

        // Evaluate the status of on-going operations to see if the job
        // has finished.
        //
        if (_numFinished == _numLaunched) {
            if (_numSuccess == _numLaunched) {
                if (_numFailedLocks) {
                    // Make another iteration (and another one, etc. as many as needed)
                    // before it succeeds or fails.
                    restart ();
                    return;
                } else {
                    setState (State::FINISHED, ExtendedState::SUCCESS);
                    break;
                }
            } else {
                setState (State::FINISHED, ExtendedState::FAILED);
                break;
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) {
        notify ();
    }
}

void ReplicateJob::release (unsigned int chunk) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);

    Chunk chunkObj {_databaseFamily, chunk};
    _controller->serviceProvider().chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica