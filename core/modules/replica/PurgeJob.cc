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
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

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

PurgeJob::pointer PurgeJob::create(
                        std::string const& databaseFamily,
                        unsigned int numReplicas,
                        Controller::pointer const& controller,
                        std::string const& parentJobId,
                        callback_type onFinish,
                        Job::Options const& options) {
    return PurgeJob::pointer(
        new PurgeJob(databaseFamily,
                     numReplicas,
                     controller,
                     parentJobId,
                     onFinish,
                     options));
}

PurgeJob::PurgeJob(std::string const& databaseFamily,
                   unsigned int numReplicas,
                   Controller::pointer const& controller,
                   std::string const& parentJobId,
                   callback_type onFinish,
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
    _controller->serviceProvider()->chunkLocker().release(_id);
}

PurgeJobResult const& PurgeJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) { return _replicaData; }

    throw std::logic_error (
        "PurgeJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void PurgeJob::track(bool progressReport,
                     bool errorReport,
                     bool chunkLocksReport,
                     std::ostream& os) const {

    if (_state == State::FINISHED) { return; }

    if (_findAllJob) {
        _findAllJob->track(progressReport,
                           errorReport,
                           chunkLocksReport,
                           os);
    }
    util::BlockPost blockPost(1000, 2000);

    while (_numFinished < _numLaunched) {
        blockPost.wait();
        if (progressReport) {
            os  << "PurgeJob::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
        }
        if (chunkLocksReport) {
            os  << "PurgeJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
                << _controller->serviceProvider()->chunkLocker().locked(_id);
        }
    }
    if (progressReport) {
        os  << "PurgeJob::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;
    }
    if (chunkLocksReport) {
        os  << "PurgeJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
            << _controller->serviceProvider()->chunkLocker().locked(_id);
    }
}

void PurgeJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<PurgeJob>();

    _findAllJob = FindAllJob::create(
        _databaseFamily,
        _controller,
        _id,
        [self] (FindAllJob::pointer job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void PurgeJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob and (_findAllJob->state() != State::FINISHED)) {
        _findAllJob->cancel();
    }
    _findAllJob = nullptr;

    for (auto const& ptr: _jobs) {
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

void PurgeJob::restart() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    if (_findAllJob or (_numLaunched != _numFinished)) {
        throw std::logic_error ("PurgeJob::restart ()  not allowed in this object state");
    }
    _jobs.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
}

void PurgeJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<PurgeJob>();
        _onFinish(self);
    }
}

void PurgeJob::onPrecursorJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    do {
        // This lock will be automatically release beyon this scope
        // to allow deadlock-free client notifications (see the end of the method)
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled
        if (_state == State::FINISHED) { return; }

        //////////////////////////////////////////////////////////////////////////
        // Do not proceed with the replication effort in case of any problems with
        // the precursor job.

        if (_findAllJob->extendedState() != ExtendedState::SUCCESS) {
            finish(ExtendedState::FAILED);
            break;
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

        for (auto const& chunk2workers: replicaData.isGood) {
            unsigned int const  chunk    = chunk2workers.first;
            auto         const& replicas = chunk2workers.second;

            // skip the special chunk which must be present on all workers
            if (chunk == 1234567890) { continue; }

            size_t const numReplicas = replicas.size();
            if (numReplicas > _numReplicas) {
                LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
                     << "  chunk=" << chunk
                     << ", numReplicas=" << numReplicas
                     << ", _numReplicas=" << _numReplicas);
                chunk2numReplicas2delete[chunk] = numReplicas - _numReplicas;
            }
        }

        // The 'occupancy' map or workers which will be used by the replica
        // removal algorithm later. The map is initialized below is based on
        // results reported by the precursor job and it will also be dynamically
        // updated by the algorithm as new replica removal jobs for workers will
        // be issued.
        //
        // Note, this map includes chunks in any state.
        //
        std::map<std::string, size_t> worker2occupancy;

        for (auto const& chunk2databases: replicaData.chunks) {
            for (auto const& database2workers: chunk2databases.second) {
                for (auto const& worker2info: database2workers.second) {
                    std::string const& worker = worker2info.first;
                    worker2occupancy[worker]++;
                }
            }
        }
        for (auto const& entry: worker2occupancy) {
            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
                 << "  worker=" << entry.first
                 << ", occupancy=" << entry.second);
        }

        /////////////////////////////////////////////////////////////////////
        // Check which chunks are over-represented. Then find a least loaded
        // worker and launch a replica removal jobs.

        auto self = shared_from_base<PurgeJob>();

        for (auto const& chunk2replicas: chunk2numReplicas2delete) {

            unsigned int const chunk              = chunk2replicas.first;
            int                numReplicas2delete = chunk2replicas.second;

            LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
                 << "  chunk=" << chunk
                 << ", numReplicas2delete=" << numReplicas2delete);

            // Chunk locking is mandatory. If it's not possible to do this now then
            // the job will need to make another attempt later.

            if (not _controller->serviceProvider()->chunkLocker().lock({_databaseFamily, chunk}, _id)) {
                ++_numFailedLocks;
                LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish"
                     << "  chunk=" << chunk
                     << ", _numFailedLocks=" << _numFailedLocks);
                continue;
            }

            // This list of workers will be reduced as the replica will get deleted
            std::list<std::string> goodWorkersOfThisChunk;
            for (auto const& entry: replicaData.isGood.at(chunk)) {
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

                size_t      maxNumChunks = 0;   // will get updated witin the next loop
                std::string targetWorker;       // will be set to the best worker inwhen the loop is over

                for (auto const& worker: goodWorkersOfThisChunk) {
                    if (targetWorker.empty() or (worker2occupancy[worker] > maxNumChunks)) {
                        maxNumChunks = worker2occupancy[worker];
                        targetWorker = worker;
                    }
                }
                if (targetWorker.empty() or not maxNumChunks) {
                    LOGS(_log, LOG_LVL_ERROR, context() << "onPrecursorJobFinish  "
                         << "failed to find a target worker for chunk: " << chunk);
                    finish(ExtendedState::FAILED);
                    break;
                }

                // Remove the selct worker from the list, so that the next iteration (if the one
                // will happen) will be not considering this worker fro deletion.

                goodWorkersOfThisChunk.remove(targetWorker);

                // Finally, launch and register for further tracking deletion
                // a job which will affect all participating databases

                auto ptr = DeleteReplicaJob::create(
                    databaseFamily(),
                    chunk,
                    targetWorker,
                    _controller,
                    _id,
                    [self] (DeleteReplicaJob::pointer const& job) {
                        self->onDeleteJobFinish(job);
                    },
                    options()   // inherit from the current job
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
            if (_state == State::FINISHED) { break; }
        }
        if (_state == State::FINISHED) { break; }

        // Finish right away if no problematic chunks found
        if (not _jobs.size()) {
            if (not _numFailedLocks) {
                finish(ExtendedState::SUCCESS);
                break;
            } else {
                // Some of the chuks were locked and yet, no sigle job was
                // lunched. Hence we should start another iteration by requesting
                // the fresh state of the chunks within the family.
                restart();
                return;
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify(); }
}

void PurgeJob::onDeleteJobFinish(DeleteReplicaJob::pointer const& job) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onDeleteJobFinish"
         << "  databaseFamily=" << job->databaseFamily()
         << "  worker="         << job->worker()
         << "  chunk="          << job->chunk());

    do {
        // This lock will be automatically release beyond this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled
        if (_state == State::FINISHED) {
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
            for (auto const& databaseEntry: jobReplicaData.chunks.at(job->chunk())) {
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
        //
        _chunk2jobs.at(job->chunk()).erase(job->worker());
        if (_chunk2jobs.at(job->chunk()).empty()) {
            _chunk2jobs.erase(job->chunk());
            release(job->chunk());
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
                    break;
                }
            } else {
                finish(ExtendedState::FAILED);
                break;
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify(); }
}

void PurgeJob::release(unsigned int chunk) {
    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);
    Chunk chunkObj {databaseFamily(), chunk};
    _controller->serviceProvider()->chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica
