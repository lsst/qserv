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
#include "replica/FindAllJob.h"

// System headers
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ServiceProvider.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindAllJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& FindAllJob::defaultOptions() {
    static Job::Options const options{
        0,      /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}

FindAllJob::Ptr FindAllJob::create(std::string const& databaseFamily,
                                   Controller::Ptr const& controller,
                                   std::string const& parentJobId,
                                   CallbackType onFinish,
                                   Job::Options const& options) {
    return FindAllJob::Ptr(
        new FindAllJob(databaseFamily,
                       controller,
                       parentJobId,
                       onFinish,
                       options));
}

FindAllJob::FindAllJob(std::string const& databaseFamily,
                       Controller::Ptr const& controller,
                       std::string const& parentJobId,
                       CallbackType onFinish,
                       Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "FIND_ALL",
            options),
        _databaseFamily(databaseFamily),
        _databases(controller->serviceProvider()->config()->databases(databaseFamily)),
        _onFinish(onFinish),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

FindAllJobResult const& FindAllJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (state() == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "FindAllJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::string FindAllJob::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily());
}

void FindAllJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<FindAllJob>();

    for (auto&& worker: controller()->serviceProvider()->config()->workers()) {
        for (auto&& database: _databases) {
            _requests.push_back(
                controller()->findAllReplicas(
                    worker,
                    database,
                    [self] (FindAllRequest::Ptr request) {
                        self->onRequestFinish(request);
                    },
                    options().priority,
                    true,   /* keepTracking*/
                    id()    /* jobId */
                )
            );
            _numLaunched++;
        }
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (not _numLaunched) setState(lock, State::FINISHED);
    else                  setState(lock, State::IN_PROGRESS);
}

void FindAllJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            controller()->stopReplicaFindAll(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                id()        /* jobId */);
        }
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void FindAllJob::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish) {
        _onFinish(shared_from_base<FindAllJob>());
    }
}

void FindAllJob::onRequestFinish(FindAllRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  database=" << request->database()
         << " worker=" << request->worker()
         << " state=" << request->state2string(request->state())
         << " extendedState=" << request->state2string(request->extendedState()));

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.
    
    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onRequestFinish[" + request->id() + "]");

    if (state() == State::FINISHED) return;

    // Update counters and object state if needed.
    _numFinished++;
    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        _numSuccess++;
        ReplicaInfoCollection const& infoCollection = request->responseData();
        _replicaData.replicas.push_back(infoCollection);
        for (auto&& info: infoCollection) {
            _replicaData.chunks.atChunk(info.chunk())
                               .atDatabase(info.database())
                               .atWorker(info.worker()) = info;
        }
        _replicaData.workers[request->worker()] = true;
    } else {
        _replicaData.workers[request->worker()] = false;
    }

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  database=" << request->database()
         << " worker=" << request->worker()
         << " _numLaunched=" << _numLaunched
         << " _numFinished=" << _numFinished
         << " _numSuccess=" << _numSuccess);

     // Recompute the final state if this was the last requst
     // before finalizing the object state and notifying clients.

     if (_numFinished == _numLaunched) {

         // Databases participating in a chunk

         for (auto chunk: _replicaData.chunks.chunkNumbers()) {
             for (auto&& database: _replicaData.chunks.chunk(chunk).databaseNames()) {
                 _replicaData.databases[chunk].push_back(database);
             }
         }

         // Workers hosting complete chunks

         for (auto chunk: _replicaData.chunks.chunkNumbers()) {
             auto chunkMap = _replicaData.chunks.chunk(chunk);

             for (auto&& database: chunkMap.databaseNames()) {
                 auto databaseMap = chunkMap.database(database);

                 for (auto &&worker: databaseMap.workerNames()) {
                     ReplicaInfo const& replica = databaseMap.worker(worker);

                     if (replica.status() == ReplicaInfo::Status::COMPLETE) {
                         _replicaData.complete[chunk][database].push_back(worker);
                     }
                 }
             }
         }

         // Compute the 'co-location' status of chunks on all participating workers
         //
         // ATTENTION: this algorithm won't conider the actual status of
         //            chunk replicas (if they're complete, corrupts, etc.).

         for (auto chunk: _replicaData.chunks.chunkNumbers()) {
             auto chunkMap = _replicaData.chunks.chunk(chunk);

             // Build a list of participating databases for this chunk,
             // and build a list of databases for each worker where the chunk
             // is present.
             //
             // NOTE: Single-database chunks are always colocated. Note that
             //       the loop over databases below has exactly one iteration.

             std::map<std::string, size_t> worker2numDatabases;

             for (auto&& database: chunkMap.databaseNames()) {
                 auto databaseMap = chunkMap.database(database);

                 for (auto&& worker: databaseMap.workerNames()) {
                     worker2numDatabases[worker]++;
                 }
             }

             // Crosscheck the number of databases present on each worker
             // against the number of all databases participated within
             // the chunk and decide for which of those workers the 'colocation'
             // requirement is met.

             for (auto&& entry: worker2numDatabases) {
                 std::string const& worker       = entry.first;
                 size_t      const  numDatabases = entry.second;

                 _replicaData.isColocated[chunk][worker] =
                    _replicaData.databases[chunk].size() == numDatabases;
            }
        }

        // Compute the 'goodness' status of each chunk

        for (auto&& chunk2workers: _replicaData.isColocated) {
            unsigned int const chunk = chunk2workers.first;

            for (auto&& worker2collocated: chunk2workers.second) {
                std::string const& worker = worker2collocated.first;
                bool        const  isColocated = worker2collocated.second;

                // Start with the "as good as colocated" assumption, then drill down
                // into chunk participation in all databases on that worker to see
                // if this will change.
                //
                // NOTE: watch for a little optimization if the replica is not
                //       colocated.

                bool isGood = isColocated;
                if (isGood) {

                    auto chunkMap = _replicaData.chunks.chunk(chunk);

                    for (auto&& database: chunkMap.databaseNames()) {
                        auto databaseMap = chunkMap.database(database);

                        for (auto&& thisWorker: databaseMap.workerNames()) {
                            if (worker == thisWorker) {
                                ReplicaInfo const& replica = databaseMap.worker(thisWorker);
                                isGood = isGood and (replica.status() == ReplicaInfo::Status::COMPLETE);
                            }
                        }
                    }
                }
                _replicaData.isGood[chunk][worker] = isGood;
            }
        }
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS :
                                                   ExtendedState::FAILED);
    }
}

}}} // namespace lsst::qserv::replica
