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
#include "replica/FindAllJob.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindAllJob");

}  // namespace

namespace lsst { namespace qserv { namespace replica {

string FindAllJob::typeName() { return "FindAllJob"; }

FindAllJob::Ptr FindAllJob::create(string const& databaseFamily, bool saveReplicaInfo, bool allWorkers,
                                   Controller::Ptr const& controller, string const& parentJobId,
                                   CallbackType const& onFinish, int priority) {
    return FindAllJob::Ptr(new FindAllJob(databaseFamily, saveReplicaInfo, allWorkers, controller,
                                          parentJobId, onFinish, priority));
}

FindAllJob::FindAllJob(string const& databaseFamily, bool saveReplicaInfo, bool allWorkers,
                       Controller::Ptr const& controller, string const& parentJobId,
                       CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "FIND_ALL", priority),
          _databaseFamily(databaseFamily),
          _saveReplicaInfo(saveReplicaInfo),
          _allWorkers(allWorkers),
          _onFinish(onFinish),
          _databases(controller->serviceProvider()->config()->databases(databaseFamily)) {}

FindAllJobResult const& FindAllJob::getReplicaData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error("FindAllJob::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

list<pair<string, string>> FindAllJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("save_replica_info", bool2str(saveReplicaInfo()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<pair<string, string>> FindAllJob::persistentLogData() const {
    list<pair<string, string>> result;

    auto&& replicaData = getReplicaData();

    // Report workers failed to respond to the requests

    for (auto&& workerInfo : replicaData.workers) {
        auto&& worker = workerInfo.first;

        bool const responded = workerInfo.second;
        if (not responded) {
            result.emplace_back("failed-qserv-worker", worker);
        }
    }

    // Per-worker counters for the following categories:
    //
    //   chunks:
    //     the total number of chunks found on the workers regardless of
    //     the chunk status
    //
    //   collocated-replicas:
    //     the number of "collocated" replicas
    //
    //   good-replicas:
    //     the number of "good" replicas

    map<string, map<string, size_t>> workerCategoryCounter;

    for (auto&& infoCollection : replicaData.replicas) {
        for (auto&& info : infoCollection) {
            workerCategoryCounter[info.worker()]["chunks"]++;
        }
    }
    for (auto&& chunkItr : replicaData.isColocated) {
        for (auto&& workerItr : chunkItr.second) {
            auto&& worker = workerItr.first;
            bool const isCollocated = workerItr.second;
            if (isCollocated) workerCategoryCounter[worker]["collocated-replicas"]++;
        }
    }
    for (auto&& chunkItr : replicaData.isGood) {
        for (auto&& workerItr : chunkItr.second) {
            auto&& worker = workerItr.first;
            bool const isGood = workerItr.second;
            if (isGood) workerCategoryCounter[worker]["good-replicas"]++;
        }
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

void FindAllJob::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto const self = shared_from_base<FindAllJob>();

    auto const workerNames = allWorkers() ? controller()->serviceProvider()->config()->allWorkers()
                                          : controller()->serviceProvider()->config()->workers();

    for (auto&& worker : workerNames) {
        _replicaData.workers[worker] = false;
        for (auto&& database : _databases) {
            _workerDatabaseSuccess[worker][database] = false;
            _requests.push_back(controller()->findAllReplicas(
                    worker, database, saveReplicaInfo(),
                    [self](FindAllRequest::Ptr request) { self->_onRequestFinish(request); }, priority(),
                    true, /* keepTracking*/
                    id()  /* jobId */
                    ));
            _numLaunched++;
        }
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (not _numLaunched) finish(lock, ExtendedState::SUCCESS);
}

void FindAllJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr : _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            controller()->stopById<StopFindAllRequest>(ptr->worker(), ptr->id(), nullptr, /* onFinish */
                                                       priority(), true,                  /* keepTracking */
                                                       id() /* jobId */);
        }
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess = 0;
}

void FindAllJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<FindAllJob>(lock, _onFinish);
}

void FindAllJob::_onRequestFinish(FindAllRequest::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  database=" << request->database() << " worker=" << request->worker()
                   << " state=" << request->state2string());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "" + string(__func__) + "[" + request->id() + "]");

    if (state() == State::FINISHED) return;

    // Update counters and object state if needed.
    _numFinished++;
    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        _numSuccess++;
        ReplicaInfoCollection const& infoCollection = request->responseData();
        _replicaData.replicas.push_back(infoCollection);
        for (auto&& info : infoCollection) {
            _replicaData.chunks.atChunk(info.chunk()).atDatabase(info.database()).atWorker(info.worker()) =
                    info;
        }
        _workerDatabaseSuccess[request->worker()][request->database()] = true;
    }

    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  database=" << request->database() << " worker=" << request->worker()
                   << " _numLaunched=" << _numLaunched << " _numFinished=" << _numFinished
                   << " _numSuccess=" << _numSuccess);

    // Recompute the final state if this was the last request
    // before finalizing the object state and notifying clients.

    if (_numFinished == _numLaunched) {
        // Compute the final state of the workers participated in the operation

        for (auto const& workerEntry : _workerDatabaseSuccess) {
            auto const& worker = workerEntry.first;
            _replicaData.workers[worker] = true;
            for (auto const& databaseEntry : workerEntry.second) {
                bool const success = databaseEntry.second;
                if (not success) {
                    _replicaData.workers[worker] = false;
                    break;
                }
            }
        }

        // Databases participating in a chunk

        for (auto chunk : _replicaData.chunks.chunkNumbers()) {
            for (auto&& database : _replicaData.chunks.chunk(chunk).databaseNames()) {
                _replicaData.databases[chunk].push_back(database);
            }
        }

        // Workers hosting complete chunks

        for (auto chunk : _replicaData.chunks.chunkNumbers()) {
            auto chunkMap = _replicaData.chunks.chunk(chunk);

            for (auto&& database : chunkMap.databaseNames()) {
                auto databaseMap = chunkMap.database(database);

                for (auto&& worker : databaseMap.workerNames()) {
                    ReplicaInfo const& replica = databaseMap.worker(worker);

                    if (replica.status() == ReplicaInfo::Status::COMPLETE) {
                        _replicaData.complete[chunk][database].push_back(worker);
                    }
                }
            }
        }

        // Compute the 'collocation' status of chunks on all participating workers
        //
        // ATTENTION: this algorithm won't consider the actual status of
        //            chunk replicas (if they're complete, corrupts, etc.).

        for (auto chunk : _replicaData.chunks.chunkNumbers()) {
            auto chunkMap = _replicaData.chunks.chunk(chunk);

            // Build a list of participating databases for this chunk,
            // and build a list of databases for each worker where the chunk
            // is present.
            //
            // NOTE: Single-database chunks are always collocated. Note that
            //       the loop over databases below has exactly one iteration.

            map<string, size_t> worker2numDatabases;

            for (auto&& database : chunkMap.databaseNames()) {
                auto databaseMap = chunkMap.database(database);

                for (auto&& worker : databaseMap.workerNames()) {
                    worker2numDatabases[worker]++;
                }
            }

            // Crosscheck the number of databases present on each worker
            // against the number of all databases participated within
            // the chunk and decide for which of those workers the 'colocation'
            // requirement is met.

            for (auto&& entry : worker2numDatabases) {
                string const& worker = entry.first;
                size_t const numDatabases = entry.second;

                _replicaData.isColocated[chunk][worker] =
                        _replicaData.databases[chunk].size() == numDatabases;
            }
        }

        // Compute the 'goodness' status of each chunk

        for (auto&& chunk2workers : _replicaData.isColocated) {
            unsigned int const chunk = chunk2workers.first;

            for (auto&& worker2collocated : chunk2workers.second) {
                string const& worker = worker2collocated.first;
                bool const isColocated = worker2collocated.second;

                // Start with the "as good as collocated" assumption, then drill down
                // into chunk participation in all databases on that worker to see
                // if this will change.
                //
                // NOTE: watch for a little optimization if the replica is not
                //       collocated.

                bool isGood = isColocated;
                if (isGood) {
                    auto chunkMap = _replicaData.chunks.chunk(chunk);

                    for (auto&& database : chunkMap.databaseNames()) {
                        auto databaseMap = chunkMap.database(database);

                        for (auto&& thisWorker : databaseMap.workerNames()) {
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
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS : ExtendedState::FAILED);
    }
}

}}}  // namespace lsst::qserv::replica
