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
#include "replica/jobs/FindAllJob.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
#include "replica/requests/http/HttpFindAllRequest.h"
#include "replica/requests/http/HttpStopRequest.h"
#else
#include "replica/requests/FindAllRequest.h"
#include "replica/requests/StopRequest.h"
#endif
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindAllJob");

}  // namespace

namespace lsst::qserv::replica {

string FindAllJob::typeName() { return "FindAllJob"; }

shared_ptr<FindAllJob> FindAllJob::create(string const& databaseFamily, bool saveReplicaInfo, bool allWorkers,
                                          shared_ptr<Controller> const& controller, string const& parentJobId,
                                          CallbackType const& onFinish, int priority) {
    return shared_ptr<FindAllJob>(new FindAllJob(databaseFamily, saveReplicaInfo, allWorkers, controller,
                                                 parentJobId, onFinish, priority));
}

FindAllJob::FindAllJob(string const& databaseFamily, bool saveReplicaInfo, bool allWorkers,
                       shared_ptr<Controller> const& controller, string const& parentJobId,
                       CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "FIND_ALL", priority),
          _databaseFamily(databaseFamily),
          _saveReplicaInfo(saveReplicaInfo),
          _allWorkers(allWorkers),
          _onFinish(onFinish) {}

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
    // Report workers failed to respond to the requests
    list<pair<string, string>> result;
    for (auto const& workerInfo : getReplicaData().workers) {
        auto const& workerName = workerInfo.first;
        bool const responded = workerInfo.second;
        if (!responded) {
            result.emplace_back("failed-qserv-worker", workerName);
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
    for (auto const& infoCollection : getReplicaData().replicas) {
        for (auto const& info : infoCollection) {
            workerCategoryCounter[info.worker()]["chunks"]++;
        }
    }
    for (auto const& chunkItr : getReplicaData().isColocated) {
        for (auto const& workerItr : chunkItr.second) {
            auto const& workerName = workerItr.first;
            bool const isCollocated = workerItr.second;
            if (isCollocated) workerCategoryCounter[workerName]["collocated-replicas"]++;
        }
    }
    for (auto const& chunkItr : getReplicaData().isGood) {
        for (auto const& workerItr : chunkItr.second) {
            auto const& workerName = workerItr.first;
            bool const isGood = workerItr.second;
            if (isGood) workerCategoryCounter[workerName]["good-replicas"]++;
        }
    }
    for (auto const& workerItr : workerCategoryCounter) {
        auto const& workerName = workerItr.first;
        string val = "worker=" + workerName;

        for (auto const& categoryItr : workerItr.second) {
            auto const& category = categoryItr.first;
            size_t const counter = categoryItr.second;
            val += " " + category + "=" + to_string(counter);
        }
        result.emplace_back("worker-stats", val);
    }
    return result;
}

void FindAllJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    auto const config = controller()->serviceProvider()->config();
    auto const databases = config->databases(databaseFamily());
    auto const workerNames = allWorkers() ? config->allWorkers() : config->workers();
    bool const keepTracking = true;
    for (auto const& workerName : workerNames) {
        _replicaData.workers[workerName] = false;
        for (auto const& databaseName : databases) {
            _workerDatabaseSuccess[workerName][databaseName] = false;
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
            _requests.push_back(HttpFindAllRequest::createAndStart(
                    controller(), workerName, databaseName, saveReplicaInfo(),
                    [self = shared_from_base<FindAllJob>()](shared_ptr<HttpFindAllRequest> const& request) {
                        self->_onRequestFinish(request);
                    },
                    priority(), keepTracking, id()));
            _numLaunched++;
        }
#else
            _requests.push_back(FindAllRequest::createAndStart(
                    controller(), workerName, databaseName, saveReplicaInfo(),
                    [self = shared_from_base<FindAllJob>()](shared_ptr<FindAllRequest> const& request) {
                        self->_onRequestFinish(request);
                    },
                    priority(), keepTracking, id()));
            _numLaunched++;
        }
#endif
    }

    // In case if no workers or database are present in the Config at this time.
    if (_numLaunched == 0) finish(lock, ExtendedState::SUCCESS);
}

void FindAllJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.
    auto const noCallbackOnFinish = nullptr;
    bool const keepTracking = true;
    for (auto const& ptr : _requests) {
        ptr->cancel();
        if (ptr->state() != RequestI::State::FINISHED) {
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
            HttpStopRequest::createAndStart(controller(), ptr->workerName(), ptr->id(), noCallbackOnFinish,
                                            priority(), keepTracking, id());
#else
            StopRequest::createAndStart(controller(), ptr->workerName(), ptr->id(), noCallbackOnFinish,
                                        priority(), keepTracking, id());
#endif
        }
    }
    _requests.clear();
    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess = 0;
}

void FindAllJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<FindAllJob>(lock, _onFinish);
}

#ifdef QSERV_REPLICA_PROTOCOL_HTTP
void FindAllJob::_onRequestFinish(shared_ptr<HttpFindAllRequest> const& request) {
#else
void FindAllJob::_onRequestFinish(shared_ptr<FindAllRequest> const& request) {
#endif
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  database=" << request->database() << " worker=" << request->workerName()
                   << " state=" << request->state2string());

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + "" + string(__func__) + "[" + request->id() + "]");
    if (state() == State::FINISHED) return;

    // Update counters and object state if needed.
    _numFinished++;
    if (request->extendedState() == RequestI::ExtendedState::SUCCESS) {
        _numSuccess++;
        ReplicaInfoCollection const& infoCollection = request->responseData();
        _replicaData.replicas.push_back(infoCollection);
        for (auto const& info : infoCollection) {
            _replicaData.chunks.atChunk(info.chunk()).atDatabase(info.database()).atWorker(info.worker()) =
                    info;
        }
        _workerDatabaseSuccess[request->workerName()][request->database()] = true;
    }
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  database=" << request->database() << " worker=" << request->workerName()
                   << " _numLaunched=" << _numLaunched << " _numFinished=" << _numFinished
                   << " _numSuccess=" << _numSuccess);

    // Recompute the final state if this was the last request
    // before finalizing the object state and notifying clients.
    if (_numFinished == _numLaunched) {
        // Compute the final state of the workers participated in the operation
        for (auto const& workerEntry : _workerDatabaseSuccess) {
            auto const& workerName = workerEntry.first;
            _replicaData.workers[workerName] = true;
            for (auto const& databaseEntry : workerEntry.second) {
                bool const success = databaseEntry.second;
                if (not success) {
                    _replicaData.workers[workerName] = false;
                    break;
                }
            }
        }

        // Databases participating in a chunk
        for (auto chunk : _replicaData.chunks.chunkNumbers()) {
            for (auto const& databaseName : _replicaData.chunks.chunk(chunk).databaseNames()) {
                _replicaData.databases[chunk].push_back(databaseName);
            }
        }

        // Workers hosting complete chunks
        for (auto chunk : _replicaData.chunks.chunkNumbers()) {
            auto chunkMap = _replicaData.chunks.chunk(chunk);
            for (auto const& databaseName : chunkMap.databaseNames()) {
                auto databaseMap = chunkMap.database(databaseName);

                for (auto const& workerName : databaseMap.workerNames()) {
                    ReplicaInfo const& replica = databaseMap.worker(workerName);
                    if (replica.status() == ReplicaInfo::Status::COMPLETE) {
                        _replicaData.complete[chunk][databaseName].push_back(workerName);
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
            for (auto const& databaseName : chunkMap.databaseNames()) {
                auto databaseMap = chunkMap.database(databaseName);
                for (auto const& workerName : databaseMap.workerNames()) {
                    worker2numDatabases[workerName]++;
                }
            }

            // Crosscheck the number of databases present on each worker
            // against the number of all databases participated within
            // the chunk and decide for which of those workers the 'colocation'
            // requirement is met.
            for (auto const& entry : worker2numDatabases) {
                string const& workerName = entry.first;
                size_t const numDatabases = entry.second;
                _replicaData.isColocated[chunk][workerName] =
                        _replicaData.databases[chunk].size() == numDatabases;
            }
        }

        // Compute the 'goodness' status of each chunk
        for (auto const& chunk2workers : _replicaData.isColocated) {
            unsigned int const chunk = chunk2workers.first;
            for (auto const& worker2collocated : chunk2workers.second) {
                string const& workerName = worker2collocated.first;
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
                    for (auto const& databaseName : chunkMap.databaseNames()) {
                        auto databaseMap = chunkMap.database(databaseName);
                        for (auto const& thisWorkerName : databaseMap.workerNames()) {
                            if (workerName == thisWorkerName) {
                                ReplicaInfo const& replica = databaseMap.worker(thisWorkerName);
                                isGood = isGood and (replica.status() == ReplicaInfo::Status::COMPLETE);
                            }
                        }
                    }
                }
                _replicaData.isGood[chunk][workerName] = isGood;
            }
        }
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS : ExtendedState::FAILED);
    }
}

}  // namespace lsst::qserv::replica
