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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindAllJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

FindAllJob::pointer FindAllJob::create(std::string const& databaseFamily,
                                       Controller::pointer const& controller,
                                       std::string const& parentJobId,
                                       callback_type onFinish,
                                       int  priority,
                                       bool exclusive,
                                       bool preemptable) {
    return FindAllJob::pointer(
        new FindAllJob(databaseFamily,
                       controller,
                       parentJobId,
                       onFinish,
                       priority,
                       exclusive,
                       preemptable));
}

FindAllJob::FindAllJob(std::string const& databaseFamily,
                       Controller::pointer const& controller,
                       std::string const& parentJobId,
                       callback_type onFinish,
                       int  priority,
                       bool exclusive,
                       bool preemptable)
    :   Job(controller,
            parentJobId,
            "FIND_ALL",
            priority,
            exclusive,
            preemptable),
        _databaseFamily(databaseFamily),
        _databases(controller->serviceProvider()->config()->databases(databaseFamily)),
        _onFinish(onFinish),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

FindAllJobResult const& FindAllJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) { return _replicaData; }

    throw std::logic_error(
        "FindAllJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void FindAllJob::track(bool progressReport,
                       bool errorReport,
                       bool chunkLocksReport,
                       std::ostream& os) const {

    if (_state == State::FINISHED) { return; }

    util::BlockPost blockPost(1000, 2000);

    while (_numFinished < _numLaunched) {
        blockPost.wait();

        if (progressReport) {
            os  << "FindAllJob::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
        }
        if (chunkLocksReport) {
            os  << "FindAllJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
                << _controller->serviceProvider()->chunkLocker().locked(_id);
        }
    }
    if (progressReport) {
        os  << "FindAllJob::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;
    }
    if (chunkLocksReport) {
        os  << "FindAllJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
            << _controller->serviceProvider()->chunkLocker().locked(_id);
    }
    if (errorReport and (_numLaunched - _numSuccess)) {
        replica::reportRequestState(_requests, os);
    }
}

void FindAllJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<FindAllJob>();

    for (auto const& worker: _controller->serviceProvider()->config()->workers()) {
        for (auto const& database: _databases) {
            _requests.push_back(
                _controller->findAllReplicas(
                    worker,
                    database,
                    [self] (FindAllRequest::pointer request) {
                        self->onRequestFinish(request);
                    },
                    0,      /* priority */
                    true,   /* keepTracking*/
                    _id     /* jobId */
                )
            );
            _numLaunched++;
        }
    }

    // In case if no workers or database are present in the Configuration
    // at this time.
    if (not _numLaunched) { setState(State::FINISHED); }
    else                  { setState(State::IN_PROGRESS); }
}

void FindAllJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto const& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            _controller->stopReplicaFindAll(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
        }
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void FindAllJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<FindAllJob>();
        _onFinish(self);
    }
}

void FindAllJob::onRequestFinish(FindAllRequest::pointer const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  database=" << request->database()
         << " worker=" << request->worker());

    do {
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled
        if (_state == State::FINISHED) { return; }

        // Update counters and object state if needed.
        _numFinished++;
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _numSuccess++;
            ReplicaInfoCollection const& infoCollection = request->responseData();
            _replicaData.replicas.emplace_back (infoCollection);
            for (auto const& info: infoCollection) {
                _replicaData.chunks[info.chunk()][info.database()][info.worker()] = info;
            }
            _replicaData.workers[request->worker()] = true;
        } else {
            _replicaData.workers[request->worker()] = false;
        }
        if (_numFinished == _numLaunched) {
            setState(State::FINISHED,
                     _numSuccess == _numLaunched ? ExtendedState::SUCCESS :
                                                   ExtendedState::FAILED);
        }
    } while (false);

    // Note that access to the job's public API shoul not be locked while
    // notifying a caller (if the callback function was povided) in order to avoid
    // the circular deadlocks.

    if (_state == State::FINISHED) {

        // Databases participating in a chunk
        //
        for (auto const& chunk2databases: _replicaData.chunks) {
            unsigned int const chunk = chunk2databases.first;

            for (auto const& database2workers: chunk2databases.second) {
                std::string const& database = database2workers.first;

                _replicaData.databases[chunk].push_back(database);
            }
        }

        // Workers hosting complete chunks
        //
        for (auto const& chunk2databases: _replicaData.chunks) {
            unsigned int const chunk = chunk2databases.first;

            for (auto const& database2workers: chunk2databases.second) {
                std::string const& database = database2workers.first;

                for (auto const& worker2info: database2workers.second) {
                    std::string const& worker  = worker2info.first;
                    ReplicaInfo const& replica = worker2info.second;

                    if (replica.status() == ReplicaInfo::Status::COMPLETE)
                        _replicaData.complete[chunk][database].push_back(worker);
                }
            }
        }

        // Compute the 'co-location' status of chunks on all participating workers
        //
        // ATTENTION: this algorithm won't conider the actual status of
        //            chunk replicas (if they're complete, corrupts, etc.).
        //
        for (auto const& chunk2databases: _replicaData.chunks) {
            unsigned int const chunk = chunk2databases.first;

            // Build a list of participating databases for this chunk,
            // and build a list of databases for each worker where the chunk
            // is present.
            //
            // NOTE: Single-database chunks are always colocated. Note that
            //       the loop over databases below has exactly one iteration.

            std::map<std::string, size_t> worker2numDatabases;

            for (auto const& database2workers: chunk2databases.second) {
                for (auto const& worker2info: database2workers.second) {
                    std::string const& worker = worker2info.first;
                    worker2numDatabases[worker]++;
                }
            }

            // Crosscheck the number of databases present on each worker
            // against the number of all databases participated within
            // the chunk and decide for which of those workers the 'colocation'
            // requirement is met.

            for (auto const& entry: worker2numDatabases) {
                std::string const& worker       = entry.first;
                size_t      const  numDatabases = entry.second;

                _replicaData.isColocated[chunk][worker] =
                    _replicaData.databases[chunk].size() == numDatabases;
            }
        }

        // Compute the 'goodness' status of each chunk

        for (auto const& chunk2workers: _replicaData.isColocated) {
            unsigned int const chunk = chunk2workers.first;

            for (auto const& worker2collocated: chunk2workers.second) {
                std::string const& worker      = worker2collocated.first;
                bool        const  isColocated = worker2collocated.second;

                // Start with the "as good as colocated" assumption, then drill down
                // into chunk participation in all databases on that worker to see
                // if this will change.
                //
                // NOTE: watch for a little optimization if the replica is not
                //       colocated.

                bool isGood = isColocated;
                if (isGood) {
                    for (auto const& chunk2databases: _replicaData.chunks[chunk]) {
                        for (auto const& database2workers: chunk2databases.second) {
                            if (worker == database2workers.first) {
                                ReplicaInfo const& replica = database2workers.second;
                                isGood = isGood and (replica.status() == ReplicaInfo::Status::COMPLETE);
                            }
                        }
                    }
                }
                _replicaData.isGood[chunk][worker] = isGood;
            }
        }

        // Finally, notify a caller
        notify();
    }
}

}}} // namespace lsst::qserv::replica
