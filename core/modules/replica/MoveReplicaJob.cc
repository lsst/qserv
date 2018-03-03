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
#include "replica/MoveReplicaJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/BlockPost.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceProvider.h"


// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.MoveReplicaJob");

template <class COLLECTION>
void countRequestStates(size_t& numLaunched,
                        size_t& numFinished,
                        size_t& numSuccess,
                        COLLECTION const& collection) {

    using namespace lsst::qserv::replica;

    numLaunched = collection.size();
    numFinished = 0;
    numSuccess  = 0;

    for (auto const& ptr: collection) {
        if (ptr->state() == Request::State::FINISHED) {
            numFinished++;
            if (ptr->extendedState() == Request::ExtendedState::SUCCESS) {
                numSuccess++;
            }
        }
    }
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

MoveReplicaJob::pointer MoveReplicaJob::create(std::string const&         databaseFamily,
                                               unsigned int               chunk,
                                               std::string const&         sourceWorker,
                                               std::string const&         destinationWorker,
                                               bool                       purge,
                                               Controller::pointer const& controller,
                                               callback_type              onFinish,
                                               int                        priority,
                                               bool                       exclusive,
                                               bool                       preemptable) {
    return MoveReplicaJob::pointer(
        new MoveReplicaJob(databaseFamily,
                           chunk,
                           sourceWorker,
                           destinationWorker,
                           purge,
                           controller,
                           onFinish,
                           priority,
                           exclusive,
                           preemptable));
}

MoveReplicaJob::MoveReplicaJob(std::string const&         databaseFamily,
                               unsigned int               chunk,
                               std::string const&         sourceWorker,
                               std::string const&         destinationWorker,
                               bool                       purge,
                               Controller::pointer const& controller,
                               callback_type              onFinish,
                               int                        priority,
                               bool                       exclusive,
                               bool                       preemptable)
    :   Job(controller,
            "MOVE_REPLICA",
            priority,
            exclusive,
            preemptable),
        _databaseFamily(databaseFamily),
        _chunk(chunk),
        _sourceWorker(sourceWorker),
        _destinationWorker(destinationWorker),
        _purge(purge),
        _onFinish(onFinish) {

    if (not _controller->serviceProvider().config()->isKnownDatabaseFamily(_databaseFamily)) {
        throw std::invalid_argument(
                        "MoveReplicaJob::MoveReplicaJob ()  the database family is unknown: " +
                        _databaseFamily);
    }
    _controller->serviceProvider().assertWorkerIsValid(_sourceWorker);
    _controller->serviceProvider().assertWorkerIsValid(_destinationWorker);
}

MoveReplicaJobResult const& MoveReplicaJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) { return _replicaData; }

    throw std::logic_error(
        "MoveReplicaJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void MoveReplicaJob::track(bool progressReport,
                           bool errorReport,
                           bool chunkLocksReport,
                           std::ostream& os) const {

    if (_state == State::FINISHED) { return; }
 
    BlockPost blockPost(1000, 2000);

    size_t numLaunched;
    size_t numFinished;
    size_t numSuccess;

    while (true) {
        blockPost.wait();

        ::countRequestStates(numLaunched, numFinished, numSuccess,
                             _replicationRequests);
        if (progressReport) {
            os  << "MoveReplicaJob::track(replicateRequest)  "
                << "launched: " << numLaunched << ", "
                << "finished: " << numFinished << ", "
                << "success: "  << numSuccess
                << std::endl;
        }
        if (numLaunched == numFinished) {
            if (errorReport and (numLaunched - numSuccess)) {
                replica::reportRequestState(_replicationRequests, os);
            }
            break;
        }
    }
    while (true) {
        blockPost.wait();

        ::countRequestStates(numLaunched, numFinished, numSuccess,
                             _deleteRequests);
        if (progressReport) {
            os  << "MoveReplicaJob::track(deleteRequest)  "
                << "launched: " << numLaunched << ", "
                << "finished: " << numFinished << ", "
                << "success: "  << numSuccess
                << std::endl;
        }
        if (numLaunched == numFinished) {
            if (errorReport and (numLaunched - numSuccess)) {
                replica::reportRequestState(_deleteRequests, os);
            }
            break;
        }
    }
}

void MoveReplicaJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Make sure no such replicas exist yet at the destination

    std::vector<ReplicaInfo> destinationReplicas;
    if (not _controller->serviceProvider().databaseServices()->findWorkerReplicas(
                destinationReplicas,
                _chunk,
                _destinationWorker,
                _databaseFamily)) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** failed to find replicas ** "
             << " chunk: "  << _chunk
             << " worker: " << _destinationWorker);

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }
    if (destinationReplicas.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** destination worker already has " << destinationReplicas.size() << " replicas ** "
             << " chunk: "  << _chunk
             << " worker: " << _destinationWorker);

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }

    // Get all databases for which this chunk is in the COMPLETE state on
    // at the source worker.
    //
    // Alternative options would be:
    // 1. launching requests for all databases of the family and then see
    //    filter them on a result status (something like FILE_ROPEN)
    //
    // 2. launching FindRequest for each member of the database family to
    //    see if the chunk is available on a source node.

    std::vector<ReplicaInfo> sourceReplicas;
    if (not _controller->serviceProvider().databaseServices()->findWorkerReplicas(
                sourceReplicas,
                _chunk,
                _sourceWorker,
                _databaseFamily)) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  ** failed to find replicas ** "
             << " chunk: "  << _chunk
             << " worker: " << _sourceWorker);

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }
    if (not sourceReplicas.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** source worker has no replicas to be moved ** "
             << " chunk: "  << _chunk
             << " worker: " << _sourceWorker);

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    } 

    // Launch the replication requests first. After (if) they all will
    // succeed the next optional stage will be launched to remove replicas
    // from the source worker.

    auto self = shared_from_base<MoveReplicaJob>();

    for (auto const& replica: sourceReplicas) {
        ReplicationRequest::pointer const ptr =
            _controller->replicate(
                _destinationWorker,
                replica.worker(),
                replica.database(),
                replica.chunk(),
                [self] (ReplicationRequest::pointer ptr) {
                    self->onRequestFinish(ptr);
                },
                _priority,
                true,   /* keepTracking */
                true,   /* allowDuplicate */
                _id     /* jobId */);
        _replicationRequests.emplace_back(ptr);
    }
    setState(State::IN_PROGRESS);
}

void MoveReplicaJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto const& ptr: _replicationRequests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            _controller->stopReplication(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }
    _replicationRequests.clear();

    for (auto const& ptr: _deleteRequests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            _controller->stopReplicaDelete(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }
    _deleteRequests.clear();

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void MoveReplicaJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<MoveReplicaJob>();
        _onFinish(self);
    }
}

void MoveReplicaJob::onRequestFinish(ReplicationRequest::pointer const& request) {

    std::string  const database          = request->database(); 
    std::string  const destinationWorker = request->worker();
    std::string  const sourceWorker      = request->sourceWorker();
    unsigned int const chunk             = request->chunk();

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish(ReplicationeRequest)"
         << "  database="          << database
         << "  destinationWorker=" << destinationWorker
         << "  sourceWorker="      << sourceWorker
         << "  chunk="             << chunk);

    do {
        // This lock will be automatically release beyon this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) { return; }

        // Update stats
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _replicaData.createdReplicas.emplace_back(request->responseData());
            _replicaData.createdChunks[chunk][database][destinationWorker] = request->responseData();
        }
        
        // Evaluate the status of on-going operations to see if the replica creation
        // stage has finished.

        size_t numLaunched;
        size_t numFinished;
        size_t numSuccess;

        ::countRequestStates(numLaunched, numFinished, numSuccess,
                             _replicationRequests);

        if (numFinished == numLaunched) {
            if (numSuccess == numLaunched) {
                if (_purge) {

                    // Launch the second stage of requests to eliminate replicas
                    // from the source worker.

                    auto self = shared_from_base<MoveReplicaJob>();

                    for (auto const& replicatePtr: _replicationRequests) {
                        DeleteRequest::pointer ptr =
                            _controller->deleteReplica(
                                replicatePtr->sourceWorker(),
                                replicatePtr->database(),
                                replicatePtr->chunk(),
                                [self] (DeleteRequest::pointer ptr) {
                                    self->onRequestFinish(ptr);
                                },
                                _priority,
                                true,   /* keepTracking */
                                true,   /* allowDuplicate */
                                _id     /* jobId */
                            );
                        _deleteRequests.emplace_back(ptr);
                    }
                } else {
                    // Otherwise, we're done.
                    setState(State::FINISHED, ExtendedState::SUCCESS);
                }
            } else {
                setState(State::FINISHED, ExtendedState::FAILED);
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify(); }
}

void MoveReplicaJob::onRequestFinish(DeleteRequest::pointer const& request) {

    std::string  const database = request->database(); 
    std::string  const worker   = request->worker(); 
    unsigned int const chunk    = request->chunk();

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish(DeleteRequest)"
         << "  database=" << database
         << "  worker="   << worker
         << "  chunk="    << chunk);

    do {
        // This lock will be automatically release beyon this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) { return; }

        // Update stats
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _replicaData.deletedReplicas.emplace_back(request->responseData());
            _replicaData.deletedChunks[chunk][database][worker] = request->responseData();
        }
        
        // Evaluate the status of on-going operations to see if the job
        // has finished.

        size_t numLaunched;
        size_t numFinished;
        size_t numSuccess;

        ::countRequestStates(numLaunched, numFinished, numSuccess,
                             _deleteRequests);

        if (numFinished == numLaunched)
            setState(State::FINISHED,
                     numSuccess == numLaunched ? ExtendedState::SUCCESS :
                                                 ExtendedState::FAILED);
    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify (); }
}

}}} // namespace lsst::qserv::replica