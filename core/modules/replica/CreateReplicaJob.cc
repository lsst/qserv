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
#include "replica/CreateReplicaJob.h"

// System headers
#include <algorithm>
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ErrorReporting.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.CreateReplicaJob");

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

Job::Options const& CreateReplicaJob::defaultOptions() {
    static Job::Options const options{
        -2,     /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}

CreateReplicaJob::pointer CreateReplicaJob::create(std::string const& databaseFamily,
                                                   unsigned int chunk,
                                                   std::string const& sourceWorker,
                                                   std::string const& destinationWorker,
                                                   Controller::pointer const& controller,
                                                   std::string const& parentJobId,
                                                   callback_type onFinish,
                                                   Job::Options const& options) {
    return CreateReplicaJob::pointer(
        new CreateReplicaJob(databaseFamily,
                           chunk,
                           sourceWorker,
                           destinationWorker,
                           controller,
                           parentJobId,
                           onFinish,
                           options));
}

CreateReplicaJob::CreateReplicaJob(std::string const& databaseFamily,
                                   unsigned int chunk,
                                   std::string const& sourceWorker,
                                   std::string const& destinationWorker,
                                   Controller::pointer const& controller,
                                   std::string const& parentJobId,
                                   callback_type onFinish,
                                   Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "CREATE_REPLICA",
            options),
        _databaseFamily(databaseFamily),
        _chunk(chunk),
        _sourceWorker(sourceWorker),
        _destinationWorker(destinationWorker),
        _onFinish(onFinish) {

    if (not _controller->serviceProvider()->config()->isKnownDatabaseFamily(_databaseFamily)) {
        throw std::invalid_argument(
                        "CreateReplicaJob::CreateReplicaJob ()  the database family is unknown: " +
                        _databaseFamily);
    }
    _controller->serviceProvider()->assertWorkerIsValid(_sourceWorker);
    _controller->serviceProvider()->assertWorkerIsValid(_destinationWorker);
}

CreateReplicaJobResult const& CreateReplicaJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) { return _replicaData; }

    throw std::logic_error(
        "CreateReplicaJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void CreateReplicaJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Make sure no such replicas exist yet at the destination

    std::vector<ReplicaInfo> destinationReplicas;
    if (not _controller->serviceProvider()->databaseServices()->findWorkerReplicas(
                destinationReplicas,
                chunk(),
                destinationWorker(),
                databaseFamily())) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** failed to find replicas ** "
             << " chunk: "  << chunk()
             << " worker: " << destinationWorker());

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }
    if (destinationReplicas.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** destination worker already has " << destinationReplicas.size() << " replicas ** "
             << " chunk: "  << chunk()
             << " worker: " << destinationWorker());

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
    if (not _controller->serviceProvider()->databaseServices()->findWorkerReplicas(
                sourceReplicas,
                chunk(),
                sourceWorker(),
                databaseFamily())) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  ** failed to find replicas ** "
             << " chunk: "  << chunk()
             << " worker: " << sourceWorker());

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }
    if (not sourceReplicas.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** source worker has no replicas to be moved ** "
             << " chunk: "  << chunk()
             << " worker: " << sourceWorker());

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }

    // Launch the replication requests first. After (if) they all will
    // succeed the next optional stage will be launched to remove replicas
    // from the source worker.
    //
    // VERY IMPORTANT: the requests are sent for participating databases
    // only because some catalogs may not have a full coverage

    auto self = shared_from_base<CreateReplicaJob>();

    for (auto const& replica: sourceReplicas) {
        ReplicationRequest::pointer const ptr =
            _controller->replicate(
                destinationWorker(),
                sourceWorker(),
                replica.database(),
                chunk(),
                [self] (ReplicationRequest::pointer ptr) {
                    self->onRequestFinish(ptr);
                },
                options().priority,
                true,   /* keepTracking */
                true,   /* allowDuplicate */
                _id     /* jobId */);
        _requests.emplace_back(ptr);
    }
    setState(State::IN_PROGRESS);
}

void CreateReplicaJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto const& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            _controller->stopReplication(
                destinationWorker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }
    _requests.clear();
}

void CreateReplicaJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<CreateReplicaJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void CreateReplicaJob::onRequestFinish(ReplicationRequest::pointer const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish(ReplicationeRequest)"
         << "  database="          << request->database()
         << "  destinationWorker=" << destinationWorker()
         << "  sourceWorker="      << sourceWorker()
         << "  chunk="             << chunk());

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) { return; }

    do {
        // This lock will be automatically release beyon this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Update stats
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _replicaData.replicas.emplace_back(request->responseData());
            _replicaData.chunks[chunk()][request->database()][destinationWorker()] = request->responseData();
        }

        // Evaluate the status of on-going operations to see if the replica creation
        // stage has finished.

        size_t numLaunched;
        size_t numFinished;
        size_t numSuccess;

        ::countRequestStates(numLaunched, numFinished, numSuccess,
                             _requests);

        if (numFinished == numLaunched) {
            if (numSuccess == numLaunched) {

                // Notify Qserv about the change in a disposposition of replicas
                //
                // NOTE: The current implementation will not be affected by a result
                //       of the operation. Neither any upstream notifications will be
                //       sent to a requestor of this job.

                ServiceProvider::pointer const serviceProvider = _controller->serviceProvider();
                if (serviceProvider->config()->xrootdAutoNotify()) {
                    qservAddReplica(chunk(),
                                    databaseFamily(),
                                    destinationWorker());
                }
                finish(ExtendedState::SUCCESS);
            } else {
                finish(ExtendedState::FAILED);
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify(); }
}

}}} // namespace lsst::qserv::replica
