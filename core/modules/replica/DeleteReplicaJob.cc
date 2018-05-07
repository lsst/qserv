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
#include "replica/DeleteReplicaJob.h"

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
#define LOCK(MUTEX) std::lock_guard<std::mutex> lock(MUTEX)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DeleteReplicaJob");

template <class COLLECTION>
void countRequestStates(size_t& numLaunched,
                        size_t& numFinished,
                        size_t& numSuccess,
                        COLLECTION const& collection) {

    using namespace lsst::qserv::replica;

    numLaunched = collection.size();
    numFinished = 0;
    numSuccess  = 0;

    for (auto&& ptr: collection) {
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

Job::Options const& DeleteReplicaJob::defaultOptions() {
    static Job::Options const options{
        -2,     /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}

DeleteReplicaJob::Ptr DeleteReplicaJob::create(std::string const& databaseFamily,
                                                   unsigned int chunk,
                                                   std::string const& worker,
                                                   Controller::Ptr const& controller,
                                                   std::string const& parentJobId,
                                                   CallbackType onFinish,
                                                   Job::Options const& options) {
    return DeleteReplicaJob::Ptr(
        new DeleteReplicaJob(databaseFamily,
                             chunk,
                             worker,
                             controller,
                             parentJobId,
                             onFinish,
                             options));
}

DeleteReplicaJob::DeleteReplicaJob(std::string const& databaseFamily,
                                   unsigned int chunk,
                                   std::string const& worker,
                                   Controller::Ptr const& controller,
                                   std::string const& parentJobId,
                                   CallbackType onFinish,
                                   Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "DELETE_REPLICA",
            options),
        _databaseFamily(databaseFamily),
        _chunk(chunk),
        _worker(worker),
        _onFinish(onFinish) {

    if (not _controller->serviceProvider()->config()->isKnownDatabaseFamily(_databaseFamily)) {
        throw std::invalid_argument(
                        "DeleteReplicaJob::DeleteReplicaJob ()  the database family is unknown: " +
                        _databaseFamily);
    }
    _controller->serviceProvider()->assertWorkerIsValid(_worker);
}

DeleteReplicaJobResult const& DeleteReplicaJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "DeleteReplicaJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void DeleteReplicaJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Get all databases for which this chunk is in the COMPLETE state on
    // at the worker.
    //
    // Alternative options would be:
    // 1. launching requests for all databases of the family and then see
    //    filter them on a result status (something like FILE_ROPEN)
    //
    // 2. launching FindRequest for each member of the database family to
    //    see if the chunk is available on a source node.

    if (not _controller->serviceProvider()->databaseServices()->findWorkerReplicas(
                _replicas,
                chunk(),
                worker(),
                databaseFamily())) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  ** failed to find replicas ** "
             << " chunk: "  << chunk()
             << " worker: " << worker());

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }
    if (not _replicas.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** worker has no replicas to be deleted ** "
             << " chunk: "  << chunk()
             << " worker: " << worker());

        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }

    // Notify Qserv about the change in a disposposition of replicas
    // if the notification is required before actually deleting the replica.
    //
    // ATTENTION: only for ACTUALLY participating databases

    ServiceProvider::Ptr const serviceProvider = _controller->serviceProvider();
    if (serviceProvider->config()->xrootdAutoNotify()) {

        std::vector<std::string> databases;
        for (auto&& replica: _replicas) {
            databases.push_back(replica.database());
        }

        auto self = shared_from_base<DeleteReplicaJob>();

        bool const force = true;    // force the removal regardless of the replica
                                    // usage status. See the implementation of the
                                    // corresponiding worker management service for
                                    // specific detail on what "remove" means in
                                    // that service's context.
        qservRemoveReplica(
            chunk(),
            databases,
            worker(),
            force,
            [self] (RemoveReplicaQservMgtRequest::Ptr const& request) {

                switch (request->extendedState()) {

                    // If there is a solid confirmation from Qserv on source node that the replica
                    // is not being used and it won't be used then it's safe to proceed with
                    // the second stage of requests to actually eliminate replica's
                    // files from the source worker.
                    case QservMgtRequest::ExtendedState::SUCCESS:
                        self->beginDeleteReplica();
                        return;

                    // Otherwise set an appropriate status of the operation, finish them
                    // job and notify the caller.
                    case QservMgtRequest::ExtendedState::SERVER_IN_USE:
                        self->finish(ExtendedState::QSERV_IN_USE);
                        break;
                    default:
                        self->finish(ExtendedState::QSERV_FAILED);
                        break;
                }
                self->notify();
            }
        );
    } else {
        beginDeleteReplica();
    }
    setState(State::IN_PROGRESS);
}

void DeleteReplicaJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            _controller->stopReplicaDelete(
                worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }
    _requests.clear();
}

void DeleteReplicaJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<DeleteReplicaJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void DeleteReplicaJob::beginDeleteReplica() {

    auto self = shared_from_base<DeleteReplicaJob>();

    // VERY IMPORTANT: the requests are sent for participating databases
    // only because some catalogs may not have a full coverage

    for (auto&& replica: _replicas) {
        DeleteRequest::Ptr ptr =
            _controller->deleteReplica(
                worker(),
                replica.database(),
                chunk(),
                [self] (DeleteRequest::Ptr ptr) {
                    self->onRequestFinish(ptr);
                },
                options().priority,
                true,   /* keepTracking */
                true,   /* allowDuplicate */
                _id     /* jobId */
            );
        _requests.push_back(ptr);
    }
}

void DeleteReplicaJob::onRequestFinish(DeleteRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish(DeleteRequest)"
         << "  database=" << request->database()
         << "  worker=" << worker()
         << "  chunk=" << chunk());

    LOCK(_mtx);

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) return;

    // Update stats
    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        _replicaData.replicas.push_back(request->responseData());
        _replicaData.chunks[chunk()][request->database()][worker()] = request->responseData();
    }

    // Evaluate the status of on-going operations to see if the job
    // has finished.

    size_t numLaunched;
    size_t numFinished;
    size_t numSuccess;

    ::countRequestStates(numLaunched, numFinished, numSuccess,
                         _requests);

    if (numFinished == numLaunched) {
        finish(numSuccess == numLaunched ? ExtendedState::SUCCESS :
                                           ExtendedState::FAILED);
        notify ();
    }
}

}}} // namespace lsst::qserv::replica
