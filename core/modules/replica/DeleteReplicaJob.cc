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
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/ErrorReporting.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

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
}

DeleteReplicaJobResult const& DeleteReplicaJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (state() == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "DeleteReplicaJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::string DeleteReplicaJob::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily(),
                              chunk(),
                              worker());
}

void DeleteReplicaJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

   // Check if configuration parameters are valid

    auto const& config = controller()->serviceProvider()->config();

    if (not (config->isKnownDatabaseFamily(databaseFamily()) and
             config->isKnownWorker(worker()))) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  ** MISCONFIGURED ** "
             << " database family: '" << databaseFamily() << "'"
             << " worker: '" << worker() << "'");

        setState(lock,
                 State::FINISHED,
                 ExtendedState::CONFIG_ERROR);
        return;
    }

    // Get all databases for which this chunk is in the COMPLETE state on
    // at the worker.
    //
    // Alternative options would be:
    // 1. launching requests for all databases of the family and then see
    //    filter them on a result status (something like FILE_ROPEN)
    //
    // 2. launching FindRequest for each member of the database family to
    //    see if the chunk is available on a source node.

    if (not controller()->serviceProvider()->databaseServices()->findWorkerReplicas(
                _replicas,
                chunk(),
                worker(),
                databaseFamily())) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  ** failed to find replicas ** "
             << " chunk: "  << chunk()
             << " worker: " << worker());

        setState(lock,
                 State::FINISHED,
                 ExtendedState::FAILED);
        return;
    }
    if (not _replicas.size()) {
        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
             << "** worker has no replicas to be deleted ** "
             << " chunk: "  << chunk()
             << " worker: " << worker());

        setState(lock,
                 State::FINISHED,
                 ExtendedState::FAILED);
        return;
    }

    // Notify Qserv about the change in a disposposition of replicas
    // if the notification is required before actually deleting the replica.
    //
    // ATTENTION: only for ACTUALLY participating databases

    ServiceProvider::Ptr const serviceProvider = controller()->serviceProvider();
    if (not serviceProvider->config()->xrootdAutoNotify()) {

        // Start rigth away
        beginDeleteReplica(lock);

    } else {

        // Notify Qserv first. Then start once a confirmation is received

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
            lock,
            chunk(),
            databases,
            worker(),
            force,
            [self] (RemoveReplicaQservMgtRequest::Ptr const& request) {

                util::Lock lock(self->_mtx, self->context() + "startImpl:qservRemoveReplica");

                switch (request->extendedState()) {

                    // If there is a solid confirmation from Qserv on source node that the replica
                    // is not being used and it won't be used then it's safe to proceed with
                    // the second stage of requests to actually eliminate replica's
                    // files from the source worker.
                    case QservMgtRequest::ExtendedState::SUCCESS:
                        self->beginDeleteReplica(lock);
                        return;

                    // Otherwise set an appropriate status of the operation, finish them
                    // job and notify the caller.
                    case QservMgtRequest::ExtendedState::SERVER_CHUNK_IN_USE:
                        self->finish(lock, ExtendedState::QSERV_CHUNK_IN_USE);
                        break;
                    default:
                        self->finish(lock, ExtendedState::QSERV_FAILED);
                        break;
                }
            }
        );
    }
    setState(lock, State::IN_PROGRESS);
}

void DeleteReplicaJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            controller()->stopReplicaDelete(
                worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                id()        /* jobId */);
    }
    _requests.clear();
}

void DeleteReplicaJob::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish) {
        _onFinish(shared_from_base<DeleteReplicaJob>());
    }
}

void DeleteReplicaJob::beginDeleteReplica(util::Lock const& lock) {

    auto self = shared_from_base<DeleteReplicaJob>();

    // VERY IMPORTANT: the requests are sent for participating databases
    // only because some catalogs may not have a full coverage

    for (auto&& replica: _replicas) {
        DeleteRequest::Ptr ptr =
            controller()->deleteReplica(
                worker(),
                replica.database(),
                chunk(),
                [self] (DeleteRequest::Ptr ptr) {
                    self->onRequestFinish(ptr);
                },
                options(lock).priority,
                true,   /* keepTracking */
                true,   /* allowDuplicate */
                id()    /* jobId */
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

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.
    
    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onRequestFinish");

    if (state() == State::FINISHED) return;

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
        finish(lock, numSuccess == numLaunched ? ExtendedState::SUCCESS :
                                                 ExtendedState::FAILED);
    }
}

}}} // namespace lsst::qserv::replica
