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
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/DatabaseMySQL.h"
#include "replica/Configuration.h"
#include "replica/LockUtils.h"
#include "util/BlockPost.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.MoveReplicaJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& MoveReplicaJob::defaultOptions() {
    static Job::Options const options{
        -2,     /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}

MoveReplicaJob::Ptr MoveReplicaJob::create(std::string const& databaseFamily,
                                           unsigned int chunk,
                                           std::string const& sourceWorker,
                                           std::string const& destinationWorker,
                                           bool purge,
                                           Controller::Ptr const& controller,
                                           std::string const& parentJobId,
                                           CallbackType onFinish,
                                           Job::Options const& options) {
    return MoveReplicaJob::Ptr(
        new MoveReplicaJob(databaseFamily,
                           chunk,
                           sourceWorker,
                           destinationWorker,
                           purge,
                           controller,
                           parentJobId,
                           onFinish,
                           options));
}

MoveReplicaJob::MoveReplicaJob(std::string const& databaseFamily,
                               unsigned int chunk,
                               std::string const& sourceWorker,
                               std::string const& destinationWorker,
                               bool purge,
                               Controller::Ptr const& controller,
                               std::string const& parentJobId,
                               CallbackType onFinish,
                               Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "MOVE_REPLICA",
            options),
        _databaseFamily(databaseFamily),
        _chunk(chunk),
        _sourceWorker(sourceWorker),
        _destinationWorker(destinationWorker),
        _purge(purge),
        _onFinish(onFinish),
        _createReplicaJob(nullptr),
        _deleteReplicaJob(nullptr) {

    if (not _controller->serviceProvider()->config()->isKnownDatabaseFamily(_databaseFamily)) {
        throw std::invalid_argument("MoveReplicaJob: the database family is unknown: " +
                                    _databaseFamily);
    }
    _controller->serviceProvider()->assertWorkerIsValid(_sourceWorker);
    _controller->serviceProvider()->assertWorkerIsValid(_destinationWorker);
    if (_sourceWorker == _destinationWorker) {
        throw std::invalid_argument("MoveReplicaJob: source and destination workers are the same");
    }
}

MoveReplicaJobResult const& MoveReplicaJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "MoveReplicaJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::string MoveReplicaJob::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily(),
                              chunk(),
                              sourceWorker(),
                              destinationWorker(),
                              purge() ? 1 : 0);
}

void MoveReplicaJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    ASSERT_LOCK(_mtx, context() + "startImpl");

    auto self = shared_from_base<MoveReplicaJob>();
    _createReplicaJob = CreateReplicaJob::create(
        databaseFamily(),
        chunk(),
        sourceWorker(),
        destinationWorker(),
        _controller,
        _id,
        [self] (CreateReplicaJob::Ptr const& job) {
            self->onCreateJobFinish();
        },
        options()   // inherit from the current job
    );
    _createReplicaJob->start();

    setState(State::IN_PROGRESS);
}

void MoveReplicaJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    ASSERT_LOCK(_mtx, context() + "cancelImpl");

    if (_createReplicaJob and (_createReplicaJob->state() != Job::State::FINISHED)) {
        _createReplicaJob->cancel();
    }
    if (_deleteReplicaJob and (_deleteReplicaJob->state() != Job::State::FINISHED)) {
        _deleteReplicaJob->cancel();
    }
}

void MoveReplicaJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<MoveReplicaJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void MoveReplicaJob::onCreateJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onCreateJobFinish");

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "onCreateJobFinish");

    if (_state == State::FINISHED) return;

    if (_createReplicaJob->extendedState() == Job::ExtendedState::SUCCESS) {

        // Extract stats
        _replicaData.createdReplicas = _createReplicaJob->getReplicaData().replicas;
        _replicaData.createdChunks   = _createReplicaJob->getReplicaData().chunks;

        // Initiate the second stage (which is optional) - deleting the replica
        // at the source
        if (_purge) {

            auto self = shared_from_base<MoveReplicaJob>();
            _deleteReplicaJob = DeleteReplicaJob::create(
                databaseFamily(),
                chunk(),
                sourceWorker(),
                _controller,
                _id,
                [self] (DeleteReplicaJob::Ptr const& job) {
                    self->onDeleteJobFinish();
                },
                options()   // inherit from the current job
            );
            _deleteReplicaJob->start();
        } else {
            // Otherwise, we're done
            finish(ExtendedState::SUCCESS);
        }

    } else {
        // Carry over a state of the child job
        finish(_createReplicaJob->extendedState());
    }
    if (_state == State::FINISHED) notify();
}

void MoveReplicaJob::onDeleteJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onDeleteJobFinish()");

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (_state == State::FINISHED) return;

    LOCK(_mtx, context() + "onDeleteJobFinish");

    if (_state == State::FINISHED) return;

    // Extract stats
    if (_deleteReplicaJob->extendedState() == Job::ExtendedState::SUCCESS) {
        _replicaData.deletedReplicas = _deleteReplicaJob->getReplicaData().replicas;
        _replicaData.deletedChunks   = _deleteReplicaJob->getReplicaData().chunks;
    }

    // Carry over a state of the child job
    finish(_deleteReplicaJob->extendedState());

    notify();
}

}}} // namespace lsst::qserv::replica
