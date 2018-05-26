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
        _onFinish(onFinish) {
}

MoveReplicaJobResult const& MoveReplicaJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (state() == State::FINISHED) return _replicaData;

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

void MoveReplicaJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Check if configuration parameters are valid

    auto const& config = controller()->serviceProvider()->config();

    if (not (config->isKnownDatabaseFamily(databaseFamily()) and
             config->isKnownWorker(sourceWorker()) and
             config->isKnownWorker(destinationWorker()) and
             (sourceWorker() != destinationWorker()))) {

        LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  ** MISCONFIGURED ** "
             << " database family: '" << databaseFamily() << "'"
             << " source worker: '" << sourceWorker() << "'"
             << " destination worker: '" << destinationWorker() << "'");

        setState(lock,
                 State::FINISHED,
                 ExtendedState::CONFIG_ERROR);
        return;
    }

    // As the first step, create a new replica at the destination.
    // The current one will be (if requested) purged after a successful
    // completion of the first step.

    auto self = shared_from_base<MoveReplicaJob>();

    _createReplicaJob = CreateReplicaJob::create(
        databaseFamily(),
        chunk(),
        sourceWorker(),
        destinationWorker(),
        controller(),
        id(),
        [self] (CreateReplicaJob::Ptr const& job) {
            self->onCreateJobFinish();
        },
        options()   // inherit from the current job
    );
    _createReplicaJob->start();

    setState(lock, State::IN_PROGRESS);
}

void MoveReplicaJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    if (_createReplicaJob and (_createReplicaJob->state() != Job::State::FINISHED)) {
        _createReplicaJob->cancel();
    }
    if (_deleteReplicaJob and (_deleteReplicaJob->state() != Job::State::FINISHED)) {
        _deleteReplicaJob->cancel();
    }
}

void MoveReplicaJob::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish) {
        _onFinish(shared_from_base<MoveReplicaJob>());
    }
}

void MoveReplicaJob::onCreateJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onCreateJobFinish");

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onCreateJobFinish");

    if (state() == State::FINISHED) return;

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
                controller(),
                id(),
                [self] (DeleteReplicaJob::Ptr const& job) {
                    self->onDeleteJobFinish();
                },
                options()   // inherit from the current job
            );
            _deleteReplicaJob->start();
        } else {
            // Otherwise, we're done
            finish(lock, ExtendedState::SUCCESS);
        }

    } else {

        // Carry over a state of the child job

        finish(lock, _createReplicaJob->extendedState());
    }
}

void MoveReplicaJob::onDeleteJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onDeleteJobFinish()");

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquering the lock to recheck the state in case if it
    // has transitioned while acquering the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onDeleteJobFinish");

    if (state() == State::FINISHED) return;

    // Extract stats

    if (_deleteReplicaJob->extendedState() == Job::ExtendedState::SUCCESS) {
        _replicaData.deletedReplicas = _deleteReplicaJob->getReplicaData().replicas;
        _replicaData.deletedChunks   = _deleteReplicaJob->getReplicaData().chunks;
    }

    // Carry over a state of the child job
    finish(lock, _deleteReplicaJob->extendedState());
}

}}} // namespace lsst::qserv::replica
