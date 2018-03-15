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
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.MoveReplicaJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

MoveReplicaJob::pointer MoveReplicaJob::create(std::string const& databaseFamily,
                                               unsigned int chunk,
                                               std::string const& sourceWorker,
                                               std::string const& destinationWorker,
                                               bool purge,
                                               Controller::pointer const& controller,
                                               std::string const& parentJobId,
                                               callback_type onFinish,
                                               int  priority,
                                               bool exclusive,
                                               bool preemptable) {
    return MoveReplicaJob::pointer(
        new MoveReplicaJob(databaseFamily,
                           chunk,
                           sourceWorker,
                           destinationWorker,
                           purge,
                           controller,
                           parentJobId,
                           onFinish,
                           priority,
                           exclusive,
                           preemptable));
}

MoveReplicaJob::MoveReplicaJob(std::string const& databaseFamily,
                               unsigned int chunk,
                               std::string const& sourceWorker,
                               std::string const& destinationWorker,
                               bool purge,
                               Controller::pointer const& controller,
                               std::string const& parentJobId,
                               callback_type onFinish,
                               int  priority,
                               bool exclusive,
                               bool preemptable)
    :   Job(controller,
            parentJobId,
            "MOVE_REPLICA",
            priority,
            exclusive,
            preemptable),
        _databaseFamily(databaseFamily),
        _chunk(chunk),
        _sourceWorker(sourceWorker),
        _destinationWorker(destinationWorker),
        _purge(purge),
        _onFinish(onFinish),
        _createReplicaJob(nullptr),
        _deleteReplicaJob(nullptr) {
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

    util::BlockPost blockPost(1000, 2000);

    while (_state != State::FINISHED) {
        blockPost.wait();
        if (_createReplicaJob) {
            _createReplicaJob->track(progressReport,
                                     errorReport,
                                     chunkLocksReport,
                                     os);
        }
        if (_deleteReplicaJob) {
            _deleteReplicaJob->track(progressReport,
                                     errorReport,
                                     chunkLocksReport,
                                     os);
        }
    }
}

void MoveReplicaJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<MoveReplicaJob>();
    _createReplicaJob = CreateReplicaJob::create(
        databaseFamily(),
        chunk(),
        sourceWorker(),
        destinationWorker(),
        _controller,
        _id,
        [self] (CreateReplicaJob::pointer const& job) {
            self->onCreateJobFinish();
        }
    );
    _createReplicaJob->start();

    setState(State::IN_PROGRESS);
}

void MoveReplicaJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    if (_createReplicaJob and (_createReplicaJob->state() != Job::State::FINISHED)) {
        _createReplicaJob->cancel();
    }
    if (_deleteReplicaJob and (_deleteReplicaJob->state() != Job::State::FINISHED)) {
        _deleteReplicaJob->cancel();
    }
    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void MoveReplicaJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<MoveReplicaJob>();
        _onFinish(self);
    }
}

void MoveReplicaJob::onCreateJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onCreateJobFinish");

    do {
        // This lock will be automatically release beyond this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled or expired
        if (_state == State::FINISHED) { return; }

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
                    [self] (DeleteReplicaJob::pointer const& job) {
                        self->onDeleteJobFinish();
                    }
                );
                _deleteReplicaJob->start();
            } else {
                // Otherwise, we're done
                setState(State::FINISHED, ExtendedState::SUCCESS);
            }

        } else {
            // Carry over a state of the child job
            setState(State::FINISHED, _createReplicaJob->extendedState());
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify(); }
}

void MoveReplicaJob::onDeleteJobFinish() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onDeleteJobFinish()");

    do {
        // This lock will be automatically release beyond this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Ignore the callback if the job was cancelled or expired
        if (_state == State::FINISHED) { return; }

        // Extract stats
        if (_deleteReplicaJob->extendedState() == Job::ExtendedState::SUCCESS) {
            _replicaData.deletedReplicas = _deleteReplicaJob->getReplicaData().replicas;
            _replicaData.deletedChunks   = _deleteReplicaJob->getReplicaData().chunks;
        }

        // Carry over a state of the child job
        setState(State::FINISHED, _deleteReplicaJob->extendedState());

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED) { notify(); }
}

}}} // namespace lsst::qserv::replica
