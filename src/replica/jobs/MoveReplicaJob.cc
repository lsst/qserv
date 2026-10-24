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
#include "replica/jobs/MoveReplicaJob.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/jobs/CreateReplicaJob.h"
#include "replica/jobs/DeleteReplicaJob.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.MoveReplicaJob");
}  // namespace

namespace lsst::qserv::replica {

string MoveReplicaJob::typeName() { return "MoveReplicaJob"; }

shared_ptr<MoveReplicaJob> MoveReplicaJob::create(string const& databaseFamily, unsigned int chunk,
                                           string const& sourceWorker, string const& destinationWorker,
                                           bool purge, shared_ptr<Controller> const& controller,
                                           string const& parentJobId, CallbackType const& onFinish,
                                           int priority) {
    return shared_ptr<MoveReplicaJob>(new MoveReplicaJob(databaseFamily, chunk, sourceWorker, destinationWorker,
                                                  purge, controller, parentJobId, onFinish, priority));
}

MoveReplicaJob::MoveReplicaJob(string const& databaseFamily, unsigned int chunk, string const& sourceWorker,
                               string const& destinationWorker, bool purge, shared_ptr<Controller> const& controller,
                               string const& parentJobId, CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "MOVE_REPLICA", priority),
          _databaseFamily(databaseFamily),
          _chunk(chunk),
          _sourceWorker(sourceWorker),
          _destinationWorker(destinationWorker),
          _purge(purge),
          _onFinish(onFinish) {}

MoveReplicaJobResult const& MoveReplicaJob::getReplicaData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    if (state() == State::FINISHED) return _replicaData;
    throw logic_error("MoveReplicaJob::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

list<pair<string, string>> MoveReplicaJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("chunk", to_string(chunk()));
    result.emplace_back("source_worker", sourceWorker());
    result.emplace_back("destination_worker", destinationWorker());
    result.emplace_back("purge", bool2str(purge()));
    return result;
}

list<pair<string, string>> MoveReplicaJob::persistentLogData() const {
    list<pair<string, string>> result;

    auto const& replicaData = getReplicaData();

    // Per-worker counters for the following categories:
    //
    //   created-chunks:
    //     the total number of chunks created on the workers as a result
    //     of the operation
    //
    //   deleted-chunks:
    //     the total number of chunks deleted from the workers as a result
    //     of the operation
    map<string, map<string, size_t>> workerCategoryCounter;

    for (auto const& info : replicaData.createdReplicas) {
        workerCategoryCounter[info.worker()]["created-chunks"]++;
    }
    for (auto const& info : replicaData.deletedReplicas) {
        workerCategoryCounter[info.worker()]["deleted-chunks"]++;
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

void MoveReplicaJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Check if configuration parameters are valid
    auto const config = controller()->serviceProvider()->config();
    if (not(config->isKnownDatabaseFamily(databaseFamily()) and config->isKnownWorker(sourceWorker()) and
            config->isKnownWorker(destinationWorker()) and (sourceWorker() != destinationWorker()))) {
        LOGS(_log, LOG_LVL_ERROR,
             context() << __func__ << "  ** MISCONFIGURED ** "
                       << " database family: '" << databaseFamily() << "'"
                       << " source worker: '" << sourceWorker() << "'"
                       << " destination worker: '" << destinationWorker() << "'");

        finish(lock, ExtendedState::CONFIG_ERROR);
        return;
    }

    // As the first step, create a new replica at the destination. The current one will
    // be (if requested) purged after a successful completion of the first step.
    _createReplicaJob = CreateReplicaJob::create(
            databaseFamily(), chunk(), sourceWorker(), destinationWorker(), controller(), id(),
            [self = shared_from_base<MoveReplicaJob>()](shared_ptr<CreateReplicaJob> const& job) { self->_onCreateJobFinish(); },
            priority()
    );
    _createReplicaJob->start();
}

void MoveReplicaJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    if (_createReplicaJob && _createReplicaJob->state() != Job::State::FINISHED) {
        _createReplicaJob->cancel();
    }
    if (_deleteReplicaJob && _deleteReplicaJob->state() != Job::State::FINISHED) {
        _deleteReplicaJob->cancel();
    }
}

void MoveReplicaJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<MoveReplicaJob>(lock, _onFinish);
}

void MoveReplicaJob::_onCreateJobFinish() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    if (_createReplicaJob->extendedState() == Job::ExtendedState::SUCCESS) {
        // Extract stats
        _replicaData.createdReplicas = _createReplicaJob->getReplicaData().replicas;
        _replicaData.createdChunks = _createReplicaJob->getReplicaData().chunks;

        // Initiate the second stage (which is optional) - deleting the replica
        // at the source
        if (_purge) {
            _deleteReplicaJob = DeleteReplicaJob::create(
                    databaseFamily(), chunk(), sourceWorker(), controller(), id(),
                    [self = shared_from_base<MoveReplicaJob>()](shared_ptr<DeleteReplicaJob> const& job) { self->_onDeleteJobFinish(); },
                    priority()
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

void MoveReplicaJob::_onDeleteJobFinish() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    // Extract stats
    if (_deleteReplicaJob->extendedState() == Job::ExtendedState::SUCCESS) {
        _replicaData.deletedReplicas = _deleteReplicaJob->getReplicaData().replicas;
        _replicaData.deletedChunks = _deleteReplicaJob->getReplicaData().chunks;
    }

    // Carry over a state of the child job
    finish(lock, _deleteReplicaJob->extendedState());
}

}  // namespace lsst::qserv::replica
