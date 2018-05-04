    /*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/QservSyncJob.h"

// System headers
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservSyncJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& QservSyncJob::defaultOptions() {
    static Job::Options const options{
        2,      /* priority */
        true,   /* exclusive */
        false   /* preemptive */
    };
    return options;
}

QservSyncJob::Ptr QservSyncJob::create(std::string const& databaseFamily,
                                       Controller::Ptr const& controller,
                                       std::string const& parentJobId,
                                       bool force,
                                       CallbackType onFinish,
                                       Job::Options const& options) {
    return QservSyncJob::Ptr(
        new QservSyncJob(databaseFamily,
                       controller,
                       parentJobId,
                       force,
                       onFinish,
                       options));
}

QservSyncJob::QservSyncJob(std::string const& databaseFamily,
                           Controller::Ptr const& controller,
                           std::string const& parentJobId,
                           bool force,
                           CallbackType onFinish,
                           Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "QSERV:SYNC",
            options),
        _databaseFamily(databaseFamily),
        _force(force),
        _onFinish(onFinish),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

QservSyncJobResult const& QservSyncJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) { return _replicaData; }

    throw std::logic_error(
        "QservSyncJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void QservSyncJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto const databases        = _controller->serviceProvider()->config()->databases(_databaseFamily);
    auto const databaseServices = _controller->serviceProvider()->databaseServices();
    auto const qservMgtServices = _controller->serviceProvider()->qservMgtServices();
    auto const self             = shared_from_base<QservSyncJob>();

    for (auto&& worker: _controller->serviceProvider()->config()->workers()) {

        // Pull replicas from the database for the worker

        QservReplicaCollection newReplicas;
        for (auto&& database: databases) {

            std::vector<ReplicaInfo> replicas;
            if (not databaseServices->findWorkerReplicas(replicas,
                                                         worker,
                                                         database)) {

                LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  failed to pull replicas for worker: "
                     << worker << ", database: " << database);

                // Set this state and cleanup before aborting the job
                setState(State::FINISHED, ExtendedState::FAILED);
                cancelImpl();

                return;
            }
            for (auto&& info: replicas) {
                newReplicas.emplace_back(
                    QservReplica{
                        info.chunk(),
                        info.database(),
                        0   /* useCount (UNUSED) */
                    }
                );
            }
        }

        // Submit a request to the worker

        _requests.push_back(
            qservMgtServices->setReplicas(
                worker,
                newReplicas,
                _force,
                _id,    /* jobId */
                [self] (SetReplicasQservMgtRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                }
            )
        );
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.
    if (not _numLaunched) { setState(State::FINISHED); }
    else                  { setState(State::IN_PROGRESS); }
}

void QservSyncJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    for (auto&& ptr: _requests) {
        ptr->cancel();
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void QservSyncJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<QservSyncJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void QservSyncJob::onRequestFinish(SetReplicasQservMgtRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  worker=" << request->worker()
         << " state=" << request->state2string(request->state())
         << " extendedState=" << request->state2string(request->extendedState()));

    // Ignore the callback if the job was cancelled, expired, etc.``
    if (_state == State::FINISHED) { return; }

    do {
        // Note that access to the job's public API should not be locked while
        // notifying a caller (if the callback function was povided) in order to avoid
        // the circular deadlocks.
        LOCK_GUARD;

        LOGS(_log, LOG_LVL_DEBUG, context()
             << "onRequestFinish  worker=" << request->worker()
             << " ** LOCK_GUARD **");

        // Update counters and object state if needed.
        _numFinished++;
        if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
            _numSuccess++;
            _replicaData.prevReplicas[request->worker()] = request->replicas();
            _replicaData.newReplicas [request->worker()] = request->newReplicas();
            _replicaData.workers     [request->worker()] = true;
        } else {
            _replicaData.workers     [request->worker()] = false;
        }

        LOGS(_log, LOG_LVL_DEBUG, context()
             << "onRequestFinish  worker=" << request->worker()
             << " _numLaunched=" << _numLaunched
             << " _numFinished=" << _numFinished
             << " _numSuccess=" << _numSuccess);

        if (_numFinished == _numLaunched) {
            finish(_numSuccess == _numLaunched ? ExtendedState::SUCCESS :
                                                 ExtendedState::FAILED);
        }

    } while (false);

    // Finally, notify a caller in the deadlock-free zone`
    if (_state == State::FINISHED) { notify(); }
}

}}} // namespace lsst::qserv::replica
