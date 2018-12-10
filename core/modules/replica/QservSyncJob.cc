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


std::string QservSyncJob::typeName() { return "QservSyncJob"; }


QservSyncJob::Ptr QservSyncJob::create(std::string const& databaseFamily,
                                       unsigned int requestExpirationIvalSec,
                                       bool force,
                                       Controller::Ptr const& controller,
                                       std::string const& parentJobId,
                                       CallbackType const& onFinish,
                                       Job::Options const& options) {
    return QservSyncJob::Ptr(
        new QservSyncJob(databaseFamily,
                         requestExpirationIvalSec,
                         force,
                         controller,
                         parentJobId,
                         onFinish,
                         options));
}

QservSyncJob::QservSyncJob(std::string const& databaseFamily,
                           unsigned int requestExpirationIvalSec,
                           bool force,
                           Controller::Ptr const& controller,
                           std::string const& parentJobId,
                           CallbackType const& onFinish,
                           Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "QSERV_SYNC",
            options),
        _databaseFamily(databaseFamily),
        _requestExpirationIvalSec(requestExpirationIvalSec),
        _force(force),
        _onFinish(onFinish),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

QservSyncJobResult const& QservSyncJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (state() == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "QservSyncJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::list<std::pair<std::string,std::string>> QservSyncJob::extendedPersistentState() const {
    std::list<std::pair<std::string,std::string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("force",           force() ? "1" : "0");
    return result;
}

void QservSyncJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto const databases        = controller()->serviceProvider()->config()->databases(databaseFamily());
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const qservMgtServices = controller()->serviceProvider()->qservMgtServices();
    auto const self             = shared_from_base<QservSyncJob>();

    for (auto&& worker: controller()->serviceProvider()->config()->workers()) {

        // Pull replicas from the database for the worker

        QservReplicaCollection newReplicas;
        for (auto&& database: databases) {

            std::vector<ReplicaInfo> replicas;
            try {
                databaseServices->findWorkerReplicas(replicas,
                                                     worker,
                                                     database);
            } catch (std::exception const&) {

                LOGS(_log, LOG_LVL_DEBUG, context()
                     << "startImpl  failed to pull replicas for worker: "
                     << worker << ", database: " << database);

                // Set this state and cleanup before aborting the job

                setState(lock, State::FINISHED, ExtendedState::FAILED);

                cancelImpl(lock);

                return;
            }
            for (auto&& info: replicas) {
                newReplicas.push_back(
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
                force(),
                id(),   /* jobId */
                [self] (SetReplicasQservMgtRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                _requestExpirationIvalSec
            )
        );
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.
    if (not _numLaunched) setState(lock, State::FINISHED);
    else                  setState(lock, State::IN_PROGRESS);
}

void QservSyncJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    for (auto&& ptr: _requests) {
        ptr->cancel();
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void QservSyncJob::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    notifyDefaultImpl<QservSyncJob>(lock, _onFinish);
}

void QservSyncJob::onRequestFinish(SetReplicasQservMgtRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  worker=" << request->worker()
         << " state=" << request->state2string());

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onRequestFinish");

    if (state() == State::FINISHED) return;

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
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS :
                                                   ExtendedState::FAILED);
    }
}

}}} // namespace lsst::qserv::replica
