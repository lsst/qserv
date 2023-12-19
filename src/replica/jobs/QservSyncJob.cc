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
#include "replica/jobs/QservSyncJob.h"

// System headers
#include <map>
#include <stdexcept>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/services/DatabaseServices.h"
#include "replica/qserv/QservMgtServices.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservSyncJob");

}  // namespace

namespace lsst::qserv::replica {

string QservSyncJob::typeName() { return "QservSyncJob"; }

QservSyncJob::Ptr QservSyncJob::create(string const& databaseFamily, unsigned int requestExpirationIvalSec,
                                       bool force, Controller::Ptr const& controller,
                                       string const& parentJobId, CallbackType const& onFinish,
                                       int priority) {
    return QservSyncJob::Ptr(new QservSyncJob(databaseFamily, requestExpirationIvalSec, force, controller,
                                              parentJobId, onFinish, priority));
}

QservSyncJob::QservSyncJob(string const& databaseFamily, unsigned int requestExpirationIvalSec, bool force,
                           Controller::Ptr const& controller, string const& parentJobId,
                           CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "QSERV_SYNC", priority),
          _databaseFamily(databaseFamily),
          _requestExpirationIvalSec(requestExpirationIvalSec),
          _force(force),
          _onFinish(onFinish) {}

QservSyncJobResult const& QservSyncJob::getReplicaData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error("QservSyncJob::" + string(__func__) +
                      "  the method can't be called while "
                      "the job hasn't finished");
}

list<pair<string, string>> QservSyncJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("force", bool2str(force()));
    return result;
}

list<pair<string, string>> QservSyncJob::persistentLogData() const {
    list<pair<string, string>> result;

    auto&& replicaData = getReplicaData();

    for (auto&& workerInfo : replicaData.workers) {
        auto&& worker = workerInfo.first;

        // Report workers failed to respond to the synchronization requests
        // and ignore them in the subsequent replica comparison report.

        bool const responded = workerInfo.second;
        if (not responded) {
            result.emplace_back("failed-qserv-worker", worker);
            continue;
        }

        // Find and report (if any) differences between the known replica disposition
        // and the one reported by workers.

        auto&& prevReplicas = replicaData.prevReplicas.at(worker);
        auto&& newReplicas = replicaData.newReplicas.at(worker);

        QservReplicaCollection inPrevReplicasOnly;
        QservReplicaCollection inNewReplicasOnly;

        if (diff2(prevReplicas, newReplicas, inPrevReplicasOnly, inNewReplicasOnly)) {
            auto const val = "worker=" + worker +
                             " removed-replicas=" + to_string(inPrevReplicasOnly.size()) +
                             " added-replicas=" + to_string(inNewReplicasOnly.size());
            result.emplace_back("worker-stats", val);
        }
    }
    return result;
}

void QservSyncJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto const databases = controller()->serviceProvider()->config()->databases(databaseFamily());
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const qservMgtServices = controller()->serviceProvider()->qservMgtServices();
    auto const self = shared_from_base<QservSyncJob>();

    // As a first step, before submitting requests to Qserv workers, pull replicas
    // from the database for each worker. This step must succeed to avoid cancelling
    // jobs should any problem with the database operation happened.

    map<string, unique_ptr<QservReplicaCollection>> worker2newReplicas;
    for (auto&& worker : controller()->serviceProvider()->config()->workers()) {
        auto newReplicas = make_unique<QservReplicaCollection>();
        for (auto&& database : databases) {
            vector<ReplicaInfo> replicas;
            try {
                databaseServices->findWorkerReplicas(replicas, worker, database);
            } catch (exception const&) {
                LOGS(_log, LOG_LVL_DEBUG,
                     context() << __func__ << "  failed to pull replicas for worker: " << worker
                               << ", database: " << database);

                finish(lock, ExtendedState::FAILED);
                return;
            }
            for (auto&& info : replicas) {
                newReplicas->push_back(QservReplica{
                        info.chunk(), info.database(), 0 /* useCount (UNUSED) */
                });
            }
        }
        worker2newReplicas.emplace(make_pair(worker, move(newReplicas)));
    }

    // Submit requests to the workers

    for (auto&& worker : controller()->serviceProvider()->config()->workers()) {
        _requests.push_back(qservMgtServices->setReplicas(
                worker, *(worker2newReplicas[worker]), databases, force(), id(), /* jobId */
                [self](SetReplicasQservMgtRequest::Ptr const& request) { self->_onRequestFinish(request); },
                _requestExpirationIvalSec));
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (not _numLaunched) finish(lock, ExtendedState::SUCCESS);
}

void QservSyncJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    for (auto&& ptr : _requests) {
        ptr->cancel();
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess = 0;
}

void QservSyncJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<QservSyncJob>(lock, _onFinish);
}

void QservSyncJob::_onRequestFinish(SetReplicasQservMgtRequest::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  worker=" << request->workerName()
                   << " state=" << request->state2string());

    if (state() == State::FINISHED) return;

    replica::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // Update counters and object state if needed.

    _numFinished++;
    if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
        _numSuccess++;
        _replicaData.prevReplicas[request->workerName()] = request->replicas();
        _replicaData.newReplicas[request->workerName()] = request->newReplicas();
        _replicaData.workers[request->workerName()] = true;
    } else {
        _replicaData.workers[request->workerName()] = false;
    }

    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  worker=" << request->workerName() << " _numLaunched=" << _numLaunched
                   << " _numFinished=" << _numFinished << " _numSuccess=" << _numSuccess);

    if (_numFinished == _numLaunched) {
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS : ExtendedState::FAILED);
    }
}

}  // namespace lsst::qserv::replica
