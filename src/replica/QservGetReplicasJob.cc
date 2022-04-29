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
#include "replica/QservGetReplicasJob.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservGetReplicasJob");

}  // namespace

namespace lsst { namespace qserv { namespace replica {

string QservGetReplicasJob::typeName() { return "QservGetReplicasJob"; }

QservGetReplicasJob::Ptr QservGetReplicasJob::create(string const& databaseFamily, bool inUseOnly,
                                                     bool allWorkers, Controller::Ptr const& controller,
                                                     string const& parentJobId, CallbackType const& onFinish,
                                                     int priority) {
    return QservGetReplicasJob::Ptr(new QservGetReplicasJob(databaseFamily, inUseOnly, allWorkers, controller,
                                                            parentJobId, onFinish, priority));
}

QservGetReplicasJob::QservGetReplicasJob(string const& databaseFamily, bool inUseOnly, bool allWorkers,
                                         Controller::Ptr const& controller, string const& parentJobId,
                                         CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "QSERV_GET_REPLICAS", priority),
          _databaseFamily(databaseFamily),
          _inUseOnly(inUseOnly),
          _allWorkers(allWorkers),
          _onFinish(onFinish) {}

QservGetReplicasJobResult const& QservGetReplicasJob::getReplicaData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _replicaData;

    throw logic_error("QservGetReplicasJob::" + string(__func__) +
                      "  the method can't be called while "
                      "the job hasn't finished");
}

list<pair<string, string>> QservGetReplicasJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("in_use_only", bool2str(inUseOnly()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<pair<string, string>> QservGetReplicasJob::persistentLogData() const {
    list<pair<string, string>> result;

    auto&& replicaData = getReplicaData();

    // Report workers failed to respond to the requests

    for (auto&& workerInfo : replicaData.workers) {
        auto&& worker = workerInfo.first;

        bool const responded = workerInfo.second;
        if (not responded) {
            result.emplace_back("failed-worker", worker);
        }
    }

    // Per-worker counters for the number of chunks reported by each
    // worker (for the responding workers only)

    for (auto&& itr : replicaData.replicas) {
        auto&& worker = itr.first;
        auto&& qservReplicas = itr.second;
        string const val = "worker=" + worker + " chunks=" + to_string(qservReplicas.size());
        result.emplace_back("worker-stats", val);
    }
    return result;
}

void QservGetReplicasJob::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto self = shared_from_base<QservGetReplicasJob>();

    auto const workerNames = allWorkers() ? controller()->serviceProvider()->config()->allWorkers()
                                          : controller()->serviceProvider()->config()->workers();

    for (auto&& worker : workerNames) {
        _replicaData.workers[worker] = false;
        auto const request = controller()->serviceProvider()->qservMgtServices()->getReplicas(
                databaseFamily(), worker, inUseOnly(), id(),
                [self](GetReplicasQservMgtRequest::Ptr const& request) { self->_onRequestFinish(request); });
        if (not request) {
            LOGS(_log, LOG_LVL_ERROR,
                 context() << __func__
                           << "  failed to submit GetReplicasQservMgtRequest to Qserv worker: " << worker);

            finish(lock, ExtendedState::FAILED);
            return;
        }
        _requests.push_back(request);
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (not _numLaunched) finish(lock, ExtendedState::SUCCESS);
}

void QservGetReplicasJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    for (auto&& ptr : _requests) {
        ptr->cancel();
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess = 0;
}

void QservGetReplicasJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<QservGetReplicasJob>(lock, _onFinish);
}

void QservGetReplicasJob::_onRequestFinish(GetReplicasQservMgtRequest::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  databaseFamily=" << request->databaseFamily()
                   << " worker=" << request->worker() << " state=" << request->state2string());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // Update counters and object state if needed.

    _numFinished++;

    if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
        _numSuccess++;

        // Merge results of the request into the summary data collection
        // of the job.

        _replicaData.replicas[request->worker()] = request->replicas();
        for (auto&& replica : request->replicas()) {
            _replicaData.useCount.atChunk(replica.chunk)
                    .atDatabase(replica.database)
                    .atWorker(request->worker()) = replica.useCount;
        }
        _replicaData.workers[request->worker()] = true;
    }

    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  databaseFamily=" << request->databaseFamily()
                   << " worker=" << request->worker() << " _numLaunched=" << _numLaunched
                   << " _numFinished=" << _numFinished << " _numSuccess=" << _numSuccess);

    if (_numFinished == _numLaunched) {
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS : ExtendedState::FAILED);
    }
}

}}}  // namespace lsst::qserv::replica
