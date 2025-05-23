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
#include "replica/jobs/ClusterHealthJob.h"

// System headers
#include <stdexcept>
#include <thread>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/contr/Controller.h"
#include "replica/qserv/QservMgtServices.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ClusterHealthJob");

}  // namespace

namespace lsst::qserv::replica {

// ----------------------
//  Class: ClusterHealth
// ----------------------

ClusterHealth::ClusterHealth(vector<string> const& workers) : _good(false) {
    for (auto const& worker : workers) {
        _replication[worker] = false;
        _qserv[worker] = false;
    }
}

void ClusterHealth::updateReplicationState(string const& worker, bool state) {
    _replication[worker] = state;
    _updateSummaryState();
}

void ClusterHealth::updateQservState(string const& worker, bool state) {
    _qserv[worker] = state;
    _updateSummaryState();
}

void ClusterHealth::_updateSummaryState() {
    for (auto&& entry : _replication) {
        if (not entry.second) {
            _good = false;
            return;
        }
    }
    for (auto&& entry : _qserv) {
        if (not entry.second) {
            _good = false;
            return;
        }
    }
    _good = true;
}

// -------------------------
//  Class: ClusterHealthJob
// -------------------------

string ClusterHealthJob::typeName() { return "ClusterHealthJob"; }

ClusterHealthJob::Ptr ClusterHealthJob::create(unsigned int timeoutSec, bool allWorkers,
                                               Controller::Ptr const& controller, string const& parentJobId,
                                               CallbackType const& onFinish, int priority) {
    return ClusterHealthJob::Ptr(
            new ClusterHealthJob(timeoutSec, allWorkers, controller, parentJobId, onFinish, priority));
}

ClusterHealthJob::ClusterHealthJob(unsigned int timeoutSec, bool allWorkers,
                                   Controller::Ptr const& controller, string const& parentJobId,
                                   CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "CLUSTER_HEALTH", priority),
          _timeoutSec(timeoutSec == 0 ? controller->serviceProvider()->config()->get<unsigned int>(
                                                "controller", "request-timeout-sec")
                                      : timeoutSec),
          _allWorkers(allWorkers),
          _onFinish(onFinish),
          _health(allWorkers ? controller->serviceProvider()->config()->allWorkers()
                             : controller->serviceProvider()->config()->workers()) {}

ClusterHealth const& ClusterHealthJob::clusterHealth() const {
    replica::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return _health;

    throw logic_error(context() + string(__func__) + "  can't use this operation before finishing the job");
}

list<pair<string, string>> ClusterHealthJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("timeout_sec", to_string(timeoutSec()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

list<pair<string, string>> ClusterHealthJob::persistentLogData() const {
    list<pair<string, string>> result;

    auto&& health = clusterHealth();

    for (auto&& entry : health.qserv()) {
        auto worker = entry.first;
        auto responded = entry.second;

        if (not responded) {
            result.emplace_back("failed-qserv-worker", worker);
        }
    }
    for (auto&& entry : health.replication()) {
        auto worker = entry.first;
        auto responded = entry.second;

        if (not responded) {
            result.emplace_back("failed-replication-worker", worker);
        }
    }
    return result;
}

void ClusterHealthJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto self = shared_from_base<ClusterHealthJob>();

    // There is nothing special about this value. This is just an arbitrary
    // string to be sent to a worker.
    string const testData = "123";

    auto workers = allWorkers() ? controller()->serviceProvider()->config()->allWorkers()
                                : controller()->serviceProvider()->config()->workers();

    for (auto const& worker : workers) {
        auto const replicationRequest = ServiceStatusRequest::createAndStart(
                controller(), worker,
                [self](ServiceStatusRequest::Ptr request) { self->_onRequestFinish(request); }, priority(),
                id(), timeoutSec());
        _requests[replicationRequest->id()] = replicationRequest;
        ++_numStarted;

        auto const qservRequest = controller()->serviceProvider()->qservMgtServices()->echo(
                worker, testData, id(), /* jobId */
                [self](TestEchoQservMgtRequest::Ptr request) { self->_onRequestFinish(request); },
                timeoutSec());
        _qservRequests[replicationRequest->id()] = qservRequest;
        ++_numStarted;
    }

    // Finish right away if no workers were configured yet

    if (0 == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

void ClusterHealthJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    for (auto&& entry : _requests) {
        auto const& request = entry.second;
        request->cancel();
    }
    _requests.clear();

    for (auto&& entry : _qservRequests) {
        auto const& request = entry.second;
        request->cancel();
    }
    _qservRequests.clear();
}

void ClusterHealthJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    notifyDefaultImpl<ClusterHealthJob>(lock, _onFinish);
}

void ClusterHealthJob::_onRequestFinish(ServiceStatusRequest::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[replication] worker=" << request->workerName());

    if (state() == State::FINISHED) return;

    replica::Lock lock(_mtx, context() + string(__func__) + "[replication]");

    if (state() == State::FINISHED) return;

    _health.updateReplicationState(request->workerName(),
                                   request->extendedState() == Request::ExtendedState::SUCCESS);

    if (++_numFinished == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

void ClusterHealthJob::_onRequestFinish(TestEchoQservMgtRequest::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[qserv] worker=" << request->workerName());

    if (state() == State::FINISHED) return;

    replica::Lock lock(_mtx, context() + string(__func__) + "[qserv]");

    if (state() == State::FINISHED) return;

    _health.updateQservState(request->workerName(),
                             request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS);

    if (++_numFinished == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

}  // namespace lsst::qserv::replica
