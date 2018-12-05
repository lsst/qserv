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
#include "replica/ClusterHealthJob.h"

// System headers
#include <stdexcept>
#include <thread>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Controller.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ClusterHealthJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

// ----------------------
//  Class: ClusterHealth
// ----------------------

ClusterHealth::ClusterHealth(std::vector<std::string> const& workers)
    :   _good(false) {

    for (auto const& worker: workers) {
        _replication[worker] = false;
        _qserv      [worker] = false;
    }
}

void ClusterHealth::updateReplicationState(std::string const& worker,
                                           bool state) {
    _replication[worker] = state;
    updateSummaryState();
}

void ClusterHealth::updateQservState(std::string const& worker,
                                     bool state) {
    _qserv[worker] = state;
    updateSummaryState();
}

void ClusterHealth::updateSummaryState() {
    for (auto&& entry: _replication) {
        if (not entry.second) {
            _good = false;
            return;
        }
    }
    for (auto&& entry: _qserv) {
        if (not entry.second) {
            _good = false;
            return;
        }
    }
    _good = true;
}

Job::Options const& ClusterHealthJob::defaultOptions() {
    static Job::Options const options{
        3,      /* priority */
        false,  /* exclusive */
        true    /* preemptive */
    };
    return options;
}

// -------------------------
//  Class: ClusterHealthJob
// -------------------------

std::string ClusterHealthJob::typeName() { return "ClusterHealthJob"; }


ClusterHealthJob::Ptr ClusterHealthJob::create(unsigned int timeoutSec,
                                               Controller::Ptr const& controller,
                                               std::string const& parentJobId,
                                               CallbackType const& onFinish,
                                               Job::Options const& options) {
    return ClusterHealthJob::Ptr(
        new ClusterHealthJob(timeoutSec,
                             controller,
                             parentJobId,
                             onFinish,
                             options));
}

ClusterHealthJob::ClusterHealthJob(unsigned int timeoutSec,
                                   Controller::Ptr const& controller,
                                   std::string const& parentJobId,
                                   CallbackType const& onFinish,
                                   Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "CLUSTER_HEALTH",
            options),
        _timeoutSec(timeoutSec == 0
                    ? controller->serviceProvider()->config()->controllerRequestTimeoutSec()
                    : timeoutSec),
        _onFinish(onFinish),
        _health(controller->serviceProvider()->config()->workers()),
        _numStarted(0),
        _numFinished(0) {
}

ClusterHealth const& ClusterHealthJob::clusterHealth() const {
 
    util::Lock lock(_mtx, context() + "clusterHealth");
 
    if (state() == State::FINISHED) return _health;

    throw std::logic_error(
            context() + "clusterHealth  can't use this operation before finishing the job");
}

std::list<std::pair<std::string,std::string>> ClusterHealthJob::extendedPersistentState() const {
    std::list<std::pair<std::string,std::string>> result;
    result.emplace_back("timeout_sec",std::to_string(timeoutSec()));
    return result;
}

void ClusterHealthJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<ClusterHealthJob>();

    // There is nothing special about this value. This is just an arbitrary
    // string to be sent to a worker.
    std::string const testData = "123";

    for (auto const& worker: controller()->serviceProvider()->config()->workers()) {

        auto const replicationRequest = controller()->statusOfWorkerService(
            worker,
            [self] (ServiceStatusRequest::Ptr request) {
                self->onRequestFinish(request);
            },
            id(),   /* jobId */
            timeoutSec()
        );
        _requests[replicationRequest->id()] = replicationRequest;
        ++_numStarted;

        auto const qservRequest = controller()->serviceProvider()->qservMgtServices()->echo(
            worker,
            testData,
            id(),   /* jobId */
            [self] (TestEchoQservMgtRequest::Ptr request) {
                self->onRequestFinish(request);
            },
            timeoutSec()
        );
        _qservRequests[replicationRequest->id()] = qservRequest;
        ++_numStarted;
    }
    
    // Finish right away if no workers were configured yet

    if (0 == _numStarted) setState(lock, State::FINISHED, ExtendedState::SUCCESS);
    else                  setState(lock, State::IN_PROGRESS);
}

void ClusterHealthJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    for (auto&& entry: _requests) {
        auto const& request = entry.second;
        request->cancel();
    }
    _requests.clear();

    for (auto&& entry: _qservRequests) {
        auto const& request = entry.second;
        request->cancel();
    }
    _qservRequests.clear();
}

void ClusterHealthJob::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    notifyDefaultImpl<ClusterHealthJob>(lock, _onFinish);
}

void ClusterHealthJob::onRequestFinish(ServiceStatusRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish[replication]  worker=" << request->worker());

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onRequestFinish[replication]");

    if (state() == State::FINISHED) return;

    _health.updateReplicationState(request->worker(),
                                   request->extendedState() == Request::ExtendedState::SUCCESS);

    if (++_numFinished == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

void ClusterHealthJob::onRequestFinish(TestEchoQservMgtRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish[qserv]  worker=" << request->worker());

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + "onRequestFinish[qserv]");

    if (state() == State::FINISHED) return;

    _health.updateQservState(request->worker(),
                             request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS);

    if (++_numFinished == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

}}} // namespace lsst::qserv::replica
