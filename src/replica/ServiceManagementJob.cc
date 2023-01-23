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
#include "replica/ServiceManagementJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ServiceManagementBaseJob");

}  // namespace

namespace lsst::qserv::replica {

string ServiceManagementBaseJob::typeName() { return "ServiceManagementBaseJob"; }

ServiceManagementBaseJob::ServiceManagementBaseJob(string const& requestName, bool allWorkers,
                                                   unsigned int requestExpirationIvalSec,
                                                   Controller::Ptr const& controller,
                                                   string const& parentJobId, int priority)
        : Job(controller, parentJobId, requestName, priority),
          _allWorkers(allWorkers),
          _requestExpirationIvalSec(requestExpirationIvalSec) {}

ServiceManagementJobResult const& ServiceManagementBaseJob::getResultData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error("ServiceManagementBaseJob::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

void ServiceManagementBaseJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto const workerNames = allWorkers() ? controller()->serviceProvider()->config()->allWorkers()
                                          : controller()->serviceProvider()->config()->workers();

    for (auto&& worker : workerNames) {
        _resultData.serviceState[worker] = ServiceState();
        _resultData.workers[worker] = false;
        _requests.push_back(submitRequest(worker));
    }

    // In case if no workers are present in the Configuration
    // at this time.

    if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
}

void ServiceManagementBaseJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    for (auto&& ptr : _requests) ptr->cancel();
    _requests.clear();
    _numFinished = 0;
}

void ServiceManagementBaseJob::onRequestFinish(ServiceManagementRequestBase::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  worker=" << request->worker() << " id=" << request->id()
                   << " type=" << request->type() << " state=" << request->state2string());

    if (state() == State::FINISHED) return;

    replica::Lock lock(_mtx, context() + string(__func__) + "[" + request->id() + "]");

    if (state() == State::FINISHED) return;

    // Update counters and object state if needed.
    _numFinished++;
    if (request->extendedState() == Request::ExtendedState::SUCCESS) {
        _resultData.serviceState[request->worker()] = request->getServiceState();
        _resultData.workers[request->worker()] = true;
    }

    // Check for the completion condition of the job
    if (_requests.size() == _numFinished) {
        size_t const numSucceeded = count_if(
                _requests.begin(), _requests.end(), [](ServiceManagementRequestBase::Ptr const& ptr) {
                    return ptr->extendedState() == Request::ExtendedState::SUCCESS;
                });
        finish(lock, numSucceeded == _numFinished ? ExtendedState::SUCCESS : ExtendedState::FAILED);
    }
}

}  // namespace lsst::qserv::replica
