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
#include "replica/jobs/ServiceManagementJob.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/services/ServiceProvider.h"

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
                                                   std::shared_ptr<Controller> const& controller,
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
    auto const config = controller()->serviceProvider()->config();
    auto const workerNames = allWorkers() ? config->allWorkers() : config->workers();
    for (auto const& workerName : workerNames) {
        _resultData.serviceState[workerName] = ServiceStateInfo();
        _resultData.workers[workerName] = false;
        _requests.push_back(submitRequest(workerName));
    }

    // In case if no workers are present in the Config at this time.
    if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
}

void ServiceManagementBaseJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    for (auto const& ptr : _requests) {
        ptr->cancel();
    }
    _requests.clear();
    _numFinished = 0;
}

#ifdef QSERV_REPLICA_PROTOCOL_HTTP
void ServiceManagementBaseJob::onRequestFinish(std::shared_ptr<HttpSvcMgtRequestBase> const& request) {
#else
void ServiceManagementBaseJob::onRequestFinish(std::shared_ptr<ServiceManagementRequestBase> const& request) {
#endif
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "  worker=" << request->workerName() << " id=" << request->id()
                   << " type=" << request->type() << " state=" << request->state2string());

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + string(__func__) + "[" + request->id() + "]");
    if (state() == State::FINISHED) return;

    // Update counters and object state if needed.
    _numFinished++;
    if (request->extendedState() == RequestI::ExtendedState::SUCCESS) {
        _resultData.serviceState[request->workerName()] = request->getServiceState();
        _resultData.workers[request->workerName()] = true;
    }

    // Check for the completion condition of the job
    if (_requests.size() == _numFinished) {
        size_t const numSucceeded =
                count_if(_requests.begin(), _requests.end(),
#ifdef QSERV_REPLICA_PROTOCOL_HTTP
                         [](std::shared_ptr<HttpSvcMgtRequestBase> const& ptr) {
#else
                         [](std::shared_ptr<ServiceManagementRequestBase> const& ptr) {
#endif
                             return ptr->extendedState() == RequestI::ExtendedState::SUCCESS;
                         });
        finish(lock, numSucceeded == _numFinished ? ExtendedState::SUCCESS : ExtendedState::FAILED);
    }
}

}  // namespace lsst::qserv::replica
