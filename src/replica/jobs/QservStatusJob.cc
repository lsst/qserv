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
#include "replica/jobs/QservStatusJob.h"

// System headers
#include <stdexcept>
#include <thread>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/config/Configuration.h"
#include "replica/contr/Controller.h"
#include "replica/qserv/QservMgtServices.h"
#include "replica/services/ServiceProvider.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservStatusJob");

}  // namespace

namespace lsst::qserv::replica {

string QservStatusJob::typeName() { return "QservStatusJob"; }

QservStatusJob::Ptr QservStatusJob::create(unsigned int timeoutSec, bool allWorkers,
                                           Controller::Ptr const& controller, string const& parentJobId,
                                           wbase::TaskSelector const& taskSelector,
                                           CallbackType const& onFinish, int priority) {
    return QservStatusJob::Ptr(new QservStatusJob(timeoutSec, allWorkers, controller, parentJobId,
                                                  taskSelector, onFinish, priority));
}

QservStatusJob::QservStatusJob(unsigned int timeoutSec, bool allWorkers, Controller::Ptr const& controller,
                               string const& parentJobId, wbase::TaskSelector const& taskSelector,
                               CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "QSERV_STATUS", priority),
          _timeoutSec(timeoutSec == 0 ? controller->serviceProvider()->config()->get<unsigned int>(
                                                "controller", "request-timeout-sec")
                                      : timeoutSec),
          _allWorkers(allWorkers),
          _taskSelector(taskSelector),
          _onFinish(onFinish) {}

QservStatus const& QservStatusJob::qservStatus() const {
    replica::Lock const lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return _qservStatus;
    throw logic_error(context() + string(__func__) + "  can't use this operation before finishing the job");
}

list<pair<string, string>> QservStatusJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("timeout_sec", to_string(timeoutSec()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    result.emplace_back("include_tasks", bool2str(taskSelector().includeTasks));
    result.emplace_back("num_query_ids", to_string(taskSelector().queryIds.size()));
    result.emplace_back("num_task_states", to_string(taskSelector().taskStates.size()));
    return result;
}

list<pair<string, string>> QservStatusJob::persistentLogData() const {
    list<pair<string, string>> result;
    for (auto&& entry : qservStatus().workers) {
        auto worker = entry.first;
        auto responded = entry.second;
        if (not responded) {
            result.emplace_back("failed-worker", worker);
        }
    }
    return result;
}

void QservStatusJob::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    auto self = shared_from_base<QservStatusJob>();
    auto workers = allWorkers() ? controller()->serviceProvider()->config()->allWorkers()
                                : controller()->serviceProvider()->config()->workers();
    for (auto const& worker : workers) {
        _qservStatus.workers[worker] = false;
        _qservStatus.info[worker] = json::object();
        auto const request = controller()->serviceProvider()->qservMgtServices()->status(
                worker, id(), /* jobId */
                taskSelector(),
                [self](GetStatusQservMgtRequest::Ptr request) { self->_onRequestFinish(request); },
                timeoutSec());
        _requests[request->id()] = request;
        ++_numStarted;
    }

    // Finish right away if no workers were configured yet
    if (0 == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

void QservStatusJob::cancelImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    for (auto&& entry : _requests) {
        auto const& request = entry.second;
        request->cancel();
    }
    _requests.clear();
}

void QservStatusJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<QservStatusJob>(lock, _onFinish);
}

void QservStatusJob::_onRequestFinish(GetStatusQservMgtRequest::Ptr const& request) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << "[qserv]"
                   << "  worker=" << request->workerName());

    if (state() == State::FINISHED) return;
    replica::Lock const lock(_mtx, context() + string(__func__) + "[qserv]");
    if (state() == State::FINISHED) return;

    if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
        _qservStatus.workers[request->workerName()] = true;
        _qservStatus.info[request->workerName()] = request->info();
    }
    if (++_numFinished == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

}  // namespace lsst::qserv::replica
