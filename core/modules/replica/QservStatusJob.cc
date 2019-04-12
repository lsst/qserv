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
#include "replica/QservStatusJob.h"

// System headers
#include <stdexcept>
#include <thread>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Controller.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservStatusJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& QservStatusJob::defaultOptions() {
    static Job::Options const options{
        0,      /* priority */
        false,  /* exclusive */
        true    /* preemptive */
    };
    return options;
}


string QservStatusJob::typeName() { return "QservStatusJob"; }


QservStatusJob::Ptr QservStatusJob::create(unsigned int timeoutSec,
                                           bool allWorkers,
                                           Controller::Ptr const& controller,
                                           string const& parentJobId,
                                           CallbackType const& onFinish,
                                           Job::Options const& options) {
    return QservStatusJob::Ptr(
        new QservStatusJob(timeoutSec,
                           allWorkers,
                           controller,
                           parentJobId,
                           onFinish,
                           options));
}


QservStatusJob::QservStatusJob(unsigned int timeoutSec,
                               bool allWorkers,
                               Controller::Ptr const& controller,
                               string const& parentJobId,
                               CallbackType const& onFinish,
                               Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "QSERV_STATUS",
            options),
        _timeoutSec(timeoutSec == 0
                    ? controller->serviceProvider()->config()->controllerRequestTimeoutSec()
                    : timeoutSec),
        _allWorkers(allWorkers),
        _onFinish(onFinish),
        _numStarted(0),
        _numFinished(0) {
}


QservStatus const& QservStatusJob::qservStatus() const {
 
    util::Lock lock(_mtx, context() + __func__);
 
    if (state() == State::FINISHED) return _qservStatus;

    throw logic_error(
            context() + string(__func__) + "  can't use this operation before finishing the job");
}


list<pair<string,string>> QservStatusJob::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("timeout_sec", to_string(timeoutSec()));
    result.emplace_back("all_workers", allWorkers() ? "1" : "0");
    return result;
}


list<pair<string,string>> QservStatusJob::persistentLogData() const {

    list<pair<string,string>> result;

    for (auto&& entry: qservStatus().workers) {

        auto worker = entry.first;
        auto responded = entry.second;
        
        if (not responded) {
            result.emplace_back("failed-worker", worker);
        }
    }
    return result;
}


void QservStatusJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto self = shared_from_base<QservStatusJob>();

    auto workers = allWorkers()
        ? controller()->serviceProvider()->config()->allWorkers()
        : controller()->serviceProvider()->config()->workers();

    for (auto const& worker: workers) {

        _qservStatus.workers[worker] = false;
        _qservStatus.info[worker] = json::object();

        auto const request = controller()->serviceProvider()->qservMgtServices()->status(
            worker,
            id(),   /* jobId */
            [self] (GetStatusQservMgtRequest::Ptr request) {
                self->_onRequestFinish(request);
            },
            timeoutSec()
        );
        _requests[request->id()] = request;
        ++_numStarted;
    }
    
    // Finish right away if no workers were configured yet

    if (0 == _numStarted) setState(lock, State::FINISHED, ExtendedState::SUCCESS);
    else                  setState(lock, State::IN_PROGRESS);
}


void QservStatusJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    for (auto&& entry: _requests) {
        auto const& request = entry.second;
        request->cancel();
    }
    _requests.clear();
}


void QservStatusJob::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    notifyDefaultImpl<QservStatusJob>(lock, _onFinish);
}


void QservStatusJob::_onRequestFinish(GetStatusQservMgtRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[qserv]"
         << "  worker=" << request->worker());

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" requests reporting
    // their completion while the job termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + string(__func__) + "[qserv]");

    if (state() == State::FINISHED) return;

    if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
        _qservStatus.workers[request->worker()] = true;
        _qservStatus.info[request->worker()] = request->info();
    }
    if (++_numFinished == _numStarted) finish(lock, ExtendedState::SUCCESS);
}

}}} // namespace lsst::qserv::replica
