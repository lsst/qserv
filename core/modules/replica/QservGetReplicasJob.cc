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
#include "replica/QservGetReplicasJob.h"

// System headers
#include <future>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservGetReplicasJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& QservGetReplicasJob::defaultOptions() {
    static Job::Options const options{
        0,      /* priority */
        false,  /* exclusive */
        true    /* exclusive */
    };
    return options;
}


std::string QservGetReplicasJob::typeName() { return "QservGetReplicasJob"; }


QservGetReplicasJob::Ptr QservGetReplicasJob::create(
                                    std::string const& databaseFamily,
                                    bool inUseOnly,
                                    bool allWorkers,
                                    Controller::Ptr const& controller,
                                    std::string const& parentJobId,
                                    CallbackType const& onFinish,
                                    Job::Options const& options) {
    return QservGetReplicasJob::Ptr(
        new QservGetReplicasJob(databaseFamily,
                                inUseOnly,
                                allWorkers,
                                controller,
                                parentJobId,
                                onFinish,
                                options));
}

QservGetReplicasJob::QservGetReplicasJob(
                       std::string const& databaseFamily,
                       bool inUseOnly,
                       bool allWorkers,
                       Controller::Ptr const& controller,
                       std::string const& parentJobId,
                       CallbackType const& onFinish,
                       Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "QSERV_GET_REPLICAS",
            options),
        _databaseFamily(databaseFamily),
        _inUseOnly(inUseOnly),
        _allWorkers(allWorkers),
        _onFinish(onFinish),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

QservGetReplicasJobResult const& QservGetReplicasJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (state() == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "QservGetReplicasJob::getReplicaData  the method can't be called while the job hasn't finished");
}

std::list<std::pair<std::string,std::string>> QservGetReplicasJob::extendedPersistentState() const {
    std::list<std::pair<std::string,std::string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("in_use_only",     inUseOnly()  ? "1" : "0");
    result.emplace_back("all_workers",     allWorkers() ? "1" : "0");
    return result;
}

void QservGetReplicasJob::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<QservGetReplicasJob>();

    auto const workerNames = allWorkers() ?
        controller()->serviceProvider()->config()->allWorkers() :
        controller()->serviceProvider()->config()->workers();

    for (auto&& worker: workerNames) {
        _replicaData.workers[worker] = false;
        auto const request = controller()->serviceProvider()->qservMgtServices()->getReplicas(
            databaseFamily(),
            worker,
            inUseOnly(),
            id(),
            [self] (GetReplicasQservMgtRequest::Ptr const& request) {
                self->onRequestFinish(request);
            }
        );
        if (not request) {

            LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
                 << "failed to submit GetReplicasQservMgtRequest to Qserv worker: " << worker);

            setState(lock, State::FINISHED, ExtendedState::FAILED);
            return;
        }
        _requests.push_back(request);
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.
    if (not _numLaunched) setState(lock, State::FINISHED);
    else                  setState(lock, State::IN_PROGRESS);
}

void QservGetReplicasJob::cancelImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    for (auto&& ptr: _requests) {
        ptr->cancel();
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void QservGetReplicasJob::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    notifyDefaultImpl<QservGetReplicasJob>(lock, _onFinish);
}

void QservGetReplicasJob::onRequestFinish(GetReplicasQservMgtRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  databaseFamily=" << request->databaseFamily()
         << " worker=" << request->worker()
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

        // Merge results of the request into the summary data collection
        // of the job.

        _replicaData.replicas[request->worker()] = request->replicas();
        for (auto&& replica: request->replicas()) {
            _replicaData.useCount.atChunk(replica.chunk)
                                 .atDatabase(replica.database)
                                 .atWorker(request->worker()) = replica.useCount;
        }
        _replicaData.workers[request->worker()] = true;
    }

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  databaseFamily=" << request->databaseFamily()
         << " worker=" << request->worker()
         << " _numLaunched=" << _numLaunched
         << " _numFinished=" << _numFinished
         << " _numSuccess=" << _numSuccess);

    if (_numFinished == _numLaunched) {
        finish(lock, _numSuccess == _numLaunched ? ExtendedState::SUCCESS :
                                                   ExtendedState::FAILED);
    }
}

}}} // namespace lsst::qserv::replica
