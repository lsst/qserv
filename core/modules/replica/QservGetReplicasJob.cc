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

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

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

QservGetReplicasJob::Ptr QservGetReplicasJob::create(
                                    std::string const& databaseFamily,
                                    Controller::Ptr const& controller,
                                    std::string const& parentJobId,
                                    bool inUseOnly,
                                    CallbackType onFinish,
                                    Job::Options const& options) {
    return QservGetReplicasJob::Ptr(
        new QservGetReplicasJob(databaseFamily,
                                controller,
                                parentJobId,
                                inUseOnly,
                                onFinish,
                                options));
}

QservGetReplicasJob::QservGetReplicasJob(
                       std::string const& databaseFamily,
                       Controller::Ptr const& controller,
                       std::string const& parentJobId,
                       bool inUseOnly,
                       CallbackType onFinish,
                       Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "QSERV:GET_REPLICAS",
            options),
        _databaseFamily(databaseFamily),
        _inUseOnly(inUseOnly),
        _onFinish(onFinish),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

QservGetReplicasJobResult const& QservGetReplicasJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED) return _replicaData;

    throw std::logic_error(
        "QservGetReplicasJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void QservGetReplicasJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<QservGetReplicasJob>();

    for (auto&& worker: _controller->serviceProvider()->config()->workers()) {
        auto const request = _controller->serviceProvider()->qservMgtServices()->getReplicas(
            _databaseFamily,
            worker,
            _inUseOnly,
            _id,
            [self] (GetReplicasQservMgtRequest::Ptr const& request) {
                self->onRequestFinish(request);
            }
        );
        if (not request) {
            LOGS(_log, LOG_LVL_ERROR, context() << "startImpl  "
                 << "failed to submit GetReplicasQservMgtRequest to Qserv worker: " << worker);
            setState(State::FINISHED, ExtendedState::FAILED);
            return;
        }
        _requests.push_back(request);
        _numLaunched++;
    }

    // In case if no workers or database are present in the Configuration
    // at this time.
    if (not _numLaunched) setState(State::FINISHED);
    else                  setState(State::IN_PROGRESS);
}

void QservGetReplicasJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    for (auto&& ptr: _requests) {
        ptr->cancel();
    }
    _requests.clear();

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void QservGetReplicasJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<QservGetReplicasJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void QservGetReplicasJob::onRequestFinish(GetReplicasQservMgtRequest::Ptr const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  databaseFamily=" << request->databaseFamily()
         << " worker=" << request->worker()
         << " state=" << request->state2string(request->state())
         << " extendedState=" << request->state2string(request->extendedState()));

    LOCK_GUARD;

    // Ignore the callback if the job was cancelled, expired, etc.``
    if (_state == State::FINISHED) return;

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  databaseFamily=" << request->databaseFamily()
         << " worker=" << request->worker()
         << " ** LOCK_GUARD **");

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
    } else {
        _replicaData.workers[request->worker()] = false;
    }

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  databaseFamily=" << request->databaseFamily()
         << " worker=" << request->worker()
         << " _numLaunched=" << _numLaunched
         << " _numFinished=" << _numFinished
         << " _numSuccess=" << _numSuccess);

    if (_numFinished == _numLaunched) {
        finish(_numSuccess == _numLaunched ? ExtendedState::SUCCESS :
                                             ExtendedState::FAILED);
        notify();
    }
}

}}} // namespace lsst::qserv::replica
