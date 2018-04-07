/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/DeleteWorkerJob.h"

// System headers
#include <algorithm>
#include <atomic>
#include <future>
#include <stdexcept>
#include <tuple>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/DatabaseServices.h"
#include "replica/ErrorReporting.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DeleteWorkerJob");

/**
 * Count the total number of entries in the input collection,
 * the number of finished entries, nd the total number of succeeded
 * entries.
 *
 * The "entries" in this context are either derivatives of the Request
 * or Job types.
 *
 * @param collection - a collection of entries to be analyzed
 * @return - a tuple of three elements
 */
template <class T>
std::tuple<size_t,size_t,size_t> counters(std::list<typename T::pointer> const& collection) {
    size_t total    = 0;
    size_t finished = 0;
    size_t success  = 0;
    for (auto&& ptr: collection) {
        total++;
        if (ptr->state() == T::State::FINISHED) {
            finished++;
            if (ptr->extendedState() == T::ExtendedState::SUCCESS) {
                success++;
            }
        }
    }
    return std::make_tuple(total, finished, success);
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Job::Options const& DeleteWorkerJob::defaultOptions() {
    static Job::Options const options{
        2,      /* priority */
        true,   /* exclusive */
        false   /* exclusive */
    };
    return options;
}

DeleteWorkerJob::pointer DeleteWorkerJob::create(std::string const& worker,
                                                 bool permanentDelete,
                                                 Controller::pointer const& controller,
                                                 std::string const& parentJobId,
                                                 callback_type onFinish,
                                                 Job::Options const& options) {
    return DeleteWorkerJob::pointer(
        new DeleteWorkerJob(worker,
                            permanentDelete,
                            controller,
                            parentJobId,
                            onFinish,
                            options));
}

DeleteWorkerJob::DeleteWorkerJob(std::string const& worker,
                                 bool permanentDelete,
                                 Controller::pointer const& controller,
                                 std::string const& parentJobId,
                                 callback_type onFinish,
                                 Job::Options const& options)
    :   Job(controller,
            parentJobId,
            "DELETE_WORKER",
            options),
        _worker(worker),
        _permanentDelete(permanentDelete),
        _onFinish(onFinish),
        _numLaunched(0),
        _numFinished(0),
        _numSuccess(0) {
}

DeleteWorkerJobResult const& DeleteWorkerJob::getReplicaData() const {

    LOGS(_log, LOG_LVL_DEBUG, context());

    if (_state == State::FINISHED) { return _replicaData; }

    throw std::logic_error(
        "DeleteWorkerJob::getReplicaData()  the method can't be called while the job hasn't finished");
}

void DeleteWorkerJob::startImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    util::BlockPost blockPost(1000, 2000);

    auto self = shared_from_base<DeleteWorkerJob>();

    // Check the status of the worker service, and if it's still running
    // try to get as much info from it as possible

    std::atomic<bool> statusRequestFinished{false};

    auto const statusRequest = _controller->statusOfWorkerService(
        _worker,
        [&statusRequestFinished](ServiceStatusRequest::pointer const& request) {
            statusRequestFinished = true;
        },
        _id,    /* jobId */
        60      /* requestExpirationIvalSec */
    );
    while (not statusRequestFinished) {
        LOGS(_log, LOG_LVL_DEBUG, context() << "wait for worker service status");
        blockPost.wait();
    }
    if (statusRequest->extendedState() == Request::ExtendedState::SUCCESS) {
        if (statusRequest->getServiceState().state == ServiceState::State::RUNNING) {

            // Make sure the service won't be executing any other "leftover"
            // requests which may be interfeering with the current job's requests

            std::atomic<bool> drainRequestFinished{false};

            auto const drainRequest = _controller->drainWorkerService(
                _worker,
                [&drainRequestFinished](ServiceDrainRequest::pointer const& request) {
                    drainRequestFinished = true;
                },
                _id,    /* jobId */
                60      /* requestExpirationIvalSec */
            );
            while (not drainRequestFinished) {
                LOGS(_log, LOG_LVL_DEBUG, context() << "wait for worker service drain");
                blockPost.wait();
            }
            if (drainRequest->extendedState() == Request::ExtendedState::SUCCESS) {
                if (drainRequest->getServiceState().state == ServiceState::State::RUNNING) {

                    // Try to get the most recent state the worker's replicas
                    // for all known databases

                    for (auto&& database: _controller->serviceProvider()->config()->databases()) {
                        auto const request = _controller->findAllReplicas(
                            _worker,
                            database,
                            [self] (FindAllRequest::pointer const& request) {
                                self->onRequestFinish(request);
                            }
                        );
                        _findAllRequests.push_back(request);
                        _numLaunched++;
                    }

                    // The rest will be happening in a method processing the completion
                    // of the above launched requests.

                    setState(State::IN_PROGRESS);
                    return;
                }
            }
        }
    }

    // Since the worker is not available then go straight to a point
    // at which we'll be changing its state within the replication system
    disableWorker();

    setState(State::IN_PROGRESS);
    return;
}

void DeleteWorkerJob::cancelImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& ptr: _findAllRequests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            _controller->stopReplicaFindAll(
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
        }
    }

    // Stop chained jobs (if any) as well

    for (auto&& ptr: _findAllJobs)   { ptr->cancel(); }
    for (auto&& ptr: _replicateJobs) { ptr->cancel(); }
}

void DeleteWorkerJob::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<DeleteWorkerJob>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

void DeleteWorkerJob::onRequestFinish(FindAllRequest::pointer const& request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish"
         << "  worker="   << request->worker()
         << "  database=" << request->database());

    // Ignore the callback if the job was cancelled
    if (_state == State::FINISHED) { return; }

    do {
        // This lock will be automatically release beyond this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        _numFinished++;
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _numSuccess++;
        }

    } while (false);

    // Evaluate the status of on-going operations to see if the job
    // has finished. If so then proceed to the next stage of the job.
    //
    // ATTENTION: we don't care about the completion status of the requests
    // because the're related to a worker which is going t be removed, and
    // this worker may already be experiencing problems.
    //
    if (_numFinished == _numLaunched) {
        disableWorker();
    }
}

void
DeleteWorkerJob::disableWorker() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "disableWorker");

    // Temporary disable this worker from the configuration. If it's requsted
    // to be permanently deleted this will be done only after all other relevamnt
    // operations of this job will be done.

    _controller->serviceProvider()->config()->disableWorker(_worker);

    // Launch the chained jobs to get chunk disposition within the rest
    // of the cluster

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;

    auto self = shared_from_base<DeleteWorkerJob>();

    for (auto&& databaseFamily: _controller->serviceProvider()->config()->databaseFamilies()) {
        FindAllJob::pointer job = FindAllJob::create(
            databaseFamily,
            _controller,
            _id,
            [self] (FindAllJob::pointer job) {
                self->onJobFinish(job);
            }
        );
        job->start();
        _findAllJobs.push_back(job);
        _numLaunched++;
    }
}

void DeleteWorkerJob::onJobFinish(FindAllJob::pointer const& job) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onJobFinish(FindAllJob) "
         << " databaseFamily: " << job->databaseFamily());

    // Ignore the callback if the job was cancelled (or otherwise failed)
    if (_state == State::FINISHED) { return; }

    do {
        // This lock will be automatically released beyond this scope
        // to allow client notifications (see the end of the method)

        LOCK_GUARD;

        _numFinished++;

        if (job->extendedState() == ExtendedState::SUCCESS) {

            _numSuccess++;

            if (_numFinished == _numLaunched) {

                // Launch chained jobs to ensure the minimal replication level
                // which might be affected by the worker removal.

                _numLaunched = 0;
                _numFinished = 0;
                _numSuccess  = 0;

                auto self = shared_from_base<DeleteWorkerJob>();

                for (auto&& databaseFamily: _controller->serviceProvider()->config()->databaseFamilies()) {
                    ReplicateJob::pointer const job = ReplicateJob::create(
                        databaseFamily,
                        0,  /* numReplicas -- pull from Configuration */
                        _controller,
                        _id,
                        [self] (ReplicateJob::pointer job) {
                            self->onJobFinish(job);
                        }
                    );
                    job->start();
                    _replicateJobs.push_back(job);
                    _numLaunched++;
                }
            }
        } else {
            finish(ExtendedState::FAILED);
            break;
        }

    } while (false);

    // Note that access to the job's public API should not be locked while
    // notifying a caller (if the callback function was povided) in order to avoid
    // the circular deadlocks.

    if (_state == State::FINISHED) { notify (); }
}

void DeleteWorkerJob::onJobFinish(ReplicateJob::pointer const& job) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onJobFinish(ReplicateJob) "
         << " databaseFamily: " << job->databaseFamily()
         << " numReplicas: " << job->numReplicas()
         << " state: " << Job::state2string(job->state(), job->extendedState()));

    // Ignore the callback if the job was cancelled (or otherwise failed)
    if (_state == State::FINISHED) { return; }

    do {
        // This lock will be automatically release beyond this scope
        // to allow client notifications (see the end of the method)

        LOCK_GUARD;

        _numFinished++;

        if (job->extendedState() == ExtendedState::SUCCESS) {

            _numSuccess++;

            LOGS(_log, LOG_LVL_DEBUG, context() << "onJobFinish(ReplicateJob)  "
                 << "job->getReplicaData().chunks.size(): " << job->getReplicaData().chunks.size());

            // Merge results into the current job's result object
            _replicaData.chunks[job->databaseFamily()] = job->getReplicaData().chunks;

        } else {
            finish(ExtendedState::FAILED);
            break;
        }
        if (_numFinished == _numLaunched) {

            // Construct a collection of orphan replicas if possible

            ReplicaInfoCollection replicas;
            if (_controller->serviceProvider()->databaseServices()->findWorkerReplicas(replicas, _worker)) {
                for (ReplicaInfo const& replica: replicas) {
                    unsigned int const chunk    = replica.chunk();
                    std::string const& database = replica.database();

                    bool replicated = false;
                    for (auto&& databaseFamilyEntry: _replicaData.chunks) {
                        auto const& chunks = databaseFamilyEntry.second;
                        replicated = replicated or
                            (chunks.count(chunk) and chunks.at(chunk).count(database));
                    }
                    if (not replicated) {
                        _replicaData.orphanChunks[chunk][database] = replica;
                    }
                }
            }

            // TODO: if the list of orphan chunks is not empty then consider bringing
            // back the disabled worker (if the service still responds) in the read-only
            // mode and try using it for redistributing those chunks accross the cluster.
            //
            // NOTE: this could be a complicated procedure which needs to be thought
            // through.
            ;

            // Do this only if requested, and only in case of the successful
            // completion of the job
            if (_permanentDelete) {
                _controller->serviceProvider()->config()->deleteWorker(_worker);
            }
            finish(ExtendedState::SUCCESS);
            break;
        }

    } while (false);

    // Note that access to the job's public API should not be locked while
    // notifying a caller (if the callback function was povided) in order to avoid
    // the circular deadlocks.

    if (_state == State::FINISHED) { notify (); }
}

}}} // namespace lsst::qserv::replica
