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
#include "replica/JobController.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.JobController");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

//////////////////////////////////////////////////////////////////////
//////////////////////////  JobWrapperImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////

/**
 * Request-type specific wrappers
 */
template <class  T>
struct JobWrapperImpl
    :   JobWrapper {

    /// The implementation of the vurtual method defined in the base class
    virtual void notify() {
        if (_onFinish == nullptr) return;
        _onFinish(_job);
    }

    JobWrapperImpl(typename T::Ptr const& job,
                   typename T::CallbackType onFinish)
        :   JobWrapper(),
            _job(job),
            _onFinish(onFinish) {
    }

    /// Destructor
    ~JobWrapperImpl() override = default;

    Job::Ptr job() const override { return _job; }

private:

    // The context of the operation

    typename T::Ptr _job;
    typename T::CallbackType _onFinish;
};

////////////////////////////////////////////////////////////////////
//////////////////////////  JobController  //////////////////////////
////////////////////////////////////////////////////////////////////

std::string JobController::state2string(State state) {
    switch (state) {
        case State::NOT_RUNNING: return "NOT_RUNNING";
        case State::IS_RUNNING:  return "IS_RUNNING";
        case State::IS_STOPPING: return "IS_STOPPING";        
        default:
            throw std::logic_error("JobController::state2string  incomplete implementation of the method");
    }
}

JobController::Ptr JobController::create(ServiceProvider::Ptr const& serviceProvider) {
    return JobController::Ptr(
        new JobController(serviceProvider));
}

JobController::JobController(ServiceProvider::Ptr const& serviceProvider)
    :   _serviceProvider(serviceProvider),
        _controller(Controller::create (serviceProvider)),
        _state(State::NOT_RUNNING) {
}

bool JobController::run() {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::run");

    util::Lock lock(_mtx, "JobController::run");

    switch (_state) {

        case State::NOT_RUNNING:

            _controller->run();

            _state = State::IS_RUNNING;
            return true;

        case State::IS_RUNNING:  return true;
        case State::IS_STOPPING: return false;
        
        default:
            throw std::logic_error("JobController::run  incomplete implementation of the method");
    }
}

void JobController::stop() {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::stop");

    util::Lock lock(_mtx, "JobController::stop");

    switch (_state) {

        case State::IS_RUNNING:

            // This should also cancel all outstanding requests

            _controller->stop();

            // Cancel all registered jobs (if any). The jobs will report their completion by
            // calling this Controller's method onFinish.

            for (auto&& entry: _registry) {
                entry.second->job()->cancel();
            }

            // If the Registry is empty then we are done. Otherwise the final state
            // transition will happen when the last job will report to this Controller's
            // method onFinish.

            _state = _registry.empty() ? State::NOT_RUNNING : State::IS_STOPPING;
            return;

        case State::IS_STOPPING: return;
        case State::NOT_RUNNING: return;
        
        default:
            throw std::logic_error("JobController::stop  incomplete implementation of the method");
    }
}

FindAllJob::Ptr JobController::findAll(std::string const& databaseFamily,
                                       bool saveReplicaInfo,
                                       FindAllJob::CallbackType onFinish,
                                       std::string const& parentJobId,
                                       Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::findAll");

    util::Lock lock(_mtx, "JobController::findAll");

    if (State::IS_RUNNING != _state) return nullptr;

    auto const self = shared_from_this();
    auto const job = FindAllJob::create(
        databaseFamily,
        saveReplicaInfo,
        _controller,
        parentJobId,
        [self] (FindAllJob::Ptr job) {
            self->onFinish(job);
        },
        options
    );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<FindAllJob>>(job, onFinish);

    // Initiate the job

    job->start();

    return job;
}

FixUpJob::Ptr JobController::fixUp(std::string const& databaseFamily,
                                   FixUpJob::CallbackType onFinish,
                                   std::string const& parentJobId,
                                   Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::fixUp");

    util::Lock lock(_mtx, "JobController::fixUp");

    if (State::IS_RUNNING != _state) return nullptr;

    auto const self = shared_from_this();
    auto const job = FixUpJob::create(
        databaseFamily,
        _controller,
        parentJobId,
        [self] (FixUpJob::Ptr job) {
            self->onFinish(job);
        },
        options
    );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<FixUpJob>>(job, onFinish);

    // Initiate the job

    job->start();

    return job;
}

PurgeJob::Ptr JobController::purge(std::string const& databaseFamily,
                                   unsigned int numReplicas,
                                   PurgeJob::CallbackType onFinish,
                                   std::string const& parentJobId,
                                   Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::purge");

    util::Lock lock(_mtx, "JobController::purge");

    if (State::IS_RUNNING != _state) return nullptr;

    auto const self = shared_from_this();
    auto const job = PurgeJob::create(
        databaseFamily,
        numReplicas,
        _controller,
        parentJobId,
        [self] (PurgeJob::Ptr job) {
            self->onFinish(job);
        },
        options
    );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<PurgeJob>>(job, onFinish);

    // Initiate the job

    job->start();

    return job;
}

ReplicateJob::Ptr JobController::replicate(std::string const& databaseFamily,
                                           unsigned int numReplicas,
                                           ReplicateJob::CallbackType onFinish,
                                           std::string const& parentJobId,
                                           Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::replicate");

    util::Lock lock(_mtx, "JobController::replicate");

    if (State::IS_RUNNING != _state) return nullptr;

    auto const self = shared_from_this();
    auto const job = ReplicateJob::create(
        databaseFamily,
        numReplicas,
        _controller,
        parentJobId,
        [self] (ReplicateJob::Ptr job) {
            self->onFinish(job);
        },
        options
    );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<ReplicateJob>>(job, onFinish);

    // Initiate the job

    job->start();

    return job;
}

VerifyJob::Ptr JobController::verify(VerifyJob::CallbackType onFinish,
                                     VerifyJob::CallbackTypeOnDiff onReplicaDifference,
                                     size_t maxReplicas,
                                     bool computeCheckSum,
                                     std::string const& parentJobId,
                                     Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::verify");

    util::Lock lock(_mtx, "JobController::verify");

    if (State::IS_RUNNING != _state) return nullptr;

    auto const self = shared_from_this();
    auto const job = VerifyJob::create(
        _controller,
        parentJobId,
        [self] (VerifyJob::Ptr job) {
            self->onFinish(job);
        },
        onReplicaDifference,
        maxReplicas,
        computeCheckSum,
        options
    );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<VerifyJob>>(job, onFinish);

    // Initiate the job

    job->start();

    return job;
}

DeleteWorkerJob::Ptr JobController::deleteWorker(std::string const& worker,
                                                 bool permanentDelete,
                                                 DeleteWorkerJob::CallbackType onFinish,
                                                 std::string const& parentJobId,
                                                 Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::deleteWorker");

    util::Lock lock(_mtx, "JobController::deleteWorker");

    if (State::IS_RUNNING != _state) return nullptr;

    auto const self = shared_from_this();
    auto const job = DeleteWorkerJob::create(
        worker,
        permanentDelete,
        _controller,
        parentJobId,
        [self] (DeleteWorkerJob::Ptr job) {
            self->onFinish(job);
        },
        options
    );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<DeleteWorkerJob>>(job, onFinish);

    // Initiate the job

    job->start();

    return job;
}

void JobController::onFinish(Job::Ptr const& job) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::onFinish  jobId=" << job->id());

    // Find the job wrapper in case if the job is still registered

    JobWrapper::Ptr ptr;
    {
        util::Lock lock(_mtx, "JobController::onFinish:1");

        // Remove this job from the registry (if it's still there)

        auto itr = _registry.find(job->id());
        if (_registry.end() != itr) {
            ptr = itr->second;
            _registry.erase(itr);
        }
    }

    // IMPORTANT: calling the notification from the lock-free zone to
    // avoid possible deadlocks in case if a client code will try calling
    // back the Controller from the callback function. Another reason of
    // doing this is to prevent locking the API in case of a prolonged
    // execution of the callback function (which can run an arbitrary code
    // not controlled from this implementation.).

    if (ptr != nullptr) ptr->notify();

    // Finish the state transition in case if the Job Controller was being stopped
    // and this was the last request
    //
    // NOTE: the state transition should be happening after sending notifications
    //       to outstanding jobs to ensure all clients were notified.
    //       
    {
        util::Lock lock(_mtx, "JobController::onFinish:2");

        if (State::IS_STOPPING == _state) {
            if (_registry.empty()) {
                _state = State::NOT_RUNNING;
            }
        }
    }
}

}}} // namespace lsst::qserv::replica
