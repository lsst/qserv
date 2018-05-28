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
#include "replica/Configuration.h"
#include "replica/Performance.h"
#include "util/BlockPost.h"

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
                   typename T::CallbackType  onFinish)
        :   JobWrapper(),
            _job(job),
            _onFinish(onFinish) {
    }

    /// Destructor
    ~JobWrapperImpl() override = default;

    Job::Ptr job() const override { return _job; }

private:

    // The context of the operation

    typename T::Ptr       _job;
    typename T::CallbackType _onFinish;
};

////////////////////////////////////////////////////////////////////
//////////////////////////  JobController  //////////////////////////
////////////////////////////////////////////////////////////////////

JobController::Ptr JobController::create(ServiceProvider::Ptr const& serviceProvider) {
    return JobController::Ptr(
        new JobController(serviceProvider));
}

JobController::JobController(ServiceProvider::Ptr const& serviceProvider)
    :   _serviceProvider(serviceProvider),
        _controller(Controller::create (serviceProvider)),
        _stop(false) {
}

void JobController::run() {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::run");

    util::Lock lock(_mtx, "JobController::run");

    if (not isRunning()) {

        // Run the controller in its own thread.

        _controller->run();

        JobController::Ptr self = shared_from_this();

        _thread.reset(
            new std::thread(
                [self] () {

                    // This will prevent the scheduler from existing unless
                    // instructed to do so

                    util::BlockPost blockPost(0, 1000); // values of parameters are meaningless
                                                        // in this context because the object will
                                                        // be always used to wait for a specific interval

                    unsigned int const wakeUpIvalMillisec =
                        1000 * self->_serviceProvider->config()->jobSchedulerIvalSec();

                    while (blockPost.wait(wakeUpIvalMillisec)) {

                        // Initiate the stopping sequence if requested
                        if (self->_stop) {

                            // Cancel all outstanding jobs
                            self->cancelAll();

                            // Block here waiting before the controller will stop
                            self->_controller->stop();

                            // Quit the thread
                            return;
                        }

                        // Check if there are jobs scheduled to run on the periodic basis
                        self->runScheduled();
                    }
                }
            )
        );
    }
}

bool JobController::isRunning() const {
    return _thread.get() != nullptr;
}

void JobController::stop() {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::stop");

    // IMPORTANT:
    //
    //   Never attempt running these operations within util::Lock lock(_mtx)
    //   due to a possibile deadlock when asynchronous handlers will be
    //   calling the thread-safe methods. A problem is that until they finish
    //   in a clean way the thread will never finish, and the application will
    //   hang on _thread->join().

    // util::Lock lock(_mtx)  (disabled)

    if (not isRunning()) return;

    // Tell the thread to finish
    _stop = true;

    // Join with the thread before clearning up the pointer
    _thread->join();
    _thread.reset(nullptr);

    _stop = false;
}

void JobController::join() {
    LOGS(_log, LOG_LVL_DEBUG, "JobController::join");
    if (_thread) _thread->join();
}

FindAllJob::Ptr JobController::findAll(std::string const& databaseFamily,
                                       bool saveReplicaInfo,
                                       FindAllJob::CallbackType onFinish,
                                       Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::findAll");

    util::Lock lock(_mtx, "JobController::findAll");

    auto const self = shared_from_this();
    auto const job =
        FindAllJob::create(
            databaseFamily,
            saveReplicaInfo,
            _controller,
            std::string(),
            [self] (FindAllJob::Ptr job) {
                self->onFinish (job);
            },
            options
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<FindAllJob>>(job, onFinish);

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start();

    return job;
}

FixUpJob::Ptr JobController::fixUp(std::string const& databaseFamily,
                                   FixUpJob::CallbackType onFinish,
                                   Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::fixUp");

    util::Lock lock(_mtx, "JobController::fixUp");

    auto const self = shared_from_this();
    auto const job =
        FixUpJob::create(
            databaseFamily,
            _controller,
            std::string(),
            [self] (FixUpJob::Ptr job) {
                self->onFinish (job);
            },
            options
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<FixUpJob>>(job, onFinish);

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start();

    return job;
}

PurgeJob::Ptr JobController::purge(std::string const& databaseFamily,
                                   unsigned int numReplicas,
                                   PurgeJob::CallbackType onFinish,
                                   Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::purge");

    util::Lock lock(_mtx, "JobController::purge");

    auto const self = shared_from_this();
    auto const job =
        PurgeJob::create(
            databaseFamily,
            numReplicas,
            _controller,
            std::string(),
            [self] (PurgeJob::Ptr job) {
                self->onFinish (job);
            },
            options
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<PurgeJob>>(job, onFinish);

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start();

    return job;
}

ReplicateJob::Ptr JobController::replicate(std::string const& databaseFamily,
                                           unsigned int numReplicas,
                                           ReplicateJob::CallbackType onFinish,
                                           Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::replicate");

    util::Lock lock(_mtx, "JobController::replicate");

    auto const self = shared_from_this();
    auto const job =
        ReplicateJob::create(
            databaseFamily,
            numReplicas,
            _controller,
            std::string(),
            [self] (ReplicateJob::Ptr job) {
                self->onFinish (job);
            },
            options
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<ReplicateJob>>(job, onFinish);

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start();

    return job;
}

VerifyJob::Ptr JobController::verify(VerifyJob::CallbackType onFinish,
                                     VerifyJob::CallbackTypeOnDiff onReplicaDifference,
                                     size_t maxReplicas,
                                     bool computeCheckSum,
                                     Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::verify");

    util::Lock lock(_mtx, "JobController::verify");

    auto const self = shared_from_this();
    auto const job =
        VerifyJob::create(
            _controller,
            std::string(),
            [self] (VerifyJob::Ptr job) {
                self->onFinish (job);
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
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start();

    return job;
}

DeleteWorkerJob::Ptr JobController::deleteWorker(
                                            std::string const& worker,
                                            bool permanentDelete,
                                            DeleteWorkerJob::CallbackType onFinish,
                                            Job::Options const& options) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::deleteWorker");

    util::Lock lock(_mtx, "JobController::deleteWorker");

    auto const self = shared_from_this();
    auto const job =
        DeleteWorkerJob::create(
            worker,
            permanentDelete,
            _controller,
            std::string(),
            [self] (DeleteWorkerJob::Ptr job) {
                self->onFinish (job);
            },
            options
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<DeleteWorkerJob>>(job, onFinish);

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start();

    return job;
}

void JobController::runQueued() {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::runQueued");

    util::Lock lock(_mtx, "JobController::runQueued");

    if (not isRunning()) return;

    // TODO:
    // Go through the input queue and evaluate which jobs should star
    // now based on their scheduling criteria and on the status of
    // the in-progres jobs (if any).
    ;
}

void JobController::runScheduled() {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::runScheduled");

    // Load the scheduled jobs (if any) from the database to see which ones
    // need to be injected into the input queue.
    //
    // NOTE: don't prolifirate the lock's scope to avoid an imminent deadlock
    //       when calling mehods which are called later.
    {
        util::Lock lock(_mtx, "JobController::runScheduled");

        if (not isRunning()) return;

        // TODO:
        ;
    }

    // Check the input (new jobs) queue to see if there are any requests
    // to be run.
    runQueued();
}

void JobController::cancelAll() {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::cancelAll");

    util::Lock lock(_mtx, "JobController::cancelAll");

    if (not isRunning()) return;

    // TODO:
    ;
}

void JobController::onFinish(Job::Ptr const& job) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController::onFinish  jobId=" << job->id());

    JobWrapper::Ptr wrapper;
    {
        util::Lock lock(_mtx, "JobController::onFinish");
        wrapper = _registry[job->id()];
        _registry.erase(job->id());

        // Move the job from the in-progress queue into the completed one
        ;
    }

    // Check the input (new jobs) queue to see if there are any requests
    // to be run.
    runQueued();

    // IMPORTANT: calling the notification from th elock-free zone to
    // avoid possible deadlocks in case if a client code will try to call
    // back the Scheduler from the callback function. Another reason of
    // doing this is to prevent locking the API in case of a prolonged
    // execution of the callback function (which can run an arbitrary code
    // not controlled from this implementation.).

    wrapper->notify();
}

}}} // namespace lsst::qserv::replica
