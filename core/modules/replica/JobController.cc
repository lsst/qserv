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
#include "replica/BlockPost.h"
#include "replica/DeleteWorkerJob.h"
#include "replica/FindAllJob.h"
#include "replica/FixUpJob.h"
#include "replica/Performance.h"
#include "replica/PurgeJob.h"
#include "replica/ReplicateJob.h"
#include "replica/ServiceProvider.h"
#include "replica/VerifyJob.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

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
    virtual void notify () {
        if (_onFinish == nullptr) return;
        _onFinish (_job);
    }

    JobWrapperImpl (typename T::pointer const& job,
                    typename T::callback_type  onFinish)
        :   JobWrapper(),
            _job      (job),
            _onFinish (onFinish) {
    }

    /// Destructor
    ~JobWrapperImpl() override = default;

    Job::pointer job () const override { return _job; }

private:

    // The context of the operation

    typename T::pointer       _job;
    typename T::callback_type _onFinish;
};

////////////////////////////////////////////////////////////////////
//////////////////////////  JobController  //////////////////////////
////////////////////////////////////////////////////////////////////

JobController::pointer JobController::create (ServiceProvider& serviceProvider) {
    return JobController::pointer (
        new JobController (serviceProvider));
}

JobController::JobController (ServiceProvider& serviceProvider)
    :   _serviceProvider (serviceProvider),
        _controller      (Controller::create (serviceProvider)),
        _stop            (false) {
}

void JobController::run () {

    LOGS(_log, LOG_LVL_DEBUG, "JobController  run");

    LOCK_GUARD;

    if (!isRunning()) {

        // Run the controller in its own thread.
        
        _controller->run ();

        JobController::pointer self = shared_from_this();

        _thread.reset (
            new std::thread (
                [self] () {
        
                    // This will prevent the scheduler from existing unless
                    // instructed to do so
                    
                    BlockPost blockPost (0, 1000);  // values of parameters are meaningless
                                                    // in this context because the object will
                                                    // be always used to wait for a specific interval

                    unsigned int const wakeUpIvalMillisec =
                        1000 * self->_serviceProvider.config()->jobSchedulerIvalSec();

                    while (blockPost.wait (wakeUpIvalMillisec)) {

                        // Initiate the stopping sequence if requested
                        if (self->_stop) {

                            // Cancel all outstanding jobs
                            self->cancelAll ();

                            // Block here waiting before the controller will stop
                            self->_controller->stop ();

                            // Quit the thread
                            return;
                        }

                        // Check if there are jobs scheduled to run on the periodic basis
                        self->runScheduled ();
                    }
                }
            )
        );
    }
}

bool JobController::isRunning () const {
    return _thread.get() != nullptr;
}

void JobController::stop () {

    LOGS(_log, LOG_LVL_DEBUG, "JobController  stop");

    if (!isRunning()) return;

    // IMPORTANT:
    //
    //   Never attempt running these operations within LOCK_GUARD
    //   due to a possibile deadlock when asynchronous handlers will be
    //   calling the thread-safe methods. A problem is that until they finish
    //   in a clean way the thread will never finish, and the application will
    //   hang on _thread->join().

    // LOCK_GUARD  (disabled)

    // Tell the thread to finish    
    _stop = true;

    // Join with the thread before clearning up the pointer
    _thread->join();
    _thread.reset (nullptr);

    _stop = false;
}

void JobController::join () {
    LOGS(_log, LOG_LVL_DEBUG, "JobController  join");
    if (_thread) _thread->join();
}

FindAllJob::pointer JobController::findAll (std::string const&        databaseFamily,
                                            FindAllJob::callback_type onFinish,
                                            int                       priority,
                                            bool                      exclusive,
                                            bool                      preemptable) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController  findAll");

    LOCK_GUARD;

    JobController::pointer self = shared_from_this();

    FindAllJob::pointer job =
        FindAllJob::create (
            databaseFamily,
            _controller,
            [self] (FindAllJob::pointer job) {
                self->onFinish (job);
            },
            priority,
            exclusive,
            preemptable
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<FindAllJob>> (job, onFinish);  

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start ();

    return job;
}

FixUpJob::pointer JobController::fixUp (std::string const&      databaseFamily,
                                        FixUpJob::callback_type onFinish,
                                        int                     priority,
                                        bool                    exclusive,
                                        bool                    preemptable) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController  fixUp");

    LOCK_GUARD;

    JobController::pointer self = shared_from_this();

    FixUpJob::pointer job =
        FixUpJob::create (
            databaseFamily,
            _controller,
            [self] (FixUpJob::pointer job) {
                self->onFinish (job);
            },
            priority,
            exclusive,
            preemptable
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<FixUpJob>> (job, onFinish);  

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start ();

    return job;
}

PurgeJob::pointer JobController::purge (std::string const&      databaseFamily,
                                        unsigned int            numReplicas,
                                        PurgeJob::callback_type onFinish,
                                        int                     priority,
                                        bool                    exclusive,
                                        bool                    preemptable) {
    
    LOGS(_log, LOG_LVL_DEBUG, "JobController  purge");

    JobController::pointer self = shared_from_this();

    PurgeJob::pointer job =
        PurgeJob::create (
            databaseFamily,
            numReplicas,
            _controller,
            [self] (PurgeJob::pointer job) {
                self->onFinish (job);
            },
            priority,
            exclusive,
            preemptable
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<PurgeJob>> (job, onFinish);  

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start ();

    return job;
}

ReplicateJob::pointer JobController::replicate (std::string const&          databaseFamily,
                                                unsigned int                numReplicas,
                                                ReplicateJob::callback_type onFinish,
                                                int                         priority,
                                                bool                        exclusive,
                                                bool                        preemptable) {
    
    LOGS(_log, LOG_LVL_DEBUG, "JobController  replicate");

    LOCK_GUARD;

    JobController::pointer self = shared_from_this();

    ReplicateJob::pointer job =
        ReplicateJob::create (
            databaseFamily,
            numReplicas,
            _controller,
            [self] (ReplicateJob::pointer job) {
                self->onFinish (job);
            },
            priority,
            exclusive,
            preemptable
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<ReplicateJob>> (job, onFinish);  

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start ();

    return job;
}

VerifyJob::pointer JobController::verify (VerifyJob::callback_type         onFinish,
                                          VerifyJob::callback_type_on_diff onReplicaDifference,
                                          int                              priority,
                                          bool                             exclusive,
                                          bool                             preemptable) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController  verify");

    LOCK_GUARD;

    JobController::pointer self = shared_from_this();

    VerifyJob::pointer job =
        VerifyJob::create (
            _controller,
            [self] (VerifyJob::pointer job) {
                self->onFinish (job);
            },
            onReplicaDifference,
            priority,
            exclusive,
            preemptable
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<VerifyJob>> (job, onFinish);  

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start ();

    return job;
}

DeleteWorkerJob::pointer JobController::deleteWorker (
                                            std::string const&             worker,
                                            bool                           permanentDelete,
                                            DeleteWorkerJob::callback_type onFinish,
                                            int                            priority,
                                            bool                           exclusive,
                                            bool                           preemptable) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController  deleteWorker");

    LOCK_GUARD;

    JobController::pointer self = shared_from_this();

    DeleteWorkerJob::pointer job =
        DeleteWorkerJob::create (
            worker,
            permanentDelete,
            _controller,
            [self] (DeleteWorkerJob::pointer job) {
                self->onFinish (job);
            },
            priority,
            exclusive,
            preemptable
        );

    // Register the job (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[job->id()] =
        std::make_shared<JobWrapperImpl<DeleteWorkerJob>> (job, onFinish);  

    // Initiate the job
    //
    // FIXME: don't start the job right away. Put the request into the priority queue
    // and call the scheduler's method to evaluate jobs in the queue to
    // to see which should be started next (if any).

    job->start ();

    return job;
}

void JobController::runQueued () {

    if (!isRunning()) return;

    LOGS(_log, LOG_LVL_DEBUG, "JobController  runQueued");

    LOCK_GUARD;
    
    // Go through the input queue and evaluate which jobs should star
    // now based on their scheduling criteria and on the status of
    // the in-progres jobs (if any).
}

void JobController::runScheduled () {

    if (!isRunning()) return;

    LOGS(_log, LOG_LVL_DEBUG, "JobController  runScheduled");

    // Load the scheduled jobs (if any) from the database to see which ones
    // need to be injected into the input queue.
    //
    // NOTE: don't prolifirate the lock's scope to avoid an imminent deadlock
    //       when calling mehods which are called later.
    {
        LOCK_GUARD;
    
        // TODO:
        ;
    }

    // Check the input (new jobs) queue to see if there are any requests
    // to be run.
    runQueued ();
}

void JobController::cancelAll () {

    if (!isRunning()) return;

    LOGS(_log, LOG_LVL_DEBUG, "JobController  cancelAll");

    LOCK_GUARD;
}

void JobController::onFinish (Job::pointer const& job) {

    LOGS(_log, LOG_LVL_DEBUG, "JobController  onFinish  jobId=" << job->id());

    JobWrapper::pointer wrapper;
    {
        LOCK_GUARD;
        wrapper = _registry[job->id()];
        _registry.erase (job->id());

        // Move the job from the in-progress queue into the completed one
        ;
    }

    // Check the input (new jobs) queue to see if there are any requests
    // to be run.
    runQueued ();

    // IMPORTANT: calling the notification from th elock-free zone to
    // avoid possible deadlocks in case if a client code will try to call
    // back the Scheduler from the callback function. Another reason of
    // doing this is to prevent locking the API in case of a prolonged
    // execution of the callback function (which can run an arbitrary code
    // not controlled from this implementation.).

    wrapper->notify();}

}}} // namespace lsst::qserv::replica