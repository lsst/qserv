// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_JOB_CONTROLLER_H
#define LSST_QSERV_REPLICA_JOB_CONTROLLER_H

/// JobController.h declares:
///
/// class JobController
/// (see individual class documentation for more information)

// System headers
#include <algorithm>
#include <atomic>
#include <list>
#include <memory> 
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/Job.h"

// Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class DeleteWorkerJob;
class FindAllJob;
class FixUpJob;
class PurgeJob;
class ReplicaDiff;
class ReplicateJob;
class ServiceProvider;
class VerifyJob;

/**
 * The base class for implementing requests registry as a polymorphic
 * collection to store active jobs. Pure virtual methods of
 * the class will be overriden by request-type-specific implementations
 * (see struct JobWrappeImpl<JOB_TYPE> in the .cc file) capturing
 * type-dependant pointer and a callback function.
 */
struct JobWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<JobWrapper> pointer;

    /// Destructor
    virtual ~JobWrapper() {}

    /// This method will be called upon a completion of a request
    /// to notify a subscriber on the event.
    virtual void notify ()=0;

    /// Return a pointer to the stored job object
    virtual Job::pointer job () const=0;
};

/**
  * Class JobController is a front-end interface for processing
  * jobs fro connected clients.
  */
class JobController
    :   public std::enable_shared_from_this<JobController> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<JobController> pointer;

    // Forward declarations for class-specific pointers

    typedef std::shared_ptr<FindAllJob>      FindAllJob_pointer;
    typedef std::shared_ptr<PurgeJob>        PurgeJob_pointer;
    typedef std::shared_ptr<ReplicateJob>    ReplicateJob_pointer;
    typedef std::shared_ptr<FixUpJob>        FixUpJob_pointer;
    typedef std::shared_ptr<VerifyJob>       VerifyJob_pointer;
    typedef std::shared_ptr<DeleteWorkerJob> DeleteWorkerJob_pointer;

    typedef std::function<void(FindAllJob_pointer)>      FindAllJob_callback_type;
    typedef std::function<void(PurgeJob_pointer)>        PurgeJob_callback_type;
    typedef std::function<void(ReplicateJob_pointer)>    ReplicateJob_callback_type;
    typedef std::function<void(FixUpJob_pointer)>        FixUpJob_callback_type;
    typedef std::function<void(VerifyJob_pointer)>       VerifyJob_callback_type;
    typedef std::function<void(DeleteWorkerJob_pointer)> DeleteWorkerJob_callback_type;

    typedef std::function<void(VerifyJob_pointer,
                               ReplicaDiff const&,
                               std::vector<ReplicaDiff> const&)> VerifyJob_callback_type_on_diff;

    /// The priority queue for pointers to the new (unprocessed) jobs.
    /// Using inheritance to get access to the protected data members 'c'
    /// representing the internal container.
    struct PriorityQueueType
        :   std::priority_queue<Job::pointer,
                                std::vector<Job::pointer>,
                                JobCompare> {

        /// The beginning of the container to allow the iterator protocol
        decltype(c.begin()) begin () {
            return c.begin();
        }

        /// The end of the container to allow the iterator protocol
        decltype(c.end()) end () {
            return c.end();
        }

        /// Remove an entry from the queue by its identifier
        bool remove (std::string const& id) {
            auto itr = std::find_if (
                c.begin(),
                c.end(),
                [&id] (Job::pointer const& ptr) {
                    return ptr->id() == id;
                }
            );
            if (itr != c.end()) {
                c.erase(itr);
                std::make_heap(c.begin(), c.end(), comp);
                return true;
            }
            return false;
        }
    };

    /// Ordinary collection of pointers for jobs in other (than new/unprocessed)
    /// states
    typedef std::list<Job::pointer> CollectionType;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, other services
     */
    static pointer create (ServiceProvider &serviceProvider);

    // Default construction and copy semantics are prohibited

    JobController () = delete;
    JobController (JobController const&) = delete;
    JobController &operator= (JobController const&) = delete;

    /// Destructor
    virtual ~JobController ();

    /**
     * Run the scheduler in a dedicated thread unless it's already running.
     * It's safe to call this method multiple times from any thread.
     */
    void run ();

    /**
     * Check if the service is running.
     *
     * @return true if the scheduler is running.
     */
    bool isRunning () const;

    /**
     * Stop the scheduler. This method will guarantee that all outstanding
     * opeations will finish and not aborted.
     *
     * This operation will also result in stopping the internal thread
     * in which the scheduler is being run.
     */
    void stop ();

    /**
     * Join with a thread in which the scheduler is being run (if any).
     * If the scheduler was not started or if it's stopped the the method
     * will return immediattely.
     *
     * This method is meant to be used for integration of the scheduler into
     * a larger multi-threaded application which may require a proper
     * synchronization between threads.
     */
    void join ();
    
    /**
     * Submit a job for finding all replicas and updating replica status
     * in the database family.
     *
     * @param databaseFamily - the name of a database family
     * @param onFinish       - a callback function to be called upon a completion of the job
     * @param priority       - set the desired job priority (larger values
     *                         mean higher priorities). A job with the highest
     *                         priority will be select from an input queue by
     *                         the JobController.
     * @param exclusive      - set to 'true' to indicate that the job can't be
     *                         running simultaneously alongside other jobs.
     * @param preemptable    - set to 'true' to indicate that this job can be
     *                         interrupted to give a way to some other job of
     *                         high importancy.
     */
    FindAllJob_pointer findAll (std::string const&       databaseFamily,
                                FindAllJob_callback_type onFinish    = nullptr,
                                int                      priority    = 0,
                                bool                     exclusive   = false,
                                bool                     preemptable = true);

    /**
     * Submit a job for fixin up all non-colocateds replicas.
     *
     * @param databaseFamily - the name of a database family
     * @param onFinish       - a callback function to be called upon a completion of the job
     * @param priority       - set the desired job priority (larger values
     *                         mean higher priorities). A job with the highest
     *                         priority will be select from an input queue by
     *                         the JobController.
     * @param exclusive      - set to 'true' to indicate that the job can't be
     *                         running simultaneously alongside other jobs.
     * @param preemptable    - set to 'true' to indicate that this job can be
     *                         interrupted to give a way to some other job of
     *                         high importancy.
     */
    FixUpJob_pointer fixUp (std::string const&     databaseFamily,
                            FixUpJob_callback_type onFinish    = nullptr,
                            int                    priority    = 2,
                            bool                   exclusive   = true,
                            bool                   preemptable = false);

    /**
     * Submit a job for bringing the number of each chunk's replicas down
     * to a desired level.
     * 
     * @param databaseFamily - the name of a database family
     * @param numReplicas    - the maximum number of replicas allowed for each chunk
     *                         (if set to 0 then the value of the parameter will be pulled
     *                         from the Configuration)
     * @param onFinish       - a callback function to be called upon a completion of the job
     * @param priority       - set the desired job priority (larger values
     *                         mean higher priorities). A job with the highest
     *                         priority will be select from an input queue by
     *                         the JobController.
     * @param exclusive      - set to 'true' to indicate that the job can't be
     *                         running simultaneously alongside other jobs.
     * @param preemptable    - set to 'true' to indicate that this job can be
     *                         interrupted to give a way to some other job of
     *                         high importancy.
     */
    PurgeJob_pointer purge (std::string const&     databaseFamily,
                            unsigned int           numReplicas,
                            PurgeJob_callback_type onFinish    = nullptr,
                            int                    priority    = -1,
                            bool                   exclusive   = false,
                            bool                   preemptable = true);

    /**
     * Submit a job for bringing the number of each chunk's replicas up
     * to a desired level.
     * 
     * @param databaseFamily - the name of a database family
     * @param numReplicas    - the maximum number of replicas allowed for each chunk
     *                         (if set to 0 then the value of the parameter will be pulled
     *                         from the Configuration)
     * @param onFinish       - a callback function to be called upon a completion of the job
     * @param priority       - set the desired job priority (larger values
     *                         mean higher priorities). A job with the highest
     *                         priority will be select from an input queue by
     *                         the JobController.
     * @param exclusive      - set to 'true' to indicate that the job can't be
     *                         running simultaneously alongside other jobs.
     * @param preemptable    - set to 'true' to indicate that this job can be
     *                         interrupted to give a way to some other job of
     *                         high importancy.
     */
    ReplicateJob_pointer replicate (std::string const&         databaseFamily,
                                    unsigned int               numReplicas,
                                    ReplicateJob_callback_type onFinish    = nullptr,
                                    int                        priority    = 1,
                                    bool                       exclusive   = true,
                                    bool                       preemptable = true);

    /**
     * Submit a job for verifying integrity of known replicas, updating their status
     * accross all databases and workers.
     *
     * @param onFinish       - a callback function to be called upon a completion of the job
     @ @param onReplicaDifference - a callback function to be called when two replicas won't match
     * @param priority       - set the desired job priority (larger values
     *                         mean higher priorities). A job with the highest
     *                         priority will be select from an input queue by
     *                         the JobController.
     * @param exclusive      - set to 'true' to indicate that the job can't be
     *                         running simultaneously alongside other jobs.
     * @param preemptable    - set to 'true' to indicate that this job can be
     *                         interrupted to give a way to some other job of
     *                         high importancy.
     */
    VerifyJob_pointer verify (VerifyJob_callback_type         onFinish            = nullptr,
                              VerifyJob_callback_type_on_diff onReplicaDifference = nullptr,
                              int                             priority            = -2,
                              bool                            exclusive           = false,
                              bool                            preemptable         = true);

    /**
     * Submit a job for disabling or permanently deleting (depends on the corresponding
     * option) a worker from teh replication setup.
     * 
     * @param worker         - the name of a worker
     * @param permanentDelete - if set to 'true' the worker record will be completelly wiped out
     *                          from the configuration
     * @param onFinish       - a callback function to be called upon a completion of the job
     * @param priority       - set the desired job priority (larger values
     *                         mean higher priorities). A job with the highest
     *                         priority will be select from an input queue by
     *                         the JobController.
     * @param exclusive      - set to 'true' to indicate that the job can't be
     *                         running simultaneously alongside other jobs.
     * @param preemptable    - set to 'true' to indicate that this job can be
     *                         interrupted to give a way to some other job of
     *                         high importancy.
     */
    DeleteWorkerJob_pointer deleteWorker (std::string const&            worker,
                                          bool                          permanentDelete,
                                          DeleteWorkerJob_callback_type onFinish    = nullptr,
                                          int                           priority    = 2,
                                          bool                          exclusive   = true,
                                          bool                          preemptable = false);

    // TODO: add job inspection methods

private:   

    /**
     * The constructor of the class.
     *
     * @see JobController::create()
     */
    JobController (ServiceProvider& serviceProvider);

    /**
     * Check is there are any jobs in the input queue which are eligible
     * to be run immediatelly based on their scheduling attributes, such
     * as: 'priority', 'exclusive' or 'preemptable' modes. If so then launch
     * them.
     */
    void runQueued ();

    /**
     * Check is there are any time-based jobs which are supposed to run on
     * the periodic basis. If so then launch them.
     *
     * The jobs of this type will be pulled from the database each time
     * the metghod is called. If there are the ones which are ready to run
     * the jobs will be put into the input queue and the previously
     * defined method JobController::runQueuedJobs() will be invoked.
     */
    void runScheduled ();

    /**
     * Stop all in-progress jobs and do *NOT* start the new ones.
     */
    void cancelAll ();

    /**
     * The callback method to be called upon a completion of a job.
     * This may also invoke method JobController::runQueuedJobs()
     *
     * @param job - a reference to the job
     */
    void onFinish (Job::pointer const& job);

private:

    /// Services used by the processor
    ServiceProvider& _serviceProvider;

    /// A dedciated instance of the Controller for executing requests
    Controller::pointer _controller;

    /// This thread will run the asynchronous prosessing of the jobs
    std::unique_ptr<std::thread> _thread;

    /// Mutex guarding the queues
    mutable std::mutex _mtx;

    /// The flag to be raised to tell the running thread to stop.
    /// The thread will reset this flag when it finishes.
    std::atomic<bool> _stop;
 
    /// Job wrappers registered by their unique identifiers for
    /// to allow an efficient lookup and for type-specific ntifications
    /// upon their completion.
    std::map<std::string, JobWrapper::pointer> _registry;

    /// New unprocessed jobs
    PriorityQueueType _newJobs;

    /// Jobs which are being processed
    CollectionType _inProgressJobs;

    /// Completed (succeeded or otherwise) jobs
    CollectionType _finishedJobs;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_JOB_CONTROLLER_H