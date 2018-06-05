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
#ifndef LSST_QSERV_REPLICA_JOBCONTROLLER_H
#define LSST_QSERV_REPLICA_JOBCONTROLLER_H

/// JobController.h declares:
///
/// class JobController
/// (see individual class documentation for more information)

// System headers
#include <algorithm>
#include <atomic>
#include <memory>
#include <queue>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/DeleteWorkerJob.h"
#include "replica/FindAllJob.h"
#include "replica/FixUpJob.h"
#include "replica/Job.h"
#include "replica/PurgeJob.h"
#include "replica/ReplicateJob.h"
#include "replica/ServiceProvider.h"
#include "replica/VerifyJob.h"
#include "util/Mutex.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations

/**
 * Class JobWrapper is the base for implementing requests registry as a polymorphic
 * collection to store active jobs. Pure virtual methods of the class will be
 * overriden by request-type-specific implementations (see struct JobWrappeImpl<JOB_TYPE>
 * in the .cc file) capturing type-dependant pointer and a callback function.
 */
struct JobWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<JobWrapper> Ptr;

    virtual ~JobWrapper() = default;

    /// For subscriber notification upon a completion of a requeston
    virtual void notify()=0;

    /// @return a pointer to the stored job object
    virtual Job::Ptr job() const=0;
};

/**
 * Struct PriorityQueueType extends the priority queue for job pointers
 * to allow using the queue in the forEach iteration of C++11.
 * Its implementation is relying upon the inheritance to get access to
 * the protectedvdata member 'c' representing the internal container.
 */
struct PriorityQueueType
    :   std::priority_queue<Job::Ptr,
                            std::vector<Job::Ptr>,
                            JobCompare> {

    /// @return reference to the beginning of the container to allow the iterator protocol
    decltype(c.begin()) begin() {
        return c.begin();
    }

    /// @return reference to the end of the container to allow the iterator protocol
    decltype(c.end()) end() {
        return c.end();
    }

    /// Remove an entry from the queue by its identifier
    bool remove(std::string const& id) {
        auto itr = std::find_if(
            c.begin(),
            c.end(),
            [&id] (Job::Ptr const& ptr) {
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

/**
  * Class JobController is a front-end interface for controlling jobs.
  */
class JobController
    :   public std::enable_shared_from_this<JobController> {

public:

    /**
     * The enum is meant to support the Controller's state machine.
     */
    enum State {

        // Not running (ether never started or was stopped). Job submission request can't be
        // accepted in this state. All job submission request will return 'nullptr'.
        NOT_RUNNING,
        
        // The Controller is running and accepting new jobs
        IS_RUNNING,

        // Is being stopped. No new job submition requests are allowed.
        IS_STOPPING
    };

    /// @return string representaiton of a state
    static std::string state2string(State state);
 
    /// The pointer type for instances of the class
    typedef std::shared_ptr<JobController> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers). Once finished the new object will be in State::NOT_RUNNING.
     * The Controller will need to be started (see method JobController::run) before
     * it will be able to accept job submission requests.
     *
     * @param serviceProvider - for configuration, other services
     *
     * @return pointer to the new object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider);

    // Default construction and copy semantics are prohibited

    JobController() = delete;
    JobController(JobController const&) = delete;
    JobController& operator=(JobController const&) = delete;

    virtual ~JobController() = default;

    /// @return current state of the Controller
    State state() const { return _state; }

    /**
     * Run the Job Controller unless it's already running. This operation assumes
     * that the Controller is either in State::NOT_RUNNING or State::IS_RUNNING.
     * If the Controller happens to be in State::IS_RUNNING then no further actions
     * will be taken.
     *
     * @return 'true' if the operation was successful (or if the controller is already
     * running), or 'false' if the controller was being stopped (in State::IS_STOPPING).
     */
    bool run();

    /**
     * Stop the Job Controller if it's still running (being in either State::IS_RUNNING or
     * State::IS_STOPPING). As a result of this operation the Controller will turn into
     * either State::IS_STOPPING (if there are outstanding jobs which are still being cancelled)
     * or State::NOT_RUNNING (if no outstanding jobs were detected). No actions will be made
     * if the Controller was in State::NOT_RUNNING
     *
     * This method will also order a cancellation of the outstanding operations to allow
     * them to finish gracefully. Note that this method will NOT block a calling thread
     * neither it will wait before the job will finish (get cancelled). It's up to
     * a caller of this method to track the Controller's state to ensure that it gets
     * into State::NOT_RUNNING.
     */
    void stop();

    /**
     * Submit a job for finding all replicas and updating replica status
     * in the database family.
     *
     * @param databaseFamily  - name of a database family
     * @param saveReplicaInfo - save replica info in a database
     * @param onFinish        - (optional) callback function to be called upon a completion of the job
     * @param parentJobId     - (optional) identifier of a parent job
     * @param options         - (optional) job options
     *
     * @return pointer to the submitted job, or nullptr if the operation isn't allowed
     * in the current state of the Controller.
     */
    FindAllJob::Ptr findAll(std::string const& databaseFamily,
                            bool saveReplicaInfo,
                            FindAllJob::CallbackType onFinish = nullptr,
                            std::string const& parentJobId = std::string(),
                            Job::Options const& options = FindAllJob::defaultOptions());

    /**
     * Submit a job for fixing up all non-colocated replicas.
     *
     * @param databaseFamily - name of a database family
     * @param onFinish       - (optional) callback function to be called upon a completion of the job
     * @param parentJobId    - (optional) identifier of a parent job
     * @param options        - (optional) job options
     *
     * @return pointer to the submitted job, or nullptr if the operation isn't allowed
     * in the current state of the Controller.
     */
    FixUpJob::Ptr fixUp(std::string const& databaseFamily,
                        FixUpJob::CallbackType onFinish = nullptr,
                        std::string const& parentJobId = std::string(),
                        Job::Options const& options = FixUpJob::defaultOptions());

    /**
     * Submit a job for bringing the number of each chunk's replicas down
     * to a desired level.
     *
     * @param databaseFamily - name of a database family
     * @param numReplicas    - (optional) maximum number of replicas allowed for each chunk
     *                         (if set to 0 then the value of the parameter will be pulled
     *                         from the Configuration)
     * @param onFinish       - (optional) callback function to be called upon a completion of the job
     * @param parentJobId    - (optional) identifier of a parent job
     * @param options        - (optional) job options
     * 
     * @return pointer to the submitted job, or nullptr if the operation isn't allowed
     * in the current state of the Controller.
     */
    PurgeJob::Ptr purge(std::string const& databaseFamily,
                        unsigned int numReplicas = 0,
                        PurgeJob::CallbackType onFinish = nullptr,
                        std::string const& parentJobId = std::string(),
                        Job::Options const& options = PurgeJob::defaultOptions());

    /**
     * Submit a job for bringing the number of each chunk's replicas up
     * to a desired level.
     *
     * @param databaseFamily - name of a database family
     * @param numReplicas    - (optional) maximum number of replicas allowed for each chunk
     *                         (if set to 0 then the value of the parameter will be pulled
     *                         from the Configuration)
     * @param onFinish       - (optional) callback function to be called upon a completion of the job
     * @param parentJobId    - (optional) identifier of a parent job
     * @param options        - (optional) job options
     * 
     * @return pointer to the submitted job, or nullptr if the operation isn't allowed
     * in the current state of the Controller.
     */
    ReplicateJob::Ptr replicate(std::string const& databaseFamily,
                                unsigned int numReplicas = 0,
                                ReplicateJob::CallbackType onFinish = nullptr,
                                std::string const& parentJobId = std::string(),
                                Job::Options const& options = ReplicateJob::defaultOptions());

    /**
     * Submit a job for verifying integrity of known replicas, updating their status
     * accross all databases and workers.
     *
     * @param onFinish            - (optional) callback function to be called upon a completion of the job
     @ @param onReplicaDifference - (optional) callback function to be called when two replicas won't match
     * @param maxReplicas         - (optional)  maximum number of replicas to process simultaneously.
     *                              If the parameter is set to 0 (the default value) then 1 replica
     *                              will be assumed.
     * @param computeCheckSum     - (optional) tell a worker server to compute check/control sum on each file
     * @param parentJobId         - (optional) identifier of a parent job
     * @param options             - (optional) job options
     *
     * @return pointer to the submitted job, or nullptr if the operation isn't allowed
     * in the current state of the Controller.
     */
    VerifyJob::Ptr verify(VerifyJob::CallbackType onFinish = nullptr,
                              VerifyJob::CallbackTypeOnDiff onReplicaDifference = nullptr,
                              size_t maxReplicas = 0,
                              bool computeCheckSum = false,
                              std::string const& parentJobId = std::string(),
                              Job::Options const& options = VerifyJob::defaultOptions());

    /**
     * Submit a job for disabling or permanently deleting (depends on the corresponding
     * option) a worker from the replication setup.
     *
     * @param worker          - name of a worker
     * @param permanentDelete - if set to 'true' the worker record will be completelly wiped out
     *                          from the configuration
     * @param onFinish        - (optional) callback function to be called upon a completion of the job
     * @param parentJobId     - (optional) identifier of a parent job
     * @param options         - (optional) job options
     * 
     * @return pointer to the submitted job, or nullptr if the operation isn't allowed
     * in the current state of the Controller.
     */
    DeleteWorkerJob::Ptr deleteWorker(std::string const& worker,
                                      bool permanentDelete,
                                      DeleteWorkerJob::CallbackType onFinish = nullptr,
                                      std::string const& parentJobId = std::string(),
                                      Job::Options const& options = DeleteWorkerJob::defaultOptions());

private:

    /**
     * The constructor of the class.
     *
     * @see JobController::create()
     */
    JobController(ServiceProvider::Ptr const& serviceProvider);

    /**
     * The callback method to be called upon a completion of a job.
     * The finished jobs will get removed from the Job Controller's registry.
     *
     * @param job - a reference to the job
     */
    void onFinish(Job::Ptr const& job);

private:

    /// Services used by the processor
    ServiceProvider::Ptr _serviceProvider;

    /// A dedciated instance of the Controller for executing requests
    Controller::Ptr _controller;

    /// The current state
    std::atomic<State> _state;

    /// Job wrappers registered by their unique identifiers for
    /// to allow an efficient lookup and for type-specific ntifications
    /// upon their completion.
    std::map<std::string, JobWrapper::Ptr> _registry;

    /// Mutex guarding the queues
    mutable util::Mutex _mtx;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_JOBCONTROLLER_H
