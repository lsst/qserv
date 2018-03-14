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
#ifndef LSST_QSERV_REPLICA_JOB_H
#define LSST_QSERV_REPLICA_JOB_H

/// Job.h declares:
///
/// class Job
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <mutex>
#include <ostream>
#include <string>

// Qserv headers
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/Controller.h"
#include "replica/RemoveReplicaQservMgtRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class Job is a base class for a family of replication jobs within
  * the master server.
  */
class Job
    :   public std::enable_shared_from_this<Job>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Job> pointer;

    /// Primary public state of the job
    enum State {

        /// The job has been constructed, and no attempt to execute it has
        /// been made.
        CREATED,

        /// The job is in a progress
        IN_PROGRESS,

        /// The job is finihed. See extended status for more details
        /// (the completion status, etc.)
        FINISHED
    };

    /// Return the string representation of the primary state
    static std::string state2string(State state) ;

    /// Refined public sub-state of the job once it's FINISHED as per
    /// the above defined primary state.
    enum ExtendedState {

        /// No extended state exists at this time
        NONE,

        /// The job has been fully implemented
        SUCCESS,

        /// The job has failed
        FAILED,

        /// Qserv notification failed
        QSERV_FAILED,

        /// Qserv reported that the source chunk is in use and couldn't be removed
        QSERV_IN_USE,

        /// Expired due to a timeout (as per the Configuration)
        EXPIRED,

        /// Explicitly cancelled on the client-side (similar to EXPIRED)
        CANCELLED
    };

    /// Return the string representation of the extended state
    static std::string state2string(ExtendedState state) ;

    /// Return the string representation of the combined state
    static std::string state2string(State state, ExtendedState extendedState) {
        return state2string(state) + "::" +state2string(extendedState);
    }

    // Default construction and copy semantics are prohibited

    Job() = delete;
    Job(Job const&) = delete;
    Job& operator=(Job const&) = delete;

    /// Destructor
    virtual ~Job() = default;

    /// @return a reference to the Controller,
    Controller::pointer controller() { return _controller; }

    /// @return the optional identifier of a parent job
    std::string const& parentJobId() const { return _parentJobId; }

    /// @return a string representing a type of a job.
    std::string const& type() const { return _type; }

    /// @return a unique identifier of the job
    std::string const& id() const { return _id; }

    /// @return the priority of the job
    int priority() const { return _priority; }

    /// @return the flag indicating of this job can't be run simultaneously
    /// along with other jobs.
    bool exclusive() const { return _exclusive; }

    /// @return 'true' if the job is allowed to be interrupted by some
    /// by other jobs.
    bool preemptable() const { return _preemptable; }

    /// @return the primary status of the job
    State state() const { return _state; }

    /// Return the extended state of the job when it's finished
    ExtendedState extendedState() const { return _extendedState; }

    /**
     * @return a start time (milliseconds since UNIX Epoch) or 0 before method start()
     * is called to actually begin wexecuting the job.
     */
    uint64_t beginTime() const { return _beginTime; }

    /**
     * @return the end time (milliseconds since UNIX Epoch) or 0 before job
     * is finished.
     */
    uint64_t endTime() const { return _endTime; }

    /**
     * Reset the state (if needed) and begin processing the job.
     */
    void start();

    /**
     * Explicitly cancel the job and all relevant requests which may be still
     * in flight.
     */
    void cancel();

    /**
     * Block the calling thread while the job is being executed. Make periodic
     * progress reports if requested. Print error reports on failed requests
     * if reuested.
     *
     * NOTE: this operation will *NOT* block the request execution or request
     *       callbacks because they'd be running in a separate thread from which
     *       requests and jobs are launched.
     *
     * @param os             - an output stream for monitoring and error printouts
     * @param progressReport - triggers periodic printout onto an output stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     * @chunkLocksReport     - print a report on chunks which are still allocated by
     *                         the job as the operation progresses.
     */
    virtual void track(bool progressReport,
                       bool errorReport,
                       bool chunkLocksReport,
                       std::ostream& os) const=0;

    /// Return the context string for debugging and diagnostic printouts
    std::string context() const;

protected:

    /// Return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /**
     * Construct the request with the pointer to the services provider.
     *
     * @param controller  - for launching requests
     * @param parentJobId - optional identifier of a parent job
     * @param type        - its type name
     * @param priority    - set the desired job priority (larger values
     *                      mean higher priorities). A job with the highest
     *                      priority will be select from an input queue by
     *                      the JobScheduler.
     * @param exclusive   - set to 'true' to indicate that the job can't be
     *                      running simultaneously alongside other jobs.
     * @param preemptable - set to 'true' to indicate that this job can be
     *                      interrupted to give a way to some other job of
     *                      high importancy.
     */
    Job(Controller::pointer const& controller,
        std::string const& parentJobId,
        std::string const& type,
        int  priority,
        bool exclusive,
        bool preemptable);

    /**
      * This method is supposed to be provided by subclasses for additional
      * subclass-specific actions to begin processing the request.
      */
    virtual void startImpl()=0;

    /**
      * This method is supposed to be provided by subclasses
      * to finalize request processing as required by the subclass.
      */
    virtual void cancelImpl()=0;

    /**
      * This method is supposed to be provided by subclasses
      * to notify a caller by invoking a subclass-specific callback
      * function registered for the completion of the job.
      */
    virtual void notify()=0;

    /**
     * Notify Qserv about a new chunk added to its database.
     *
     * @param chunk - chunk number
     * @param databaseFamily - the name of a database family
     * @param worker - the name of a worker to be notified
     * @param onFinish - optional callback funciton to be called upon completion
     *        of the operation
     */
    void qservAddReplica(unsigned int chunk,
                         std::string const& databaseFamily,
                         std::string const& worker,
                         AddReplicaQservMgtRequest::callback_type onFinish=nullptr);

    /**
      * Notify Qserv about a new chunk added to its database.
      *
      * @param chunk - chunk number
      * @param databaseFamily - the name of a database family
      * @param worker - the name of a worker to be notified
      * @param force - the flag indicating of the removal should be done regardless
      *        of the usage status of the replpica
      * @param onFinish - optional callback funciton to be called upon completion
      *        of the operation
      */
    void qservRemoveReplica(unsigned int chunk,
                            std::string const& databaseFamily,
                            std::string const& worker,
                            bool force,
                            RemoveReplicaQservMgtRequest::callback_type onFinish=nullptr);

    /**
     * Ensure the object is in the deseride internal state. Throw an
     * exception otherwise.
     *
     * NOTES: normally this condition should never been seen unless
     *        there is a problem with the application implementation
     *        or the underlying run-time system.
     *
     * @param desiredState - the desired state
     *
     * @throws std::logic_error
     */
    void assertState(State desiredState) const;

    /**
     * Set the desired primary and extended state.
     *
     * The change of the state is done via a method to allow extra actions
     * at this step, such as:
     *
     * - reporting change state in a debug stream
     * - verifying the correctness of the state transition
     *
     * @param state         - the new primary state
     * @param extendedState - the new extended state
     */
    void setState(State state,
                  ExtendedState extendedState=ExtendedState::NONE);

protected:

    /// The unique identifier of the job
    std::string _id;

    /// The Controller for performing requests
    Controller::pointer _controller;

    /// The unique identifier of the parent job
    std::string _parentJobId;

    /// The type of the job
    std::string _type;

    // Job scheduling attributes

    int  _priority;
    bool _exclusive;
    bool _preemptable;

    /// Primary state of the job
    State _state;

    /// Extended state of the job
    ExtendedState _extendedState;

    // Start and end times (milliseconds since UNIX Epoch)

    uint64_t _beginTime;
    uint64_t _endTime;

    /// Mutex guarding internal state
    mutable std::mutex _mtx;
};

/// Comparision type for strict weak ordering reaquired by std::priority_queue
struct JobCompare {

    /// Order requests by their priorities
    bool operator()(Job::pointer const& lhs,
                    Job::pointer const& rhs) const {

        return lhs->priority() < rhs->priority();
    }
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_JOB_H
