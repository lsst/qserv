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
#include <atomic>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

// THird party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/Controller.h"
#include "replica/RemoveReplicaQservMgtRequest.h"
#include "util/Mutex.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
namespace database {
namespace mysql {
class Connection;
}}

/**
  * Class Job is a base class for a family of replication jobs within
  * the master server.
  */
class Job
    :   public std::enable_shared_from_this<Job>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Job> Ptr;

    /// The pointer type for the database connector which provides a database-specific
    /// SQL generation services.
    typedef std::shared_ptr<database::mysql::Connection> SqlGeneratorPtr;

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

        /// Problems with job configuration found
        CONFIG_ERROR,

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

    /// @return string representation of the extended state
    static std::string state2string(ExtendedState state) ;

    /// @return string representation of the combined state
    static std::string state2string(State state, ExtendedState extendedState) {
        return state2string(state) + "::" +state2string(extendedState);
    }

    /// The job options container
    struct Options {

        /// the priority level
        int  priority;

        /// the flag indicating of this job can't be run simultaneously
        /// along with other jobs.
        bool exclusive;

        /// the flag indicating if the job is allowed to be interrupted by some
        /// by other jobs.
        bool preemptable;
    };

    // Default construction and copy semantics are prohibited

    Job() = delete;
    Job(Job const&) = delete;
    Job& operator=(Job const&) = delete;

    virtual ~Job() = default;

    /// @return a reference to the Controller,
    Controller::Ptr controller() const { return _controller; }

    /// @return the optional identifier of a parent job
    std::string const& parentJobId() const { return _parentJobId; }

    /// @return a string representing a type of a job.
    std::string const& type() const { return _type; }

    /// @return a unique identifier of the job
    std::string const& id() const { return _id; }

    /// @return the primary status of the job
    State state() const { return _state; }

    /// @return the extended state of the job when it's finished
    ExtendedState extendedState() const { return _extendedState; }

    /// @return string representation of the combined state of the object
    std::string state2string() const { return state2string(_state, _extendedState); }

    /// @return job options
    Options const& options() const { return _options; }

    /**
     * Modify job options
     *
     * @param newOptions - new options to be set
     * @return the previous state of the options
     */
    Options setOptions(Options const& newOptions);

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

    /// @return the context string for debugging and diagnostic printouts
    std::string context() const;

    /**
     * @return a string representation for a subclass's persistent state
     * ready to be insert into the corresponding table as the values string
     * of the SQL INSERT statement:
     *     INSERT INTO <job-specific-table> VALUES <result-of-this-method>
     *
     * Note, that the result string must include round brackets as reaquired
     * by the SQL standard. The string values need to be properly escaped and
     * santized as required by the corresponiding database service (which
     * is passed as parameter into the method).
     *
     * The table name will be automatically deduced from a job-specific value
     * returned by method Job::type().
     *
     * @param gen - pointer to the SQL statements generation service
     */
    virtual std::string extendedPersistentState(SqlGeneratorPtr const& gen) const {
        return std::string();
    }

protected:

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
    Job(Controller::Ptr const& controller,
        std::string const& parentJobId,
        std::string const& type,
        Options const& options);

    /// Return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /**
      * This method is supposed to be provided by subclasses for additional
      * subclass-specific actions to begin processing the request.
      *
      * @param lock - the lock must be acquired by a caller of the method
      */
    virtual void startImpl(util::Lock const& lock) = 0;

    /**
     * The sequence of actions to be executed when the job is transitioning into
     * the finished state (regardless of a specific exended state).
     *
     * NOTES:
     * 1. normally this is mandatory method which is supposrd to be called either
     *    internally witin this class on the job expiration (internal timer) or
     *    cancellation (as requested externally by a user).
     * 
     * 2. the only methods which are allowed to turn objects into the FINISHED
     *    extended state are user-provided methods startImpl().
     *
     * @param lock          - the lock must be acquired by a caller of the method
     * @param extendedState - specific state to be set upon the completion
     */
    void finish(util::Lock const& lock,
                ExtendedState extendedState);

    /**
      * This method is supposed to be provided by subclasses
      * to finalize request processing as required by the subclass.
      *
      * @param lock - the lock must be acquired by a caller of the method
      */
    virtual void cancelImpl(util::Lock const& lock) = 0;

    /**
      * This method is supposed to be provided by subclasses
      * to notify a caller by invoking a subclass-specific callback
      * function registered for the completion of the job.
      */
    virtual void notifyImpl() = 0;

    /**
     * Notify Qserv about a new chunk added to its database.
     *
     * @param lock      - the lock must be acquired by a caller of the method
     * @param chunk     - chunk number
     * @param databases - the names of databases
     * @param worker    - the name of a worker to be notified
     * @param onFinish  - (optional) callback funciton to be called upon completion
     *                    of the operation
     */
    void qservAddReplica(util::Lock const& lock,
                         unsigned int chunk,
                         std::vector<std::string> const& databases,
                         std::string const& worker,
                         AddReplicaQservMgtRequest::CallbackType onFinish=nullptr);

    /**
      * Notify Qserv about a new chunk added to its database.
      *
      * @param lock      - the lock must be acquired by a caller of the method
      * @param chunk     - chunk number
      * @param databases - the names of databases
      * @param worker    - the name of a worker to be notified
      * @param force     - the flag indicating of the removal should be done regardless
      *                    of the usage status of the replpica
      * @param onFinish  - (optional) callback funciton to be called upon completion
      *                    of the operation
      */
    void qservRemoveReplica(util::Lock const& lock,
                            unsigned int chunk,
                            std::vector<std::string> const& databases,
                            std::string const& worker,
                            bool force,
                            RemoveReplicaQservMgtRequest::CallbackType onFinish=nullptr);

    /**
     * Ensure the object is in the deseride internal state. Throw an
     * exception otherwise.
     *
     * NOTES: normally this condition should never been seen unless
     *        there is a problem with the application implementation
     *        or the underlying run-time system.
     *
     * @param lock         - the lock must be acquired by a caller of the method
     * @param desiredState - desired state
     * @param context      - context from which the state test is requested
     *
     * @throws std::logic_error
     */
    void assertState(util::Lock const& lock,
                     State desiredState,
                     std::string const& context) const;

    /**
     * Set the desired primary and extended state.
     *
     * The change of the state is done via a method to allow extra actions
     * at this step, such as:
     *
     * - reporting change state in a debug stream
     * - verifying the correctness of the state transition
     *
     * @param lock          - the lock must be acquired by a caller of the method
     * @param state         - the new primary state
     * @param extendedState - (optional) new extended state
     */
    void setState(util::Lock const& lock,
                  State state,
                  ExtendedState extendedState=ExtendedState::NONE);

private:

    /**
     * Start the timer (if the corresponidng Configuration parameter is set`).
     * When the time will expire then the callback method heartbeat() which is
     * defined below will be called.
     *
     * @param lock - the lock must be acquired by a caller of the method
     */
    void startHeartbeatTimer(util::Lock const& lock);

    /**
     * Job heartbeat timer's handler. The heartbeat interval (if any)
     * is configured via the configuraton service. When the timer expires
     * the job would update the corresponding field in a database and restart
     * the time.
     *
     * @param ec - error code to be evaluated
     */
    void heartbeat(boost::system::error_code const& ec);

    /**
     * Start the timer (if the corresponidng Configuration parameter is set`).
     * When the time will expire then the callback method expired() which is
     * defined below will be called.
     *
     * @param lock - the lock must be acquired by a caller of the method
     */
    void startExpirationTimer(util::Lock const& lock);

    /**
     * Job expiration timer's handler. The expiration interval (if any)
     * is configured via the configuraton service. When the job expires
     * it finishes with completion status FINISHED::EXPIRED.
     *
     * @param ec - error code to be evaluated
     */
    void expired(boost::system::error_code const& ec);

    /**
     * This method will begin an optional user protocol upon a completion
     * of a job (if any user-supplied callback function was provided).
     * The method will eventually use subclass-specific method notifyImpl().
     *
     * @see Job::notifyImpl
     */
    void notify();

protected:

    /// Mutex guarding internal state
    mutable util::Mutex _mtx;

private:

    /// The unique identifier of the job
    std::string _id;

    /// The Controller for performing requests
    Controller::Ptr _controller;

    /// The unique identifier of the parent job
    std::string _parentJobId;

    /// The type of the job
    std::string _type;

    // Job options

    Options _options;

    /// Primary state of the job
    std::atomic<State> _state;

    /// Extended state of the job
    std::atomic<ExtendedState> _extendedState;

    // Start and end times (milliseconds since UNIX Epoch)

    uint64_t _beginTime;
    uint64_t _endTime;

    // The timer is used to update the corresponidng timestamp within
    // the database for easier tracking of the dead jobs
    unsigned int _heartbeatTimerIvalSec;
    std::unique_ptr<boost::asio::deadline_timer> _heartbeatTimerPtr;

    /// This timer is used (if configured) to limit the total run time
    /// of a job. The timer starts when the job is started. And it's
    /// explicitly finished when a job finishes (successfully or not).
    ///
    /// If the time has a chance to expire then the request would finish
    /// with status: FINISHED::EXPIRED.
    unsigned int _expirationIvalSec;
    std::unique_ptr<boost::asio::deadline_timer> _expirationTimerPtr;
};

/// Comparision type for strict weak ordering reaquired by std::priority_queue
struct JobCompare {

    /// Order requests by their priorities
    bool operator()(Job::Ptr const& lhs,
                    Job::Ptr const& rhs) const {

        return lhs->options().priority < rhs->options().priority;
    }
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_JOB_H
