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
#ifndef LSST_QSERV_REPLICA_REQUEST_H
#define LSST_QSERV_REPLICA_REQUEST_H

/// Request.h declares:
///
/// class Request
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <mutex>
#include <string>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Common.h"
#include "replica/Performance.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Controller;
class WorkerInfo;

/**
  * Class Request is a base class for a family of requests within
  * the master server.
  */
class Request
    :   public std::enable_shared_from_this<Request>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Request> pointer;

    /// Primary public state of the request
    enum State {

        /// The request has been constructed, and no attempt to execute it has
        /// been made.
        CREATED,

        /// The request is in a progress
        IN_PROGRESS,

        /// The request is finihed. See extended status for more details
        /// (the completion status, etc.)
        FINISHED
    };

    /// @return the string representation of the primary state
    static std::string state2string(State state) ;

    /// Refined public sub-state of the requiest once it's FINISHED as per
    /// the above defined primary state.
    enum ExtendedState {

        /// No extended state exists at this time
        NONE,

        /// The request has been fully implemented
        SUCCESS,

        /// The request could not be implemented due to an unrecoverable
        /// cliend-side error.
        CLIENT_ERROR,

        /// Server reports that the request can not be implemented due to incorrect parameters, etc.
        SERVER_BAD,

        /// The request could not be implemented due to an unrecoverable
        /// server-side error.
        SERVER_ERROR,

        /// The request is queued for processing by the server
        SERVER_QUEUED,

        /// The request is being processed by the server
        SERVER_IN_PROGRESS,

        /// The request is being cancelled by the server
        SERVER_IS_CANCELLING,

        /// The request is found as cancelled on the server
        SERVER_CANCELLED,

        /// Expired due to a timeout (as per the Configuration)
        EXPIRED,

        /// Explicitly cancelled on the client-side (similar to EXPIRED)
        CANCELLED
    };

    /// @return the string representation of the extended state
    static std::string state2string(ExtendedState state) ;

    /// @return the string representation of the compbined state
    static std::string state2string(State state,
                                    ExtendedState extendedState);

    /// @return the string representation of the compbined state
    static std::string state2string(State state,
                                    ExtendedState extendedState,
                                    ExtendedCompletionStatus extendedServerStatus);

    // Default construction and copy semantics are prohibited

    Request() = delete;
    Request(Request const&) = delete;
    Request& operator=(Request const&) = delete;

    /// Destructor
    virtual ~Request() = default;

    /// @return reference to the service provider,
    ServiceProvider::pointer const& serviceProvider() { return _serviceProvider; }

    /// @return a string representing a type of a request.
    std::string const& type() const { return _type; }

    /// @return a unique identifier of the request
    std::string const& id() const { return _id; }

    /**
     * @return an effective identifier of a remote (worker-side) requst.
     *
     * Normally this is the same request as the one a request object is created with
     * unless allowing to track duplicate requests (see constructor'x options: 'keepTracking'
     * and 'allowDuplicate') and after the one is found.
     */
    std::string const& remoteId() const;

    /// @return the priority level of the request
    int priority() const { return _priority; }

    /// @return a unique identifier of the request
    std::string const& worker() const { return _worker; }

    /// @return the primary status of the request
    State state() const { return _state; }

    /// @return the extended state of the request when it finished.
    ExtendedState extendedState() const { return _extendedState; }

    /// @return a status code received from a worker server
    ExtendedCompletionStatus extendedServerStatus() const { return _extendedServerStatus; }

    /// @return the performance info
    Performance const& performance() const { return _performance; }

    /// @return the Controller (if set)
    std::shared_ptr<Controller> const& controller() const { return _controller; }

    /**
     * Reset the state (if needed) and begin processing the request.
     *
     * This is supposed to be the first operation to be called upon a creation
     * of the request. A caller may optionally provide a pointer to an instance
     * of the Controller class which (if set) may be used by subclasses for saving
     * their state in a database.
     *
     * NOTE: only the first call with the non-default pointer to the Controller
     * will be considering for building an associaion with the Controller.
     *
     * @param controller - an optional pointer to an instance of the Controller
     * @param jobId      - an optional identifier of a job specifying a context
     *                     in which a request will be executed.
     * @param requestExpirationIvalSec
     *                   - an optional parameter (if differs from 0)
     *                     allowing to override the default value of the corresponding
     *                     parameter from the Configuration.
     */
    void start(std::shared_ptr<Controller> const& controller=nullptr,
               std::string const& jobId="",
               unsigned int requestExpirationIvalSec=0);

    /**
     * Return an identifier if the owning job (if the request has started)
     *
     * @throws std::logic_error - if the request hasn't started
     */
    std::string const& jobId() const;

    /**
     * Explicitly cancel any asynchronous operation(s) and put the object into
     * the FINISHED::CANCELLED state. This operation is very similar to the
     * timeout-based request expiration, except it's requested explicitly.
     *
     * ATTENTION: this operation won't affect the remote (server-side) state
     * of the operation in case if the request was queued.
     */
    void cancel();

    /// @return the context string for debugging and diagnostic printouts
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
     * NOTE: options 'keepTracking' and 'allowDuplicate' have effect for
     *       specific request only.
     *
     * @param serviceProvider - a provider of various services
     * @param type            - its type name (used informally for debugging)
     * @param worker          - the name of a worker
     * @io_service            - BOOST ASIO service
     * @priority              - may affect an execution order of the request by
     *                          the worker service. Higher number means higher
     *                          priority.
     * @param keepTracking    - keep tracking the request before it finishes or fails
     * @param allowDuplicate  - follow a previously made request if the current one duplicates it
     */
    Request(ServiceProvider::pointer const& serviceProvider,
            boost::asio::io_service& io_service,
            std::string const& type,
            std::string const& worker,
            int  priority,
            bool keepTracking,
            bool allowDuplicate);

    /**
      * This method is supposed to be provided by subclasses for additional
      * subclass-specific actions to begin processing the request.
      */
    virtual void startImpl()=0;

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is configured via the configuraton service. When the request expires
     * it finishes with completion status FINISHED::EXPIRED.
     */
    void expired(boost::system::error_code const& ec);

    /**
     * Finalize request processing (as reported by subclasses)
     *
     * This is supposed to be the last operation to be called by subclasses
     * upon a completion of the request.
     */
    void finish(ExtendedState extendedState);

    /**
      * This method is supposed to be provided by subclasses
      * to finalize request processing as required by the subclass.
      */
    virtual void finishImpl()=0;

    /**
     * This method is supposed to be provided by subclasses to handle
     * request completion steps, such as notifying a party which initiated
     * the request, etc.
     */
    virtual void notify()=0;

    /**
     * Return 'true' if the operation was aborted.
     *
     * USAGE NOTES:
     *
     *    Nomally this method is supposed to be called as the first action
     *    witin asynchronous handlers to figure out if an on-going aynchronous
     *    operation was cancelled for some reason. Should this be the case
     *    the caller is supposed to quit right away. It will be up to a code
     *    which initiated the abort to take care of putting the object into
     *    a proper state.
     */
    bool isAborted(boost::system::error_code const& ec) const;

    /**
     * Ensure the object is in the deseride internal state. Throw an
     * exception otherwise.
     *
     * NOTES: normally this condition should never been seen unless
     *        there is a problem with the application implementation
     *        or the underlying run-time system.
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
     */
    void setState(State state,
                  ExtendedState extendedStat);

protected:

    // Parameters of the object

    ServiceProvider::pointer _serviceProvider;

    std::string _type;
    std::string _id;                    ///< own identifier
    std::string _duplicateRequestId;    ///< effective identifier of a remote (worker-side) request where applies
    std::string _worker;

    int  _priority;
    bool _keepTracking;
    bool _allowDuplicate;

    // Primary and extended states of the request

    State         _state;
    ExtendedState _extendedState;

    ExtendedCompletionStatus _extendedServerStatus;

    /// Performance counters
    Performance _performance;

    /// Buffer for data moved over the network
    std::shared_ptr<ProtocolBuffer> _bufferPtr;

    // Parameters of a worker obtained from a configuration
    WorkerInfo const& _workerInfo;

    /// This time is used to in the communication protocol for requests
    /// which may require multiple retries or any time spacing between network
    /// operation.
    ///
    /// The current class doesn't manager this time. It's here just because
    /// it's required by virtually all implementations of requests. And it's
    /// up to subclasses to manage this timer.
    ///
    /// The only role of this class (apart from providing the objects)
    /// is to set up the default timer interval from a condifuration.
    unsigned int                _timerIvalSec;
    boost::asio::deadline_timer _timer;

    /// This timer is used (if configured) to limit the total run time
    /// of a request. The timer starts when the request is started. And it's
    /// explicitly finished when a request finishes (successfully or not).
    ///
    /// If the time has a chance to expire then the request would finish
    /// with status: FINISHED::EXPIRED.
    unsigned int                _requestExpirationIvalSec;
    boost::asio::deadline_timer _requestExpirationTimer;

    /// Mutex guarding internal state
    mutable std::mutex _mtx;

    /// The optional association with the Controller
    std::shared_ptr<Controller> _controller;

    /// The job context of a request
    std::string _jobId;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_H
