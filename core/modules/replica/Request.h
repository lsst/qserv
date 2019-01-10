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

// System headers
#include <atomic>
#include <map>
#include <memory>
#include <string>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/Performance.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Controller;

/**
  * Class Request is a base class for a family of requests within
  * the master server.
  */
class Request
    :   public std::enable_shared_from_this<Request>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Request> Ptr;

    /**
     * Type State represents a primary public state of the request
     */
    enum State {

        /// The request has been constructed, and no attempt to execute it has
        /// been made.
        CREATED,

        /// The request is in a progress
        IN_PROGRESS,

        /// The request is finished. See extended status for more details
        /// (the completion status, etc.)
        FINISHED
    };

    /// @return the string representation of the primary state
    static std::string state2string(State state);

    /**
     * Type ExtendedState represents a refined public sub-state of the request
     * once it's FINISHED as per the above defined primary state.
     */
    enum ExtendedState {

        /// No extended state exists at this time
        NONE,

        /// The request has been fully implemented
        SUCCESS,

        /// The request could not be implemented due to an unrecoverable
        /// client-side error.
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
        TIMEOUT_EXPIRED,

        /// Explicitly cancelled on the client-side (similar to TIMEOUT_EXPIRED)
        CANCELLED
    };

    /// @return the string representation of the extended state
    static std::string state2string(ExtendedState state);

    /// @return the string representation of the combined state
    static std::string state2string(State state,
                                    ExtendedState extendedState);

    /// @return the string representation of the combined state
    static std::string state2string(State state,
                                    ExtendedState extendedState,
                                    ExtendedCompletionStatus extendedServerStatus);

    // Default construction and copy semantics are prohibited

    Request() = delete;
    Request(Request const&) = delete;
    Request& operator=(Request const&) = delete;

    virtual ~Request();

    /// @return reference to the service provider,
    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }

    /// @return a string representing a type of a request.
    std::string const& type() const { return _type; }

    /// @return a unique identifier of the request
    std::string const& id() const { return _id; }

    /**
     * @return an effective identifier of a remote (worker-side) request.
     *
     * Normally this is the same request as the one a request object is created with
     * unless allowing to track duplicate requests (see constructor's options: 'keepTracking'
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
    Performance performance() const;

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
     * will be considering for building an association with the Controller.
     *
     * @param controller - (optional) pointer to an instance of the Controller
     * @param jobId      - (optional) identifier of a job specifying a context
     *                     in which a request will be executed.
     * @param requestExpirationIvalSec
     *                   - (optional) parameter (if differs from 0)
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

    /// @return string representation of the combined state of the object
    std::string state2string() const;

    /// @return the context string for debugging and diagnostic printouts
    std::string context() const;

    /**
     * @return a dictionary of parameters and the corresponding values to
     * be stored in a database for a request.
     *
     * ATTENTION: this method will be called only if the previously defined
     *            method Request::savePersistentState() has a non-trivial
     *            implementation by a subclass.
     */
    virtual std::list<std::pair<std::string,std::string>> extendedPersistentState() const {
        return std::list<std::pair<std::string,std::string>>();
    }

protected:

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
    Request(ServiceProvider::Ptr const& serviceProvider,
            boost::asio::io_service& io_service,
            std::string const& type,
            std::string const& worker,
            int  priority,
            bool keepTracking,
            bool allowDuplicate);

    /// Return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /// @return  keep tracking the request before it finishes or fails
    bool keepTracking() const { return _keepTracking; }

    /// @return follow a previously made request if the current one duplicates it
    bool allowDuplicate() const { return _allowDuplicate; }

    /// @return pointer to a buffer for data moved over the network
    std::shared_ptr<ProtocolBuffer> const& buffer() const { return _bufferPtr; }

    /// @return reference onto a timer used for ordering asynchronous delays
    boost::asio::deadline_timer& timer() { return _timer; }

    /// @return suggested interval (seconds) between retries in communications with workers
    unsigned int timerIvalSec() const { return _timerIvalSec; }

    /**
    * @param lock - lock on a mutex must be acquired before calling this method
     * @return the performance info
     */
    Performance performance(util::Lock const& lock) const;

    /// @return reference to the performance counters object
    Performance& mutablePerformance() { return _performance; }

    /**
      * Update a state of the extended status variable
      * 
      * @param lock   - lock on a mutex must be acquired before calling this method
      * @param status - new status to be set
      */
    void setExtendedServerStatus(util::Lock const& lock,
                                 ExtendedCompletionStatus status) { _extendedServerStatus = status; }

    /**
     * Set an effective identifier of a remote (worker-side) request
     * 
     * @param lock - lock on a mutex must be acquired before calling this method
     * @param id   - identifier to be set
     */
    void setDuplicateRequestId(util::Lock const& lock,
                               std::string const& id) { _duplicateRequestId = id; }

    /**
      * This method is supposed to be provided by subclasses for additional
      * subclass-specific actions to begin processing the request.
      * 
      * @param lock - a lock on a mutex must be acquired before calling this method
      */
    virtual void startImpl(util::Lock const& lock) = 0;

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is configured via the configuration service. When the request expires
     * it finishes with completion status FINISHED::TIMEOUT_EXPIRED.
     *
     * @param ec - error code to be checked
     */
    void expired(boost::system::error_code const& ec);

    /**
     * Finalize request processing (as reported by subclasses)
     *
     * This is supposed to be the last operation to be called by subclasses
     * upon a completion of the request.
     *
     * @param lock          - a lock on a mutex must be acquired before calling this method
     * @param extendedState - new extended state
     */
    void finish(util::Lock const& lock,
                ExtendedState extendedState);

    /**
      * This method is supposed to be provided by subclasses
      * to finalize request processing as required by the subclass.
      *
      * @param lock - a lock on a mutex must be acquired before calling this method
      */
    virtual void finishImpl(util::Lock const& lock) = 0;

    /**
      * This method is supposed to be provided by subclasses to save the request's
      * state into a database.
      *
      * The default implementation of the method is intentionally left empty
      * to allow requests not to have the persistent state.
      * 
      * @param lock - a lock on a mutex must be acquired before calling this method
      */
    virtual void savePersistentState(util::Lock const& lock) {}

    /**
     * Return 'true' if the operation was aborted.
     *
     * USAGE NOTES:
     *
     *    Normally this method is supposed to be called as the first action
     *    within asynchronous handlers to figure out if an on-going asynchronous
     *    operation was cancelled for some reason. Should this be the case
     *    the caller is supposed to quit right away. It will be up to a code
     *    which initiated the abort to take care of putting the object into
     *    a proper state.
     *
     * @param ec - error code to be checked
     *
     * @return 'true' if the code corresponds to the operation abort
     */
    bool isAborted(boost::system::error_code const& ec) const;

    /**
     * Ensure the object is in the desired internal state. Throw an
     * exception otherwise.
     *
     * NOTES: normally this condition should never be seen unless
     *        there is a problem with the application implementation
     *        or the underlying run-time system.
     *
     * @param lock         - a lock on a mutex must be acquired before calling this method
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
     * @param lock          - a lock on a mutex must be acquired before calling this method
     * @param state         - new primary state
     * @param extendedState - new extended state
     */
    void setState(util::Lock const& lock,
                  State state,
                  ExtendedState extendedStat=ExtendedState::NONE);

    /**
     * This method will begin an optional user protocol upon a completion
     * of a job (if any user-supplied callback function was provided).
     * The callback is supposed to be made asynchronously to avoid blocking
     * the current thread.
     *
     * This method has to be provided by subclasses to forward
     * notification on request completion to a client which initiated
     * the request, etc.
     *
     * The standard implementation of this method in a context of some
     * subclass 'T' should looks like this:
     * @code
     *   void T::notify(util::Lock const& lock) {
     *       notifyDefaultImpl<T>(lock, _onFinish);
     *   }
     * @code
     * @see Request::notifyDefaultImpl
     *
     * @param lock - the lock must be acquired by a caller of the method
     */
    virtual void notify(util::Lock const& lock) = 0;

    /**
     * The helper function which pushes up-stream notifications on behalf of
     * subclasses. Upon a completion of this method the callback function
     * object will get reset to 'nullptr'.
     *
     * Note, this default implementation works for callback functions which
     * accept a single parameter - a smart reference onto an object of
     * the corresponding subclass. Subclasses with more complex signatures of
     * their callbacks should have their own implementations which may look
     * similarly to this one.
     *
     * @param lock     - the lock must be acquired by a caller of the method
     * @param onFinish - callback function (if set) to be called
     */
    template <class T>
    void notifyDefaultImpl(util::Lock const& lock,
                           typename T::CallbackType& onFinish) {    
    
        if (nullptr != onFinish) {
    
            // Clearing the stored callback after finishing the up-stream notification
            // has two purposes:
            //
            // 1. it guaranties (exactly) one time notification
            // 2. it breaks the up-stream dependency on a caller object if a shared
            //    pointer to the object was mentioned as the lambda-function's closure
    
            serviceProvider()->io_service().post(
                std::bind(
                    std::move(onFinish),
                    shared_from_base<T>()
                )
            );
            onFinish = nullptr;
        }
    }

protected:

    /// Mutex guarding internal state. This object is made protected
    /// to allow subclasses use it.
    mutable util::Mutex _mtx;

private:

    /// The global counter for the number of instances of any subclasses
    static std::atomic<size_t> _numClassInstances;

    ServiceProvider::Ptr const _serviceProvider;

    /// The name of a request type (defined by subclasses)
    std::string const _type;

    /// A unique identifier of a request
    std::string const _id;

    /// An effective identifier of a remote (worker-side) request where
    /// this applies. Note that the duplicate requests are discovered
    /// in a course of communication with worker services.
    std::string _duplicateRequestId;

    ///The name of a worker
    std::string const _worker;

    /// The priority level of a request
    int const _priority;

    /// The flag which will enables continuous tracking of the request before
    /// it finishes or fails
    bool const _keepTracking;

    /// Follow (if 'true') a previously made request if the current one duplicates it
    bool const _allowDuplicate;


    // 2-level state of a request

    std::atomic<State>         _state;
    std::atomic<ExtendedState> _extendedState;

    /// Request status reported by a worker (where this applies)
    std::atomic<ExtendedCompletionStatus> _extendedServerStatus;

    /// Performance counters
    Performance _performance;

    /// Buffer for data moved over the network
    std::shared_ptr<ProtocolBuffer> _bufferPtr;

    /// Cached worker descriptor obtained from a configuration
    WorkerInfo _workerInfo;

    /// This timer is used to in the communication protocol for requests
    /// which may require multiple retries or any time spacing between network
    /// operation.
    ///
    /// The current class doesn't manager this time. It's here just because
    /// it's required by virtually all implementations of requests. And it's
    /// up to subclasses to manage this timer.
    ///
    /// The only role of this class (apart from providing the objects)
    /// is to set up the default timer interval from a configuration.
    unsigned int                _timerIvalSec;
    boost::asio::deadline_timer _timer;

    /// This timer is used (if configured) to limit the total run time
    /// of a request. The timer starts when the request is started. And it's
    /// explicitly finished when a request finishes (successfully or not).
    ///
    /// If the time has a chance to expire then the request would finish
    /// with status: FINISHED::TIMEOUT_EXPIRED.
    unsigned int                _requestExpirationIvalSec;
    boost::asio::deadline_timer _requestExpirationTimer;

    /// The optional association with the Controller
    std::shared_ptr<Controller> _controller;

    /// The job context of a request
    std::string _jobId;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_H
