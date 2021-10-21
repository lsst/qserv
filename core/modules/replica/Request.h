/*
 * LSST Data Management System
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
#include <condition_variable>
#include <map>
#include <memory>
#include <iostream>
#include <string>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/Performance.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class Controller;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class Request is a base class for a family of requests within
  * the master server.
  */
class Request : public std::enable_shared_from_this<Request>  {
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

        /// The request was just created and is being waited to be queued for processing by the server
        SERVER_CREATED,

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
                                    ProtocolStatusExt extendedServerStatus);

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
     * Normally this is the same request as the one a request object is created with
     * unless allowing to track duplicate requests (see constructor's options: 'keepTracking'
     * and 'allowDuplicate') and after the one is found.
     *
     * @return an effective identifier of a remote (worker-side) request.
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
    ProtocolStatusExt extendedServerStatus() const { return _extendedServerStatus; }

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
     * @note Only the first call with the non-default pointer to the Controller
     *   will be considering for building an association with the Controller.
     *
     * @param controller A (optional) pointer to an instance of the Controller.
     * @param jobId An (optional) identifier of a job specifying a context
     *   in which a request will be executed.
     * @param requestExpirationIvalSecAn (optional) parameter (if differs from 0)
     *   allowing to override the default value of the corresponding parameter from
     *   the Configuration.
     */
    void start(std::shared_ptr<Controller> const& controller=nullptr,
               std::string const& jobId="",
               unsigned int requestExpirationIvalSec=0);

    /// Wait for the completion of the request
    void wait();

    /**
     * Return an identifier if the owning job (if the request has started)
     *
     * @throws std::logic_error If the request hasn't started.
     */
    std::string const& jobId() const;

    /**
     * Explicitly cancel any asynchronous operation(s) and put the object into
     * the FINISHED::CANCELLED state. This operation is very similar to the
     * timeout-based request expiration, except it's requested explicitly.
     *
     * @note This operation won't affect the remote (server-side) state
     *   of the operation in case if the request was queued.
     */
    void cancel();

    /// @return string representation of the combined state of the object
    std::string state2string() const;

    /// @return the context string for debugging and diagnostic printouts
    std::string context() const;

    /**
     * @return a dictionary of parameters and the corresponding values to
     *   be stored in a database for a request.
     *
     * @note This method will be called only if the previously defined
     *   method Request::savePersistentState() has a non-trivial
     *   implementation by a subclass. Also, this method is supposed to
     *   be "lock-free" as it returns extended parameters of sub-classes
     *   which (he parameters) are set by constructors of the classes.
     */
    virtual std::list<std::pair<std::string,std::string>> extendedPersistentState() const {
        return std::list<std::pair<std::string,std::string>>();
    }

    /**
     * Dump requests into the string representation. The output of the method
     * would be used in various reports. This class's implementation of the
     * method would include (as minimum):
     * - the combined (base, extended, and extended server state) of the object
     * - the performance
     * - (optionally, if 'extended' is set to true) the key/value pairs of
     *   the extended persistent state
     * 
     * Subclasses may extend the implementation of the method
     *
     * @param extended  if 'true' then include the key/value pairs of
     *   the extended persistent state
     * @return the string representation of the request object
     */
    virtual std::string toString(bool extended = false) const;

    void print(std::ostream& os = std::cout,
               bool extended = false) const {
        os << toString(extended);
    }

    static void defaultPrinter(Ptr const& ptr) {
        ptr->print(std::cout, true);
    }

protected:
    /**
     * Construct the request with the pointer to the services provider.
     *
     * @note options 'keepTracking', 'allowDuplicate' and 'disposeRequired'
     *   have effect for specific request only.
     *
     * @param serviceProvider A provider of various services (Configuration, etc.).
     * @param io_service BOOST ASIO service object
     * @param type The type name of a request (used informally for debugging, and it's
     *   also stored in the persistent state of the Replication system).
     * @param worker The name of a worker.
     * @param priority A value of the parameter may affect an execution order of
     *   the request by the worker service. It may also affect an order requests
     *   are processed locally. Higher number means higher priority.
     * @param keepTracking Keep tracking the request before it finishes or fails
     * @param allowDuplicate Follow a previously made request if the current one
     *   duplicates it.
     * @param disposeRequired The flag indicating of the worker-side request
     *   disposal is needed for a particular request. Normally, it's required for
     *   requests which are queued by workers in its processing queues.
     */
    Request(ServiceProvider::Ptr const& serviceProvider,
            boost::asio::io_service& io_service,
            std::string const& type,
            std::string const& worker,
            int  priority,
            bool keepTracking,
            bool allowDuplicate,
            bool disposeRequired);

    /// @return A shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /// @return If 'true' then track request completion (queued requests only)
    bool keepTracking() const { return _keepTracking; }

    /// @return If 'true' then follow a previously made request if the current one duplicates it.
    bool allowDuplicate() const { return _allowDuplicate; }

    /// @return If 'true' the request needs to be disposed at the worker's side upon
    ///   a completion of an operation.
    bool disposeRequired() const { return _disposeRequired; }

    /// @return A pointer to a buffer for data moved over the network.
    std::shared_ptr<ProtocolBuffer> const& buffer() const { return _bufferPtr; }

    /// @return A reference onto a timer used for ordering asynchronous delays.
    boost::asio::deadline_timer& timer() { return _timer; }

    /// @return A suggested interval (seconds) between retries in communications with workers.
    unsigned int timerIvalSec() const { return _timerIvalSec; }

    /**
     * This method allows requests to implement an adaptive tracking algorithm
     * for following request status on worker nodes. Once the first message
     * is sent to a worker the request tracking timer is launched with
     * the initial value of the interval (stored in the data
     * member Request::_currentTimeIvalMsec).
     * Each subsequent activation of the timer is made with an interval which is
     * twice as long as the previous one until a limit set in the base class
     * member is reached:
     *
     * @see Request::timerIvalSec()
     *
     * After that the above mentioned (fixed) interval will always be used
     * untill the request finishes or fails (or gets cancelled, expires, etc.)
     *
     * This algorithm addresses three problems:
     * - it allows nearly real-time response for quick requests
     * - it prevents flooding in the network
     * - it doesn't cause an excessive use of resources on either ends of
     *   the Replication system
     *
     * @return The next value of the delay expressed in milliseconds
     */
    unsigned int nextTimeIvalMsec();

    /**
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     * @return The performance info.
     */
    Performance performance(util::Lock const& lock) const;

    /// @return reference to the performance counters object
    Performance& mutablePerformance() { return _performance; }

    /**
     * Update a state of the extended status variable
     *
     * @param lock A lock on Request::_mtx must be acquired before calling this method
     * @param status The new status to be set.
     */
    void setExtendedServerStatus(util::Lock const& lock,
                                 ProtocolStatusExt status) { _extendedServerStatus = status; }

    /**
     * Set an effective identifier of a remote (worker-side) request
     *
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     * @param id An identifier to be set.
     */
    void setDuplicateRequestId(util::Lock const& lock,
                               std::string const& id) { _duplicateRequestId = id; }

    /**
     * This method is supposed to be provided by subclasses for additional
     * subclass-specific actions to begin processing the request.
     *
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     *
     */
    virtual void startImpl(util::Lock const& lock) = 0;

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is configured via the configuration service. When the request expires
     * it finishes with completion status FINISHED::TIMEOUT_EXPIRED.
     *
     * @param ec A error code to be checked.
     */
    void expired(boost::system::error_code const& ec);

    /**
     * Finalize request processing (as reported by subclasses)
     *
     * @note This is supposed to be the last operation to be called by subclasses
     *   upon a completion of the request.
     *
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     * @param extendedState The new extended state.
     */
    void finish(util::Lock const& lock,
                ExtendedState extendedState);

    /**
     * This method is supposed to be provided by subclasses
     * to finalize request processing as required by the subclass.
     *
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     */
    virtual void finishImpl(util::Lock const& lock) = 0;

    /**
      * This method is supposed to be provided by subclasses to save the request's
      * state into a database.
      *
      * The default implementation of the method is intentionally left empty
      * to allow requests not to have the persistent state.
      * 
      * @param lock A lock on Request::_mtx must be acquired before calling this method.
      */
    virtual void savePersistentState(util::Lock const& lock) {}

    /**
     * Return 'true' if the operation was aborted.
     *
     * @note Normally this method is supposed to be called as the first action
     *   within asynchronous handlers to figure out if an on-going asynchronous
     *   operation was cancelled for some reason. Should this be the case
     *   the caller is supposed to quit right away. It will be up to a code
     *   which initiated the abort to take care of putting the object into
     *   a proper state.
     *
     * @param ec A error code to be checked
     * @return 'true' if the code corresponds to the operation abort
     */
    bool isAborted(boost::system::error_code const& ec) const;

    /**
     * Ensure the object is in the desired internal state. Throw an
     * exception otherwise.
     *
     * @note Normally this condition should never be seen unless there is a problem
     *   with the application implementation or the underlying run-time system.
     *
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     * @param desiredState The desired state.
     * @param context A context from which the state test is requested.
     * @throws std::logic_error If the desired state condition is not met.
     */
    void assertState(util::Lock const& lock,
                     State desiredState,
                     std::string const& context) const;

    /**
     * Set the desired primary and extended state.
     *
     * The change of the state is done via a method to allow extra actions
     * at this step, such as:
     * - reporting change state in a debug stream
     * - verifying the correctness of the state transition
     *
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     * @param state The new primary state.
     * @param extendedState The new extended state.
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
     *
     * @see Request::notifyDefaultImpl
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
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
     * @param lock A lock on Request::_mtx must be acquired before calling this method.
     * @param onFinish A callback function (if set) to be called.
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

    /// @return A value of the interval (Note, the final value of the parameter is
    ///   set after the request has started. Before that a default value obtained by
    ///   the class's constructor from the Configuration will be returned).
    unsigned int requestExpirationIvalSec() const { return _requestExpirationIvalSec; }

    /// Mutex guarding internal state. This object is made protected
    /// to allow subclasses use it.
    mutable util::Mutex _mtx;

private:
    /// The global counter for the number of instances of any subclasses
    static std::atomic<size_t> _numClassInstances;

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;

    std::string const _type;
    std::string const _id;     /// @note UUID generated by the constructor
    std::string const _worker;

    int  const _priority;
    bool const _keepTracking;
    bool const _allowDuplicate;
    bool const _disposeRequired;

    /// An effective identifier of a remote (worker-side) request where
    /// this applies. Note that the duplicate requests are discovered
    /// in a course of communication with worker services.
    std::string _duplicateRequestId;

    // 2-level state of a request

    std::atomic<State>         _state;
    std::atomic<ExtendedState> _extendedState;

    /// Request status reported by a worker (where this applies)
    std::atomic<ProtocolStatusExt> _extendedServerStatus;

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
    /// The current class doesn't manage the timer. The object is stored here
    /// since it's required by virtually all implementations of requests. And
    /// it's up to subclasses to manage this timer.
    ///
    /// The only responsibility of the current class (apart from providing
    /// these objects to its subclasses) is to set up the default timer interval
    /// from a configuration.
    unsigned int                _timerIvalSec;
    boost::asio::deadline_timer _timer;

    /// @see Request::nextTimeIvalMsec()
    unsigned int _currentTimeIvalMsec = 10;

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

    // Synchronization primitives for implementing Request::wait()

    bool _finished = false;
    std::mutex _onFinishMtx;
    std::condition_variable _onFinishCv;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_H
