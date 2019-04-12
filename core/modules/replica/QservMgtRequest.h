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
#ifndef LSST_QSERV_REPLICA_QSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_QSERVMGTREQUEST_H

// System headers
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

// THird party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

// Forward declarations
class XrdSsiService;

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class QservMgtRequest is a base class for a family of the Qserv worker
  * management requests within the master server.
  */
class QservMgtRequest : public std::enable_shared_from_this<QservMgtRequest>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservMgtRequest> Ptr;

    /// The lock type used by the implementations
    typedef std::lock_guard<util::Mutex> LockType;

    /// The type which represents the primary public state of the request
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

    /// Type ExtendedState represents the refined public sub-state of the request
    /// once it's FINISHED as per the above defined primary state.
    enum ExtendedState {

        /// No extended state exists at this time
        NONE,

        /// The request has been fully implemented
        SUCCESS,

        /// Problems with request configuration found
        CONFIG_ERROR,

        /// Server reports that the request can not be implemented due to incorrect
        /// parameters, etc.
        SERVER_BAD,

        /// Server reports that the request can not be implemented because
        /// some of the required remote resources (chunks, etc.) are in use.
        SERVER_CHUNK_IN_USE,

        /// The request could not be implemented due to an unrecoverable
        /// server-side error.
        SERVER_ERROR,

        /// Data received from a server can't be correctly interpreted
        SERVER_BAD_RESPONSE,

        /// Expired due to a timeout (as per the Configuration)
        TIMEOUT_EXPIRED,

        /// Explicitly cancelled on the client-side (similar to TIMEOUT_EXPIRED)
        CANCELLED
    };

    /// @return the string representation of the extended state
    static std::string state2string(ExtendedState state);

    /// @return the string representation of the combined state
    static std::string state2string(State state,
                                    ExtendedState extendedState) {
        return state2string(state) + "::" +state2string(extendedState);
    }

    // Default construction and copy semantics are prohibited

    QservMgtRequest() = delete;
    QservMgtRequest(QservMgtRequest const&) = delete;
    QservMgtRequest& operator=(QservMgtRequest const&) = delete;

    virtual ~QservMgtRequest();

    /// @return reference to a provider of services
    ServiceProvider::Ptr const& serviceProvider() { return _serviceProvider; }

    /// @return string representing of the request type.
    std::string const& type() const { return _type; }

    /// @return unique identifier of the request
    std::string const& id() const { return _id; }

    /// @return name of a worker
    std::string const& worker() const { return _worker; }

    /// @return primary status of the request
    State state() const { return _state; }

    /// @return extended status of the request
    ExtendedState extendedState() const { return _extendedState; }

    /// @return string representation of the combined state of the object
    std::string state2string() const;

    /// @return error message (if any) reported by the remote service.
    std::string serverError() const;

    /// @return performance info
    Performance performance() const;

    /**
     * Return an identifier if the owning job (if the request has started)
     *
     * @throws std::logic_error - if the request hasn't started
     */
    std::string const& jobId() const;

    /**
     * Reset the state (if needed) and begin processing the request.
     *
     * This is supposed to be the first operation to be called upon a creation
     * of the request. A caller is expected to provide a pointer to an instance
     * of the XrdSsiService class for communications with the remote services.
     *
     * @param service  - a pointer to an instance of the API object for
     *                   submitting requests to remote services
     * @param jobId    - an optional identifier of a job specifying a context
     *                   in which a request will be executed.
     * @param requestExpirationIvalSec - an optional parameter (if differs from 0)
     *                   allowing to override the default value of
     *                   the corresponding parameter from the Configuration.
     */
    void start(XrdSsiService* service,
               std::string const& jobId="",
               unsigned int requestExpirationIvalSec=0);

    /// Wait for the completion of the request
    void wait();

    /**
     * Explicitly cancel any asynchronous operation(s) and put the object into
     * the FINISHED::CANCELLED state. This operation is very similar to the
     * timeout-based request expiration, except it's requested explicitly.
     */
    void cancel();

    /// @return the context string for debugging and diagnostic printouts
    std::string context() const;

    /**
     * @return
     *   a dictionary of parameters and the corresponding values to
     *   be stored in a database for a request.
     */
    virtual std::list<std::pair<std::string,std::string>> extendedPersistentState() const {
        return std::list<std::pair<std::string,std::string>>();
    }

protected:

    /**
     * Construct the request with the pointer to the services provider.
     *
     * @param serviceProvider
     *   reference to a provider of services
     *
     * @param type
     *   its type name (used informally for debugging)
     *
     * @param worker
     *   the name of a worker
     */
    QservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                    std::string const& type,
                    std::string const& worker);

    /// @return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /// @return API for submitting requests to the remote services
    XrdSsiService* service() { return _service; }

    /**
      * This method is supposed to be provided by subclasses for additional
      * subclass-specific actions to begin processing the request.
      *
      * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
      */
    virtual void startImpl(util::Lock const& lock) = 0;

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is configured via the configuration service. When the request expires
     * it finishes with completion status FINISHED::TIMEOUT_EXPIRED.
     *
     * @param ec
     *   error code to be checked
     */
    void expired(boost::system::error_code const& ec);

    /**
     * Finalize request processing (as reported by subclasses)
     *
     * This is supposed to be the last operation to be called by subclasses
     * upon a completion of the request.
     *
     * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
     *
     * @param extendedState
     *   new extended state
     *
     * @param serverError
     *   (optional) error message from a Qserv worker service
     */
    void finish(util::Lock const& lock,
                ExtendedState extendedState,
                std::string const& serverError="");

    /**
      * This method is supposed to be provided by subclasses
      * to finalize request processing as required by the subclass.
      *
      * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
      */
    virtual void finishImpl(util::Lock const& lock) = 0;

    /**
     * Start user-notification protocol (in case if user-defined notifiers
     * were provided to a subclass). The callback is expected to be made
     * asynchronously in a separate thread to avoid blocking the current thread.
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
     * @see QservMgtRequest::notifyDefaultImpl
     *
     * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
     */
    virtual void notify(util::Lock const& lock) = 0;

    /**
     * The helper function which pushes up-stream notifications on behalf of
     * subclasses. Upon a completion of this method the callback function
     * object will get reset to 'nullptr'.
     *
     * @note
     *   This default implementation works for callback functions which
     *   accept a single parameter - a smart reference onto an object of
     *   the corresponding subclass. Subclasses with more complex signatures of
     *   their callbacks should have their own implementations which may look
     *   similarly to this one.
     *
     * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
     *
     * @param onFinish
     *   callback function (if set) to be called
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

    /**
     * Ensure the object is in the desired internal state. Throw an
     * exception otherwise.
     *
     * @note
     *  normally this condition should never been seen unless
     *  there is a problem with the application implementation
     *  or the underlying run-time system.
     *
     * @param desiredState
     *   desired state
     *
     * @param context
     *   context from which the state test is requested
     *
     * @throws std::logic_error
     */
    void assertState(State desiredState,
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
     * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
     *
     * @param state
     *   primary state
     *
     * @param extendedState 
     *   extended state of the request
     */
    void setState(util::Lock const& lock,
                  State state,
                  ExtendedState extendedState=ExtendedState::NONE);

    /**
     * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
     *
     * @return
     *   server error string (if any)
     */
    std::string serverError(util::Lock const& lock) const;

    /**
     * @param lock
     *   a lock on QservMgtRequest::_mtx must be acquired before calling this method
     *
     * @return
     *   performance info
     */
    Performance performance(util::Lock const& lock) const;

protected:

    /// Mutex guarding internal state (also used by subclasses)
    mutable util::Mutex _mtx;

private:

    /// The global counter for the number of instances of any subclass
    static std::atomic<size_t> _numClassInstances;

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;

    std::string const _type;
    std::string const _id;
    std::string const _worker;

    // Two-level state of a request

    std::atomic<State>         _state;
    std::atomic<ExtendedState> _extendedState;

    /// Error message (if any) reported by the remote service
    std::string _serverError;

    /// Performance counters
    Performance _performance;

    /// An identifier of the parent job which started the request
    std::string _jobId;

    /// An API for submitting requests to the remote services
    XrdSsiService* _service;

    /// This timer is used (if configured) to limit the total run time
    /// of a request. The timer starts when the request is started. And it's
    /// explicitly finished when a request finishes (successfully or not).
    ///
    /// If the time has a chance to expire then the request would finish
    /// with status: FINISHED::TIMEOUT_EXPIRED.
    unsigned int                _requestExpirationIvalSec;
    boost::asio::deadline_timer _requestExpirationTimer;

    // Synchronization primitives for implementing QservMgtRequest::wait()

    std::mutex _onFinishMtx;
    std::condition_variable _onFinishCv;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_QSERVMGTREQUEST_H
