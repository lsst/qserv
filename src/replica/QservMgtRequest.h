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
#include <list>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/AsyncReq.h"
#include "http/Method.h"
#include "replica/Mutex.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * QservMgtRequest is a base class for a family of the Qserv management requests
 * sent to different services.
 */
class QservMgtRequest : public std::enable_shared_from_this<QservMgtRequest> {
public:
    typedef std::shared_ptr<QservMgtRequest> Ptr;

    /// The lock type used by the implementations
    typedef std::lock_guard<replica::Mutex> LockType;

    /// The type which represents the primary public state of the request
    enum State {
        CREATED,      ///< The request has been constructed, and no attempt to execute
                      ///  it has been made.
        IN_PROGRESS,  ///< The request is in a progress.
        FINISHED      ///< The request is finished. See extended status for more details
                      ///  (the completion status, etc.)
    };

    /// @return the string representation of the primary state
    static std::string state2string(State state);

    /// Type ExtendedState represents the refined public sub-state of the request
    /// once it's FINISHED as per the above defined primary state.
    enum ExtendedState {
        NONE,                 ///< No extended state exists at this time.
        SUCCESS,              ///< The request has been fully implemented.
        CONFIG_ERROR,         ///< Problems with request configuration were detected.
        BODY_LIMIT_ERROR,     ///< Response's body is larger than requested.
        SERVER_BAD,           ///< Server reports that the request can not be implemented because
                              ///  of configuration or request's parameters problems.
        SERVER_CHUNK_IN_USE,  ///< Server reports that the request can not be implemented because
                              ///  some of the required remote resources (chunks, etc.) are in use.
        SERVER_ERROR,         ///< The request could not be implemented due to an unrecoverable
                              ///  server-side error.
        SERVER_BAD_RESPONSE,  ///< Data received from a server can't be correctly interpreted.
        TIMEOUT_EXPIRED,      ///< Expired due to a timeout.
        CANCELLED             ///< Explicitly cancelled on the client-side.
    };

    /// @return the string representation of the extended state
    static std::string state2string(ExtendedState state);

    /// @return the string representation of the combined state
    static std::string state2string(State state, ExtendedState extendedState) {
        return state2string(state) + "::" + state2string(extendedState);
    }

    QservMgtRequest() = delete;
    QservMgtRequest(QservMgtRequest const&) = delete;
    QservMgtRequest& operator=(QservMgtRequest const&) = delete;

    virtual ~QservMgtRequest();

    /// @return reference to a provider of services
    std::shared_ptr<ServiceProvider> const& serviceProvider() const { return _serviceProvider; }

    /// @return string representing of the request type.
    std::string const& type() const { return _type; }

    /// @return unique identifier of the request
    std::string const& id() const { return _id; }

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
     * @return An identifier if the owning job (if the request has started).
     * @throws std::logic_error If the request hasn't started.
     */
    std::string const& jobId() const;

    /**
     * @return The info object returned by the service.
     * @throw std::logic_error if called before the request finishes or if it failed.
     */
    nlohmann::json const& info() const;

    /**
     * @brief Begin processing the request.
     * @param jobId (Optional) identifier of a job specifying a context of the request.
     * @param requestExpirationIvalSec (Optional) parameter (if differs from 0) allowing
     *   to override the default value of the corresponding parameter from the Configuration.
     */
    void start(std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    /// Wait for the completion of the request
    void wait();

    /**
     * @brief Explicitly cancel any asynchronous operation(s) and put the object into
     *   the FINISHED::CANCELLED state. This operation is very similar to the
     *   timeout-based request expiration, except it's requested explicitly.
     */
    void cancel();

    /// @return The context string for debugging and diagnostic printouts.
    std::string context() const;

    /// @return A dictionary of parameters and the corresponding values to be stored
    ///   in a database for a request.
    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const {
        return std::list<std::pair<std::string, std::string>>();
    }

protected:
    /**
     * @brief Construct the request with the pointer to the services provider.
     * @param serviceProvider Is required to access configuration services.
     * @param type The type name of he request (used for debugging and error reporting).
     * @param remoteServiceKey The key assiciated with the remote service.
     * @param remoteServiceId The unique identifier of the remote service.
     */
    QservMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& type,
                    std::string const& remoteServiceKey, std::string const& remoteServiceId);

    /// @return A shared pointer of the desired subclass (no dynamic type checking).
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /**
     * @brief Create an HTTP request.
     *
     * This method is required to be provided by subclasses for creating
     * subclass-specific requests using the coresponding helper methods.
     *
     * @see QservMgtRequest::createHttpReq
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     */
    virtual void createHttpReqImpl(replica::Lock const& lock) = 0;

    /**
     * @brief Create an HTTP "GET" request, but do not start it yet.
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @param service The REST service (w/o the query part) to be called.
     * @param (optional) HTTP query for the request.
     * @throw std::logic_error If the method is called while the curent state
     *   is not State::CREATED, or if the HTTP request was already  created.
     */
    void createHttpReq(replica::Lock const& lock, std::string const& service,
                       std::string const& query = std::string());

    /**
     * @brief Create an HTTP request ("POST", "PUT" or "DELETE") that has the JSON body,
     *   but do not start it yet.
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @param method An HTTP method for the request,
     * @param service The complete target (including the REST service and the query part) to be called.
     * @param body A JSON object to be sent in the request's body.
     * @throw std::logic_error If the method is called while the curent state
     *   is not State::CREATED, or if the HTTP request was already  created.
     */
    void createHttpReq(replica::Lock const& lock, http::Method method, std::string const& target,
                       nlohmann::json const& body);

    /**
     * @brief Notify a subclass that a data object was was succesfully retrieved
     *   from the service.
     *
     * This method allows subclasses to implement the optional result validation and processing,
     * including a translation of the JSON object into the subclas-specific result type.
     *
     * @note Any exceptions thrown by the method will result in setting the status
     *   ExtendedState::SERVER_BAD_RESPONSE to indicate a problem with interpreting the data.
     *   The method is also required to report its final verdican on the status of the object.
     *   Normally, it's going to be ExtendedState::SUCCESS. However, a subclass may set
     *   a different status, depending on its findings.
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @param data The JSON result to be processed by a subclass.
     * @return The final verdict made by the subclass on the completion status.
     */
    virtual ExtendedState dataReady(replica::Lock const& lock, nlohmann::json const& data) {
        return ExtendedState::SUCCESS;
    }

    /**
     * @brief Finalize request processing (as reported by subclasses)
     * @note This is supposed to be the last operation to be called by subclasses
     *   upon a completion of the request.
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @param extendedState The new extended state.
     * @param serverError (optional) error message from a service.
     */
    void finish(replica::Lock const& lock, ExtendedState extendedState, std::string const& serverError = "");

    /**
     * @brief Start user-notification protocol (in case if user-defined notifiers
     *   were provided to a subclass). The callback is expected to be made
     *   asynchronously in a separate thread to avoid blocking the current thread.
     *
     * This method has to be provided by subclasses to forward
     * notification on request completion to a client which initiated
     * the request, etc.
     *
     * The standard implementation of this method in a context of some
     * subclass 'T' should looks like this:
     * @code
     *   void T::notify(replica::Lock const& lock) {
     *       notifyDefaultImpl<T>(lock, _onFinish);
     *   }
     * @code
     * @see QservMgtRequest::notifyDefaultImpl
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     */
    virtual void notify(replica::Lock const& lock) = 0;

    /**
     * @brief The helper function which pushes up-stream notifications on behalf of
     * subclasses. Upon a completion of this method the callback function
     * object will get reset to 'nullptr'.
     * @note This default implementation works for callback functions which
     *   accept a single parameter - a smart reference onto an object of
     *   the corresponding subclass. Subclasses with more complex signatures of
     *   their callbacks should have their own implementations which may look
     *   similarly to this one.
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @param onFinish A callback function (if set) to be called.
     */
    template <class T>
    void notifyDefaultImpl(replica::Lock const& lock, typename T::CallbackType& onFinish) {
        if (nullptr != onFinish) {
            // Clearing the stored callback after finishing the up-stream notification
            // has two purposes:
            // 1. it guaranties (exactly) one time notification
            // 2. it breaks the up-stream dependency on a caller object if a shared
            //    pointer to the object was mentioned as the lambda-function's closure
            serviceProvider()->io_service().post(std::bind(std::move(onFinish), shared_from_base<T>()));
            onFinish = nullptr;
        }
    }

    /**
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @return A server error string (if any).
     */
    std::string serverError(replica::Lock const& lock) const;

    /**
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @return The performance info.
     */
    Performance performance(replica::Lock const& lock) const;

    /// Mutex guarding internal state (also used by subclasses)
    mutable replica::Mutex _mtx;

    /// @return The callback function for tracking connection parameters of the remote service.
    virtual http::AsyncReq::GetHostPort getHostPortTracker() const = 0;

    /**
     * Update the persistent state from the subclass's context.
     * @note Not all request classes have persistent states.
     * @param performance Performance stats of the request.
     * @param serverError The server error (if any).
     */
    virtual void updatePersistentState(Performance const& performance, std::string const& serverError) const {
    }

private:
    /// Extract and process data of the completed request. Notify a subclass in case of success.
    void _processResponse();

    /**
     * @brief Ensure the request is in the desired state.
     * @note The method doesn't require a lock on the mutex _mtx at all as
     *   the related member variable is atomic.
     * @param func A context from which the state test is requested (for tracing
     *   and error reporting).
     * @param mustBeStarted 'true' if the request is expected to be in any
     *  state past State::CREATED, or 'false' if it's expected to be exactly
     *  in State::CREATED.
     * @throws std::logic_error If the request was not found in the desired state.
     */
    void _assertStarted(std::string const& func, bool mustBeStarted) const;

    // Shortcut methods based on the above defined one.

    void _assertStarted(std::string const& func) const { _assertStarted(func, true); }
    void _assertNotStarted(std::string const& func) const { _assertStarted(func, false); }

    /**
     * @brief Set the desired primary and extended state.
     *
     * The change of the state is done via a method to allow extra actions
     * at this step, such as: 1) reporting change state in a debug stream,
     * or 2) verifying the correctness of the state transition.
     *
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before
     *   calling this method.
     * @param state The primary state of the request.
     * @param extendedState The extended state of the request.
     */
    void _setState(replica::Lock const& lock, State state, ExtendedState extendedState = ExtendedState::NONE);

    /// The global counter for the number of instances of any subclass
    static std::atomic<size_t> _numClassInstances;

    // Input parameters

    std::shared_ptr<ServiceProvider> const _serviceProvider;

    std::string const _type;
    std::string const _id;
    std::string const _remoteServiceKey;
    std::string const _remoteServiceId;

    // Two-level state of a request
    std::atomic<State> _state;                  ///< The primary state.
    std::atomic<ExtendedState> _extendedState;  ///< The sub-state.

    /// Error message (if any) reported by the remote service
    std::string _serverError;

    Performance _performance;  ///< Performance counters.
    std::string _jobId;        ///< An identifier of the parent job which started the request.

    std::shared_ptr<http::AsyncReq> _httpRequest;  ///< The actual request sent to the service.
    nlohmann::json _info;                          ///< The data object returned by the service.

    // Synchronization primitives for implementing QservMgtRequest::wait()

    bool _finished = false;
    std::mutex _onFinishMtx;
    std::condition_variable _onFinishCv;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_QSERVMGTREQUEST_H
