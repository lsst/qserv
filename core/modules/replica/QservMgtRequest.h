/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

/// QservMgtRequest.h declares:
///
/// class QservMgtRequest
/// (see individual class documentation for more information)

// System headers
#include <atomic>
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

// Forward declarations
namespace database {
namespace mysql {
class Connection;
}}

/**
  * Class QservMgtRequest is a base class for a family of the Qserv worker
  * management requests within the master server.
  */
class QservMgtRequest
    :   public std::enable_shared_from_this<QservMgtRequest>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<QservMgtRequest> Ptr;

    /// The pointer type for the database connector which provides a database-specific
    /// SQL generation services.
    typedef std::shared_ptr<database::mysql::Connection> SqlGeneratorPtr;

    /// The lock type used by the implementations
    typedef std::lock_guard<util::Mutex> LockType;

    /// Tye State represents the primary public state of the request
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
    static std::string state2string(State state);

    /// Type ExtendedState represebts the refined public sub-state of the requiest
    /// once it's FINISHED as per the above defined primary state.
    enum ExtendedState {

        /// No extended state exists at this time
        NONE,

        /// The request has been fully implemented
        SUCCESS,

        /// Problems with request configuration found
        CONFIG_ERROR,

        /// The request could not be implemented due to an unrecoverable
        /// cliend-side error.
        CLIENT_ERROR,

        /// Server reports that the request can not be implemented due to incorrect
        /// parameters, etc.
        SERVER_BAD,

        /// Server reports that the request can not be implemented because
        /// some of the required remote resources (chunks, etc.) are in use.
        SERVER_CHUNK_IN_USE,

        /// The request could not be implemented due to an unrecoverable
        /// server-side error.
        SERVER_ERROR,

        /// Expired due to a timeout (as per the Configuration)
        TIMEOUT_EXPIRED,

        /// Explicitly cancelled on the client-side (similar to TIMEOUT_EXPIRED)
        CANCELLED
    };

    /// @return the string representation of the extended state
    static std::string state2string(ExtendedState state);

    /// @return the string representation of the compbined state
    static std::string state2string(State state,
                                    ExtendedState extendedState) {
        return state2string(state) + "::" +state2string(extendedState);
    }

    // Default construction and copy semantics are prohibited

    QservMgtRequest() = delete;
    QservMgtRequest(QservMgtRequest const&) = delete;
    QservMgtRequest& operator=(QservMgtRequest const&) = delete;

    virtual ~QservMgtRequest() = default;

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
     * of the XrdSsiService class for communications with the rempte services.
     *
     * @param service  - a pointer to an instance of the API object for
     *                   submititng requests to remote services
     * @param jobId    - an optional identifier of a job specifying a context
     *                   in which a request will be executed.
     * @param requestExpirationIvalSec - an optional parameter (if differs from 0)
     *                   allowing to override the default value of
     *                   the corresponding parameter from the Configuration.
     */
    void start(XrdSsiService* service,
               std::string const& jobId="",
               unsigned int requestExpirationIvalSec=0);

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

    /**
     * @return a string representation for a subclass's persistent state
     * ready to be insert into the corresponding table as the values string
     * of the SQL INSERT statement:
     *     INSERT INTO <request-specific-table> VALUES <result-of-this-method>
     *
     * Note, that the result string must include round brackets as reaquired
     * by the SQL standard. The string values need to be properly escaped and
     * santized as required by the corresponiding database service (which
     * is passed as parameter into the method).
     *
     * The table name will be automatically deduced from a request-specific value
     * returned by method QservMgtRequest::type().
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
     * @param serviceProvider - reference to a provider of services
     * @param type            - its type name (used informally for debugging)
     * @param worker          - the name of a worker
     * @io_service            - BOOST ASIO service
     */
    QservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                    boost::asio::io_service& io_service,
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
      * @param lock - the lock must be acquired by a caller of the method
      */
    virtual void startImpl(util::Lock const& lock)=0;

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is configured via the configuraton service. When the request expires
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
     * @param lock          - the lock must be acquired by a caller of the method
     * @param extendedState - new extended state
     * @param serverError   - (optional) error message from a Qserv worker service
     */
    void finish(util::Lock const& lock,
                ExtendedState extendedState,
                std::string const& serverError="");

    /**
      * This method is supposed to be provided by subclasses
      * to finalize request processing as required by the subclass.
      *
      * @param lock - the lock must be acquired by a caller of the method
      */
    virtual void finishImpl(util::Lock const& lock)=0;

    /**
     * Start user-notification protocol (in case if user-defined notifiers
     * were provided to a subclass).
     *
     * @see QservMgtRequest::notifyImpl()
     */
    void notify();

    /**
     * This method is supposed to be provided by subclasses to forward
     * notification on request completion to a client which initiated
     * the request, etc.
     */
    virtual void notifyImpl()=0;

    /**
     * Ensure the object is in the deseride internal state. Throw an
     * exception otherwise.
     *
     * NOTES: normally this condition should never been seen unless
     *        there is a problem with the application implementation
     *        or the underlying run-time system.
     *
     * @param desiredState - desired state
     * @param context      - context from which the state test is requested
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
     * @param lock          - the lock must be acquired by a caller of the method
     * @param state         - primary state
     * @param extendedState - extended state
     */
    void setState(util::Lock const& lock,
                  State state,
                  ExtendedState extendedState=ExtendedState::NONE);

    /**
      * @return server error string (if any)
      * @param lock - the lock must be acquired by a caller of the method
      */
    std::string serverError(util::Lock const& lock) const;

    /**
      * @return sperformance info
      * @param lock - the lock must be acquired by a caller of the method
      */
    Performance performance(util::Lock const& lock) const;

protected:

    /// Mutex guarding internal state
    mutable util::Mutex _mtx;

private:

    ServiceProvider::Ptr _serviceProvider;

    std::string _type;
    std::string _id;
    std::string _worker;

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
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_QSERVMGTREQUEST_H
