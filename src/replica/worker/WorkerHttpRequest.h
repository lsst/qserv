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
#ifndef LSST_QSERV_REPLICA_WORKERHTTPREQUEST_H
#define LSST_QSERV_REPLICA_WORKERHTTPREQUEST_H

// System headers
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/proto/Protocol.h"
#include "replica/util/Common.h"
#include "replica/util/Mutex.h"
#include "replica/util/Performance.h"

// Forward declarations
namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Structure WorkerHttpRequestCancelled represent an exception thrown when
 * a replication request is cancelled
 */
class WorkerHttpRequestCancelled : public std::exception {
public:
    /// @return a short description of the exception
    char const* what() const noexcept override { return "cancelled"; }
};

/**
 * Class WorkerHttpRequest is the base class for a family of the worker-side
 * requests which require non-deterministic interactions with the server's
 * environment (network, disk I/O, etc.). Generally speaking, all requests
 * which can't be implemented instantaneously fall into this category.
 */
class WorkerHttpRequest : public std::enable_shared_from_this<WorkerHttpRequest> {
public:
    /// The function type for notifications on the expiration of the request
    /// given its unique identifier.
    typedef std::function<void(std::string const&)> ExpirationCallbackType;

    WorkerHttpRequest() = delete;
    WorkerHttpRequest(WorkerHttpRequest const&) = delete;
    WorkerHttpRequest& operator=(WorkerHttpRequest const&) = delete;

    /// Destructor (can't 'override' because the base class's one is not virtual)
    /// Also, non-trivial destructor is needed to stop the request expiration
    /// timer (if any was started by the constructor).
    virtual ~WorkerHttpRequest();

    // The general attributes of requests are made public to allow
    // direct access by the worker's request processor for monitoring and reporting purposes.
    // The attributes are expected to be immutable after the construction of the request.

    std::string const& worker() const { return _worker; }
    std::string const& id() const { return _hdr.id; }
    int priority() const { return _hdr.priority; }
    protocol::Status status() const { return _status; }
    protocol::StatusExt extendedStatus() const { return _extendedStatus; }

    /// Request type (name) is allowed to be refined by derived classes to provide more
    /// specific type information. This is needed for requests that support subtypes.
    virtual std::string type() const { return _type; }

    /**
     * This method is called from the initial state protocol::Status::CREATED in order
     * to start the request expiration timer. It's safe to call this operation
     * multiple times. Each invocation of the method will result in cancelling
     * the previously set timer (if any) and starting a new one.
     */
    void init();

    /**
     * This method is called from the initial state protocol::Status::CREATED in order
     * to prepare the request for processing (to respond to methods 'execute',
     * 'cancel', 'rollback' or 'reset'. The final state upon the completion
     * of the method should be protocol::Status::IN_PROGRESS.
     */
    void start();

    /**
     * This method should be invoked (repeatedly) to execute the request until
     * it returns 'true' or throws an exception. Note that returning 'true'
     * may mean both success or failure, depending on the completion status
     * of the request.
     *
     * This method is required to be called while the request state is protocol::Status::IN_PROGRESS.
     * The method will throw custom exception WorkerHttpRequestCancelled when it detects a cancellation
     * request.
     *
     * @return result of the operation as explained above
     */
    virtual bool execute() = 0;

    /**
     * Cancel execution of the request.
     *
     * The effect of the operation varies depending on the current state of
     * the request. The default (the base class's implementation) assumes
     * the following transitions:
     *
     * {protocol::Status::CREATED,protocol::Status::CANCELLED} -> protocol::Status::CANCELLED
     * {protocol::Status::IN_PROGRESS,protocol::Status::IS_CANCELLING} -> protocol::Status::IS_CANCELLING
     * {*} -> throw std::logic_error
     */
    virtual void cancel();

    /**
     * Roll back the request into its initial state and cleanup partial results
     * if possible.
     *
     * The effect of the operation varies depending on the current state of
     * the request. The default (the base class's implementation) assumes
     * the following transitions:
     *
     * {protocol::Status::CREATED, protocol::Status::IN_PROGRESS} -> protocol::Status::CREATED
     * {protocol::Status::IS_CANCELLING} -> protocol::Status::CANCELLED -> throw WorkerHttpRequestCancelled
     * {*} -> throw std::logic_error
     */
    virtual void rollback();

    /**
     * This method is called from *ANY* initial state in order to turn
     * the request back into the initial protocol::Status::CREATED.
     */
    void stop();

    /**
     * This method should be used to cancel the request expiration timer.
     * Normally this method is initiated during the external "garbage collection"
     * of requests to ensure all resources (including a copy of a smart pointer onto
     * objects of the request classes) held by timers get released.
     *
     * @note this method won't throw any exceptions so that it could
     *   be invoked from the destructor. All exceptions (should they
     *   occur during an execution of the method) will be intercepted
     *   and reported as errors to the message logger.
     */
    void dispose() noexcept;

    /**
     * This method is used to forcefully set the status of the request by WorkerProcessorThread
     * in case of exceptions escaping from method 'execute'.
     *
     * @param status primary status to be set
     * @param extendedStatus secondary status to be set
     */
    void setStatus(protocol::Status status, protocol::StatusExt extendedStatus = protocol::StatusExt::NONE) {
        replica::Lock const lock(mtx, context("WorkerHttpRequest", __func__));
        setStatus(lock, status, extendedStatus);
    }

    /**
     * Extract the extra data from the request and put it into the response object.
     * @param includeResultIfFinished (optional) flag to include results if the request has finished
     */
    nlohmann::json toJson(bool includeResultIfFinished = false) const;

    /// @return the context string
    std::string context(std::string const& className, std::string const& func) const;

protected:
    /**
     * The normal constructor of the class.
     * @param serviceProvider provider is needed to access the Configuration of
     *   a setup and for validating the input parameters
     * @param worker the name of a worker. It must be the same worker as the one
     *   where the request is going to be processed.
     * @param type the type name of a request
     * @param hdr request header (common parameters of the queued request)
     * @param req the request object received from a client (request-specific parameters)
     * @param onExpired request expiration callback function
     * @throws std::invalid_argument if the worker is unknown
     */
    WorkerHttpRequest(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& worker,
                      std::string const& type, protocol::QueuedRequestHdr const& hdr,
                      nlohmann::json const& req, ExpirationCallbackType const& onExpired);

    /**
     * The simplified constructor for testing the request parsing methods: hasParam(),
     * reqParam*() and optParam*().
     *
     * The init testing should be done in the following derived class:
     * @code
     * class TestWorkerHttpRequest : public WorkerHttpRequest {
     * public:
     *     TestWorkerHttpRequest(json const& req) : WorkerHttpRequest(req) {}
     *     virtual ~TestWorkerHttpRequest() = default;
     *     virtual bool execute() { return true; }
     *     virtual void getResult(nlohmann::json& result) const {}
     *
     *     // Bring the parsing methods to the public scope for testing
     *     using WorkerHttpRequest::hasParam;
     *     using WorkerHttpRequest::optParamBool;
     *     ..
     * };
     * @endcode
     * @param req the request object carrying the parameters the request-specific parameters.
     */
    WorkerHttpRequest(nlohmann::json const& req);

    /// @return the service provider associated with the request
    std::shared_ptr<ServiceProvider> const& serviceProvider() const { return _serviceProvider; }

    /**
     * The method is used to check if the request is entered the cancellation state.
     * The implementation assumes the following transitions:
     *
     * {protocol::Status::IN_PROGRESS} -> protocol::Status::IN_PROGRESS
     * {protocol::Status::IS_CANCELLING} -> protocol::Status::CANCELLED -> throw WorkerHttpRequestCancelled
     * {*} -> throw std::logic_error
     *
     * @param lock a lock on mtx which acquired before calling this method
     * @param context_ a scope class/method from where the method was called
     * @throws WorkerHttpRequestCancelled if the request is being cancelled.
     * @throws std::logic_error if the state is not as expected.
     */
    void checkIfCancelling(replica::Lock const& lock, std::string const& context_);

    /** Set the status
     *
     * @note this method needs to be called within a thread-safe context
     *   when moving requests between different queues.
     *
     * @param lock a lock which acquired before calling this method
     * @param status primary status to be set
     * @param extendedStatus secondary status to be set
     */
    void setStatus(replica::Lock const& lock, protocol::Status status,
                   protocol::StatusExt extendedStatus = protocol::StatusExt::NONE);

    /**
     * Fill in the information object for the specified request based on its actual type.
     * @note This method is expected to be called when the request is finished and the result
     *  is ready to be sent to a client. Therefore no synchronization is needed to read the data
     *  of the request (except for the result which is expected to be ready at this point).
     * @param result an object to be filled
     */
    virtual void getResult(nlohmann::json& result) const = 0;

    /**
     * Structure ErrorContext is used for tracking errors reported by
     * method 'reportErrorIf
     */
    struct ErrorContext {
        // State of the object
        bool failed;
        protocol::StatusExt extendedStatus;

        ErrorContext() : failed(false), extendedStatus(protocol::StatusExt::NONE) {}

        /**
         * Merge the context of another object into the current one.
         *
         * @note Only the first error code will be stored when a error condition
         *  is detected. An assumption is that the first error would usually cause
         *  a "chain reaction", hence only the first one typically matters.
         *  Other details could be found in the log files if needed.
         * @param ErrorContext input context to be merged with the current state
         */
        ErrorContext& operator||(const ErrorContext& rhs) {
            if (&rhs != this) {
                if (rhs.failed and not failed) {
                    failed = true;
                    extendedStatus = rhs.extendedStatus;
                }
            }
            return *this;
        }
    };

    /**
     * Check if the error condition is set and report the error.
     * The error message will be sent to the corresponding logging
     * stream.
     *
     * @param condition if set to 'true' then there is a error condition
     * @param extendedStatus extended status corresponding to the condition
     *   (will be ignored if no error condition is present)
     * @param errorMsg a message to be reported into the log stream
     * @return the context object encapsulating values passed in parameters
     *   'condition' and 'extendedStatus'
     */
    ErrorContext reportErrorIf(bool condition, protocol::StatusExt extendedStatus,
                               std::string const& errorMsg);

    /// Return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    // Methods for parsing the input parameters of the request. The methods will throw std::invalid_argument
    // if the parameters are not found or have invalid values. The methods are expected to be called from
    // the constructor of the derived class to validate the input parameters and to extract their values into
    // the corresponding data members of the derived class.
    //
    // Note on the parsing of the boolean parameters: the methods will try to convert the value of a parameter
    // to boolean if it's the JSON boolean value (false or true) or an integer (0 or != 0).

    bool hasParam(std::string const& name) const;
    std::string reqParamString(std::string const& name) const;
    std::string optParamString(std::string const& name,
                               std::string const& defaultValue = std::string()) const;
    bool reqParamBool(std::string const& name) const;
    bool optParamBool(std::string const& name, bool defaultValue = false) const;
    std::uint16_t reqParamUInt16(std::string const& name) const;
    std::uint16_t optParamUInt16(std::string const& name, std::uint16_t defaultValue = 0) const;
    std::uint32_t reqParamUInt32(std::string const& name) const;
    std::uint32_t optParamUInt32(std::string const& name, std::uint32_t defaultValue = 0) const;
    std::int32_t reqParamInt32(std::string const& name) const;
    std::int32_t optParamInt32(std::string const& name, std::int32_t defaultValue = 0) const;
    std::uint64_t reqParamUInt64(std::string const& name) const;
    std::uint64_t optParamUInt64(std::string const& name, std::uint64_t defaultValue = 0) const;
    double reqParamDouble(std::string const& name) const;
    double optParamDouble(std::string const& name, double defaultValue = 0.0) const;
    std::vector<std::string> reqParamStringVec(std::string const& name) const;
    std::vector<std::string> optParamStringVec(
            std::string const& name,
            std::vector<std::string> const& defaultValue = std::vector<std::string>()) const;
    std::vector<std::uint64_t> reqParamUInt64Vec(std::string const& name) const;
    std::vector<std::uint64_t> optParamUInt64Vec(
            std::string const& name,
            std::vector<std::uint64_t> const& defaultValue = std::vector<std::uint64_t>()) const;
    nlohmann::json const& reqParamVec(std::string const& name) const;
    nlohmann::json const& reqParamObj(std::string const& name) const;

    /// Mutex guarding API calls where it's needed
    mutable replica::Mutex mtx;

    /// Mutex guarding operations with the worker's data folder
    static replica::Mutex mtxDataFolderOperations;

private:
    /**
     * Extract the parameter from the request and return its value.
     * @param name the name of the parameter to be extracted
     * @return the value of the parameter
     * @throw std::invalid_argument if the parameter is not found
     */
    nlohmann::json const& _reqParam(std::string const& name) const;

    /**
     * Extract the parameter from the request and validate its value as an array.
     * @param name the name of the parameter to be extracted
     * @return the value of the parameter validated as an array
     * @throw std::invalid_argument if the parameter is not found or is not an array
     */
    nlohmann::json const& _reqParamVec(std::string const& name) const;

    /**
     * Extract the parameter from the request and validate its value as an object.
     * @param name the name of the parameter to be extracted
     * @return the value of the parameter validated as an object
     * @throw std::invalid_argument if the parameter is not found or is not an object
     */
    nlohmann::json const& _reqParamObj(std::string const& name) const;

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is obtained from the Controller-side requests or obtained from
     * the configuration service. When the request expires (and if the timer
     * is not aborted due to request disposal) then an upstream callback
     * is invoked.
     *
     * @param ec error code to be checked to see if the time was aborted
     *   by the explicit request disposal operation.
     */
    void _expired(boost::system::error_code const& ec);

    // Input parameters

    std::shared_ptr<ServiceProvider> const _serviceProvider;

    std::string const _worker;
    std::string const _type;
    protocol::QueuedRequestHdr const _hdr;
    nlohmann::json const _req;

    ExpirationCallbackType _onExpired = nullptr;  ///< The callback is reset when the request gets expired
                                                  /// or explicitly disposed.
    unsigned int const _expirationTimeoutSec =
            0;  ///< The expiration timeout in seconds (0 means no expiration)

    /// This timer is used (if configured) to limit the total duration of time
    /// a request could exist from its creation till termination. The timer
    /// starts when the request gets created. And it's explicitly finished when
    /// a request object gets destroyed.
    ///
    /// If the time has a chance to expire then the request expiration callback
    /// (if any) passed into the constructor will be invoked to notify WorkerProcessor
    /// on the expiration event.
    ///
    /// @note the timer is implemented as a pointer to avoid creating the the timer during
    /// the unit testing. Thw timer class doesn't have a default constructor and it needs
    /// to be created with an I/O service which is not available during the unit testing.
    std::unique_ptr<boost::asio::deadline_timer> _expirationTimerPtr;

    // 2-layer state of a request

    std::atomic<protocol::Status> _status{protocol::Status::CREATED};
    std::atomic<protocol::StatusExt> _extendedStatus{protocol::StatusExt::NONE};

    /// Performance counters
    WorkerPerformance _performance;
};

/**
 * Structure WorkerHttpRequestCompare is a functor representing a comparison type
 * for strict weak ordering required by std::priority_queue
 */
struct WorkerHttpRequestCompare {
    /**
     * Sort requests by their priorities
     * @param lhs pointer to a request on the left side of a logical comparison
     * @param rhs pointer to a request on the right side of a logical comparison
     * @return 'true' if the priority of 'lhs' is strictly less than the one of 'rhs'
     */
    bool operator()(std::shared_ptr<WorkerHttpRequest> const& lhs,
                    std::shared_ptr<WorkerHttpRequest> const& rhs) const {
        return lhs->priority() < rhs->priority();
    }
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERHTTPREQUEST_H
