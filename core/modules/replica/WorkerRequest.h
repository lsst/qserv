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
#ifndef LSST_QSERV_REPLICA_WORKER_REQUEST_H
#define LSST_QSERV_REPLICA_WORKER_REQUEST_H

/// WorkerRequest.h declares:
///
/// class WorkerRequest
/// (see individual class documentation for more information)

// System headers
#include <exception>
#include <memory>
#include <mutex>
#include <string>

// Qserv headers
#include "replica/Common.h"        // ExtendedCompletionStatus
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/// Exception thrown when a replication request is cancelled
struct WorkerRequestCancelled
    :   std::exception {

    /// Return the short description of the exception
    char const* what () const noexcept override {
        return "cancelled";
    }
};

/**
  * Class WorkerRequest is the base class for a family of the worker-side
  * requsts which require non-deterministic interactions with the server's
  * environment (network, disk I/O, etc.). Generally speaking, all requests
  * which can't be implemented instanteneously fall into this category.
  */
class WorkerRequest
    :   public std::enable_shared_from_this<WorkerRequest> {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerRequest> Ptr;

    /// Completion status of the request processing operation
    enum CompletionStatus {
        STATUS_NONE,           /// no processing has been attempted
        STATUS_IN_PROGRESS,
        STATUS_IS_CANCELLING,
        STATUS_CANCELLED,
        STATUS_SUCCEEDED,
        STATUS_FAILED
    };

    /// Return the string representation of the status
    static std::string status2string(CompletionStatus status);

    /// Return the string representation of the full status
    static std::string status2string(CompletionStatus status,
                                     ExtendedCompletionStatus extendedStatus);

    // Default construction and copy semantics are prohibited

    WorkerRequest() = delete;
    WorkerRequest(WorkerRequest const&) = delete;
    WorkerRequest& operator=(WorkerRequest const&) = delete;

    /// Destructor (can't say 'override' because the base class's one is not virtual)
    virtual ~WorkerRequest() = default;

    // Trivial accessors

    ServiceProvider::Ptr const& serviceProvider() { return _serviceProvider; }

    std::string const& worker() const { return _worker; }
    std::string const& type() const   { return _type; }
    std::string const& id() const     { return _id; }

    int priority() const { return _priority; }

    CompletionStatus         status() const         { return _status; }
    ExtendedCompletionStatus extendedStatus() const { return _extendedStatus; }

    /// Return the performance info
    const WorkerPerformance& performance() const { return _performance; }

    /** Set the status
     *
     * ATTENTION: this method needs to be called witin a thread-safe context
     * when moving requests between different queues.
     */
    void setStatus(CompletionStatus status,
                   ExtendedCompletionStatus extendedStatus =
                        ExtendedCompletionStatus::EXT_STATUS_NONE);

    /**
     * This method should be invoked (repeatedly) to execute the request until
     * it returns 'true' or throws an exception. Note that returning 'true'
     * may mean both success or failure, depeniding on the completion status
     * of the request.
     *
     * The method will throw custom exception WorkerRequestCancelled when
     * it detects a cancellation request.
     *
     * The default implementation of the method will do nothing, just simulate
     * processing. This can be serve as a foundation for various tests
     * of this framework.
     */
    virtual bool execute();

    /**
     * Cancel execution of the request.
     *
     * The effect of the operation varies depending on the current state of
     * the request. The default (the base class's implementation) assumes
     * the following transitions:
     * 
     *   STATUS_NONE or STATUS_CANCELLED             - transition to state STATUS_CANCELLED
     *   STATUS_IN_PROGRESS or STATUS_IS_CANCELLING  - transition to state STATUS_IS_CANCELLING
     *   other                                       - throwing std::logic_error

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
     *   STATUS_NONE or STATUS_IN_PROGRESS - transition to STATUS_NONE
     *   STATUS_IS_CANCELLING              - transition to STATUS_CANCELLED and throwing WorkerRequestCancelled
     *   other                             - throwing std::logic_error
     */
    virtual void rollback();

    /// Return the context string
    std::string context() const {
        return id() + "  " + type() + "  " + status2string(status()) + "  ";
    }

protected:

    /**
     * The normal constructor of the class.
     */
    WorkerRequest(ServiceProvider::Ptr const& serviceProvider,
                  std::string const& worker,
                  std::string const& type,
                  std::string const& id,
                  int priority);
    /**
     * This structure is used for tracking errors reported by method 'reportErrorIf
     */
    struct ErrorContext {

        // State of the object

        bool failed;
        ExtendedCompletionStatus extendedStatus;

        ErrorContext()
            :   failed(false),
                extendedStatus(ExtendedCompletionStatus::EXT_STATUS_NONE) {
        }
        
        /**
         *  Merge the context of another object into the current one.
         *  
         *  Note, only the first error code will be stored when a error condition
         *  is detected. An assumption is that the first error would usually cause
         *  a "chain reaction", hence only the first one typically matters.
         *  Other details could be found in the log files if needed.
         */
        ErrorContext& operator|| (const ErrorContext &rhs) {
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
     * @param condition      - if set to 'true' then there is a error condition
     * @param extendedStatus - extended status corresponding to the condition
     *                         (will be ignored if no error condition is present)
     * @param errorMsg       - a message to be reported into the log stream
     *
     * @return the context object encapculating values passed in parameters
     * 'condition' and 'extendedStatus'
     */
    ErrorContext reportErrorIf(bool condition,
                               ExtendedCompletionStatus extendedStatus,
                               std::string const& errorMsg);

protected:

    ServiceProvider::Ptr _serviceProvider;

    std::string _worker;
    std::string _type;
    std::string _id;

    int _priority;
    
    CompletionStatus         _status;
    ExtendedCompletionStatus _extendedStatus;

    /// Performance counters
    WorkerPerformance _performance;

    /// The number of milliseconds since the beginning of the request processing.
    /// This members is used by the default implementation of method execute()
    /// to simulate request processing.
    unsigned int _durationMillisec;

    /// Mutex guarding operations with the worker's data folder
    static std::mutex _mtxDataFolderOperations;

    /// Mutex guarding API calls where it's needed
    static std::mutex _mtx;
};


/// Comparision type for strict weak ordering reaquired by std::priority_queue
struct WorkerRequestCompare {

    /// Order requests by their priorities
    bool operator()(WorkerRequest::Ptr const& lhs,
                    WorkerRequest::Ptr const& rhs) const {

        return lhs->priority() < rhs->priority();
    }
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_REQUEST_H
