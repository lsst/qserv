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
#ifndef LSST_QSERV_REPLICA_REQUEST_TRACKER_H
#define LSST_QSERV_REPLICA_REQUEST_TRACKER_H

/// RequestTracker.h declares:
///
/// class RequestTrackerBase
/// class CommonRequestTracker
/// class AnyRequestTracker
///
/// (see individual class documentation for more information)

// System headers
#include <atomic>
#include <ostream>
#include <stdexcept>
#include <string>
#include <list>

// Qserv headers
#include "replica/ErrorReporting.h"
#include "replica/Request.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The base class implements a type-independent foundation for the common
 * tracker for a collection of homogenious requests whose type is specified
 * as the template parameter of the class.
 */
class RequestTrackerBase {

public:

    // Default construction and copy semantics are prohibited

    RequestTrackerBase() = delete;
    RequestTrackerBase(RequestTrackerBase const&) = delete;
    RequestTrackerBase& operator=(RequestTrackerBase const&) = delete;

    virtual ~RequestTrackerBase() = default;

    /**
     * Block the calling thread until all request are finished. Then post
     * a summary report on failed requests if the optional flag 'errorReport'
     * was specified when constructing the tracker object. A progress of
     * the request execution will also be reported if the optional flag
     * 'progressReport' is passed into the constructor.
     */
    void track() const;

    /**
     * Cancel all outstanding requests
     *
     * ATTENTION: by default this operation will *NOT* contact worker
     * services to stop on-going or queued requests unless the optional
     * flag 'propagateToServers' is set to 'true'.
     *
     * @param propagateToServers - propagate operation to servers (depends
     *                             on a request type as not all requests
     *                             can be canceled)
     */
    void cancel(bool propagateToServers=false);

    /**
     * The method will reset the tracket to the initial (empty) state. Please,
     * make sure there are no outstanding requests which may be still executing.
     *
     * @throws std::logic_error - if there is at least one outstanding
     *                            requests.
     */
    void reset();

protected:

    /**
     * The constructor sets up tracking options.
     *
     * @param os             - an output stream for monitoring and error printouts
     * @param progressReport - triggers periodic printout onto an output stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    explicit RequestTrackerBase(std::ostream& os,
                                bool progressReport=true,
                                bool errorReport=false);

    /**
     * The method to be implemented by a subclass in order to print
     * type-specific report.
     *
     * @param os - an output stream for the printout
     */
    virtual void printErrorReport(std::ostream& os) const=0;

    /**
     * The method to be implemented by a subclass is supposed to return all
     * requests which are known to the subclass.
     */
    virtual std::list<Request::Ptr> getRequests() const=0;

    /**
     * The method to be implemented by a subclass is supposed to clear
     * a collection of all requests known to the subclass.
     *
     * NOTE: It's guaranteed that the base class's counters will stay
     * intact when this method is called.
     */
    virtual void resetImpl()=0;

protected:

    // The counter of requests which will be updated. They need to be atomic
    // to avoid race condition between the onFinish() callbacks executed within
    // the Controller's thread and this thread.

    std::atomic<size_t> _numLaunched;   ///< the total number of requests launched
    std::atomic<size_t> _numFinished;   ///< the total number of finished requests
    std::atomic<size_t> _numSuccess;    ///< the number of successfully completed requests

private:

    std::ostream& _os;

    bool _progressReport;
    bool _errorReport;
};

/**
 * The class implements a type-aware common tracker for a collection of
 * homogenious requests whose type is specified as the template parameter
 * of the class.
 */
template <class T>
class CommonRequestTracker
    :   public RequestTrackerBase {

public:

    // Default construction and copy semantics are prohibited

    CommonRequestTracker() = delete;
    CommonRequestTracker(CommonRequestTracker const&) = delete;
    CommonRequestTracker& operator=(CommonRequestTracker const&) = delete;

    /**
     * The constructor sets up tracking options.
     *
     * @param os             - an output stream for monitoring and error printouts
     * @param progressReport - triggers periodic printout onto an output stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    explicit CommonRequestTracker(std::ostream& os,
                                  bool progressReport=true,
                                  bool errorReport=false)
        :   RequestTrackerBase(os,
                               progressReport,
                               errorReport) {
    }

    ~CommonRequestTracker() override = default;

    /**
     * The callback function to be registered with each request
     * injected into the tracker.
     */
    void onFinish(typename T::Ptr ptr) {
        RequestTrackerBase::_numFinished++;
        if (ptr->extendedState() == Request::ExtendedState::SUCCESS) {
            RequestTrackerBase::_numSuccess++;
        }
    }

    /**
     * Add a request to be tracked. Note that in order to be tracked
     * requests needs to be constructed with the above specified function
     */
    void add(typename T::Ptr const& ptr) {
        RequestTrackerBase::_numLaunched++;
        requests.push_back(ptr);
    }

protected:

    /**
     * Implement the corresponding method defined in the base class.
     *
     * @see RequestTrackerBase::printErrorReport
     */
    void printErrorReport(std::ostream& os) const override {
        replica::reportRequestState(requests, os);
    }

    /**
     * Implement the corresponding method defined in the base class.
     *
     * @see RequestTrackerBase::getRequests
     */
    std::list<Request::Ptr> getRequests() const override {
        std::list<Request::Ptr> result;
        for (auto&& ptr: requests) { result.push_back(ptr); }
        return result;
    }

    /**
     * Implement the corresponding method defined in the base class.
     *
     * @see RequestTrackerBase::resetImpl
     */
    void resetImpl() override {
        requests.clear();
    }

public:

    /// All requests that were launched
    std::list<typename T::Ptr> requests;
};


/**
 * The class implements a type-aware request tracker for a collection of
 * heterogenious requests.
 */
class AnyRequestTracker
    :   public RequestTrackerBase {

public:

    // Default construction and copy semantics are prohibited

    AnyRequestTracker() = delete;
    AnyRequestTracker(AnyRequestTracker const&) = delete;
    AnyRequestTracker& operator=(AnyRequestTracker const&) = delete;

    /**
     * The constructor sets up tracking options.
     *
     * @param os             - an output stream for monitoring and error printouts
     * @param progressReport - triggers periodic printout onto an output stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    explicit AnyRequestTracker(std::ostream& os,
                               bool progressReport=true,
                               bool errorReport=false);

    ~AnyRequestTracker() override = default;

    /// The callback function to be registered with each request
    /// injected into the tracker.
    void onFinish(Request::Ptr const& ptr);

    /**
     * Add a request to be tracked. Note that in order to be tracked
     * requests needs to be constructed with the above specified function
     */
    void add(Request::Ptr const& ptr);

protected:

    /**
     * Implement the corresponding method defined in the base class.
     *
     * @see RequestTrackerBase::printErrorReport
     */
    void printErrorReport(std::ostream& os) const override;

    /**
     * Implement the corresponding method defined in the base class.
     *
     * @see RequestTrackerBase::getRequests
     */
    std::list<Request::Ptr> getRequests() const override;

    /**
     * Implement the corresponding method defined in the base class.
     *
     * @see RequestTrackerBase::resetImpl
     */
    void resetImpl() override;

public:

    /// All requests that were launched
    std::list<Request::Ptr> requests;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_TRACKER_H
