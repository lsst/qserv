// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
// class Executive is in charge of "executing" user query fragments on
// a qserv cluster.

#ifndef LSST_QSERV_QDISP_EXECUTIVE_H
#define LSST_QSERV_QDISP_EXECUTIVE_H

// System headers
#include <vector>

// Third-party headers
#include "boost/thread.hpp" // boost::mutex

// Local headers
#include "global/ResourceUnit.h"
#include "global/stringTypes.h"
#include "qdisp/TransactionSpec.h"
#include "qdisp/ExecStatus.h"
#include "util/Callable.h"
#include "util/threadSafe.h"

// Forward declarations
class XrdSsiService;

namespace lsst {
namespace qserv {
namespace qdisp {
class MessageStore;
class QueryResource;
class ResponseRequester;

/// class Executive manages the execution of tasks for a UserQuery, while
/// maintaining minimal information about the tasks themselves.
class Executive {
public:
    typedef std::shared_ptr<Executive> Ptr;
    typedef std::map<int, ExecStatus::Ptr> StatusMap;

    struct Config {
        typedef std::shared_ptr<Config> Ptr;
        Config(std::string const& serviceUrl_)
            : serviceUrl(serviceUrl_) {}
        Config(int,int) : serviceUrl(getMockStr()) {}

        std::string serviceUrl; ///< XrdSsi service URL, e.g. localhost:1094
        static std::string getMockStr() {return "Mock";};
    };

    /// Specification for something executable by the Executive
    struct Spec {
        ResourceUnit resource; // path, e.g. /q/LSST/23125
        std::string request; // encoded request
        std::shared_ptr<ResponseRequester> requester;
    };

    /// Construct an Executive.
    /// If c->serviceUrl == Config::getMockStr(), then use XrdSsiServiceMock
    /// instead of a real XrdSsiService
    Executive(Config::Ptr c, std::shared_ptr<MessageStore> ms);

    /// Add an item with a reference number (not necessarily a chunk number)
    void add(int refNum, Spec const& s);

    /// Block until execution is completed
    /// @return true if execution was successful
    bool join();

    /// Notify the executive that an item has completed
    void markCompleted(int refNum, bool success);

    /// Try to squash/abort an item in progress
    void requestSquash(int refNum);

    /// Squash everything. should we block?
    void squash();

    bool getEmpty() {return _empty.get();}

    /// @return number of items in flight.
    int getNumInflight(); // non-const, requires a mutex.

    /// @return a description of the current execution progress.
    std::string getProgressDesc() const;

    static std::shared_ptr<util::UnaryCallable<void, bool> > newNotifier(Executive& e, int refNum);


private:
    typedef std::shared_ptr<ResponseRequester> RequesterPtr;
    typedef std::map<int, RequesterPtr> RequesterMap;

    class DispatchAction;
    friend class DispatchAction;
    void _dispatchQuery(int refNum,
                        Spec const& spec,
                        ExecStatus::Ptr execStatus);

    void _setup();
    bool _shouldRetry(int refNum);
    ExecStatus::Ptr _insertNewStatus(int refNum, ResourceUnit const& r);

    bool _track(int refNum, RequesterPtr r);
    void _unTrack(int refNum);

    void _reapRequesters(boost::unique_lock<boost::mutex> const& requestersLock);
    void _reportStatuses();

    void _waitAllUntilEmpty();

    // for debugging
    void _printState(std::ostream& os);

    Config _config; ///< Personal copy of config
    util::Flag<bool> _empty;
    std::shared_ptr<MessageStore> _messageStore; ///< MessageStore for logging
    XrdSsiService* _service; ///< RPC interface
    RequesterMap _requesters; ///< RequesterMap for results from submitted tasks
    StatusMap _statuses; ///< Statuses of submitted tasks
    int _requestCount; ///< Count of submitted tasks
    bool _cancelled; ///< Has execution been cancelled?

    // Mutexes
    boost::mutex _requestersMutex;
    boost::condition_variable _requestersEmpty;
    mutable boost::mutex _statusesMutex;
    boost::mutex _retryMutex;
    boost::mutex _cancelledMutex;

    typedef std::map<int,int> IntIntMap;
    IntIntMap _retryMap; ///< Counter for task retries.

}; // class Executive

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_EXECUTIVE_H
