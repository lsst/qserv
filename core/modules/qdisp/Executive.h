// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#include <atomic>
#include <mutex>
#include <sstream>
#include <vector>

// Qserv headers
#include "global/ResourceUnit.h"
#include "global/stringTypes.h"
#include "qdisp/JobStatus.h"
#include "qdisp/ResponseRequester.h"
#include "util/Callable.h"
#include "util/MultiError.h"
#include "util/threadSafe.h"

// Forward declarations
class XrdSsiService;

namespace lsst {
namespace qserv {
namespace qdisp {

class JobQuery;
class MessageStore;
class QueryResource;

/** Description of a job managed by the executive
 */
class JobDescription {
public:
    JobDescription(int id, ResourceUnit const& resource, std::string const& payload,
        std::shared_ptr<ResponseHandler> const& respHandler)
        : _id{id}, _resource{resource}, _payload{payload}, _respHandler{respHandler} {};

    int id() const { return _id; }
    ResourceUnit const& resource() const { return _resource; }
    std::string const& payload() const { return _payload; }
    std::shared_ptr<ResponseHandler> respHandler() { return _respHandler; }
    std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, JobDescription const& jd);
private:
    int _id; // Job's Id number.
    ResourceUnit _resource; // path, e.g. /q/LSST/23125
    std::string _payload; // encoded request
    std::shared_ptr<ResponseHandler> _respHandler; // probably MergingHandler
};
std::ostream& operator<<(std::ostream& os, JobDescription const& jd);

/// class Executive manages the execution of tasks for a UserQuery, while
/// maintaining minimal information about the tasks themselves.
class Executive {
public:
    typedef std::shared_ptr<Executive> Ptr;
    typedef std::map<int, std::shared_ptr<JobQuery>> JobMap;

    struct Config {
        typedef std::shared_ptr<Config> Ptr;
        Config(std::string const& serviceUrl_)
            : serviceUrl(serviceUrl_) {}
        Config(int,int) : serviceUrl(getMockStr()) {}

        std::string serviceUrl; ///< XrdSsi service URL, e.g. localhost:1094
        static std::string getMockStr() {return "Mock";};
    };

    /// Construct an Executive.
    /// If c->serviceUrl == Config::getMockStr(), then use XrdSsiServiceMock
    /// instead of a real XrdSsiService
    Executive(Config::Ptr c, std::shared_ptr<MessageStore> ms);

    ~Executive();

    /// Add an item with a reference number
    void add(JobDescription const& s);

    /// Block until execution is completed
    /// @return true if execution was successful
    bool join();

    /// Notify the executive that an item has completed
    void markCompleted(int refNum, bool success);

    /// Try to squash/abort an item in progress
    void requestSquash(int refNum);

    /// Squash everything. should we block?
    void squash();

    bool getEmpty() { return _empty; }

    /// @return number of items in flight.
    int getNumInflight(); // non-const, requires a mutex.

    /// @return a description of the current execution progress.
    std::string getProgressDesc() const;

    /// @return true if cancelled
    bool getCancelled() { return _cancelled; }

    XrdSsiService* getXrdSsiService() { return _xrdSsiService; }

    std::shared_ptr<JobQuery> getJobQuery(int id);

private:
    void _setup();

    bool _track(int refNum, std::shared_ptr<JobQuery> const& r);
    void _unTrack(int refNum);
    bool _addJobToMap(std::shared_ptr<JobQuery> const& job);

    void _reapRequesters(std::unique_lock<std::mutex> const& requestersLock);

    void _updateProxyMessages();

    void _waitAllUntilEmpty();

    // for debugging
    void _printState(std::ostream& os);

    Config _config; ///< Personal copy of config
    std::atomic<bool> _empty {true};
    std::shared_ptr<MessageStore> _messageStore; ///< MessageStore for logging
    XrdSsiService* _xrdSsiService; ///< RPC interface
    JobMap _jobMap; ///< Contains information about all jobs.
    JobMap _incompleteJobs; ///< Map of incomplete jobs.

    /** Execution errors */
    util::MultiError _multiError;

    int _requestCount; ///< Count of submitted tasks
    std::atomic<bool> _cancelled {false}; ///< Has execution been cancelled?

    // Mutexes
    std::mutex _incompleteJobsMutex; ///< protect incompleteJobs map.

    /** Used to record execution errors */
    mutable std::mutex _errorsMutex;

    std::condition_variable _allJobsComplete;
    mutable std::mutex _jobsMutex;

};

class MarkCompleteFunc {
public:
    typedef std::shared_ptr<MarkCompleteFunc> Ptr;

    MarkCompleteFunc(Executive* e, int jobId) : _executive(e), _jobId(jobId) {}
    virtual ~MarkCompleteFunc() {}

    virtual void operator()(bool success) {
        if (_executive) {
            _executive->markCompleted(_jobId, success);
        }
    }

private:
    Executive* _executive;
    int _jobId;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_EXECUTIVE_H
