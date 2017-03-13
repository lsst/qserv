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
#include "global/intTypes.h"
#include "global/ResourceUnit.h"
#include "global/stringTypes.h"
#include "qdisp/JobDescription.h"
#include "qdisp/JobStatus.h"
#include "qdisp/ResponseHandler.h"
#include "util/InstanceCount.h"
#include "util/MultiError.h"
#include "util/threadSafe.h"

// Forward declarations
class XrdSsiService;

namespace lsst {
namespace qserv {
namespace qdisp {

class JobQuery;
class LargeResultMgr;
class MessageStore;
class QueryResource;


/// class Executive manages the execution of jobs for a UserQuery, while
/// maintaining minimal information about the jobs themselves.
class Executive : public std::enable_shared_from_this<Executive> {
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
    static Executive::Ptr newExecutive(Config::Ptr const& c, std::shared_ptr<MessageStore> const& ms,
            std::shared_ptr<LargeResultMgr> const& largeResultMgr);

    ~Executive();

    /// Add an item with a reference number
    std::shared_ptr<JobQuery> add(JobDescription const& s);

    /// Start all jobs added with add().
    void startAllJobs();

    /// Start a specific job, jobQuery must have been created with add();
    void startJob(std::shared_ptr<JobQuery> const& jobQuery);

    /// Block until execution is completed
    /// @return true if execution was successful
    bool join();

    /// Notify the executive that an item has completed
    void markCompleted(int refNum, bool success);

    /// Squash all the jobs.
    void squash();

    bool getEmpty() { return _empty; }

    void setQueryId(QueryId id);
    QueryId getId() const { return _id; }
    std::string const& getIdStr() const { return _idStr; }

    std::shared_ptr<JobQuery> getJobQuery(int id);

    /// @return number of items in flight.
    int getNumInflight(); // non-const, requires a mutex.

    /// @return a description of the current execution progress.
    std::string getProgressDesc() const;

    /// @return true if cancelled
    bool getCancelled() { return _cancelled; }

    XrdSsiService* getXrdSsiService() { return _xrdSsiService; }

    std::shared_ptr<LargeResultMgr> getLargeResultMgr() { return _largeResultMgr; }

    bool xrdSsiProvision(std::shared_ptr<QueryResource> &jobQueryResource,
                         std::shared_ptr<QueryResource> const& sourceQr);

    std::mutex sumMtx; // &&&
    int cancelLockQSEASum{0}; // &&&
    int jobQueryQSEASum{0}; // &&&
    int addJobQSEASum{0}; // &&&
    int trackQSEASum{0}; // &&&
    int endQSEASum{0}; // &&&

private:
    Executive(Config::Ptr const& c, std::shared_ptr<MessageStore> const& ms,
              std::shared_ptr<LargeResultMgr> const& largeResultMgr);

    void _setup();

    bool _track(int refNum, std::shared_ptr<JobQuery> const& r);
    void _unTrack(int refNum);
    bool _addJobToMap(std::shared_ptr<JobQuery> const& job);
    std::string _getIncompleteJobsString(int maxToList);

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
    std::shared_ptr<LargeResultMgr> _largeResultMgr;

    /** Execution errors */
    util::MultiError _multiError;

    int _requestCount; ///< Count of submitted jobs
    util::Flag<bool> _cancelled {false}; ///< Has execution been cancelled.

    // Mutexes
    std::mutex _incompleteJobsMutex; ///< protect incompleteJobs map.

    /** Used to record execution errors */
    mutable std::mutex _errorsMutex;

    std::condition_variable _allJobsComplete;
    mutable std::recursive_mutex _jobsMutex;

    QueryId _id{0}; ///< Unique identifier for this query.
    std::string    _idStr{QueryIdHelper::makeIdStr(0, true)};
    util::InstanceCount _instC{"Executive"};
};

class MarkCompleteFunc {
public:
    typedef std::shared_ptr<MarkCompleteFunc> Ptr;

    MarkCompleteFunc(Executive::Ptr const& e, int jobId) : _executive(e), _jobId(jobId) {}
    virtual ~MarkCompleteFunc() {}

    virtual void operator()(bool success) {
        auto exec = _executive.lock();
        if (exec != nullptr) {
            exec->markCompleted(_jobId, success);
        }
    }

private:
    std::weak_ptr<Executive> _executive;
    int _jobId;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_EXECUTIVE_H
