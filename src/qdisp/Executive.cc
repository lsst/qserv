// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 AURA/LSST.
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

/**
 * @file
 *
 * @brief Executive. It executes and tracks jobs from a user query.
 *
 * TODO: Consider merging RequesterMap and StatusMap. Originally, RequesterMap
 * was separate from StatusMap to reduce contention when things are just
 * updating statuses, but if the contention is small, we can simplify by
 * combining them (Requester, status) into a single map.
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "qdisp/Executive.h"

// System headers
#include <algorithm>
#include <cassert>
#include <chrono>
#include <deque>
#include <functional>
#include <iostream>
#include <sstream>

// Third-party headers
#include "boost/format.hpp"
#include "XrdSsi/XrdSsiErrInfo.hh"
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiResource.hh"
#include "XrdSsi/XrdSsiService.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "ccontrol/MergingHandler.h"
#include "ccontrol/msgCode.h"
#include "ccontrol/TmpTableName.h"
#include "ccontrol/UserQuerySelect.h"
#include "global/LogContext.h"
#include "global/ResourceUnit.h"
#include "qdisp/CzarStats.h"
#include "qdisp/JobQuery.h"
#include "qdisp/QueryRequest.h"
#include "qdisp/ResponseHandler.h"
#include "qdisp/XrdSsiMocks.h"
#include "query/QueryContext.h"
#include "qproc/QuerySession.h"
#include "qmeta/Exceptions.h"
#include "qmeta/MessageStore.h"
#include "qmeta/QStatus.h"
#include "query/SelectStmt.h"
#include "rproc/InfileMerger.h"
#include "util/AsyncTimer.h"
#include "util/Bug.h"
#include "util/EventThread.h"

using namespace std;

extern XrdSsiProvider* XrdSsiProviderClient;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.Executive");

string getErrorText(XrdSsiErrInfo& e) {
    ostringstream os;
    int errCode;
    os << "XrdSsiError " << e.Get(errCode);
    os << " Code=" << errCode;
    return os.str();
}

}  // anonymous namespace

namespace lsst::qserv::qdisp {

mutex Executive::_executiveMapMtx;                      ///< protects _executiveMap
map<QueryId, std::weak_ptr<Executive>> Executive::_executiveMap;  ///< Map of executives for queries in progress.

////////////////////////////////////////////////////////////////////////
// class Executive implementation
////////////////////////////////////////////////////////////////////////
Executive::Executive(ExecutiveConfig const& c, shared_ptr<qmeta::MessageStore> const& ms,
                     SharedResources::Ptr const& sharedResources, shared_ptr<qmeta::QStatus> const& qStatus,
                     shared_ptr<qproc::QuerySession> const& querySession)
        : _config(c),
          _messageStore(ms),
          _qdispPool(sharedResources->getQdispPool()),
          _qMeta(qStatus),
          _querySession(querySession) {
    _secondsBetweenQMetaUpdates = chrono::seconds(_config.secondsBetweenChunkUpdates);
    _setup();
    _setupLimit();
    qdisp::CzarStats::get()->addQuery();
}

Executive::~Executive() {
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::~Executive() " << getIdStr());
    qdisp::CzarStats::get()->deleteQuery();
    qdisp::CzarStats::get()->deleteJobs(_incompleteJobs.size());
    // Remove this executive from the map.
    if (getExecutiveFromMap(getId()) != nullptr) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) + " pointer in map should be invalid QID=" << getId());
    }
    // Real XrdSsiService objects are unowned, but mocks are allocated in _setup.
    delete dynamic_cast<XrdSsiServiceMock*>(_xrdSsiService);
    if (_asyncTimer != nullptr) {
        _asyncTimer->cancel();
        qdisp::CzarStats::get()->untrackQueryProgress(_id);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::~Executive() " << getIdStr() << " end");
}

Executive::Ptr Executive::create(ExecutiveConfig const& c, shared_ptr<qmeta::MessageStore> const& ms,
                                 SharedResources::Ptr const& sharedResources,
                                 shared_ptr<qmeta::QStatus> const& qMeta,
                                 shared_ptr<qproc::QuerySession> const& querySession,
                                 boost::asio::io_service& asioIoService) {
    LOGS(_log, LOG_LVL_DEBUG, "Executive::" << __func__);
    Executive::Ptr ptr(new Executive(c, ms, sharedResources, qMeta, querySession));

    // Start the query progress monitoring timer (if enabled). The query status
    // will be sampled on each expiration event of the timer. Note that the timer
    // gets restarted automatically for as long as the context (the current
    // Executive object) still exists.
    //
    // IMPORTANT: The weak pointer dependency (unlike the regular shared pointer)
    // is required here to allow destroying the Executive object without explicitly
    // stopping the timer.
    auto const czarStatsUpdateIvalSec = cconfig::CzarConfig::instance()->czarStatsUpdateIvalSec();
    if (czarStatsUpdateIvalSec > 0) {
        ptr->_asyncTimer = util::AsyncTimer::create(
                asioIoService, std::chrono::milliseconds(czarStatsUpdateIvalSec * 1000),
                [self = std::weak_ptr<Executive>(ptr)](auto expirationIvalMs) -> bool {
                    auto ptr = self.lock();
                    string const msg = string("Executive::") + __func__ +
                                       " expirationIvalMs: " + to_string(expirationIvalMs.count()) + " ms";
                    if (ptr != nullptr) {
                        ptr->_updateStats();
                        LOGS(_log, LOG_LVL_DEBUG, msg + " " + ptr->getIdStr());
                        return true;
                    }
                    LOGS(_log, LOG_LVL_DEBUG, msg);
                    return false;
                });
        ptr->_asyncTimer->start();
    }
    return ptr;
}

void Executive::_updateStats() const {
    LOGS(_log, LOG_LVL_DEBUG, "Executive::" << __func__);
    qdisp::CzarStats::get()->updateQueryProgress(_id, getNumInflight());
}

void Executive::setQueryId(QueryId id) {
    if (_queryIdSet.exchange(true) == true) {
        throw util::Bug(ERR_LOC, "Executive::setQueryId called more than once _id=" + to_string(_id) +
                                         " id=" + to_string(id));
    }
    _id = id;
    _idStr = QueryIdHelper::makeIdStr(_id);

    // Insert into the global executive map.
    {
        lock_guard<mutex> lgMap(_executiveMapMtx);
        _executiveMap[_id] = shared_from_this();
    }
    qdisp::CzarStats::get()->trackQueryProgress(_id);
}

Executive::Ptr Executive::getExecutiveFromMap(QueryId qId) {
    lock_guard<mutex> lgMap(_executiveMapMtx);
    auto iter = _executiveMap.find(qId);
    if (iter == _executiveMap.end()) {
        return nullptr;
    }
    Executive::Ptr exec = iter->second.lock();
    if (exec == nullptr) {
        _executiveMap.erase(iter);
    }
    return exec;
}

UberJob::Ptr Executive::findUberJob(UberJobId ujId) {
    lock_guard<mutex> lgMap(_uberJobsMapMtx);
    auto iter = _uberJobsMap.find(ujId);
    if (iter == _uberJobsMap.end()) {
        return nullptr;
    }
    return iter->second;
}

/// Add a new job to executive queue, if not already in. Not thread-safe.
///
JobQuery::Ptr Executive::add(JobDescription::Ptr const& jobDesc) {
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::add start");
    JobQuery::Ptr jobQuery;
    {
        // Create the JobQuery and put it in the map.
        auto jobStatus = make_shared<qmeta::JobStatus>();
        Ptr thisPtr = shared_from_this();
        MarkCompleteFunc::Ptr mcf = make_shared<MarkCompleteFunc>(thisPtr, jobDesc->id());
        jobQuery = JobQuery::create(thisPtr, jobDesc, jobStatus, mcf, _id);

        QSERV_LOGCONTEXT_QUERY_JOB(jobQuery->getQueryId(), jobQuery->getJobId());

        {
            lock_guard<recursive_mutex> lock(_cancelled.getMutex());
            if (_cancelled) {
                LOGS(_log, LOG_LVL_DEBUG,
                     "Executive already cancelled, ignoring add(" << jobDesc->id() << ")");
                return nullptr;
            }

            if (!_addJobToMap(jobQuery)) {
                LOGS(_log, LOG_LVL_ERROR, "Executive ignoring duplicate job add");
                return jobQuery;
            }

            if (!_track(jobQuery->getJobId(), jobQuery)) {
                LOGS(_log, LOG_LVL_ERROR, "Executive ignoring duplicate track add");
                return jobQuery;
            }

            _addToChunkJobMap(jobQuery);  // &&&uj
        }

        if (_empty.exchange(false)) {
            LOGS(_log, LOG_LVL_DEBUG, "Flag _empty set to false");
        }
        ++_requestCount;
    }

    QSERV_LOGCONTEXT_QUERY_JOB(jobQuery->getQueryId(), jobQuery->getJobId());

    LOGS(_log, LOG_LVL_WARN, "&&& Executive::add end");
    return jobQuery;
}

void Executive::runJobQuery(JobQuery::Ptr const& jobQuery) {
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::runJobQuery start");
    bool started = jobQuery->runJob();
    if (!started && isLimitRowComplete()) {
        markCompleted(jobQuery->getJobId(), false);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::runJobQuery end");
}

void Executive::queueJobStart(PriorityCommand::Ptr const& cmd) {
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::queueJobStart start");
    _jobStartCmdList.push_back(cmd);
    if (_scanInteractive) {
        _qdispPool->queCmd(cmd, 0);
    } else {
        _qdispPool->queCmd(cmd, 1);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::queueJobStart end");
}

void Executive::queueFileCollect(PriorityCommand::Ptr const& cmd) {
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::queueFileCollect start");

    if (_scanInteractive) {
        _qdispPool->queCmd(cmd, 3);
    } else {
        _qdispPool->queCmd(cmd, 4);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::queueFileCollect end");
}

/// &&&uj &&&&&&&&&&&&&&&&&&&&&& NEED CODE put this as command in qdisppool.
void Executive::runUberJob(std::shared_ptr<UberJob> const& uberJob) {
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::runUberJob start");
    bool started = uberJob->runUberJob();
    /* &&&uj
    if (!started && isLimitRowComplete()) {
        uberJob->callMarkCompleteFunc(false);
    }
    */
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::runUberJob end");
}

void Executive::waitForAllJobsToStart() {
    LOGS(_log, LOG_LVL_INFO, "waitForAllJobsToStart");
    LOGS(_log, LOG_LVL_WARN, "&&& waitForAllJobsToStart  start");
    // Wait for each command to start.
    while (true) {
        bool empty = _jobStartCmdList.empty();
        if (empty) break;
        auto cmd = move(_jobStartCmdList.front());
        _jobStartCmdList.pop_front();
        cmd->waitComplete();
    }
    LOGS(_log, LOG_LVL_INFO, "waitForAllJobsToStart done");
    LOGS(_log, LOG_LVL_WARN, "&&& waitForAllJobsToStart  end");
}

// If the executive has not been cancelled, then we simply start the query.
// @return true if query was actually started (i.e. we were not cancelled)
//
bool Executive::startQuery(shared_ptr<JobQuery> const& jobQuery) {  // &&&
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::startQuery start");
    lock_guard<recursive_mutex> lock(_cancelled.getMutex());
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::startQuery a");

    // If we have been cancelled, then return false.
    //
    if (_cancelled) return false;

    // Construct a temporary resource object to pass to ProcessRequest().
    //   Interactive Queries should have an Affinity of XrdSsiResource::None or Weak while
    //   Scans should have an affinity of Strong
    XrdSsiResource::Affinity affinity = (_scanInteractive) ? XrdSsiResource::Weak : XrdSsiResource::Strong;
    XrdSsiResource jobResource(jobQuery->getDescription()->resource().path(), "", jobQuery->getIdStr(), "", 0,
                               affinity);

    // Now construct the actual query request and tie it to the jobQuery. The
    // shared pointer is used by QueryRequest to keep itself alive, sloppy design.
    // Note that JobQuery calls StartQuery that then calls JobQuery, yech!
    //
    QueryRequest::Ptr qr = QueryRequest::create(jobQuery);
    jobQuery->setQueryRequest(qr);

    // Start the query. The rest is magically done in the background.
    //
    // &&&uj sending mechanism needs to change to be like  AddReplicaQservMgtRequest::createHttpReqImp
    getXrdSsiService()->ProcessRequest(*(qr.get()), jobResource);
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::startQuery end");
    return true;
}

Executive::ChunkIdJobMapType Executive::unassignedChunksInQuery() {  // &&&
    lock_guard<mutex> lck(_chunkToJobMapMtx);

    ChunkIdJobMapType unassignedMap;
    for (auto const& [key, jobPtr] : _chunkToJobMap) {
        if (!jobPtr->isInUberJob()) {
            unassignedMap[key] = jobPtr;
        }
    }
    return unassignedMap;
}

void Executive::addUberJobs(std::vector<std::shared_ptr<UberJob>> const& uJobsToAdd) {  // &&&
    lock_guard<mutex> lck(_uberJobsMapMtx);
    for (auto const& uJob : uJobsToAdd) {
        UberJobId ujId = uJob->getJobId();
        _uberJobsMap[ujId] = uJob;
    }
}

string Executive::dumpUberJobCounts() const {
    stringstream os;
    os << "exec=" << getIdStr();
    int totalJobs = 0;
    {
        lock_guard<mutex> ujmLck(_uberJobsMapMtx);
        for (auto const& [ujKey, ujPtr] : _uberJobsMap) {
            int jobCount = ujPtr->getJobCount();
            totalJobs += jobCount;
            os << "{" << ujKey << ":" << ujPtr->getIdStr() << " jobCount=" << jobCount << "}";
        }
    }
    {
        lock_guard<recursive_mutex> jmLck(_jobMapMtx);
        os << " ujTotalJobs=" << totalJobs << " execJobs=" << _jobMap.size();
    }
    return os.str();
}

void Executive::assignJobsToUberJobs() {
    auto uqs = _userQuerySelect.lock();
    /// &&& NEED CODE put on qdisppool &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    uqs->buildAndSendUberJobs();
}

bool Executive::startUberJob(UberJob::Ptr const& uJob) {  // &&&

    lock_guard<recursive_mutex> lock(_cancelled.getMutex());

    // If this has been cancelled, then return false.
    //
    if (_cancelled) return false;

    // &&&uj NEED CODE to put call to runUberJob into the priority queue.

    return true;
}

JobQuery::Ptr Executive::getSharedPtrForRawJobPtr(JobQuery* jqRaw) {  //&&&
    assert(jqRaw != nullptr);
    int jobId = jqRaw->getJobId();
    lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
    auto iter = _jobMap.find(jobId);
    if (iter == _jobMap.end()) {
        throw util::Bug(ERR_LOC, "Could not find the entry for jobId=" + to_string(jobId));
    }
    JobQuery::Ptr jq = iter->second;
    return jq;
}

/// &&& doc
void Executive::addMultiError(int errorCode, std::string const& errorMsg, int errorState) {
    util::Error err(errorCode, errorMsg, errorState);
    {
        lock_guard<mutex> lock(_errorsMutex);
        _multiError.push_back(err);
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) + " multiError:" << _multiError.size() << ":" << _multiError);
    }
}

/// Add a JobQuery to this Executive.
/// Return true if it was successfully added to the map.
///
bool Executive::_addJobToMap(JobQuery::Ptr const& job) {
    auto entry = pair<int, JobQuery::Ptr>(job->getJobId(), job);
    lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
    bool res = _jobMap.insert(entry).second;
    _totalJobs = _jobMap.size();
    return res;
}

bool Executive::join() {
    // To join, we make sure that all of the chunks added so far are complete.
    // Check to see if _requesters is empty, if not, then sleep on a condition.
    _waitAllUntilEmpty();
    // Okay to merge. probably not the Executive's responsibility
    struct successF {
        static bool func(Executive::JobMap::value_type const& entry) {
            qmeta::JobStatus::Info const& esI = entry.second->getStatus()->getInfo();
            LOGS(_log, LOG_LVL_TRACE, "entry state:" << (void*)entry.second.get() << " " << esI);
            LOGS(_log, LOG_LVL_WARN, "&&& entry state:" << (void*)entry.second.get() << " " << esI);
            return (esI.state == qmeta::JobStatus::RESPONSE_DONE) ||
                   (esI.state == qmeta::JobStatus::COMPLETE);
        }
    };

    int sCount = 0;
    {
        lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
        sCount = count_if(_jobMap.begin(), _jobMap.end(), successF::func);
    }
    if (sCount == _requestCount) {
        LOGS(_log, LOG_LVL_INFO,
             "Query execution succeeded all: " << _requestCount << " jobs dispatched and completed.");
    } else if (isLimitRowComplete()) {
        LOGS(_log, LOG_LVL_INFO,
             "Query execution succeeded enough (LIMIT): " << sCount << " jobs out of " << _requestCount
                                                          << " completed.");
    } else {
        LOGS(_log, LOG_LVL_ERROR,
             "Query execution failed: " << _requestCount << " jobs dispatched, but only " << sCount
                                        << " jobs completed");
    }
    _empty = (sCount == _requestCount);
    LOGS(_log, LOG_LVL_DEBUG,
         "Flag set to _empty=" << _empty << ", sCount=" << sCount << ", requestCount=" << _requestCount);
    return _empty || isLimitRowComplete();
}

void Executive::markCompleted(JobId jobId, bool success) {
    ResponseHandler::Error err;
    string idStr = QueryIdHelper::makeIdStr(_id, jobId);
    LOGS(_log, LOG_LVL_DEBUG, "Executive::markCompleted " << success);
    if (!success && !isLimitRowComplete()) {
        {
            lock_guard<mutex> lock(_incompleteJobsMutex);
            auto iter = _incompleteJobs.find(jobId);
            if (iter != _incompleteJobs.end()) {
                auto jobQuery = iter->second;
                err = jobQuery->getDescription()->respHandler()->getError();
            } else {
                string msg = "Executive::markCompleted failed to find TRACKED " + idStr +
                             " size=" + to_string(_incompleteJobs.size());
                // If the user query has been cancelled, this is expected for jobs that have not yet
                // been tracked. Otherwise, this indicates a serious problem.
                if (!getCancelled()) {
                    LOGS(_log, LOG_LVL_WARN, msg << " " << _getIncompleteJobsString(-1));
                    throw util::Bug(ERR_LOC, msg);
                } else {
                    LOGS(_log, LOG_LVL_DEBUG, msg);
                }
                return;
            }
        }
        LOGS(_log, LOG_LVL_WARN,
             "Executive: error executing " << err << " (status: " << err.getStatus() << ")");
        {
            lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
            auto job = _jobMap[jobId];
            string id = job->getIdStr() + "<>" + idStr;
            /* &&&
            auto jState = job->getStatus()->getInfo().state;
            // Don't overwrite existing error states.
            if (jState != qmeta::JobStatus::CANCEL && jState != qmeta::JobStatus::RESPONSE_ERROR &&
                jState != qmeta::JobStatus::RESULT_ERROR && jState != qmeta::JobStatus::MERGE_ERROR) {
                job->getStatus()->updateInfo(id, qmeta::JobStatus::RESULT_ERROR, "EXECFAIL", err.getCode(),
                                             err.getMsg());
            }
            */
            // Don't overwrite existing error states.
            job->getStatus()->updateInfoNoErrorOverwrite(id, qmeta::JobStatus::RESULT_ERROR, "EXECFAIL",
                                                         err.getCode(), err.getMsg());
        }
        {
            lock_guard<mutex> lock(_errorsMutex);
            _multiError.push_back(err);
            LOGS(_log, LOG_LVL_TRACE,
                 "Currently " << _multiError.size() << " registered errors: " << _multiError);
        }
    }
    _unTrack(jobId);
    if (!success && !isLimitRowComplete()) {
        LOGS(_log, LOG_LVL_ERROR,
             "Executive: requesting squash, cause: " << " failed (code=" << err.getCode() << " "
                                                     << err.getMsg() << ")");
        squash();  // ask to squash
    }
}

void Executive::squash() {
    LOGS(_log, LOG_LVL_WARN, "&&&uj Executive::squash() qid=" << getId() << " str=" << getIdStr());
    bool alreadyCancelled = _cancelled.exchange(true);
    if (alreadyCancelled) {
        LOGS(_log, LOG_LVL_DEBUG, "Executive::squash() already cancelled! refusing. qid=" << getId());
        return;
    }

    LOGS(_log, LOG_LVL_INFO, "Executive::squash Trying to cancel all queries... qid=" << getId());
    deque<JobQuery::Ptr> jobsToCancel;
    {
        lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
        for (auto const& jobEntry : _jobMap) {
            jobsToCancel.push_back(jobEntry.second);
        }
    }

    for (auto const& job : jobsToCancel) {
        job->cancel();
    }
    LOGS(_log, LOG_LVL_DEBUG, "Executive::squash done");
}

void Executive::_squashSuperfluous() {
    if (_cancelled) {
        LOGS(_log, LOG_LVL_INFO, "squashSuperfluous() irrelevant as query already cancelled");
        return;
    }

    LOGS(_log, LOG_LVL_INFO, "Executive::squashSuperflous Trying to cancel incomplete jobs");
    deque<JobQuery::Ptr> jobsToCancel;
    {
        lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
        for (auto const& jobEntry : _jobMap) {
            JobQuery::Ptr jq = jobEntry.second;
            // It's important that none of the cancelled queries
            // try to remove their rows from the result.
            if (jq->getStatus()->getInfo().state != qmeta::JobStatus::COMPLETE &&
                jq->getStatus()->getInfo().state != qmeta::JobStatus::CANCEL) {
                jobsToCancel.push_back(jobEntry.second);
            }
        }
    }

    for (auto const& job : jobsToCancel) {
        job->cancel(true);
    }
    LOGS(_log, LOG_LVL_DEBUG, "Executive::squashSuperfluous done");
}

int Executive::getNumInflight() const {
    unique_lock<mutex> lock(_incompleteJobsMutex);
    return _incompleteJobs.size();
}

string Executive::getProgressDesc() const {
    ostringstream os;
    {
        lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
        auto first = true;
        for (auto entry : _jobMap) {
            JobQuery::Ptr job = entry.second;
            if (!first) {
                os << "\n";
            }
            first = false;
            os << "Ref=" << entry.first << " " << job;
        }
    }
    string msg_progress = os.str();
    LOGS(_log, LOG_LVL_ERROR, msg_progress);
    return msg_progress;
}

void Executive::_setup() {
    XrdSsiErrInfo eInfo;
    _empty.store(true);
    _requestCount = 0;
    // If unit testing, load the mock service.
    if (_config.serviceUrl.compare(_config.getMockStr()) == 0) {
        _xrdSsiService = new XrdSsiServiceMock(this);
    } else {
        static XrdSsiService* xrdSsiServiceStatic =
                XrdSsiProviderClient->GetService(eInfo, _config.serviceUrl);
        _xrdSsiService = xrdSsiServiceStatic;
    }
    if (!_xrdSsiService) {
        LOGS(_log, LOG_LVL_DEBUG,
             _id << " Error obtaining XrdSsiService in Executive: "
                    "serviceUrl="
                 << _config.serviceUrl << " " << getErrorText(eInfo));
    }
    assert(_xrdSsiService);
}

/** Add (jobId,r) entry to _requesters map if not here yet
 *  else leave _requesters untouched.
 *
 *  @param jobId id of the job related to current chunk query
 *  @param r pointer to job which will store chunk query result
 *
 *  @return true if (jobId,r) was added to _requesters
 *          false if this entry was previously in the map
 */
bool Executive::_track(int jobId, shared_ptr<JobQuery> const& r) {
    int size = -1;
    {
        lock_guard<mutex> lock(_incompleteJobsMutex);
        if (_incompleteJobs.find(jobId) != _incompleteJobs.end()) {
            LOGS(_log, LOG_LVL_WARN,
                 "Attempt for TRACKING " << " failed as jobId already found in incomplete jobs. "
                                         << _getIncompleteJobsString(-1));
            return false;
        }
        _incompleteJobs[jobId] = r;
        size = _incompleteJobs.size();
        qdisp::CzarStats::get()->addJob();
    }
    LOGS(_log, LOG_LVL_DEBUG, "Success TRACKING size=" << size);
    return true;
}

void Executive::_unTrack(int jobId) {
    bool untracked = false;
    int incompleteJobs = _totalJobs;
    string s;
    bool logSome = false;
    {
        lock_guard<mutex> lock(_incompleteJobsMutex);
        auto i = _incompleteJobs.find(jobId);
        if (i != _incompleteJobs.end()) {
            _incompleteJobs.erase(i);
            untracked = true;
            incompleteJobs = _incompleteJobs.size();
            if (_incompleteJobs.empty()) _allJobsComplete.notify_all();
            qdisp::CzarStats::get()->deleteJobs(1);
        }
        auto sz = _incompleteJobs.size();
        logSome = (sz < 50) || (sz % 1000 == 0) || !untracked;
        if (logSome || LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
            // Log up to 5 incomplete jobs. Very useful when jobs do not finish.
            s = _getIncompleteJobsString(5);
        }
    }
    bool logDebug = untracked || isLimitRowComplete();
    LOGS(_log, (logDebug ? LOG_LVL_DEBUG : LOG_LVL_WARN),
         "Executive UNTRACKING " << (untracked ? "success" : "failed") << "::" << s);
    // Every time a chunk completes, consider sending an update to QMeta.
    // Important chunks to log: first, last, middle
    // limiting factors: no more than one update a minute (config)
    if (untracked) {
        auto now = chrono::system_clock::now();
        unique_lock<mutex> lastUpdateLock(_lastQMetaMtx);
        if (now - _lastQMetaUpdate > _secondsBetweenQMetaUpdates || incompleteJobs == _totalJobs / 2 ||
            incompleteJobs == 0) {
            _lastQMetaUpdate = now;
            lastUpdateLock.unlock();  // unlock asap, _qMeta write can be slow.
            int completedJobs = _totalJobs - incompleteJobs;
            if (_qMeta != nullptr) {
                // This is not vital (logging), if it fails keep going.
                try {
                    _qMeta->queryStatsTmpChunkUpdate(_id, completedJobs);
                } catch (qmeta::SqlError const& e) {
                    LOGS(_log, LOG_LVL_WARN, "Failed to update StatsTmp " << e.what());
                }
            }
        }
    }
}

/// _incompleteJobsMutex must be held before calling this function.
/// @return: a string containing a list of incomplete jobs containing up to 'maxToList' jobs.
///          If maxToList is less than 0, all jobs are printed
string Executive::_getIncompleteJobsString(int maxToList) {
    ostringstream os;
    int c = 0;
    if (maxToList < 0) maxToList = _incompleteJobs.size();
    os << "_incompleteJobs listing first" << maxToList << " of (size=" << _incompleteJobs.size() << ") ";
    for (auto j = _incompleteJobs.begin(), e = _incompleteJobs.end(); j != e && c < maxToList; ++j, ++c) {
        os << j->first << " ";
    }
    return os.str();
}

void Executive::updateProxyMessages() {
    {
        // Add all messages to the message store. These will
        // be used to populate qservMeta.QMessages for this query.
        lock_guard<recursive_mutex> lockJobMap(_jobMapMtx);
        for (auto const& entry : _jobMap) {
            JobQuery::Ptr const& job = entry.second;
            auto const& info = job->getStatus()->getInfo();
            ostringstream os;
            os << info.state << " " << info.stateCode;
            if (!info.stateDesc.empty()) {
                os << " (" << info.stateDesc << ")";
            }
            os << " " << info.timeStr();
            _messageStore->addMessage(job->getDescription()->resource().chunk(), info.source, info.state,
                                      os.str(), info.severity, info.stateTime);
        }
    }
    {
        lock_guard<mutex> lock(_errorsMutex);
        // If there were any errors, combine them into one string and add that to
        // the _messageStore. This will be passed to the proxy for the user, if
        // there's an error.
        if (not _multiError.empty()) {
            _messageStore->addErrorMessage("MULTIERROR", _multiError.toString());
            LOGS(_log, LOG_LVL_INFO, "MULTIERROR:" << _multiError.toString());
        }
    }
}

/// This function blocks until it has reaped all the requesters.
/// Typically the requesters are handled by markCompleted().
/// _reapRequesters() deals with cases that involve errors.
void Executive::_waitAllUntilEmpty() {
    unique_lock<mutex> lock(_incompleteJobsMutex);
    int lastCount = -1;
    int count;
    int moreDetailThreshold = 10;
    int complainCount = 0;
    const chrono::seconds statePrintDelay(5);
    while (!_incompleteJobs.empty()) {
        count = _incompleteJobs.size();
        if (count != lastCount) {
            lastCount = count;
            ++complainCount;
            if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
                ostringstream os;
                if (complainCount > moreDetailThreshold) {
                    _printState(os);
                    os << "\n";
                }
                os << "Still " << count << " in flight.";
                complainCount = 0;
                lock.unlock();  // release the lock while we trigger logging.
                LOGS(_log, LOG_LVL_INFO, os.str());
                lock.lock();
            }
        }
        _allJobsComplete.wait_for(lock, statePrintDelay);
    }
}

void Executive::_addToChunkJobMap(JobQuery::Ptr const& job) {
    int chunkId = job->getDescription()->resource().chunk();
    auto entry = pair<ChunkIdType, JobQuery::Ptr>(chunkId, job);
    // LOGS(_log, LOG_LVL_WARN, "&&& _addToChunkJobMap chunkId=" << chunkId); &&&
    lock_guard<mutex> lck(_chunkToJobMapMtx);
    if (_chunkToJobMapInvalid) {
        throw util::Bug(ERR_LOC, "&&& map insert FAILED, map is already invalid");
    }
    bool inserted = _chunkToJobMap.insert(entry).second;
    if (!inserted) {
        throw util::Bug(ERR_LOC, "&&& map insert FAILED ChunkId=" + to_string(chunkId) + " already existed");
    }
}

void Executive::_setupLimit() {
    // Figure out the limit situation.
    auto qSession = _querySession.lock();
    // if qSession is nullptr, this is probably a unit test.
    if (qSession == nullptr) return;
    auto const& selectStatement = qSession->getStmt();
    bool allChunksRequired = qSession->dbgGetContext()->allChunksRequired;
    bool groupBy = selectStatement.hasGroupBy();
    bool orderBy = selectStatement.hasOrderBy();
    bool hasLimit = selectStatement.hasLimit();
    if (hasLimit) {
        _limit = selectStatement.getLimit();
        if (_limit <= 0) hasLimit = false;
    }
    _limitSquashApplies = hasLimit && !(groupBy || orderBy || allChunksRequired);
}

void Executive::addResultRows(int64_t rowCount) { _totalResultRows += rowCount; }

void Executive::checkLimitRowComplete() {
    if (!_limitSquashApplies) return;
    if (_totalResultRows < _limit) return;
    bool previousVal = _setLimitRowComplete();
    if (previousVal) {
        // already squashing etc, just return
        return;
    }
    // Set flags so queries can be squashed without canceling the entire query.
    // To explain WARN messages in the log related to this action, this
    // message is LOG_LVL_WARN.
    LOGS(_log, LOG_LVL_WARN, "LIMIT query has enough rows, canceling superfluous jobs.");
    _squashSuperfluous();
}

ostream& operator<<(ostream& os, Executive::JobMap::value_type const& v) {
    auto const& status = v.second->getStatus();
    os << v.first << ": " << *status;
    return os;
}

/// precondition: _incompleteJobsMutex is held by current thread.
void Executive::_printState(ostream& os) {
    for (auto const& entry : _incompleteJobs) {
        JobQuery::Ptr job = entry.second;
        os << *job << "\n";
    }
}

#if 0   //&&&
void Executive::buildAndSendUberJobs(int const maxChunksPerUber) {
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs a");
    vector<qdisp::UberJob::Ptr> uberJobs;

    auto czarPtr = czar::Czar::getCzar();
    auto czChunkMap = czarPtr->getCzarChunkMap();
    auto czRegistry = czarPtr->getCzarRegistry();

    LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs b");
    auto const [chunkMapPtr, workerChunkMapPtr] = czChunkMap->getMaps();  //&&&uj

    // Make a map of all jobs in the executive.
    // &&& TODO:UJ for now, just using ints. At some point, need to check that ResourceUnit databases can
    // be found for all databases in the query
    /* &&& instead of destroying the only copy, going to make a copy that only contains
     * unassigned jobs.
    qdisp::Executive::ChunkIdJobMapType chunksInQuery = _executive->getChunkJobMapAndInvalidate();
            if (_chunkToJobMapInvalid.exchange(true)) {
            throw util::Bug(ERR_LOC, "getChunkJobMapInvalidate called when map already invalid");
        }
        return _chunkToJobMap;

    */
    ChunkIdJobMapType unassignedChunksInQuery;
    {
        lock_guard<mutex> lckChunkJobMap(_chunkToJobMapMtx);
        for(auto const& [chunkId, jobPtr] : _chunkToJobMap) {
            if (!jobPtr->isInUberJob()) {
                unassignedChunksInQuery[chunkId] = jobPtr;
            }
        }
    }
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs c");

    // &&& TODO:UJ skipping in proof of concept: get a list of all databases in jobs (all jobs should use
    // the same databases) Use this to check for conflicts

    /* &&&
    // assign jobs to uberJobs
    int maxChunksPerUber = 3;  // &&&uj maybe put in config??? or set on command line??
                               // &&&uj Different queries may benefit from different values
                               // &&&uj Such as LIMIT=1 may work best with this at 1, where
                               // &&&uj 100 would be better for others.
     */
    // keep cycling through workers until no more chunks to place.

    // TODO:UJ &&&uj Once everything is an UberJob, it can start with 1 or 0.
    //int _uberJobId = qdisp::UberJob::getFirstIdNumber();

    // &&&uj
    //  - create a map of UberJobs  key=<workerId>, val=<vector<uberjob::ptr>>
    //  - for chunkId in `unassignedChunksInQuery`
    //     - use `chunkMapPtr` to find the shared scan workerId for chunkId
    //     - if not existing in the map, make a new uberjob
    //     - if existing uberjob at max jobs, create a new uberjob
    //  - once all chunks in the query have been put in uberjobs, find contact info
    //    for each worker
    //      - add worker to each uberjob.
    //  - For failures - If a worker cannot be contacted, that's an uberjob failure.
    //      - uberjob failures (due to communications problems) will result in the uberjob
    //        being broken up into multiple UberJobs going to different workers.
    //        - The best way to do this is probably to just kill the UberJob and mark all
    //          Jobs that were in that UberJob as needing re-assignment, and re-running
    //          the code here. The trick is going to be figuring out which workers are alive.
    //          Maybe force a fresh lookup from the replicator Registry when an UberJob fails.
    map<string, vector<qdisp::UberJob::Ptr>> workerJobMap;
    vector<qdisp::Executive::ChunkIdType> missingChunks;

    // unassignedChunksInQuery needs to be in numerical order so that UberJobs contain chunk numbers in
    // numerical order. The workers run shared scans in numerical order of chunk id numbers.
    // This keeps the number of partially complete UberJobs running on a worker to a minimum,
    // and should minimize the time for the first UberJob on the worker to complete.
    LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs d");
    for (auto const& [chunkId, jqPtr] : unassignedChunksInQuery) {
        LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs d1");
        auto iter = chunkMapPtr->find(chunkId);
        if (iter == chunkMapPtr->end()) {
            LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs d1a");
            missingChunks.push_back(chunkId);
            break;
        }
        LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs d2");;
        czar::CzarChunkMap::ChunkData::Ptr chunkData = iter->second;
        auto targetWorker = chunkData->getPrimaryScanWorker().lock();
        LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs d3");
        if (targetWorker == nullptr) {  //&&&uj  if (targetWorker == nullptr || this worker already tried for this chunk) {
            LOGS(_log, LOG_LVL_ERROR, "No primary scan worker for chunk=" << chunkData->dump());
            // Try to assign a different worker to this job
            auto workerHasThisChunkMap = chunkData->getWorkerHasThisMapCopy();
            bool found = false;
            for (auto wIter = workerHasThisChunkMap.begin();
                 wIter != workerHasThisChunkMap.end() && !found; ++wIter) {
                auto maybeTarg = wIter->second.lock();
                if (maybeTarg != nullptr) {
                    targetWorker = maybeTarg;
                    found = true;
                    LOGS(_log, LOG_LVL_WARN, "Alternate worker found for chunk=" << chunkData->dump());
                }
            }
            if (!found) {
                // &&&uj If enough workers are down, there will be a chunk that cannot be found.
                //       the correct course of action is probably to check the Registry, and
                //       after so many attempts, cancel the user query with a
                //       "chunk(s)[list of missing chunks]" error. Perhaps, the attemptCount
                //       in the Job or JobDescription could be used for this.
                LOGS(_log, LOG_LVL_ERROR,
                     "No primary or alternate worker found for chunk=" << chunkData->dump());
                throw util::Bug(ERR_LOC, string("No primary or alternate worker found for chunk.") +
                                                 " Crashing the program here for this reason is not "
                                                 "appropriate. &&& NEEDS CODE");
            }
        }
        // Add this job to the appropriate UberJob, making the UberJob if needed.
        LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs d4");
        string workerId = targetWorker->getWorkerId();
        auto& ujVect = workerJobMap[workerId];
        if (ujVect.empty() || ujVect.back()->getJobCount() > maxChunksPerUber) {
            LOGS(_log, LOG_LVL_WARN, "&&& Executive::buildandSendUberJobs d4a");
            auto ujId = _uberJobId++; // keep ujId consistent
            string uberResultName = _ttn->make(ujId);
            auto respHandler = make_shared<ccontrol::MergingHandler>(_infileMerger, uberResultName);
            auto thisExec = shared_from_this();
            auto uJob = qdisp::UberJob::create(thisExec, respHandler, getId(), ujId,
                                               _qMetaCzarId, targetWorker);
            ujVect.push_back(uJob);
        }
        LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew e1c6");
        ujVect.back()->addJob(jqPtr);
    }

    LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew e1c7");
    if (!missingChunks.empty()) {
        LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew e1c8");
        string errStr = string(__func__) + " a worker could not be found for these chunks ";
        for (auto const& chk : missingChunks) {
            errStr += to_string(chk) + ",";
        }
        LOGS(_log, LOG_LVL_ERROR, errStr);
        throw util::Bug(
                ERR_LOC,
                errStr + " Crashing the program here for this reason is not appropriate. &&& NEEDS CODE");
    }
    LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew e end");

    //&&&uj
    // Add worker contact info to UberJobs.
    auto const wContactMap = czRegistry->getWorkerContactMap();
    LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew f");
    for (auto const& [wIdKey, ujVect] : workerJobMap) {
        LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew f1");
        auto iter = wContactMap->find(wIdKey);
        if (iter == wContactMap->end()) {
            // &&&uj Not appropriate to throw for this. Need to re-direct all jobs to different workers.
            throw util::Bug(ERR_LOC, string(" &&&uj NEED CODE, no contact information for ") + wIdKey);
        }
        auto const& wContactInfo = iter->second;
        for (auto const& ujPtr : ujVect) {
            LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew f2");
            ujPtr->setWorkerContactInfo(wContactInfo);
        }
        LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew f3");
        _executive->addUberJobs(ujVect);
        LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew f4");
        for (auto const& ujPtr : ujVect) {
            LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew f5");
            //&&&uj IMPORTANT
            //&&&uj This just sends the test message to the worker, but the
            //      worker only parses it and sends a message back. The
            //      worker does not create and run tasks at this point.
            //      The call to runUberJob here should be replaced by a call
            //      to startUberJob that puts the call to runUberJob into
            //      the a priority queue command.
            _executive->runUberJob(ujPtr);
        }
    }

    LOGS(_log, LOG_LVL_WARN, "&&& UserQuerySelect::submitNew g");  //&&&uj

    LOGS(_log, LOG_LVL_DEBUG, "total jobs in query=" << sequence);
    _executive->waitForAllJobsToStart();

    // we only care about per-chunk info for ASYNC queries
    if (_async) {
        std::lock_guard<std::mutex> lock(chunksMtx);
        _qMetaAddChunks(chunks);
    }
}
#endif  //&&&

}  // namespace lsst::qserv::qdisp
