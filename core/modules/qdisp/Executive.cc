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
#include "ccontrol/msgCode.h"
#include "global/Bug.h"
#include "global/ResourceUnit.h"
#include "qdisp/JobQuery.h"
#include "qdisp/MessageStore.h"
#include "qdisp/QueryRequest.h"
#include "qdisp/ResponseHandler.h"
#include "qdisp/XrdSsiMocks.h"
#include "qmeta/Exceptions.h"
#include "qmeta/QStatus.h"
#include "util/EventThread.h"

extern XrdSsiProvider *XrdSsiProviderClient;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.Executive");

std::string getErrorText(XrdSsiErrInfo & e) {
    std::ostringstream os;
    int errCode;
    os << "XrdSsiError " << e.Get(errCode);
    os <<  " Code=" << errCode;
    return os.str();
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qdisp {

////////////////////////////////////////////////////////////////////////
// class Executive implementation
////////////////////////////////////////////////////////////////////////
Executive::Executive(Config const& c, std::shared_ptr<MessageStore> const& ms,
                     std::shared_ptr<QdispPool> const& qdispPool,
                     std::shared_ptr<qmeta::QStatus> const& qStatus)
    : _config(c), _messageStore(ms), _qdispPool(qdispPool), _qMeta(qStatus) {
    _secondsBetweenQMetaUpdates = std::chrono::seconds(_config.secondsBetweenChunkUpdates);
    _setup();
}


Executive::~Executive() {
    // Real XrdSsiService objects are unowned, but mocks are allocated in _setup.
    delete dynamic_cast<XrdSsiServiceMock *>(_xrdSsiService);
}


Executive::Ptr Executive::create(Config const& c, std::shared_ptr<MessageStore> const& ms,
                                 std::shared_ptr<QdispPool> const& qdispPool,
                                 std::shared_ptr<qmeta::QStatus> const& qMeta) {
    Executive::Ptr exec{new Executive(c, ms, qdispPool, qMeta)}; // make_shared dislikes private constructor.
    return exec;
}


void Executive::setQueryId(QueryId id) {
    _id = id;
    _idStr = QueryIdHelper::makeIdStr(_id);
}


/// Add a new job to executive queue, if not already in. Not thread-safe.
///
JobQuery::Ptr Executive::add(JobDescription::Ptr const& jobDesc) {
    auto timeDiff = [](std::chrono::time_point<std::chrono::system_clock> const& begin, // TEMPORARY-timing
            std::chrono::time_point<std::chrono::system_clock> const& end) -> int {
        auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
        return diff.count();
    };
    auto startQSEA = std::chrono::system_clock::now(); // TEMPORARY-timing
    std::chrono::time_point<std::chrono::system_clock> cancelLockQSEA, jobQueryQSEA,
                                                       addJobQSEA, trackQSEA; // TEMPORARY-timing
    JobQuery::Ptr jobQuery;
    {
        // Create the JobQuery and put it in the map.
        JobStatus::Ptr jobStatus = std::make_shared<JobStatus>();
        Ptr thisPtr = shared_from_this();
        MarkCompleteFunc::Ptr mcf = std::make_shared<MarkCompleteFunc>(thisPtr, jobDesc->id());
        jobQuery = JobQuery::create(thisPtr, jobDesc, jobStatus, mcf, _id);
        jobQueryQSEA = std::chrono::system_clock::now(); // TEMPORARY-timing

        {
            std::lock_guard<std::recursive_mutex> lock(_cancelled.getMutex());
            cancelLockQSEA = std::chrono::system_clock::now(); // // TEMPORARY-timing
            if (_cancelled) {
                LOGS(_log, LOG_LVL_DEBUG, "Executive already cancelled, ignoring add("
                        << jobDesc->id() << ")");
                return nullptr;
            }

            if (!_addJobToMap(jobQuery)) {
                LOGS(_log, LOG_LVL_ERROR, "Executive ignoring duplicate job add " << jobQuery->getIdStr());
                return jobQuery;
            }
            addJobQSEA = std::chrono::system_clock::now(); // TEMPORARY-timing

            if (!_track(jobQuery->getIdInt(), jobQuery)) {
                LOGS(_log, LOG_LVL_ERROR, "Executive ignoring duplicate track add" << jobQuery->getIdStr());
                return jobQuery;
            }
        }
        trackQSEA = std::chrono::system_clock::now(); // TEMPORARY-timing

        if (_empty.exchange(false)) {
            LOGS(_log, LOG_LVL_DEBUG, "Flag _empty set to false by " << jobQuery->getIdStr());
        }
        ++_requestCount;
    }
    std::string msg = "Executive::add " + jobQuery->getIdStr() + " with path=" + jobDesc->resource().path();
    LOGS(_log, LOG_LVL_DEBUG, msg);
    //_messageStore->addMessage(jobDesc.resource().chunk(), ccontrol::MSG_MGR_ADD, msg); TODO: maybe relocate.
    auto endQSEA = std::chrono::system_clock::now(); // TEMPORARY-timing
    { // TEMPORARY-timing
        std::lock_guard<std::mutex> sumLock(sumMtx);
        cancelLockQSEASum += timeDiff(startQSEA, cancelLockQSEA);
        jobQueryQSEASum += timeDiff(cancelLockQSEA, jobQueryQSEA);
        addJobQSEASum += timeDiff(jobQueryQSEA, addJobQSEA);
        trackQSEASum += timeDiff(addJobQSEA, trackQSEA);
        endQSEASum += timeDiff(trackQSEA, endQSEA);
    }
    //_queueJobStart(jobQuery);
    jobQuery->runJob();
    return jobQuery;
}


void Executive::queueJobStart(PriorityCommand::Ptr const& cmd) {
    _jobStartCmdList.push_back(cmd);
    if (_scanInteractive) {
        _qdispPool->queCmd(cmd, 0);
    } else {
        _qdispPool->queCmd(cmd, 1);
    }
}


void Executive::waitForAllJobsToStart() {
    LOGS(_log, LOG_LVL_INFO, _idStr << " waitForAllJobsToStart");
    // Wait for each command to start.
    while (true) {
        bool empty = _jobStartCmdList.empty();
        if (empty) break;
        auto cmd = std::move(_jobStartCmdList.front());
        _jobStartCmdList.pop_front();
        cmd->waitComplete();
    }
    LOGS(_log, LOG_LVL_INFO, _idStr << " waitForAllJobsToStart done");
}



// If the executive has not been cancelled, then we simply start the query.
// @return true if query was actually started (i.e. we were not cancelled)
//
bool Executive::startQuery(std::shared_ptr<JobQuery> const& jobQuery) {

    std::lock_guard<std::recursive_mutex> lock(_cancelled.getMutex());

    // If we have been cancelled, then return false.
    //
    if (_cancelled) return false;

    // Construct a temporary resource object to pass to ProcessRequest().
    //   Interactive Queries should have an Affinity of XrdSsiResource::None or Weak while
    //   Scans should have an affinity of Strong
    XrdSsiResource::Affinity affinity = (_scanInteractive) ? XrdSsiResource::Weak : XrdSsiResource::Strong;
    XrdSsiResource jobResource(jobQuery->getDescription()->resource().path(), "", jobQuery->getIdStr(),
                               "", 0, affinity);

    // Now construct the actual query request and tie it to the jobQuery. The
    // shared pointer is used by QueryRequest to keep itself alive, sloppy design.
    // Note that JobQuery calls StartQuery that then calls JobQuery, yech!
    //
    QueryRequest::Ptr qr = QueryRequest::create(jobQuery);
    jobQuery->setQueryRequest(qr);

    // Start the query. The rest is magically done in the background.
    //
    getXrdSsiService()->ProcessRequest(*(qr.get()), jobResource);
    return true;
}


/// Add a JobQuery to this Executive.
/// Return true if it was successfully added to the map.
///
bool Executive::_addJobToMap(JobQuery::Ptr const& job) {
    auto entry = std::pair<int, JobQuery::Ptr>(job->getIdInt(), job);
    std::lock_guard<std::recursive_mutex> lockJobMap(_jobMapMtx);
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
        static bool f(Executive::JobMap::value_type const& entry) {
            JobStatus::Info const& esI = entry.second->getStatus()->getInfo();
            LOGS(_log, LOG_LVL_DEBUG, "entry state:" << (void*)entry.second.get() << " " << esI);
            return (esI.state == JobStatus::RESPONSE_DONE) || (esI.state == JobStatus::COMPLETE);
        }
    };

    int sCount = 0;
    {
        std::lock_guard<std::recursive_mutex> lockJobMap(_jobMapMtx);
        sCount = std::count_if(_jobMap.begin(), _jobMap.end(), successF::f);
    }
    if (sCount == _requestCount) {
        LOGS(_log, LOG_LVL_DEBUG, "Query execution succeeded: " << _requestCount
             << " jobs dispatched and completed.");
    } else {
        LOGS(_log, LOG_LVL_ERROR, "Query execution failed: " << _requestCount
             << " jobs dispatched, but only " << sCount << " jobs completed");
    }
    _updateProxyMessages();
    bool empty = (sCount == _requestCount);
    _empty.store(empty);
    LOGS(_log, LOG_LVL_DEBUG, "Flag set to _empty=" << empty << ", sCount=" << sCount
         << ", requestCount=" << _requestCount);
    return empty;
}

void Executive::markCompleted(int jobId, bool success) {
    ResponseHandler::Error err;
    std::string idStr = QueryIdHelper::makeIdStr(_id, jobId);
    LOGS(_log, LOG_LVL_DEBUG, "Executive::markCompleted " << idStr
            << " " << success);
    if (!success) {
        {
            std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
            auto iter = _incompleteJobs.find(jobId);
            if (iter != _incompleteJobs.end()) {
                auto jobQuery =  iter->second;
                err = jobQuery->getDescription()->respHandler()->getError();
            } else {
                std::string msg = "Executive::markCompleted failed to find TRACKED " + idStr +
                        " size=" + std::to_string(_incompleteJobs.size());
                // If the user query has been cancelled, this is expected for jobs that have not yet
                // been tracked. Otherwise, this indicates a serious problem.
                if (!getCancelled()) {
                    LOGS(_log, LOG_LVL_WARN, msg << " " << _getIncompleteJobsString(-1));
                    throw Bug(msg);
                } else {
                    LOGS(_log, LOG_LVL_DEBUG, msg);
                }
                return;
            }
        }
        LOGS(_log, LOG_LVL_WARN, "Executive: error executing " << idStr
             << " " << err << " (status: " << err.getStatus() << ")");
        {
            std::lock_guard<std::recursive_mutex> lockJobMap(_jobMapMtx);
            auto job = _jobMap[jobId];
            std::string id = job->getIdStr() + "<>" + idStr;
            job->getStatus()->updateInfo(id, JobStatus::RESULT_ERROR, err.getCode(), err.getMsg());
        }
        {
            std::lock_guard<std::mutex> lock(_errorsMutex);
            _multiError.push_back(err);
            LOGS(_log, LOG_LVL_TRACE, "Currently " << _multiError.size()
                 << " registered errors: " << _multiError);
        }
    }
    _unTrack(jobId);
    if (!success) {
        LOGS(_log, LOG_LVL_ERROR, "Executive: requesting squash, cause: "
             << idStr << " failed (code=" << err.getCode() << " " << err.getMsg() << ")");
        squash(); // ask to squash
    }
}


void Executive::squash() {
    bool alreadyCancelled = _cancelled.exchange(true);
    if (alreadyCancelled) {
        LOGS(_log, LOG_LVL_DEBUG, getIdStr() << " Executive::squash() already cancelled! refusing.");
        return;
    }

    LOGS(_log, LOG_LVL_DEBUG, getIdStr() << " Executive::squash Trying to cancel all queries...");
    std::deque<JobQuery::Ptr> jobsToCancel;
    {
        std::lock_guard<std::recursive_mutex> lockJobMap(_jobMapMtx);
        for(auto const& jobEntry : _jobMap) {
            jobsToCancel.push_back(jobEntry.second);
        }
    }

    for (auto const& job : jobsToCancel) {
            job->cancel();
    }
    LOGS_DEBUG(getIdStr() << " Executive::squash done");
}

int Executive::getNumInflight() {
    std::unique_lock<std::mutex> lock(_incompleteJobsMutex);
    return _incompleteJobs.size();
}

std::string Executive::getProgressDesc() const {
    std::ostringstream os;
    {
        std::lock_guard<std::recursive_mutex> lockJobMap(_jobMapMtx);
        auto first = true;
        for (auto entry : _jobMap) {
            JobQuery::Ptr job = entry.second;
            if (!first) { os << "\n"; }
            first = false;
            os << "Ref=" << entry.first << " " << job;

        }
    }
    std::string msg_progress = os.str();
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
        _xrdSsiService = XrdSsiProviderClient->GetService(eInfo, _config.serviceUrl.c_str()); // Step 1
    }
    if (!_xrdSsiService) {
        LOGS(_log, LOG_LVL_DEBUG, _id << " Error obtaining XrdSsiService in Executive: "
             <<  getErrorText(eInfo));
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
bool Executive::_track(int jobId, std::shared_ptr<JobQuery> const& r) {
    std::string idStr = QueryIdHelper::makeIdStr(_id, jobId);
    int size = -1;
    {
        std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
        if (_incompleteJobs.find(jobId) != _incompleteJobs.end()) {
            LOGS(_log, LOG_LVL_WARN, "Attempt for TRACKING " << idStr
                 << " failed as jobId already found in incomplete jobs. "
                 << _getIncompleteJobsString(-1));
            return false;
        }
        _incompleteJobs[jobId] = r;
        size = _incompleteJobs.size();
    }
    LOGS(_log, LOG_LVL_DEBUG, "Success TRACKING " << idStr << " size=" << size);
    return true;
}

void Executive::_unTrack(int jobId) {
    bool untracked = false;
    int incompleteJobs = _totalJobs;
    std::string s;
    {
        std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
        auto i = _incompleteJobs.find(jobId);
        if (i != _incompleteJobs.end()) {
            _incompleteJobs.erase(i);
            untracked = true;
            incompleteJobs = _incompleteJobs.size();
            if (_incompleteJobs.empty()) _allJobsComplete.notify_all();
        }
        if (!untracked || LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
            // Log up to 5 incomplete jobs. Very useful when jobs do not finish.
            s = _getIncompleteJobsString(5);
        }
    }
    LOGS(_log, (untracked ? LOG_LVL_DEBUG : LOG_LVL_WARN),
         "Executive UNTRACKING " << QueryIdHelper::makeIdStr(_id, jobId)
             << " " << (untracked ? "success":"failed") << "::" << s);
    // Every time a chunk completes, consider sending an update to QMeta.
    // Important chunks to log: first, last, middle
    // limiting factors: no more than one update a minute (config)
    if (untracked) {
        auto now = std::chrono::system_clock::now();
        std::unique_lock<std::mutex> lastUpdateLock(_lastQMetaMtx);
        if (now - _lastQMetaUpdate > _secondsBetweenQMetaUpdates
           || incompleteJobs == _totalJobs/2
           || incompleteJobs == 0) {
            _lastQMetaUpdate = now;
            lastUpdateLock.unlock(); // unlock asap, _qMeta write can be slow.
            int completedJobs = _totalJobs -  incompleteJobs;
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
std::string Executive::_getIncompleteJobsString(int maxToList) {
    std::ostringstream os;
    int c = 0;
    if (maxToList < 0) maxToList = _incompleteJobs.size();
    os << "_incompleteJobs listing first" << maxToList << " of size=" << _incompleteJobs.size() << " ";
    for(auto j = _incompleteJobs.begin(), e = _incompleteJobs.end(); j != e && c < maxToList; ++j, ++c) {
        os << j->first << " ";
    }
    return os.str();
}


/** Store job status and execution errors in the current user query message store
 *
 * messageStore will be inserted in message table at the end of czar code
 * and is used to log/report error in mysql-proxy.
 *
 * @see python module lsst.qserv.czar.proxy.unlock()
 */
void Executive::_updateProxyMessages() {
    {
        std::lock_guard<std::recursive_mutex> lockJobMap(_jobMapMtx);
        for (auto const& entry : _jobMap) {
            JobQuery::Ptr const& job = entry.second;
            auto const& info = job->getStatus()->getInfo();
            std::ostringstream os;
            os << info.state << " " << info.stateCode;
            if (!info.stateDesc.empty()) {
                os << " (" << info.stateDesc << ")";
            }
            os << " " << info.stateTime;
            _messageStore->addMessage(job->getDescription()->resource().chunk(),
                    info.state, os.str());

        }
    }
    {
        std::lock_guard<std::mutex> lock(_errorsMutex);
        if (not _multiError.empty()) {
            _messageStore->addErrorMessage(_multiError.toString());
        }
    }
}

/// This function blocks until it has reaped all the requesters.
/// Typically the requesters are handled by markCompleted().
/// _reapRequesters() deals with cases that involve errors.
void Executive::_waitAllUntilEmpty() {
    std::unique_lock<std::mutex> lock(_incompleteJobsMutex);
    int lastCount = -1;
    int count;
    int moreDetailThreshold = 5;
    int complainCount = 0;
    const std::chrono::seconds statePrintDelay(5);
    while(!_incompleteJobs.empty()) {
        count = _incompleteJobs.size();
        if (count != lastCount) {
            lastCount = count;
            ++complainCount;
            if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
                std::ostringstream os;
                if (complainCount > moreDetailThreshold) {
                    _printState(os);
                    os << "\n";
                }
                os << _idStr << " Still " << count << " in flight.";
                complainCount = 0;
                lock.unlock(); // release the lock while we trigger logging.
                LOGS(_log, LOG_LVL_DEBUG, os.str());
                lock.lock();
            }
        }
        _allJobsComplete.wait_for(lock, statePrintDelay);
    }
}

std::ostream& operator<<(std::ostream& os, Executive::JobMap::value_type const& v) {
    JobStatus::Ptr status = v.second->getStatus();
    os << v.first << ": " << *status;
    return os;
}

/// precondition: _requestersMutex is held by current thread.
void Executive::_printState(std::ostream& os) {
    for (auto const& entry : _incompleteJobs) {
        JobQuery::Ptr job = entry.second;
        os << *job << "\n";
    }
}


}}} // namespace lsst::qserv::qdisp
