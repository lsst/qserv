// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
#include "XrdSsi/XrdSsiService.hh" // Resource

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/msgCode.h"
#include "global/Bug.h"
#include "global/ResourceUnit.h"
#include "qdisp/JobQuery.h"
#include "qdisp/MessageStore.h"
#include "qdisp/QueryResource.h"
#include "qdisp/ResponseHandler.h"
#include "qdisp/XrdSsiMocks.h"

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

std::atomic<int> clExecutiveInstCount{0}; // &&&

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qdisp {

////////////////////////////////////////////////////////////////////////
// class Executive implementation
////////////////////////////////////////////////////////////////////////
Executive::Executive(Config::Ptr c, std::shared_ptr<MessageStore> ms)
    : _config{*c}, _messageStore{ms} {
    _setup();
    LOGS(_log,LOG_LVL_DEBUG, "&&& clExecutiveInstCount=" << ++clExecutiveInstCount);
}

Executive::~Executive() {
    // Real XrdSsiService objects are unowned, but mocks are allocated in _setup.
    delete dynamic_cast<XrdSsiServiceMock *>(_xrdSsiService);
    LOGS(_log,LOG_LVL_DEBUG, "~&&& clExecutiveInstCount=" << --clExecutiveInstCount);
}


void Executive::setQueryId(qmeta::QueryId id) {
    _id = id;
    _idStr = qmeta::QueryIdHelper::makeIdStr(_id);
}


/// Add a new job to executive queue, if not already in. Not thread-safe.
///
void Executive::add(JobDescription const& jobDesc) {
    LOGS(_log, LOG_LVL_DEBUG, "Executive::add(" << jobDesc << ")");
    if (_cancelled) {
        LOGS(_log, LOG_LVL_DEBUG, "Executive already cancelled, ignoring add("
             << jobDesc.id() << ")");
        return;
    }
    // Create the JobQuery and put it in the map.
    JobStatus::Ptr jobStatus = std::make_shared<JobStatus>();
    MarkCompleteFunc::Ptr mcf = std::make_shared<MarkCompleteFunc>(this, jobDesc.id());
    JobQuery::Ptr jobQuery = JobQuery::newJobQuery(this, jobDesc, jobStatus, mcf, _id);

    if (!_addJobToMap(jobQuery)) {
        LOGS(_log, LOG_LVL_ERROR, "Executive ignoring duplicate job add " << jobQuery->getIdStr());
        return;
    }

    if (!_track(jobQuery->getIdInt(), jobQuery)) {
        LOGS(_log, LOG_LVL_ERROR, "Executive ignoring duplicate track add" << jobQuery->getIdStr());
        return;
    }
    if (_empty.exchange(false)) {
        LOGS(_log, LOG_LVL_DEBUG, "Flag _empty set to false by " << jobQuery->getIdStr());
    }
    ++_requestCount;
    std::string msg = "Executive: Add job with path=" + jobDesc.resource().path();
    LOGS(_log, LOG_LVL_DEBUG, msg);
    _messageStore->addMessage(jobDesc.resource().chunk(), ccontrol::MSG_MGR_ADD, msg);

    jobQuery->runJob();
}


/// If the executive has not been cancelled, this calls xrootd's Provision and
/// sets jobQueryResource = sourceQR.
/// @return true if Provision was called and sets jobQueryResource = sourceQR.
bool Executive::xrdSsiProvision(std::shared_ptr<QueryResource> &jobQueryResource,
                                std::shared_ptr<QueryResource> const& sourceQR) {
    std::lock_guard<std::recursive_mutex> lock(_cancelled.getMutex());
    if (!_cancelled) {
        jobQueryResource = sourceQR;
        getXrdSsiService()->Provision(jobQueryResource.get());
        return true;
    }
    return false;
}


/// Add a JobQuery to this Executive.
/// Return true if it was successfully added to the map.
///
bool Executive::_addJobToMap(JobQuery::Ptr const& job) {
    auto entry = std::pair<int, JobQuery::Ptr>(job->getIdInt(), job);
    return _jobMap.insert(entry).second;
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
        std::lock_guard<std::recursive_mutex> lock(_jobsMutex);
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
    std::string idStr = qmeta::QueryIdHelper::makeIdStr(_id, jobId);
    LOGS(_log, LOG_LVL_DEBUG, "Executive::markCompleted " << idStr
            << " " << success);
    if (!success) {
        {
            std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
            auto iter = _incompleteJobs.find(jobId);
            if (iter != _incompleteJobs.end()) {
                err = iter->second->getDescription().respHandler()->getError();
            } else {
                std::string msg = "Executive::markCompleted failed to find tracked " + idStr +
                        " size=" + std::to_string(_incompleteJobs.size());
                throw Bug(msg);
            }
        }
        LOGS(_log, LOG_LVL_ERROR, "Executive: error executing " << idStr
             << " " << err << " (status: " << err.getStatus() << ")");
        {
            std::lock_guard<std::recursive_mutex> lock(_jobsMutex);
            _jobMap[jobId]->getStatus()->updateInfo(JobStatus::RESULT_ERROR, err.getCode(), err.getMsg());
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
        LOGS(_log, LOG_LVL_DEBUG, _id << " Executive::squash() already cancelled! refusing.");
        return;
    }

    LOGS(_log, LOG_LVL_DEBUG, _id << " Executive::squash Trying to cancel all queries...");
    std::deque<JobQuery::Ptr> jobsToCancel;
    {
        std::lock_guard<std::recursive_mutex> lock(_jobsMutex);
        for(auto const& jobEntry : _jobMap) {
            jobsToCancel.push_back(jobEntry.second);
        }
    }

    for (auto const& job : jobsToCancel) {
        job->cancel();
    }
    LOGS_DEBUG(_id << " Executive::squash done");
}

int Executive::getNumInflight() {
    std::unique_lock<std::mutex> lock(_incompleteJobsMutex);
    return _incompleteJobs.size();
}

std::string Executive::getProgressDesc() const {
    std::ostringstream os;
    {
        std::lock_guard<std::recursive_mutex> lock(_jobsMutex);
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
    assert(r);
    std::string idStr = qmeta::QueryIdHelper::makeIdStr(_id, jobId);
    {
        std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
        if (_incompleteJobs.find(jobId) != _incompleteJobs.end()) {
            LOGS(_log, LOG_LVL_WARN, "Attempt to TRACK " << idStr
                 << " failed as jobId already found in incomplete jobs.");
            return false;
        }
        _incompleteJobs[jobId] = r;
    }
    LOGS(_log, LOG_LVL_DEBUG, "Success TRACKING " << idStr);
    return true;
}

void Executive::_unTrack(int jobId) {
    bool untracked = false;
    int size = -1;
    std::ostringstream s;
    {
        std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
        auto i = _incompleteJobs.find(jobId);
        if (i != _incompleteJobs.end()) {
            _incompleteJobs.erase(i);
            untracked = true;
            if (_incompleteJobs.empty()) _allJobsComplete.notify_all();
        }
        size = _incompleteJobs.size();
        // Log up to 5 incomplete jobs. Very useful when jobs do not finish.
        int c = 0;
        for(auto j = _incompleteJobs.begin(), e = _incompleteJobs.end(); j != e && c < 5; ++j) {
            s << j->first << " ";
            ++c;
        }
    }
    std::ostringstream os;
    os << "Executive UNTRACKING " << qmeta::QueryIdHelper::makeIdStr(_id, jobId)
       << " size=" << size << " " << (untracked ? "success":"failed") << "::" << s.str();
    if (untracked) {
        LOGS(_log, LOG_LVL_DEBUG, os.str());
    } else {
        LOGS(_log, LOG_LVL_WARN, os.str());
    }
}

/// Remove all jobs from the _incompleteJobs map that have errors.
// This function only acts when there are errors. In there are no errors,
// markCompleted() does the cleanup, while we are waiting (in _waitAllUntilEmpty()).
void Executive::_reapRequesters(std::unique_lock<std::mutex> const&) {
    for(auto iter=_incompleteJobs.begin(), e=_incompleteJobs.end(); iter != e;) {
        if (!iter->second->getDescription().respHandler()->getError().isNone()) {
            // Requester should have logged the error to the messageStore
            LOGS(_log, LOG_LVL_DEBUG, "Executive reaped requester for "
                 << qmeta::QueryIdHelper::makeIdStr(_id, iter->first));
            iter = _incompleteJobs.erase(iter);
        } else {
            ++iter;
        }
    }
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
        std::lock_guard<std::recursive_mutex> lock(_jobsMutex);
        for (auto const& entry : _jobMap) {
            JobQuery::Ptr const& job = entry.second;
            auto const& info = job->getStatus()->getInfo();
            std::ostringstream os;
            os << info.state << " " << info.stateCode;
            if (!info.stateDesc.empty()) {
                os << " (" << info.stateDesc << ")";
            }
            os << " " << info.stateTime;
            _messageStore->addMessage(job->getDescription().resource().chunk(),
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
        _reapRequesters(lock);
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

JobQuery::Ptr Executive::getJobQuery(int jobId) {
    qdisp::JobQuery::Ptr job;
    auto i = _jobMap.find(jobId);
    if (i != _jobMap.end()) {
        job = i->second;
    }
    return job;
}

}}} // namespace lsst::qserv::qdisp
