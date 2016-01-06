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

LOG_LOGGER getLogger() {
    static const LOG_LOGGER _logger(LOG_GET("lsst.qserv.qdisp.Executive"));
    return _logger;
}

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
Executive::Executive(Config::Ptr c, std::shared_ptr<MessageStore> ms)
    : _config{*c}, _messageStore{ms} {
    _setup();
}

Executive::~Executive() {
    // Real XrdSsiService objects are unowned, but mocks are allocated in _setup.
    delete dynamic_cast<XrdSsiServiceMock *>(_xrdSsiService);
}

/** Add a new job to executive queue, if not already in. Not thread-safe.
 */
void Executive::add(JobDescription const& jobDesc) {
    LOGF(getLogger(), LOG_LVL_DEBUG, "Executive::add(%1%)" % jobDesc.toString());
    if(_cancelled) {
        LOGF(getLogger(), LOG_LVL_INFO, "Executive already cancelled, ignoring add(%1%)" % jobDesc.id());
        return;
    }
    // Create the JobQuery and put it in the map.
    JobStatus::Ptr jobStatus = std::make_shared<JobStatus>();
    MarkCompleteFunc::Ptr mcf = std::make_shared<MarkCompleteFunc>(this, jobDesc.id());
    JobQuery::Ptr jobQuery = JobQuery::newJobQuery(this, jobDesc, jobStatus, mcf);
    if(!_addJobToMap(jobQuery)) {
        LOGF(getLogger(), LOG_LVL_ERROR, "Executive ignoring duplicate job add(%1%)" % jobQuery->getId());
        return;
    }

    if (!_track(jobQuery->getId(), jobQuery)) {
        LOGF(getLogger(), LOG_LVL_ERROR, "Executive ignoring duplicate track add(%1%)" % jobQuery->getId());
        return;
    }
    if (_empty.exchange(false)) {
        LOGF(getLogger(), LOG_LVL_INFO, "Flag _empty set to false by jobId %1%" % jobQuery->getId());
    }
    ++_requestCount;
    std::string msg = "Executive: Add job with path=" + jobDesc.resource().path();
    LOGF(getLogger(), LOG_LVL_INFO, "%1%" % msg);
    _messageStore->addMessage(jobDesc.resource().chunk(), ccontrol::MSG_MGR_ADD, msg);

    jobQuery->runJob();
}

/** Add a JobQuery to this Executive.
 * Return true if it was successfully added to the map.
 */
bool Executive::_addJobToMap(JobQuery::Ptr const& job) {
    auto entry = std::pair<int, JobQuery::Ptr>(job->getId(), job);
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
            LOGF_INFO("entry state:%1% %2%)" % (void*)entry.second.get() % esI);
            return (esI.state == JobStatus::RESPONSE_DONE) || (esI.state == JobStatus::COMPLETE);
        }
    };

    int sCount = 0;
    {
        std::lock_guard<std::mutex> lock(_jobsMutex);
        sCount = std::count_if(_jobMap.begin(), _jobMap.end(), successF::f);
    }
    if(sCount == _requestCount) {
        LOGF_INFO("Query execution succeeded: %1% jobs dispatched and completed." % _requestCount);
    } else {
        LOGF_ERROR("Query execution failed: %1% jobs dispatched, but only %2% jobs completed" % _requestCount % sCount);
    }
    _updateProxyMessages();
    bool empty = (sCount == _requestCount);
    _empty.store(empty);
    LOGF_DEBUG("Flag set to _empty=%1% sCount=%2% requestCount=%3%" % empty % sCount % _requestCount);
    return empty;
}

void Executive::markCompleted(int jobId, bool success) {
    ResponseHandler::Error err;
    LOGF(getLogger(), LOG_LVL_INFO, "Executive::markCompleted(%1%,%2%)" % jobId % success);
    if(!success) {
        {
            std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
            auto iter = _incompleteJobs.find(jobId);
            if(iter != _incompleteJobs.end()) {
                err = iter->second->getDescription().respHandler()->getError();
            } else {
                std::string msg =
                    (boost::format("Executive::markCompleted(%1%) "
                                   "failed to find tracked id=%2% size=%3%")
                     % (void*)this % jobId % _incompleteJobs.size()).str();
                throw Bug(msg);
            }
        }
        LOGF(getLogger(), LOG_LVL_ERROR,
             "Executive: error executing jobId=%1%: %2% (status: %3%)" % jobId % err % err.getStatus());
        {
            std::lock_guard<std::mutex> lock(_jobsMutex);
            _jobMap[jobId]->getStatus()->updateInfo(JobStatus::RESULT_ERROR, err.getCode(), err.getMsg());
        }
        {
            std::lock_guard<std::mutex> lock(_errorsMutex);
            _multiError.push_back(err);
            LOGF(getLogger(), LOG_LVL_TRACE, "Currently %2% registered errors: %1%" %
                 _multiError % _multiError.size());
        }
    }
    _unTrack(jobId);
    if(!success) {
        LOGF(getLogger(), LOG_LVL_ERROR, "Executive: requesting squash, cause: jobId=%1% failed (code=%2% %3%)"
             % jobId % err.getCode() % err.getMsg());
        squash(); // ask to squash
    }
}

void Executive::requestSquash(int jobId) {
    JobQuery::Ptr toSquash;
    {
        std::lock_guard<std::mutex> lock(_jobsMutex);
        auto iter = _jobMap.find(jobId);
        if(iter == _jobMap.end()) {
            LOGF_WARN("requestSquash invalid jobID %1%" % jobId);
            return;
        }
        toSquash = iter->second;
    }
    // release mutex before calling cancel.
    toSquash->cancel();
}

void Executive::squash() {
    bool alreadyCancelled = _cancelled.exchange(true);
    if(alreadyCancelled) {
        LOGF_WARN("Executive::squash() already cancelled! refusing.");
        return;
    }

    LOGF_INFO("Trying to cancel all queries...");
    {
        std::lock_guard<std::mutex> lock(_jobsMutex);
        for(auto const& jobEntry : _jobMap) {
            jobEntry.second->cancel();
        }
    }
}

int Executive::getNumInflight() {
    std::unique_lock<std::mutex> lock(_incompleteJobsMutex);
    return _incompleteJobs.size();
}

std::string Executive::getProgressDesc() const {
    std::ostringstream os;
    {
        std::lock_guard<std::mutex> lock(_jobsMutex);
        auto first = true;
        for (auto entry : _jobMap) {
            JobQuery::Ptr job = entry.second;
            if(!first) { os << "\n"; }
            first = false;
            os << "Ref=" << entry.first << " " << job;

        }
    }
    std::string msg_progress = os.str();
    LOGF(getLogger(), LOG_LVL_ERROR, "%1%" % msg_progress);
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
    if(!_xrdSsiService) {
        LOGF_ERROR("Error obtaining XrdSsiService in Executive: "
                   %  getErrorText(eInfo));
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
    LOGF(getLogger(), LOG_LVL_TRACE, "Attempt to add jobId=%2% to Executive (%1%) tracked jobs" % (void*)this % jobId);
    {
        std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
        if(_incompleteJobs.find(jobId) != _incompleteJobs.end()) {
            return false;
        }
        _incompleteJobs[jobId] = r;
    }
    LOGF(getLogger(), LOG_LVL_TRACE, "Success in adding jobId=%2% to Executive (%1%) tracked jobs" % (void*)this % jobId);
    return true;
}

void Executive::_unTrack(int jobId) {
    bool untracked = false;
    {
        std::lock_guard<std::mutex> lock(_incompleteJobsMutex);
        auto i = _incompleteJobs.find(jobId);
        if(i != _incompleteJobs.end()) {
            _incompleteJobs.erase(i);
            untracked = true;
            if(_incompleteJobs.empty()) _allJobsComplete.notify_all();
        }
    }
    if(untracked) {
        LOGF_INFO("Executive (%1%) UNTRACKING id=%2%" % (void*)this % jobId);
    }
}

/// Remove all jobs from the _incompleteJobs map that have errors.
// This function only acts when there are errors. In there are no errors,
// markCompleted() does the cleanup, while we are waiting (in _waitAllUntilEmpty()).
void Executive::_reapRequesters(std::unique_lock<std::mutex> const&) {
    for(auto iter=_incompleteJobs.begin(), e=_incompleteJobs.end(); iter != e;) {
        if(!iter->second->getDescription().respHandler()->getError().isNone()) {
            // Requester should have logged the error to the messageStore
            LOGF_INFO("Executive (%1%) reaped requester for jobId=%2%" % (void*)this % iter->first);
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
        std::lock_guard<std::mutex> lock(_jobsMutex);
        for (auto const& entry : _jobMap) {
            JobQuery::Ptr const& job = entry.second;
            auto const& info = job->getStatus()->getInfo();
            std::ostringstream os;
            os << info.state << " " << info.stateCode;
            if(!info.stateDesc.empty()) {
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
    //_printState(LOG_STRM(Debug));
    while(!_incompleteJobs.empty()) {
        count = _incompleteJobs.size();
        _reapRequesters(lock);
        if(count != lastCount) {
            lastCount = count;
            ++complainCount;
            if (LOG_CHECK_INFO()) {
                std::ostringstream os;
                if(complainCount > moreDetailThreshold) {
                    _printState(os);
                    os << "\n";
                }
                os << "Still " << count << " in flight.";
                complainCount = 0;
                lock.unlock(); // release the lock while we trigger logging.
                LOGF_INFO("%1%" % os.str());
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
    bool first = false;
    for (auto const& entry : _incompleteJobs) {
        JobQuery::Ptr job = entry.second;
        if(!first) {
            os << "\n";
        }
        os << *job;
        first = false;
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
