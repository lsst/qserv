// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
  * @brief Executive. It executes things.
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
#include "global/Bug.h"
#include "global/ResourceUnit.h"
#include "log/msgCode.h"
#include "qdisp/MessageStore.h"
#include "qdisp/QueryResource.h"
#include "qdisp/ResponseRequester.h"
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

/// Atomically set var to value.
/// @param m the mutex protecting var.
/// @return previous value of var.
inline bool atomicSet(bool& var, bool value, std::mutex& m) {
    std::lock_guard<std::mutex> lock(m);
    bool oldValue = var;
    var = value;
    return oldValue;
}

inline bool lockedRead(bool& var, std::mutex& m) {
    std::lock_guard<std::mutex> lock(m);
    return var;
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qdisp {

template <typename Ptr>
struct printMapSecond {
    printMapSecond(std::ostream& os_, std::string sep_)
        : os(os_), sep(sep_), first(true)  {}

    void operator()(Ptr const& p) {
        if(!first) {
            os << sep;
        }
        os << *(p.second);
        first = false;
    }
    std::ostream& os;
    std::string sep;
    bool first;
};

class NotifyExecutive : public util::UnaryCallable<void, bool> {
public:
    typedef std::shared_ptr<NotifyExecutive> Ptr;

    NotifyExecutive(qdisp::Executive& e, int jobId)
        : _executive(e), _jobId(jobId) {}

    virtual void operator()(bool success) {
        _executive.markCompleted(_jobId, success);
    }

    static Ptr newInstance(qdisp::Executive& e, int jobId) {
        return std::make_shared<NotifyExecutive>(std::ref(e), jobId);;
    }
private:
    qdisp::Executive& _executive;
    int _jobId;
};

////////////////////////////////////////////////////////////////////////
// class Executive implementation
////////////////////////////////////////////////////////////////////////
Executive::Executive(Config::Ptr c, std::shared_ptr<MessageStore> ms)
    : _config(*c),
      _empty{true},
      _messageStore(ms),
      _cancelled{false} {
    _setup();
}

Executive::~Executive() {
    // Real XrdSsiService objects are unowned, but mocks are allocated in _setup.
    delete dynamic_cast<XrdSsiServiceMock *>(_service);
}

class Executive::DispatchAction : public util::VoidCallable<void> {
public:
    typedef std::shared_ptr<DispatchAction> Ptr;
    DispatchAction(Executive& executive,
                   int jobId,
                   Executive::JobDescription const& jobDescription,
                   JobStatus::Ptr jobStatus)
        : _executive(executive), _jobId(jobId),
          _jobDescription(jobDescription), _jobStatus(jobStatus) {
    }
    virtual ~DispatchAction() {}
    virtual void operator()() {
        if(_jobDescription.requester->reset()) { // Must be able to reset state
            _executive._dispatchQuery(_jobId, _jobDescription, _jobStatus);
        }
        // If the reset fails, do nothing-- can't retry.
    }
private:
    Executive& _executive;
    int _jobId;
    JobDescription _jobDescription;
    JobStatus::Ptr _jobStatus; ///< Points at status in Executive::_statusMap
};

/* Add a new job to executive queue, if not already in. Not thread-safe.
 */
void Executive::add(int jobId, Executive::JobDescription const& jobDescription) {
    bool alreadyCancelled = lockedRead(_cancelled, _cancelledMutex);
    if(alreadyCancelled) {
        LOGF(getLogger(), LOG_LVL_INFO, "Executive already cancelled, ignoring add(%1%)" % jobId);
        return;
    }
    bool trackOk = _track(jobId, jobDescription.requester); // Remember so we can join
    if(!trackOk) {
        LOGF(getLogger(), LOG_LVL_WARN, "Ignoring duplicate add(%1%)" % jobId);
        return;
    }
    JobStatus::Ptr jobStatus = _insertNewStatus(jobId, jobDescription.resource);

    if (_empty.get()) {
        _empty.set(false);
        LOGF(getLogger(), LOG_LVL_TRACE, "Flag \"Empty\" set to false by jobId %1%" % jobId);
    }

    ++_requestCount;
    std::string msg = "Executive: Add job with path=" + jobDescription.resource.path();
    LOGF(getLogger(), LOG_LVL_INFO, "%1%" % msg);
    _messageStore->addMessage(jobDescription.resource.chunk(), log::MSG_MGR_ADD, msg);

    _dispatchQuery(jobId,
                   jobDescription,
                   jobStatus);
}

bool Executive::join() {
    // To join, we make sure that all of the chunks added so far are complete.
    // Check to see if _requesters is empty, if not, then sleep on a condition.
    _waitAllUntilEmpty();
    // Okay to merge. probably not the Executive's responsibility
    struct successF {
        static bool f(Executive::JobStatusPtrMap::value_type const& entry) {
            JobStatus::Info const& esI = entry.second->getInfo();
            LOGF_INFO("entry state:%1% %2%)" % (void*)entry.second.get() % esI);
            return (esI.state == JobStatus::RESPONSE_DONE)
                || (esI.state == JobStatus::COMPLETE); }
    };

    int sCount = 0;
    {
        std::lock_guard<std::mutex> lock(_statusesMutex);
        sCount = std::count_if(_statuses.begin(), _statuses.end(), successF::f);
    }
    if(sCount == _requestCount) {
        LOGF_INFO("Query execution succeeded: %1% jobs dispatched and completed." % _requestCount);
    } else {
        LOGF_ERROR("Query execution failed: %1% jobs dispatched, but only %2% jobs completed" % _requestCount % sCount);
    }
    _updateProxyMessages();
    bool empty = (sCount == _requestCount);
    _empty.set(empty);
    LOGF_DEBUG("Flag set to _empty=%1% sCount=%2% requestCount=%3%" % empty % sCount % _requestCount);
    return empty;
}

void Executive::markCompleted(int jobId, bool success) {
    ResponseRequester::Error err;
    LOGF(getLogger(), LOG_LVL_INFO, "Executive::markCompleted(%1%,%2%)" % jobId % success);
    if(!success) {
        {
            std::lock_guard<std::mutex> lock(_requestersMutex);
            RequesterMap::iterator i = _requesters.find(jobId);
            if(i != _requesters.end()) {
                err = i->second->getError();
            } else {
                std::string msg =
                    (boost::format("Executive::markCompleted(%1%) "
                                   "failed to find tracked id=%2% size=%3%")
                     % (void*)this % jobId % _requesters.size()).str();
                throw Bug(msg);
            }
        }
        LOGF(getLogger(), LOG_LVL_ERROR,
             "Executive: error executing jobId=%1%: %2% (status: %3%)" % jobId % err % err.getStatus());
        {
            std::lock_guard<std::mutex> lock(_statusesMutex);
            _statuses[jobId]->updateInfo(JobStatus::RESULT_ERROR, err.getCode(), err.getMsg());
        }
        {
            std::lock_guard<std::mutex> lock(_errorsMutex);
            _multiError.push_back(err);
            LOGF(getLogger(), LOG_LVL_TRACE, "Currently %2% registered errors: %1%" % _multiError % _multiError.size());
        }
    }
    _unTrack(jobId);
    if(!success) {
        LOGF(getLogger(), LOG_LVL_ERROR,
                 "Executive: requesting squash, cause: jobId=%1% failed (code=%2% %3%)" % jobId % err.getCode() % err.getMsg());
        squash(); // ask to squash
    }
}

std::shared_ptr<util::UnaryCallable<void, bool> >
Executive::newNotifier(Executive &e, int jobId) {
    return NotifyExecutive::newInstance(e, jobId);
}

void Executive::requestSquash(int jobId) {
    RequesterPtr toSquash;
    bool needToWarn = false;
    ResponseRequester::Error e;
    {
        std::lock_guard<std::mutex> lock(_requestersMutex);
        RequesterMap::iterator i = _requesters.find(jobId);
        if(i != _requesters.end()) {
            e = i->second->getError();
            if(e.isNone()) {
                toSquash = i->second; // Remember which one to squash
            } else {
                needToWarn = true;
            }
        } else {
            throw Bug("Executive::requestSquash() with invalid jobId");
        }
    }
    if(needToWarn) { // Log outside the mutex
        LOGF_WARN("Warning, requestSquash(jobId=%1%), but this job has already failed (%3%, %4%)."
                  % jobId % e.getCode() % e.getMsg());
    }

    if(toSquash) { // Squash outside of the mutex
        toSquash->cancel();
    }
}

void Executive::squash() {
    bool alreadyCancelled = atomicSet(_cancelled, true, _cancelledMutex);
    if(alreadyCancelled) {
        LOGF_WARN("Executive::squash() already cancelled! refusing.");
        return;
    }

    LOGF_INFO("Trying to cancel all queries...");
    std::vector<RequesterPtr> pendingRequesters;
    std::ostringstream os;
    os << "STATE=";
    {
        std::lock_guard<std::mutex> lock(_requestersMutex);
        _printState(os);
        RequesterMap::iterator i,e;
        for(i=_requesters.begin(), e=_requesters.end(); i != e; ++i) {
            pendingRequesters.push_back(i->second);
        }
    }
    LOGF_INFO("%1%\nLoop cancel all queries..." % os.str());
    LOGF_INFO("Enqueued requesters for cancelling...done");
    {
        std::vector<RequesterPtr>::iterator i, e;
        for(i=pendingRequesters.begin(), e=pendingRequesters.end(); i != e; ++i) {
            // Could get stuck because it waits on xrootd,
            // which may be waiting on a thread blocked in _unTrack().
            // Don't do this while holding _requestersMutex
            (**i).cancel();
        }
        LOGF_INFO("Cancelled all query requesters...done");
    }
}

struct printMapEntry {
    printMapEntry(std::ostream& os_, std::string const& sep_)
        : os(os_), sep(sep_), first(true) {}
    void operator()(Executive::JobStatusPtrMap::value_type const& entry) {
        if(!first) { os << sep; }
        os << "Ref=" << entry.first << " ";
        JobStatus const& es = *entry.second;
        os << es;
        first = false;
    }
    std::ostream& os;
    std::string const& sep;
    bool first;
};

int Executive::getNumInflight() {
    std::unique_lock<std::mutex> lock(_requestersMutex);
    return _requesters.size();
}

std::string Executive::getProgressDesc() const {
    std::ostringstream os;
    {
        std::lock_guard<std::mutex> lock(_statusesMutex);
        std::for_each(_statuses.begin(), _statuses.end(), printMapEntry(os, "\n"));
    }
    auto const & msg_progress = os.str();
    LOGF(getLogger(), LOG_LVL_ERROR, "%1%" % msg_progress);
    return std::move(msg_progress);
}

////////////////////////////////////////////////////////////////////////
// class Executive (private)
////////////////////////////////////////////////////////////////////////
void Executive::_dispatchQuery(int jobId,
                               Executive::JobDescription const& jobDescription,
                               JobStatus::Ptr jobStatus) {
    std::shared_ptr<DispatchAction> retryFunc;
    if(_shouldRetry(jobId)) { // limit retries for each request.
        retryFunc.reset(new DispatchAction(*this, jobId,
                                           jobDescription,
                                           jobStatus));
    }
    std::unique_ptr<QueryResource> r(new QueryResource(
        jobDescription.resource.path(),
        jobDescription.request,
        jobDescription.requester,
        NotifyExecutive::newInstance(*this, jobId),
        retryFunc,
        *jobStatus));
    jobStatus->updateInfo(JobStatus::PROVISION);
    _service->Provision(r.get());
    // XrdSsiService will call ProvisionDone() on r in any case, and we clean up r there.
    // FIXME: For squashing, need to hold ptr to QueryResource, so we can
    // instruct it to call XrdSsiRequest::Finished(cancel=true). Also, can send
    // cancellation into requester.
    r.release();
}

void Executive::_setup() {

    XrdSsiErrInfo eInfo;
    _empty.set(true);
    _requestCount = 0;
    // If unit testing, load the mock service.
    if (_config.serviceUrl.compare(_config.getMockStr()) == 0) {
        _service = new XrdSsiServiceMock(this);
    } else {
        _service = XrdSsiProviderClient->GetService(eInfo, _config.serviceUrl.c_str()); // Step 1
    }
    if(!_service) {
        LOGF_ERROR("Error obtaining XrdSsiService in Executive: "
                   %  getErrorText(eInfo));
    }
    assert(_service);
}

/// Check to see if a requester should retry, based on how many retries have
/// been attempted. Increments the retry counter as a side effect.
bool Executive::_shouldRetry(int jobId) {
    const int MAX_RETRY = 5;
    std::lock_guard<std::mutex> lock(_retryMutex);
    IntIntMap::iterator i = _retryMap.find(jobId);
    bool should_retry = true;
    if(i == _retryMap.end()) {
        _retryMap[jobId] = 1;
    } else if(i->second < MAX_RETRY) {
        _retryMap[jobId] = i->second + 1;
    } else {
        should_retry = false;
    }
    return should_retry;
}

JobStatus::Ptr Executive::_insertNewStatus(int jobId, ResourceUnit const& r) {
    JobStatus::Ptr es = std::make_shared<JobStatus>(r);
    std::lock_guard<std::mutex> lock(_statusesMutex);
    _statuses.insert(JobStatusPtrMap::value_type(jobId, es));
    return es;
}

bool Executive::_track(int jobId, RequesterPtr r) {
    assert(r);
    {
        LOGF_DEBUG("Executive (%1%) tracking id=%2%" % (void*)this % jobId);
        std::lock_guard<std::mutex> lock(_requestersMutex);
        if(_requesters.find(jobId) == _requesters.end()) {
            _requesters[jobId] = r;
        } else {
            return false;
        }
    }
    return true;
}

void Executive::_unTrack(int jobId) {
    bool untracked = false;
    {
        std::lock_guard<std::mutex> lock(_requestersMutex);
        RequesterMap::iterator i = _requesters.find(jobId);
        if(i != _requesters.end()) {
            _requesters.erase(i);
            untracked = true;
            if(_requesters.empty()) _requestersEmpty.notify_all();
        }
    }
    if(untracked) {
        LOGF_INFO("Executive (%1%) UNTRACKING id=%2%" % (void*)this % jobId);
    }
}

/// This function only acts when there are errors. In there are no errors,
/// markCompleted() does the cleanup, while we are waiting (in
/// _waitAllUntilEmpty()).
void Executive::_reapRequesters(std::unique_lock<std::mutex> const&) {
    RequesterMap::iterator i, e;
    while(true) {
        bool reaped = false;
        for(i=_requesters.begin(), e=_requesters.end(); i != e; ++i) {
            if(!i->second->getError().isNone()) {
                // Requester should have logged the error to the messageStore
                LOGF_INFO("Executive (%1%) reaped requester for jobId=%2%"
                          % (void*)this % i->first);
                _requesters.erase(i);
                reaped = true;
                break;
            }
        }
        if(!reaped) {
            break;
        }
    }
}

void Executive::_updateProxyMessages() {
    {
        std::lock_guard<std::mutex> lock(_statusesMutex);
        for(auto i=_statuses.begin(), e=_statuses.end(); i != e; ++i) {
            auto const & info = i->second->getInfo();
            std::ostringstream os;
            os << info.state << " " << info.stateCode;
            if(!info.stateDesc.empty()) {
                os << " (" << info.stateDesc << ")";
            }
            os << " " << info.stateTime;
            _messageStore->addMessage(info.resourceUnit.chunk(),
                                      info.state, os.str());
        }
    }
    {
        std::lock_guard<std::mutex> lock(_errorsMutex);
        if (_multiError.size()>0) {
            _messageStore->addErrorMessage(_multiError.toString());
        }
    }
}

/// This function blocks until it has reaped all the requesters.
/// Typically the requesters are handled by markCompleted().
/// _reapRequesters() deals with cases that involve errors.
void Executive::_waitAllUntilEmpty() {
    std::unique_lock<std::mutex> lock(_requestersMutex);
    int lastCount = -1;
    int count;
    int moreDetailThreshold = 5;
    int complainCount = 0;
    const std::chrono::seconds statePrintDelay(5);
    //_printState(LOG_STRM(Debug));
    while(!_requesters.empty()) {
        count = _requesters.size();
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
        _requestersEmpty.wait_for(lock, statePrintDelay);
    }
}

std::ostream& operator<<(std::ostream& os,
                         Executive::JobStatusPtrMap::value_type const& v) {
    os << v.first << ": " << *(v.second);
    return os;
}

/// precondition: _requestersMutex is held by current thread.
void Executive::_printState(std::ostream& os) {
    std::for_each(_requesters.begin(), _requesters.end(),
                  printMapSecond<RequesterMap::value_type>(os, "\n"));
    os << "\n" << getProgressDesc() << "\n";
}

}}} // namespace lsst::qserv::qdisp
