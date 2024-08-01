/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

// System headers
#include <sstream>

// Third-party headers

// Class header
#include "qdisp/JobQuery.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/LogContext.h"
#include "qdisp/Executive.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.JobQuery");
}  // anonymous namespace

using namespace std;

namespace lsst::qserv::qdisp {

JobQuery::JobQuery(Executive::Ptr const& executive, JobDescription::Ptr const& jobDescription,
                   qmeta::JobStatus::Ptr const& jobStatus,
                   shared_ptr<MarkCompleteFunc> const& markCompleteFunc, QueryId qid)
        : JobBase(),
          _executive(executive),
          _jobDescription(jobDescription),
          _markCompleteFunc(markCompleteFunc),
          _jobStatus(jobStatus),
          _qid(qid),
          _idStr(QueryIdHelper::makeIdStr(qid, getJobId())) {
    _qdispPool = executive->getQdispPool();
    LOGS(_log, LOG_LVL_TRACE, "JobQuery desc=" << _jobDescription);
}

JobQuery::~JobQuery() {
    LOGS(_log, LOG_LVL_DEBUG, "~JobQuery");
    LOGS(_log, LOG_LVL_WARN, "~JobQuery QID=" << _idStr);
}

/// Cancel response handling. Return true if this is the first time cancel has been called.
bool JobQuery::cancel(bool superfluous) {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    LOGS(_log, LOG_LVL_DEBUG, "JobQuery::cancel()");
    if (_cancelled.exchange(true) == false) {
        lock_guard<recursive_mutex> lock(_rmutex);
        // If _inSsi is true then this query request has been passed to SSI and
        // _queryRequestPtr cannot be a nullptr. Cancellation is complicated.
        bool cancelled = false;
        /* &&&
        if (_inSsi) {
            LOGS(_log, LOG_LVL_DEBUG, "cancel QueryRequest in progress");
            if (_queryRequestPtr->cancel()) {
                LOGS(_log, LOG_LVL_DEBUG, "cancelled by QueryRequest");
                cancelled = true;
            } else {
                LOGS(_log, LOG_LVL_DEBUG, "QueryRequest could not cancel");
            }
        }
        */
        if (!cancelled) {
            ostringstream os;
            os << _idStr << " cancel";
            LOGS(_log, LOG_LVL_DEBUG, os.str());
            if (!superfluous) {
                getDescription()->respHandler()->errorFlush(os.str(), -1);
            }
            auto executive = _executive.lock();
            if (executive == nullptr) {
                LOGS(_log, LOG_LVL_ERROR, " can't markComplete cancelled, executive == nullptr");
                return false;
            }
            executive->markCompleted(getJobId(), false);
        }
        if (!superfluous) {
            _jobDescription->respHandler()->processCancel();
        }
        return true;
    }
    LOGS(_log, LOG_LVL_TRACE, "cancel, skipping, already cancelled.");
    return false;
}

/// @return true if this job's executive has been cancelled.
/// There is enough delay between the executive being cancelled and the executive
/// cancelling all the jobs that it makes a difference. If either the executive,
/// or the job has cancelled, proceeding is probably not a good idea.
bool JobQuery::isQueryCancelled() {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "_executive == nullptr");
        return true;  // Safer to assume the worst.
    }
    return exec->getCancelled();
}

bool JobQuery::_setUberJobId(UberJobId ujId) {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    if (_uberJobId >= 0 && ujId != _uberJobId) {
        LOGS(_log, LOG_LVL_DEBUG,
             __func__ << " couldn't change UberJobId as ujId=" << ujId << " is owned by " << _uberJobId);
        return false;
    }
    _uberJobId = ujId;
    return true;
}

bool JobQuery::unassignFromUberJob(UberJobId ujId) {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    std::lock_guard<std::recursive_mutex> lock(_rmutex);
    if (_uberJobId < 0) {
        LOGS(_log, LOG_LVL_INFO, __func__ << " UberJobId already unassigned. attempt by ujId=" << ujId);
        return true;
    }
    if (_uberJobId != ujId) {
        LOGS(_log, LOG_LVL_ERROR,
             __func__ << " couldn't change UberJobId as ujId=" << ujId << " is owned by " << _uberJobId);
        return false;
    }
    _uberJobId = -1;

    auto exec = _executive.lock();
    // Do not increase the count as it should have been increased when the job was started.
    _jobDescription->incrAttemptCountScrubResultsJson(exec, false);
    return true;
}

int JobQuery::getAttemptCount() const {
    std::lock_guard<std::recursive_mutex> lock(_rmutex);
    return _jobDescription->getAttemptCount();
}

string const& JobQuery::getPayload() const { return _jobDescription->payload(); }

void JobQuery::callMarkCompleteFunc(bool success) { _markCompleteFunc->operator()(success); }

ostream& JobQuery::dumpOS(ostream& os) const {
    return os << "{" << getIdStr() << _jobDescription << " " << _jobStatus << "}";
}

}  // namespace lsst::qserv::qdisp
