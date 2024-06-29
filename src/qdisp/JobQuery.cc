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

JobQuery::~JobQuery() { LOGS(_log, LOG_LVL_TRACE, "~JobQuery QID=" << _idStr); }

/** Attempt to run the job on a worker.
 * @return - false if it can not setup the job or the maximum number of attempts has been reached.
 */
bool JobQuery::runJob() {  // &&&
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    LOGS(_log, LOG_LVL_DEBUG, " runJob " << *this);
    auto executive = _executive.lock();
    if (executive == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "runJob failed executive==nullptr");

        return false;
    }
    bool superfluous = executive->isLimitRowComplete();
    bool cancelled = executive->getCancelled();
    bool handlerReset = _jobDescription->respHandler()->reset();
    if (!(cancelled || superfluous) && handlerReset) {
        auto criticalErr = [this, &executive](string const& msg) {
            LOGS(_log, LOG_LVL_ERROR, msg << " " << _jobDescription << " Canceling user query!");
            executive->squash();  // This should kill all jobs in this user query.
        };

        LOGS(_log, LOG_LVL_DEBUG, "runJob checking attempt=" << _jobDescription->getAttemptCount());
        lock_guard<recursive_mutex> lock(_rmutex);
        if (_jobDescription->getAttemptCount() < executive->getMaxAttempts()) {
            bool okCount = _jobDescription->incrAttemptCountScrubResults();
            if (!okCount) {
                criticalErr("hit structural max of retries");
                return false;
            }
            if (!_jobDescription->verifyPayload()) {
                criticalErr("bad payload");
                return false;
            }
        } else {
            LOGS(_log, LOG_LVL_DEBUG, "runJob max retries");
            criticalErr("hit maximum number of retries");
            return false;
        }

        // At this point we are all set to actually run the query. We create a
        // a shared pointer to this object to prevent it from escaping while we
        // are trying to start this whole process. We also make sure we record
        // whether or not we are in SSI as cancellation handling differs.
        //
        LOGS(_log, LOG_LVL_TRACE, "runJob calls StartQuery()");
        JobQuery::Ptr jq(dynamic_pointer_cast<JobQuery>(shared_from_this()));
        _inSsi = true;
        if (executive->startQuery(jq)) {
            _jobStatus->updateInfo(_idStr, qmeta::JobStatus::REQUEST, "EXEC");
            return true;
        }
        _inSsi = false;
    }
    LOGS(_log, (superfluous ? LOG_LVL_DEBUG : LOG_LVL_WARN),
         "runJob failed. cancelled=" << cancelled << " reset=" << handlerReset);
    return false;
}
>>>>>>> 61ec83dc7 (Fails appropriately from max UberJob attempts.)

/// Cancel response handling. Return true if this is the first time cancel has been called.
bool JobQuery::cancel(bool superfluous) {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    if (_cancelled.exchange(true) == false) {
        LOGS(_log, LOG_LVL_TRACE, "JobQuery::cancel() " << superfluous);
        VMUTEX_NOT_HELD(_jqMtx);
        lock_guard lock(_jqMtx);

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
        if (!superfluous) {
            _jobDescription->respHandler()->processCancel();
        }
        return true;
    }
    LOGS(_log, LOG_LVL_TRACE, "JobQuery::cancel, skipping, already cancelled.");
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
    VMUTEX_HELD(_jqMtx);
    if (_uberJobId >= 0 && ujId != _uberJobId) {
        LOGS(_log, LOG_LVL_DEBUG,
             __func__ << " couldn't change UberJobId as ujId=" << ujId << " is owned by " << _uberJobId);
        return false;
    }
    _uberJobId = ujId;
    return true;
}

ostream& JobQuery::dumpOS(ostream& os) const {
    return os << "{" << getIdStr() << _jobDescription << " " << _jobStatus << "}";
}

bool JobQuery::unassignFromUberJob(UberJobId ujId) {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getJobId());
    VMUTEX_NOT_HELD(_jqMtx);
    lock_guard lock(_jqMtx);
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
    // Do not increase the attempt count as it should have been increased when the job was started.
    return true;
}

int JobQuery::getAttemptCount() const {
    VMUTEX_NOT_HELD(_jqMtx);
    lock_guard lock(_jqMtx);
    return _jobDescription->getAttemptCount();
}

ostream& JobQuery::dumpOS(ostream& os) const {
    return os << "{" << getIdStr() << _jobDescription << " " << _jobStatus << "}";
}

std::string JobQuery::dump() const {
    std::ostringstream os;
    dumpOS(os);
    return os.str();
}

std::ostream& operator<<(std::ostream& os, JobQuery const& jq) { return jq.dumpOS(os); }

}  // namespace lsst::qserv::qdisp
