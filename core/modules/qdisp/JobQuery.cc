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
#include "qdisp/Executive.h"
#include "qdisp/QueryRequest.h"
#include "qdisp/QueryResource.h"

namespace lsst {
namespace qserv {
namespace qdisp {

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.JobQuery");

void logErr(std::string const& msg, JobQuery* jq) {
    LOGS(_log, LOG_LVL_ERROR, msg << " " << *jq);
}
} // namespace

JobQuery::JobQuery(Executive* executive, JobDescription const& jobDescription,
                   JobStatus::Ptr const& jobStatus,
                   std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc) :
    _executive(executive), _jobDescription(jobDescription),
    _markCompleteFunc(markCompleteFunc), _jobStatus(jobStatus) {
    LOGS(_log, LOG_LVL_DEBUG, "JobQuery JQ_jobId=" << getId() << " desc=" << _jobDescription);
}

JobQuery::~JobQuery() {
    LOGS(_log, LOG_LVL_DEBUG, "~JobQuery JQ_jobId=" << getId());
}

/** Attempt to run the job on a worker.
 * @return - false if it can not setup the job or the maximum number of retries has been reached.
 */
bool JobQuery::runJob() {
    LOGS(_log, LOG_LVL_DEBUG, "runJob " << toString());
    if (_executive == nullptr) {
        logErr("runJob failed _executive=nullptr", this);
        return false;
    }
    bool cancelled = _executive->getCancelled();
    bool handlerReset = _jobDescription.respHandler()->reset();
    if (!cancelled && handlerReset) {
        auto qr = std::make_shared<QueryResource>(shared_from_this());
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        if ( _runAttemptsCount < _getMaxRetries() ) {
            ++_runAttemptsCount;
            if (_executive == nullptr) {
                logErr("JobQuery couldn't run job as executive is null", this);
                return false;
            }
            _queryResourcePtr = qr;
        } else {
            logErr("JobQuery hit maximum number of retries!", this);
            return false;
        }
        _jobStatus->updateInfo(JobStatus::PROVISION);
        _executive->getXrdSsiService()->Provision(_queryResourcePtr.get());
        return true;
    } else {
        LOGS(_log, LOG_LVL_WARN, "JobQuery Failed to RunJob failed. cancelled="
             << cancelled << " reset=" << handlerReset);
    }
    return false;
}

void JobQuery::provisioningFailed(std::string const& msg, int code) {
    LOGS(_log, LOG_LVL_ERROR, "Error provisioning, jobId=" << getId() << " msg=" << msg
         << " code=" << code << " " << *this << "\n    desc=" << _jobDescription);
    _jobStatus->updateInfo(JobStatus::PROVISION_NACK, code, msg);
    _jobDescription.respHandler()->errorFlush(msg, code);
}

/// Cancel response handling. Return true if this is the first time cancel has been called.
bool JobQuery::cancel() {
    if (_cancelled.exchange(true) == false) {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        // Nothing needs to be done for _queryResourcePtr.
        if (_queryRequestPtr) {
            _queryRequestPtr->cancel();
        }
        _jobDescription.respHandler()->processCancel();
        return true;
    }
    return false;
}

/// Reset the QueryResource pointer, but only if called by the current QueryResource.
void JobQuery::freeQueryResource(QueryResource* qr) {
    std::lock_guard<std::recursive_mutex> lock(_rmutex);
    if (qr == _queryResourcePtr.get()) {
        _queryResourcePtr.reset();
    } else {
        LOGS(_log, LOG_LVL_ERROR, "freeQueryResource called by wrong QueryResource.");
    }
}

std::string JobQuery::toString() const {
    std::ostringstream os;
    os << *this;
    return os.str();
}

std::ostream& operator<<(std::ostream& os, JobQuery const& jq) {
    return os << "{" << jq._jobDescription << " " << *jq._jobStatus << "}";
}


}}} // end namespace
