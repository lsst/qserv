/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
void logErr(std::string const& msg, JobQuery* jq) {
    std::ostringstream os;
    os << msg << " " << *jq;
    LOGF_ERROR("%1%" % os.str());
}
}

/** Attempt to run the job on a worker.
 * @return - false if it can not setup the job or the maximum number of retries has been reached.
 */
bool JobQuery::runJob() {
    LOGF_DEBUG("runJob %1%" % toString());
    if (!_executive) {
        logErr("runJob failed _executive=NULL", this);
        return false;
    }
    bool cancelled = _executive->getCancelled();
    bool handlerReset = _jobDescription.respHandler()->reset();
    if (!cancelled && handlerReset) {
        auto qr = std::make_shared<QueryResource>(shared_from_this());
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        if (shouldRetry()) {
            ++_attemptsToRun;
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
        std::ostringstream os;
        os << "JobQuery Failed to RunJob failed. cancelled=" << cancelled << " reset=" << handlerReset;
        LOGF_WARN("%1%" % os.str());
    }
    return false;
}

void JobQuery::provisioningFailed(std::string const& msg, int code) {
    std::ostringstream os;
    os << "Error provisioning, msg=" << msg << " code=" << code << " " << *this;
    LOGF_ERROR("%1%" % os.str());
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

std::string JobQuery::toString() const {
    std::ostringstream os;
    os << *this;
    return os.str();
}
std::ostream& operator<<(std::ostream& os, JobQuery const& jq) {
    return os << "{" << jq._jobDescription << " " << *jq._jobStatus << "}";
}


}}} // end namespace
