/*
 * LSST Data Management System
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

// Class header
#include "qdisp/UberJob.h"

// System headers
#include <stdexcept>

// Qserv headers


// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.UberJob");
}

namespace lsst {
namespace qserv {
namespace qdisp {

UberJob::UberJob(Executive::Ptr const& executive,
         std::shared_ptr<ResponseHandler> const& respHandler,
         int queryId, int uberJobId)
    : _executive(executive), _respHandler(respHandler), _queryId(queryId), _uberJobId(uberJobId),
      _idStr("QID=" + to_string(_queryId) + ":uber=" + to_string(uberJobId)) {
}


bool UberJob::addJob(JobQuery* job) {
    bool success = false;
    if (job->inUberJob()) {
        throw Bug("job already in UberJob job=" + job->dump() + " uberJob=" + dump());
    }
    _jobs.push_back(job);
    job->setInUberJob(true);
    success = true;
    return success;
}


bool UberJob::runUberJob() {
    throw Bug("&&&NEED_CODE - at this point, all the jobs in UberJob must be in the payload. this can be done in this function");
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId());
    auto executive = _executive.lock();
    if (executive == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "runUberJob failed executive==nullptr");
        return false;
    }
    bool cancelled = executive->getCancelled();
    bool handlerReset = _respHandler->reset();
    bool started = _started.exchange(true)
    if (!cancelled && handlerReset && !started) {
        auto criticalErr = [this, &executive](std::string const& msg) {
            LOGS(_log, LOG_LVL_ERROR, msg << " "
                    << _jobDescription << " Canceling user query!");
            executive->squash(); // This should kill all jobs in this user query.
        };

        LOGS(_log, LOG_LVL_DEBUG, "runUberJob verifying payloads");
        for(auto const& job:_jobs) {
            if (!job->verifyPayload()) {
                criticalErr("bad payload");
                return false;
            }
        }

        // At this point we are all set to actually run the queries. We create a
        // a shared pointer to this object to prevent it from escaping while we
        // are trying to start this whole process. We also make sure we record
        // whether or not we are in SSI as cancellation handling differs.
        //
        LOGS(_log, LOG_LVL_TRACE, "runJob calls StartQuery()");
        std::shared_ptr<UberJob> uJob(shared_from_this());
        _inSsi = true;
        if (executive->startQuery(uJob)) {
            _jobStatus->updateInfo(_idStr, JobStatus::REQUEST);
            return true;
        }
        _inSsi = false;
    }
    LOGS(_log, LOG_LVL_WARN, "runUberJob failed. cancelled=" << cancelled
            << " reset=" << handlerReset << " started=" << started);
    return false;
}


void UberJob::prepScrubResults() {
    throw Bug("&&& If needed, prepScrubResults should call prepScrubResults for all JobQueries in the UberJob");
}


std::ostream& UberJob::dump(std::ostream &os) const {
    os << "(workerResource=" << workerResource
       << " jobs sz=" << _jobs.size() << "(";
    for (auto const& job:_jobs) {
        JobDescription::Ptr desc = job->getDescription();
        ResourceUnit ru = desc->resource();
        os << ru.db() << ":" << ru.chunk() << ",";
    }
    os << "))";
    return os;
}


std::string UberJob::dump() const {
    std::ostringstream os;
    dump(os);
    return os.str();
}


std::ostream& operator<<(std::ostream &os, UberJob const& uberJob) {
    return uberJob.dump(os);
}


}}} //namespace lsst::qserv::qdisp

