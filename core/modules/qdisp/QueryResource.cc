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
  * @brief QueryResource. An XrdSsiService::Resource
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "qdisp/QueryResource.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qdisp/JobStatus.h"
#include "qdisp/QueryRequest.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.QueryResource");
}

namespace lsst {
namespace qserv {
namespace qdisp {

QueryResource::QueryResource(std::shared_ptr<JobQuery> const& jobQuery)
  : Resource(::strdup(jobQuery->getDescription().resource().path().c_str())),
      _jobQuery(jobQuery), _jobId(jobQuery->getIdStr()) {
    LOGS(_log, LOG_LVL_DEBUG, _jobId << " QueryResource JQ_jobId");
}

QueryResource::~QueryResource() {
    LOGS(_log, LOG_LVL_DEBUG, "~QueryResource() JQ_jobId=" << _jobId);
    std::free(const_cast<char*>(rName));
}

/// May not throw exceptions because the calling code comes from
/// xrootd land and will not catch any exceptions.
void QueryResource::ProvisionDone(XrdSsiSession* s) {
    LOGF_DEBUG("QueryResource::ProvisionDone JQ_jobId=%1%" % _jobId);
    struct Destroyer {
        Destroyer(JobQuery::Ptr const& job, QueryResource* qr)
        : _job{job},  _qr{qr} {}
        ~Destroyer() {
            // This should cause this QueryResource to be destroyed.
            _job->freeQueryResource(_qr);
        }
    private:
        JobQuery::Ptr _job;
        QueryResource* _qr;
    };
    Destroyer destroyer(_jobQuery, this);
    if(!s) {
        // Check eInfo in resource for error details
        int code = 0;
        std::string msg = eInfoGet(code);
        _jobQuery->provisioningFailed(msg, code);
        return;
    }
    if(isCancelled()) {
        return; // Don't bother doing anything if the job is cancelled.
    }
    _xrdSsiSession = s;

    QueryRequest::Ptr qr = std::make_shared<QueryRequest>(s, _jobQuery);
    _jobQuery->setQueryRequest(qr);

    // Hand off the request.
    _jobQuery->getStatus()->updateInfo(JobStatus::REQUEST);
    _xrdSsiSession->ProcessRequest(qr.get()); // xrootd will not delete the QueryRequest.
    // There are no more requests for this session.
}

const char* QueryResource::eInfoGet(int &code) {
    char const* message = eInfo.Get(code);
    return message ? message : "no message from XrdSsi, code may not be reliable";
}

bool QueryResource::isCancelled() {
    return _jobQuery->isCancelled();
}

}}} // lsst::qserv::qdisp
