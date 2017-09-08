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
  : Resource(_getRname(jobQuery->getDescription()->resource().path())),
      _jobQuery(jobQuery), _jobIdStr(jobQuery->getIdStr()) {
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << " QueryResource");
}

// This method is here to hold storage for the "c" string passed to the
// resource object until this object is deleted. Do *NOT* remove this code.
//
char const* QueryResource::_getRname(std::string const& rName) {
    _rNameHolder = strdup(rName.c_str());
    return _rNameHolder;
}

QueryResource::~QueryResource() {
    LOGS(_log, LOG_LVL_DEBUG, _jobIdStr << "~QueryResource()");
    free(_rNameHolder);
    _rNameHolder = nullptr;
}

/// May not throw exceptions because the calling code comes from
/// xrootd land and will not catch any exceptions.
void QueryResource::ProvisionDone(XrdSsiSession* s) {
    LOGS_DEBUG(_jobIdStr << " QueryResource::ProvisionDone");
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
    if (!s) {
        LOGS_WARN(_jobIdStr << "QueryResource::ProvisionDone - NO SESSION");
        // Check eInfo in resource for error details
        int code = 0;
        std::string msg = eInfoGet(code);
        _jobQuery->provisioningFailed(msg, code);
        return;
    }
    if (isQueryCancelled()) {
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


bool QueryResource::isQueryCancelled() {
    return _jobQuery->isQueryCancelled();
}



}}} // lsst::qserv::qdisp
