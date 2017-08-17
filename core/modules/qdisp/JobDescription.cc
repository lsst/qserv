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
 *
 * JobDescription.cc
 *      Author: jgates
 */

// Class header
#include "qdisp/JobDescription.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/ProtoImporter.h"
#include "proto/worker.pb.h"
#include "qdisp/ResponseHandler.h"
#include "qproc/TaskMsgFactory.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.JobDescription");
}


namespace lsst {
namespace qserv {
namespace qdisp {


JobDescription::JobDescription(QueryId qId, int jobId, ResourceUnit const& resource,
    std::shared_ptr<ResponseHandler> const& respHandler,
    std::shared_ptr<qproc::TaskMsgFactory> const& taskMsgFactory,
    std::shared_ptr<qproc::ChunkQuerySpec> const& chunkQuerySpec,
    std::string const& chunkResultName, bool mock)
    : _queryId(qId), _jobId(jobId), _qIdStr(QueryIdHelper::makeIdStr(_queryId, _jobId)),
      _resource(resource), _respHandler(respHandler),
     _taskMsgFactory(taskMsgFactory), _chunkQuerySpec(chunkQuerySpec), _chunkResultName(chunkResultName),
     _mock(mock) {
}


bool JobDescription::incrAttemptCountScrubResults() {
    if (_attemptCount >= 0) {
        _respHandler->prepScrubResults(_jobId, _attemptCount); // &&& this just needs to register job-attempt as invalid
    }
    ++_attemptCount;
    if (_attemptCount > MAX_JOB_ATTEMPTS) {
        LOGS(_log, LOG_LVL_ERROR, "attemptCount greater than maximum number of retries " << _attemptCount);
        return false;
    }
    buildPayload();
    return true;
}


void JobDescription::buildPayload() {
    std::ostringstream os;
    _taskMsgFactory->serializeMsg(*_chunkQuerySpec, _chunkResultName, _queryId, _jobId, _attemptCount, os);
    _payloads[_attemptCount] = os.str();
}


bool JobDescription::verifyPayload() const {
    proto::ProtoImporter<proto::TaskMsg> pi;
    if (!_mock && !pi.messageAcceptable(_payloads.at(_attemptCount))) {
        LOGS(_log, LOG_LVL_DEBUG, _qIdStr << " Error serializing TaskMsg.");
        return false;
    }
    return true;
}


std::ostream& operator<<(std::ostream& os, JobDescription const& jd) {
    os << "job(id=" << jd._jobId << " payloads.size=" << jd._payloads.size()
       << " ru=" << jd._resource.path() << " attemptCount="  << jd._attemptCount << ")";
    return os;
}

}}} // namespace
