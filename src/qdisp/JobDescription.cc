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
#include "util/Bug.h"
#include "qdisp/ResponseHandler.h"
#include "qproc/ChunkQuerySpec.h"
#include "qproc/TaskMsgFactory.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.JobDescription");
}

namespace lsst::qserv::qdisp {

JobDescription::JobDescription(qmeta::CzarId czarId, QueryId qId, int jobId, ResourceUnit const& resource,
                               shared_ptr<ResponseHandler> const& respHandler,
                               shared_ptr<qproc::TaskMsgFactory> const& taskMsgFactory,
                               shared_ptr<qproc::ChunkQuerySpec> const& chunkQuerySpec,
                               string const& chunkResultName, bool mock)
        : _czarId(czarId),
          _queryId(qId),
          _jobId(jobId),
          _qIdStr(QueryIdHelper::makeIdStr(_queryId, _jobId)),
          _resource(resource),
          _respHandler(respHandler),
          _taskMsgFactory(taskMsgFactory),
          _chunkQuerySpec(chunkQuerySpec),
          _chunkResultName(chunkResultName),
          _mock(mock) {}

bool JobDescription::incrAttemptCount() {
    ++_attemptCount;
    if (_attemptCount > MAX_JOB_ATTEMPTS) {
        LOGS(_log, LOG_LVL_ERROR, "attemptCount greater than maximum number of retries " << _attemptCount);
        return false;
    }
    buildPayload();
    return true;
}

bool JobDescription::incrAttemptCountScrubResultsJson() {
#if 0   //&&&uj this block needs to be reenabled but attempts need to be handled differently ???
    //&&&uj attempt failures generally result from communictaion problems. SQL errors kill the query.
    //&&&uj so lots of failed attempts indicate that qserv is unstable.
    if (_attemptCount >= 0) {
        _respHandler->prepScrubResults(_jobId, _attemptCount);  //
    }
    ++_attemptCount;
    if (_attemptCount > MAX_JOB_ATTEMPTS) {
        LOGS(_log, LOG_LVL_ERROR, "attemptCount greater than maximum number of retries " << _attemptCount);
        return false;
    }
#endif  // &&&

    ++_attemptCount;
    if (_attemptCount > MAX_JOB_ATTEMPTS) {
        LOGS(_log, LOG_LVL_ERROR, "attemptCount greater than maximum number of retries " << _attemptCount);
        return false;
    }
    // build the request
    //_payloads[_attemptCount] = os.str();
    auto js = _taskMsgFactory->makeMsgJson(*_chunkQuerySpec, _chunkResultName, _queryId, _jobId,
                                           _attemptCount, _czarId);
    LOGS(_log, LOG_LVL_ERROR, "&&& JobDescription::incrAttemptCountScrubResultsJson js=" << (*js));
    _jsForWorker = js;

    return true;
}

void JobDescription::buildPayload() {
    ostringstream os;
    _taskMsgFactory->serializeMsg(*_chunkQuerySpec, _chunkResultName, _queryId, _jobId, _attemptCount,
                                  _czarId, os);
    _payloads[_attemptCount] = os.str();
}

bool JobDescription::fillTaskMsg(proto::TaskMsg* tMsg) {  //&&&uj -probably just delete.
    //&&&uj return _taskMsgFactory->fillTaskMsg(tMsg, *_chunkQuerySpec, _chunkResultName, _queryId,
    //_jobId, _attemptCount, _czarId);
    util::Bug(ERR_LOC, "&&& JobDescription::fillTaskMsg");
    return false;
}

bool JobDescription::verifyPayload() const {  //&&&uj - is there any value to this now?
    proto::ProtoImporter<proto::TaskMsg> pi;
    if (!_mock && !pi.messageAcceptable(_payloads.at(_attemptCount))) {
        LOGS(_log, LOG_LVL_DEBUG, _qIdStr << " Error serializing TaskMsg.");
        return false;
    }
    return true;
}

bool JobDescription::getScanInteractive() const { return _chunkQuerySpec->scanInteractive; }

int JobDescription::getScanRating() const { return _chunkQuerySpec->scanInfo.scanRating; }

ostream& operator<<(ostream& os, JobDescription const& jd) {
    os << "job(id=" << jd._jobId << " payloads.size=" << jd._payloads.size() << " ru=" << jd._resource.path()
       << " attemptCount=" << jd._attemptCount << ")";
    return os;
}

}  // namespace lsst::qserv::qdisp
