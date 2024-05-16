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
 * MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the
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

// Third-party headers
#include <google/protobuf/arena.h>
#include "nlohmann/json.hpp"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "global/LogContext.h"
#include "http/Client.h"
#include "http/MetaModule.h"
#include "proto/ProtoImporter.h"
#include "proto/worker.pb.h"
#include "qdisp/JobQuery.h"
#include "util/Bug.h"
#include "util/common.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.UberJob");
}

namespace lsst { namespace qserv { namespace qdisp {

UberJob::Ptr UberJob::create(Executive::Ptr const& executive,
                             std::shared_ptr<ResponseHandler> const& respHandler, int queryId, int uberJobId,
                             qmeta::CzarId czarId,
                             czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData) {
    UberJob::Ptr uJob(new UberJob(executive, respHandler, queryId, uberJobId, czarId, workerData));
    uJob->_setup();
    return uJob;
}

UberJob::UberJob(Executive::Ptr const& executive, std::shared_ptr<ResponseHandler> const& respHandler,
                 int queryId, int uberJobId, qmeta::CzarId czarId,
                 czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData)
        : JobBase(),
          _executive(executive),
          _respHandler(respHandler),
          _queryId(queryId),
          _uberJobId(uberJobId),
          _czarId(czarId),
          _idStr("QID=" + to_string(_queryId) + ":uber=" + to_string(uberJobId)),
          _workerData(workerData) {
    _qdispPool = executive->getQdispPool();
    _jobStatus = make_shared<JobStatus>();
}

bool UberJob::addJob(JobQuery* job) {
    bool success = false;
    if (job->inUberJob()) {
        throw util::Bug(ERR_LOC, string("job already in UberJob job=") + job->dump() + " uberJob=" + dump());
    }
    _jobs.push_back(job);
    job->setInUberJob(true);
    success = true;
    return success;
}

bool UberJob::runUberJob() {
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() start");
    // &&&uj most, if not all, of this should be done in a command in the QDispPool.
    // &&&uk especially the communication parts.
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getIdInt());
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() a");
    // Build the uberjob payload for each job.
    nlohmann::json uj;
    for (auto const& jqPtr : _jobs) {
        LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() a1");
        jqPtr->getDescription()->incrAttemptCountScrubResultsJson();
    }

    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() b");
    // Send the uberjob to the worker
    auto const method = http::Method::POST;
    string const url = "http://" + _wContactInfo->wHost + ":" + to_string(_wContactInfo->wPort) + "/queryjob";
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() c " << url);
    vector<string> const headers = {"Content-Type: application/json"};
    auto const& czarConfig = cconfig::CzarConfig::instance();
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() c");
    // See xrdsvc::httpWorkerCzarModule::_handleQueryJob for json message parsing.
    json request = {{"version", http::MetaModule::version},
                    {"instance_id", czarConfig->replicationInstanceId()},
                    {"auth_key", czarConfig->replicationAuthKey()},
                    {"worker", _wContactInfo->wId},
                    {"czar",
                     {{"name", czarConfig->name()},
                      {"id", czarConfig->id()},
                      {"management-port", czarConfig->replicationHttpPort()},
                      {"management-host-name", util::get_current_host_fqdn()}}},
                    {"uberjob",
                     {{"queryid", _queryId},
                      {"uberjobid", _uberJobId},
                      {"czarid", _czarId},
                      {"jobs", json::array()}}}};

    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() d " << request);
    auto& jsUberJob = request["uberjob"];
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() e " << jsUberJob);
    auto& jsJobs = jsUberJob["jobs"];
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() f " << jsJobs);
    for (auto const& jbPtr : _jobs) {
        LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() f1");
        json jsJob = {{"jobdesc", *(jbPtr->getDescription()->getJsForWorker())}};
        jsJobs.push_back(jsJob);
        jbPtr->getDescription()->resetJsForWorker();  // no longer needed.
    }
    LOGS(_log, LOG_LVL_WARN, "&&& UberJob::runUberJob() g");
    LOGS(_log, LOG_LVL_WARN, __func__ << " &&&REQ " << request);
    string const requestContext = "Czar: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_TRACE,
         __func__ << " czarPost url=" << url << " request=" << request.dump() << " headers=" << headers[0]);
    http::Client client(method, url, request.dump(), headers);
    bool transmitSuccess = false;
    try {
        json const response = client.readAsJson();
        LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::runUberJob() response=" << response);
        if (0 != response.at("success").get<int>()) {
            LOGS(_log, LOG_LVL_WARN, "&&&uj UberJob::runUberJob() success");
            transmitSuccess = true;
        } else {
            LOGS(_log, LOG_LVL_WARN, "&&&uj NEED CODE UberJob::runUberJob() success=0");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, requestContext + " &&&uj failed, ex: " + ex.what());
    }
    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR, "&&&uj NEED CODE UberJob::runUberJob() need to try to send jobs elsewhere");
    } else {
        LOGS(_log, LOG_LVL_WARN,
             "&&&uj NEED CODE UberJob::runUberJob() need to register all jobs as transmitted to worker");
    }

#if 0   // &&&uj Everything in this block needs to happen in some manner. Where is the question
    // For simplicity in the first pass, just make a TaskMsg for each Job and append it to the
    // UberJobMsg.
    //         This is terribly inefficient and should be replaced by using a template and list of chunks that
    //         the worker fills in, much like subchunks are done now.
    {
        google::protobuf::Arena arena;
        proto::UberJobMsg* ujMsg = google::protobuf::Arena::CreateMessage<proto::UberJobMsg>(&arena);
        ujMsg->set_queryid(getQueryId());
        ujMsg->set_czarid(_czarId);
        ujMsg->set_uberjobid(_uberJobId);
        ujMsg->set_magicnumber(UberJob::getMagicNumber());
        LOGS(_log, LOG_LVL_INFO, "&&& runUberJob sz=" << _jobs.size());
        for (auto&& job : _jobs) {
            proto::TaskMsg* tMsg = ujMsg->add_taskmsgs();
            job->getDescription()->fillTaskMsg(tMsg);
        }
        ujMsg->SerializeToString(&_payload);
    }

    auto executive = _executive.lock();
    if (executive == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "runUberJob failed executive==nullptr");
        return false;
    }
    bool cancelled = executive->getCancelled();
    bool handlerReset = _respHandler->reset();
    bool started = _started.exchange(true);
    if (!cancelled && handlerReset && !started) {
        auto criticalErr = [this, &executive](std::string const& msg) {
            LOGS(_log, LOG_LVL_ERROR, msg << " " << *this << " Canceling user query!");
            executive->squash();  // This should kill all jobs in this user query.
        };

        LOGS(_log, LOG_LVL_DEBUG, "runUberJob verifying payloads");
        if (!verifyPayload()) {
            criticalErr("bad payload");
            return false;
        }

        // At this point we are all set to actually run the queries. We create a
        // a shared pointer to this object to prevent it from escaping while we
        // are trying to start this whole process. We also make sure we record
        // whether or not we are in SSI as cancellation handling differs.
        //
        LOGS(_log, LOG_LVL_TRACE, "runUberJob calls StartQuery()");
        std::shared_ptr<UberJob> uJob(dynamic_pointer_cast<UberJob>(shared_from_this()));
        _inSsi = true;
        if (executive->startUberJob(uJob)) {
            //&&&_jobStatus->updateInfo(_idStr, JobStatus::REQUEST);
            _jobStatus->updateInfo(_idStr, JobStatus::REQUEST, "EXEC");
            return true;
        }
        _inSsi = false;
    }
    LOGS(_log, LOG_LVL_WARN,
         "runUberJob failed. cancelled=" << cancelled << " reset=" << handlerReset << " started=" << started);
#endif  // &&&
    return false;
}

void UberJob::prepScrubResults() {
    // &&&uj There's a good chance this will not be needed as incomplete files will not be merged
    //       so you don't have to worry about removing rows from incomplete jobs or uberjobs
    //       from the result table.
    throw util::Bug(ERR_LOC,
                    "&&&uj If needed, should call prepScrubResults for all JobQueries in the UberJob ");
}

bool UberJob::isQueryCancelled() {
    QSERV_LOGCONTEXT_QUERY_JOB(getQueryId(), getIdInt());
    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "_executive == nullptr");
        return true;  // Safer to assume the worst.
    }
    return exec->getCancelled();
}

bool UberJob::verifyPayload() const {
    proto::ProtoImporter<proto::UberJobMsg> pi;
    if (!pi.messageAcceptable(_payload)) {
        LOGS(_log, LOG_LVL_DEBUG, _idStr << " Error serializing UberJobMsg.");
        return false;
    }
    return true;
}

void UberJob::callMarkCompleteFunc(bool success) {
    LOGS(_log, LOG_LVL_DEBUG, "UberJob::callMarkCompleteFunc success=" << success);
    if (!success) {
        throw util::Bug(ERR_LOC, "&&&NEED_CODE may need code to properly handle failed uberjob");
    }
    for (auto&& job : _jobs) {
        string idStr = job->getIdStr();
        job->getStatus()->updateInfo(idStr, JobStatus::COMPLETE, "COMPLETE");
        job->callMarkCompleteFunc(success);
    }
}

std::ostream& UberJob::dumpOS(std::ostream& os) const {
    os << "(jobs sz=" << _jobs.size() << "(";
    for (auto const& job : _jobs) {
        JobDescription::Ptr desc = job->getDescription();
        ResourceUnit ru = desc->resource();
        os << ru.db() << ":" << ru.chunk() << ",";
    }
    os << "))";
    return os;
}

}}}  // namespace lsst::qserv::qdisp
