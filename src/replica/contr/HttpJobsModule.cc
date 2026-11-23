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
#include "replica/contr/HttpJobsModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "replica/config/Config.h"
#include "replica/config/ConfigDatabaseFamily.h"
#include "replica/contr/Controller.h"
#include "replica/jobs/RebalanceJob.h"
#include "replica/jobs/PurgeJob.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void HttpJobsModule::process(Controller::Ptr const& controller, string const& taskName,
                             qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                             string const& subModuleName, http::AuthType const authType) {
    HttpJobsModule module(controller, taskName, req, resp);
    module.execute(subModuleName, authType);
}

HttpJobsModule::HttpJobsModule(Controller::Ptr const& controller, string const& taskName,
                               qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, req, resp) {}

json HttpJobsModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty())
        return _jobs();
    else if (subModuleName == "SELECT-ONE-BY-ID")
        return _oneJob();
    else if (subModuleName == "SUBMIT")
        return _submit();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpJobsModule::_jobs() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const controllerId = query().optionalString("controller_id");
    string const parentJobId = query().optionalString("parent_job_id");
    uint64_t const fromTimeStamp = query().optionalUInt64("from");
    uint64_t const toTimeStamp = query().optionalUInt64("to", numeric_limits<uint64_t>::max());
    size_t const maxEntries = query().optionalUInt64("max_entries");

    debug(string(__func__) + " controller_id=" + controllerId);
    debug(string(__func__) + " parent_job_id=" + parentJobId);
    debug(string(__func__) + " from=" + to_string(fromTimeStamp));
    debug(string(__func__) + " to=" + to_string(toTimeStamp));
    debug(string(__func__) + " max_entries=" + to_string(maxEntries));

    // Pull descriptions of the Jobs

    auto const jobs = controller()->serviceProvider()->databaseServices()->jobs(
            controllerId, parentJobId, fromTimeStamp, toTimeStamp, maxEntries);

    json jobsJson;
    for (auto&& info : jobs) {
        jobsJson.push_back(info.toJson());
    }
    json result;
    result["jobs"] = jobsJson;
    return result;
}

json HttpJobsModule::_oneJob() {
    debug(__func__);
    checkApiVersion(__func__, 58);

    auto const id = params().at("id");
    try {
        json result;
        result["job"] = controller()->serviceProvider()->databaseServices()->job(id).toJson();
        return result;

    } catch (DatabaseServicesNotFound const& ex) {
        throw http::ErrorNotFound404(__func__, "no such job found, id: " + id);
    }
}

json HttpJobsModule::_submit() {
    debug(__func__);
    checkApiVersion(__func__, 63);

    auto const jobType = body().required<string>("type");
    auto const databaseFamilyName = body().required<string>("database_family");

    debug(__func__, "type=" + jobType);
    debug(__func__, "database_family=" + databaseFamilyName);

    // This will throw an exception if no such entry existsor if the name is empty.
    auto const databaseFamily =
            controller()->serviceProvider()->config()->databaseFamilyInfo(databaseFamilyName);

    if (jobType == "PurgeJob") {
        return _submitPurgeJob(databaseFamily);
    } else if (jobType == "RebalanceJob") {
        return _submitRebalanceJob(databaseFamily);
    }
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported job type: '" + jobType + "'");
}

json HttpJobsModule::_submitPurgeJob(DatabaseFamilyInfo const& databaseFamily) {
    unsigned int numReplicas = body().optional<unsigned int>("num_replicas", 0);
    debug(__func__, "num_replicas=" + to_string(numReplicas));

    string const noParentJobId;
    PurgeJob::CallbackType const onFinish = nullptr;
    int const priority = PRIORITY_NORMAL;

    auto const job = PurgeJob::create(databaseFamily.name, numReplicas, controller(), noParentJobId, onFinish,
                                      priority);
    job->start();
    logJobStartedEvent(PurgeJob::typeName(), job, databaseFamily.name);
    job->wait();
    logJobFinishedEvent(PurgeJob::typeName(), job, databaseFamily.name);

    auto const& resultData = job->getReplicaData();
    json result = json::object();
    result["job_result"] = json::object({{"state", Job::state2string(job->state(), job->extendedState())},
                                         {"progress", job->progress().toJson()},
                                         {"result", resultData.toJson()}});
    return result;
}

json HttpJobsModule::_submitRebalanceJob(DatabaseFamilyInfo const& databaseFamily) {
    auto const estimateOnly = body().optional<int>("estimate_only", 0) != 0;
    debug(__func__, "estimate_only=" + string(estimateOnly ? "1" : "0"));

    string const noParentJobId;
    RebalanceJob::CallbackType const onFinish = nullptr;
    int const priority = PRIORITY_NORMAL;

    auto const job = RebalanceJob::create(databaseFamily.name, estimateOnly, controller(), noParentJobId,
                                          onFinish, priority);
    job->start();
    logJobStartedEvent(RebalanceJob::typeName(), job, databaseFamily.name);
    job->wait();
    logJobFinishedEvent(RebalanceJob::typeName(), job, databaseFamily.name);

    auto const& resultData = job->getReplicaData();
    json result = json::object();
    result["job_result"] = json::object({{"state", Job::state2string(job->state(), job->extendedState())},
                                         {"progress", job->progress().toJson()},
                                         {"result", resultData.toJson()}});
    return result;
}

}  // namespace lsst::qserv::replica
