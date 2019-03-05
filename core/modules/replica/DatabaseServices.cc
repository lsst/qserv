/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/DatabaseServices.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServicesMySQL.h"

using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServices");

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

json ControllerEvent::toJson() const {
    
    json event;

    event["id"]            = id;
    event["controller_id"] = controllerId;
    event["timestamp"]     = timeStamp;
    event["task"]          = task;
    event["operation"]     = operation;
    event["status"]        = status;
    event["request_id"]    = requestId;
    event["job_id"]        = jobId;
    event["kv_info"]       = json::array();
    
    json kvInfoJson = json::array();
    for (auto&& kv: kvInfo) {
        json kvJson;
        kvJson[kv.first] = kv.second;
        kvInfoJson.push_back(kvJson);
    }
    event["kv_info"] = kvInfoJson;
    return event;
}


json ControllerInfo::toJson(bool isCurrent) const {

    json info;

    info["id"]         = id;
    info["hostname"]   = hostname;
    info["pid"]        = pid;
    info["start_time"] = started;
    info["current"]    = isCurrent ? 1 : 0;

    return info;
}


json RequestInfo::toJson() const {

    json info;

    info["id"]             = id;
    info["job_id"]         = jobId;
    info["name"]           = name;
    info["worker"]         = worker;
    info["priority"]       = priority;
    info["state"]          = state;
    info["ext_state"]      = extendedState;
    info["server_status"]  = serverStatus;
    info["c_create_time"]  = controllerCreateTime;
    info["c_start_time"]   = controllerStartTime;
    info["c_finish_time"]  = controllerFinishTime;
    info["w_receive_time"] = workerReceiveTime;
    info["w_start_time"]   = workerStartTime;
    info["w_finish_time"]  = workerFinishTime;

    json extended;
    for (auto&& itr: kvInfo) {
        json attr;
        attr[itr.first] = itr.second;
        extended.push_back(attr);
    }
    info["extended"] = extended;

    return info;
}


json JobInfo::toJson() const {

    json info;

    info["id"]             = id;
    info["controller_id"]  = controllerId;
    info["parent_job_id"]  = parentJobId;
    info["type"]           = type;
    info["state"]          = state;
    info["ext_state"]      = extendedState;
    info["begin_time"]     = beginTime;
    info["heartbeat_time"] = heartbeatTime;
    info["priority"]       = priority;
    info["exclusive"]      = exclusive   ? 1 : 0;
    info["preemptable"]    = preemptable ? 1 : 0;

    json extended;
    for (auto&& itr: kvInfo) {
        json attr;
        attr[itr.first] = itr.second;
        extended.push_back(attr);
    }
    info["extended"] = extended;

    return info;
}


DatabaseServices::Ptr DatabaseServices::create(Configuration::Ptr const& configuration) {

    // If the configuration is pulled from a database then *try*
    // using the corresponding technology.

    if ("mysql" == configuration->databaseTechnology()) {
        try {
            return DatabaseServices::Ptr(new DatabaseServicesMySQL(configuration));
        } catch (database::mysql::Error const& ex) {
            LOGS(_log, LOG_LVL_ERROR,
                 "DatabaseServices::  failed to instantiate MySQL-based database services"
                 << ", error: " << ex.what()
                 << ", no such service will be available to the application.");
             throw std::runtime_error(
                 "DatabaseServices::  failed to instantiate MySQL-based database services, error: " +
                 std::string(ex.what()));
        }
    }
    throw std::runtime_error(
        "DatabaseServices::  no suitable plugin found for database technology: " +
        configuration->databaseTechnology());
}

}}} // namespace lsst::qserv::replica
