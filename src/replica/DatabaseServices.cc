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
#include "replica/DatabaseServices.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServicesMySQL.h"
#include "replica/Performance.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

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


TransactionInfo::State TransactionInfo::string2state(string const& str) {
    if ("STARTED"  == str) return STARTED;
    if ("FINISHED" == str) return FINISHED;
    if ("ABORTED"  == str) return ABORTED;
    throw runtime_error(
            "DatabaseServices::" + string(__func__) + "  unknown transaction state: '"
            + str + "'");
}


string TransactionInfo::state2string(State state) {
    switch (state) {
        case STARTED:  return "STARTED";
        case FINISHED: return "FINISHED";
        case ABORTED:  return "ABORTED";
    };
    throw runtime_error(
            "DatabaseServices::" + string(__func__) + "  unhandled transaction state");
}


json TransactionInfo::toJson() const {
    json info;
    info["id"]         = id;
    info["database"]   = database;
    info["state"]      = state2string(state);
    info["begin_time"] = beginTime;
    info["end_time"]   = endTime;
    info["context"]    = context;
    return info;
}


map<TransactionContribInfo::Status,string> const TransactionContribInfo::_transactionContribStatus2str = {
    {TransactionContribInfo::Status::IN_PROGRESS,   "IN_PROGRESS"},
    {TransactionContribInfo::Status::CREATE_FAILED, "CREATE_FAILED"},
    {TransactionContribInfo::Status::START_FAILED,  "START_FAILED"},
    {TransactionContribInfo::Status::READ_FAILED,   "READ_FAILED"},
    {TransactionContribInfo::Status::LOAD_FAILED,   "LOAD_FAILED"},
    {TransactionContribInfo::Status::CANCELLED,     "CANCELLED"},
    {TransactionContribInfo::Status::FINISHED,      "FINISHED"}
};


map<string, TransactionContribInfo::Status> const TransactionContribInfo::_transactionContribStr2status = {
    {"IN_PROGRESS",   TransactionContribInfo::Status::IN_PROGRESS},
    {"CREATE_FAILED", TransactionContribInfo::Status::CREATE_FAILED},
    {"START_FAILED",  TransactionContribInfo::Status::START_FAILED},
    {"READ_FAILED",   TransactionContribInfo::Status::READ_FAILED},
    {"LOAD_FAILED",   TransactionContribInfo::Status::LOAD_FAILED},
    {"CANCELLED",     TransactionContribInfo::Status::CANCELLED},
    {"FINISHED",      TransactionContribInfo::Status::FINISHED}
};


vector<TransactionContribInfo::Status> const TransactionContribInfo::_transactionContribStatusCodes = {
    TransactionContribInfo::Status::IN_PROGRESS,
    TransactionContribInfo::Status::CREATE_FAILED,
    TransactionContribInfo::Status::START_FAILED,
    TransactionContribInfo::Status::READ_FAILED,
    TransactionContribInfo::Status::LOAD_FAILED,
    TransactionContribInfo::Status::CANCELLED,
    TransactionContribInfo::Status::FINISHED
};


string const& TransactionContribInfo::status2str(TransactionContribInfo::Status status) {
    auto itr = _transactionContribStatus2str.find(status);
    if (itr == _transactionContribStatus2str.cend()) {
        throw invalid_argument(
                "DatabaseServices::" + string(__func__) + "  unknown status code: "
                + to_string(static_cast<int>(status)));
    }
    return itr->second;
}


TransactionContribInfo::Status TransactionContribInfo::str2status(string const& str) {
    auto itr = _transactionContribStr2status.find(str);
    if (itr == _transactionContribStr2status.cend()) {
        throw invalid_argument(
                "DatabaseServices::" + string(__func__) + "  unknown status name: " + str);
    }
    return itr->second;
}


std::vector<TransactionContribInfo::Status> const& TransactionContribInfo::statusCodes() {
    return _transactionContribStatusCodes;
}


json TransactionContribInfo::toJson() const {
    json info;
    info["id"] = id;
    info["transaction_id"] = transactionId;
    info["worker"]   = worker;
    info["database"] = database;
    info["table"]    = table;
    info["chunk"]    = chunk;
    info["overlap"]  = isOverlap ? 1 : 0;
    info["url"]      = url;

    info["async"] = async ? 1 : 0;

    info["dialect_input"] = dialectInput.toJson();

    info["http_method"]  = httpMethod;
    info["http_data"]    = httpData;
    info["http_headers"] = json(httpHeaders);

    info["num_bytes"] = numBytes;
    info["num_rows"]  = numRows;

    info["create_time"] = createTime;
    info["start_time"]  = startTime;
    info["read_time"]   = readTime;
    info["load_time"]   = loadTime;

    info["status"]        = TransactionContribInfo::status2str(status);
    info["tmp_file"]      = tmpFile;
    info["http_error"]    = httpError;
    info["system_error"]  = systemError;
    info["error"]         = error;
    info["retry_allowed"] = retryAllowed ? 1 : 0;
    return info;
}


json DatabaseIngestParam::toJson() const {
    json info;
    info["database"] = database;
    info["category"] = category;
    info["param"]    = param;
    info["value"]    = value;
    return info;
}


DatabaseServices::Ptr DatabaseServices::create(Configuration::Ptr const& config) {
    try {
        return DatabaseServices::Ptr(new DatabaseServicesMySQL(config));
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
                "DatabaseServices::" << __func__
                << "  failed to instantiate MySQL-based database services"
                << ", error: " << ex.what()
                << ", no such service will be available to the application.");
        throw;
    }
}


TransactionContribInfo DatabaseServices::startedTransactionContrib(
        TransactionContribInfo info,
        bool failed,
        TransactionContribInfo::Status statusOnFailed) {

    info.startTime = PerformanceUtils::now();
    info.status = failed ? statusOnFailed : TransactionContribInfo::Status::IN_PROGRESS;
    return updateTransactionContrib(info);
}


TransactionContribInfo DatabaseServices::readTransactionContrib(
        TransactionContribInfo info,
        bool failed,
        TransactionContribInfo::Status statusOnFailed) {

    info.readTime = PerformanceUtils::now();
    info.status = failed ? statusOnFailed : TransactionContribInfo::Status::IN_PROGRESS;
    return updateTransactionContrib(info);
}


TransactionContribInfo DatabaseServices::loadedTransactionContrib(
        TransactionContribInfo info,
        bool failed,
        TransactionContribInfo::Status statusOnFailed) {

    info.loadTime = PerformanceUtils::now();
    info.status = failed ? statusOnFailed : TransactionContribInfo::Status::FINISHED;
    return updateTransactionContrib(info);
}

}}} // namespace lsst::qserv::replica
