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
#include "replica/services/DatabaseServices.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/services/DatabaseServicesMySQL.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServices");

}  // namespace

namespace lsst::qserv::replica {

json ControllerEvent::toJson() const {
    json event;
    event["id"] = id;
    event["controller_id"] = controllerId;
    event["timestamp"] = timeStamp;
    event["task"] = task;
    event["operation"] = operation;
    event["status"] = status;
    event["request_id"] = requestId;
    event["job_id"] = jobId;
    event["kv_info"] = json::array();

    json kvInfoJson = json::array();
    for (auto&& kv : kvInfo) {
        json kvJson;
        kvJson[kv.first] = kv.second;
        kvInfoJson.push_back(kvJson);
    }
    event["kv_info"] = kvInfoJson;
    return event;
}

json ControllerInfo::toJson(bool isCurrent) const {
    json info;
    info["id"] = id;
    info["hostname"] = hostname;
    info["pid"] = pid;
    info["start_time"] = started;
    info["current"] = isCurrent ? 1 : 0;
    return info;
}

json RequestInfo::toJson() const {
    json info;

    info["id"] = id;
    info["job_id"] = jobId;
    info["name"] = name;
    info["worker"] = worker;
    info["priority"] = priority;
    info["state"] = state;
    info["ext_state"] = extendedState;
    info["server_status"] = serverStatus;
    info["c_create_time"] = controllerCreateTime;
    info["c_start_time"] = controllerStartTime;
    info["c_finish_time"] = controllerFinishTime;
    info["w_receive_time"] = workerReceiveTime;
    info["w_start_time"] = workerStartTime;
    info["w_finish_time"] = workerFinishTime;

    json extended;
    for (auto&& itr : kvInfo) {
        json attr;
        attr[itr.first] = itr.second;
        extended.push_back(attr);
    }
    info["extended"] = extended;
    return info;
}

json JobInfo::toJson() const {
    json info;
    info["id"] = id;
    info["controller_id"] = controllerId;
    info["parent_job_id"] = parentJobId;
    info["type"] = type;
    info["state"] = state;
    info["ext_state"] = extendedState;
    info["begin_time"] = beginTime;
    info["heartbeat_time"] = heartbeatTime;
    info["priority"] = priority;

    json extended;
    for (auto&& itr : kvInfo) {
        json attr;
        attr[itr.first] = itr.second;
        extended.push_back(attr);
    }
    info["extended"] = extended;
    return info;
}

set<TransactionInfo::State> const TransactionInfo::allStates = {
        TransactionInfo::State::IS_STARTING,  TransactionInfo::State::STARTED,
        TransactionInfo::State::IS_FINISHING, TransactionInfo::State::IS_ABORTING,
        TransactionInfo::State::FINISHED,     TransactionInfo::State::ABORTED,
        TransactionInfo::State::START_FAILED, TransactionInfo::State::FINISH_FAILED,
        TransactionInfo::State::ABORT_FAILED};

set<string> TransactionInfo::toStrings(set<State> const& coll) {
    set<string> result;
    for (auto const& s : coll) {
        result.insert(state2string(s));
    }
    return result;
}
TransactionInfo::State TransactionInfo::string2state(string const& str) {
    if ("IS_STARTING" == str) return State::IS_STARTING;
    if ("STARTED" == str) return State::STARTED;
    if ("IS_FINISHING" == str) return State::IS_FINISHING;
    if ("IS_ABORTING" == str) return State::IS_ABORTING;
    if ("FINISHED" == str) return State::FINISHED;
    if ("ABORTED" == str) return State::ABORTED;
    if ("START_FAILED" == str) return State::START_FAILED;
    if ("FINISH_FAILED" == str) return State::FINISH_FAILED;
    if ("ABORT_FAILED" == str) return State::ABORT_FAILED;
    throw invalid_argument("DatabaseServices::" + string(__func__) + "  unknown transaction state: '" + str +
                           "'");
}

string TransactionInfo::state2string(State state) {
    switch (state) {
        case State::IS_STARTING:
            return "IS_STARTING";
        case State::STARTED:
            return "STARTED";
        case State::IS_FINISHING:
            return "IS_FINISHING";
        case State::IS_ABORTING:
            return "IS_ABORTING";
        case State::FINISHED:
            return "FINISHED";
        case State::ABORTED:
            return "ABORTED";
        case State::START_FAILED:
            return "START_FAILED";
        case State::FINISH_FAILED:
            return "FINISH_FAILED";
        case State::ABORT_FAILED:
            return "ABORT_FAILED";
    };
    throw runtime_error("DatabaseServices::" + string(__func__) + "  unhandled transaction state " +
                        to_string(static_cast<int>(state)));
}

bool TransactionInfo::stateTransitionIsAllowed(State currentState, State newState) {
    switch (currentState) {
        case State::IS_STARTING:
            return (newState == State::STARTED) || (newState == State::START_FAILED) ||
                   (newState == State::IS_ABORTING);
        case State::STARTED:
            return (newState == State::IS_FINISHING) || (newState == State::IS_ABORTING);
        case State::IS_FINISHING:
            return (newState == State::FINISHED) || (newState == State::FINISH_FAILED) ||
                   (newState == State::IS_ABORTING);
        case State::IS_ABORTING:
            return (newState == State::ABORTED) || (newState == State::ABORT_FAILED);
        case State::START_FAILED:
        case State::FINISH_FAILED:
        case State::ABORT_FAILED:
            return newState == State::IS_ABORTING;
        default:
            return false;
    }
}

bool TransactionInfo::isValid() const {
    return id != std::numeric_limits<TransactionId>::max() && beginTime != 0;
}

json TransactionInfo::toJson() const {
    json info;
    info["id"] = id;
    info["database"] = database;
    info["state"] = state2string(state);
    info["begin_time"] = beginTime;
    info["start_time"] = startTime;
    info["transition_time"] = transitionTime;
    info["end_time"] = endTime;
    info["context"] = context;
    info["log"] = json::array();
    for (auto&& elem : log) {
        info["log"].push_back(elem.toJson());
    }
    return info;
}

json TransactionInfo::Event::toJson() const {
    json event;
    event["id"] = id;
    event["transaction_state"] = TransactionInfo::state2string(transactionState);
    event["name"] = name;
    event["time"] = time;
    event["data"] = data;
    return event;
}

json DatabaseIngestParam::toJson() const {
    json info;
    info["database"] = database;
    info["category"] = category;
    info["param"] = param;
    info["value"] = value;
    return info;
}

TableRowStatsEntry::TableRowStatsEntry(TransactionId transactionId_, unsigned int chunk_, bool isOverlap_,
                                       size_t numRows_, uint64_t updateTime_)
        : transactionId(transactionId_),
          chunk(chunk_),
          isOverlap(isOverlap_),
          numRows(numRows_),
          updateTime(updateTime_) {}

json TableRowStatsEntry::toJson() const {
    return json::object({{"transaction_id", transactionId},
                         {"chunk", chunk},
                         {"is_overlap", isOverlap ? 1 : 0},
                         {"num_rows", numRows},
                         {"update_time", updateTime}});
}

TableRowStats::TableRowStats(string const& databaseName, string const& tableName)
        : database(databaseName), table(tableName) {}

json TableRowStats::toJson() const {
    json result = json::object({{"database", database}, {"table", table}, {"entries", json::array()}});
    json& entriesJson = result["entries"];
    for (auto&& e : entries) entriesJson.push_back(e.toJson());
    return result;
}

DatabaseServices::Ptr DatabaseServices::create(Configuration::Ptr const& config) {
    try {
        return DatabaseServices::Ptr(new DatabaseServicesMySQL(config));
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             "DatabaseServices::" << __func__ << "  failed to instantiate MySQL-based database services"
                                  << ", error: " << ex.what()
                                  << ", no such service will be available to the application.");
        throw;
    }
}

TransactionContribInfo DatabaseServices::startedTransactionContrib(
        TransactionContribInfo info, bool failed, TransactionContribInfo::Status statusOnFailed) {
    info.startTime = util::TimeUtils::now();
    info.status = failed ? statusOnFailed : TransactionContribInfo::Status::IN_PROGRESS;
    return updateTransactionContrib(info);
}

TransactionContribInfo DatabaseServices::readTransactionContrib(
        TransactionContribInfo info, bool failed, TransactionContribInfo::Status statusOnFailed) {
    info.readTime = util::TimeUtils::now();
    info.status = failed ? statusOnFailed : TransactionContribInfo::Status::IN_PROGRESS;
    return updateTransactionContrib(info);
}

TransactionContribInfo DatabaseServices::loadedTransactionContrib(
        TransactionContribInfo info, bool failed, TransactionContribInfo::Status statusOnFailed) {
    info.loadTime = util::TimeUtils::now();
    info.status = failed ? statusOnFailed : TransactionContribInfo::Status::FINISHED;
    return updateTransactionContrib(info);
}

}  // namespace lsst::qserv::replica
