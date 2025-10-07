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
#include "replica/contr/HttpQservMonitorModule.h"

// System headers
#include <sstream>
#include <stdexcept>
#include <vector>

// Third party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "css/ScanTableParams.h"
#include "global/intTypes.h"
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "mysql/MySqlConfig.h"
#include "qmeta/UserTables.h"
#include "qmeta/UserTableIngestRequest.h"
#include "qmeta/types.h"
#include "replica/config/Configuration.h"
#include "replica/config/ConfigDatabase.h"
#include "replica/jobs/QservStatusJob.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLTypes.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/qserv/QservMgtServices.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"
#include "util/String.h"
#include "wbase/TaskState.h"

// LSST headers
#include "partition/Chunker.h"
#include "partition/Geometry.h"
#include "lsst/sphgeom/Chunker.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv;
using namespace lsst::qserv::replica;
using namespace lsst::qserv::replica::database::mysql;

namespace {

/**
 * Extract a value of field from a result set and store it
 *
 * @param context the context for error reporting
 * @param row     the current row from the result set
 * @param column  the name of a column
 * @param obj     JSON object where to store the values
 *
 * @throws invalid_argument  if the column is not present in a result set
 * or the value of the field is 'NULL'.
 */
template <typename T>
void parseFieldIntoJson(string const& context, Row const& row, string const& column, json& obj) {
    T val;
    if (!row.get(column, val)) {
        throw invalid_argument(context + " no column '" + column + "' found in the result set");
    }
    obj[column] = val;
}

/**
 * The complementary version of the above defined function which replaces
 * 'NULL' found in a field with the specified default value.
 */
template <typename T>
void parseFieldIntoJson(string const& context, Row const& row, string const& column, json& obj,
                        T const& defaultValue) {
    if (row.isNull(column)) {
        obj[column] = defaultValue;
        return;
    }
    parseFieldIntoJson<T>(context, row, column, obj);
}

/**
 * Extract rows selected from table qservMeta.QInfo into a JSON object.
 */
void extractQInfo(Connection::Ptr const& conn, json& result) {
    if (!conn->hasResult()) return;

    Row row;
    while (conn->next(row)) {
        QueryId queryId;
        if (!row.get("queryId", queryId)) continue;

        string query, status, submitted, completed;
        row.get("query", query);
        row.get("status", status);
        row.get("submitted", submitted);
        row.get("completed", completed);

        string const queryIdStr = to_string(queryId);
        result[queryIdStr]["query"] = query;
        result[queryIdStr]["status"] = status;
        result[queryIdStr]["submitted"] = submitted;
        result[queryIdStr]["completed"] = completed;
    }
}

/**
 * Such explicit conversion is required because the JSON library doesn't support
 * numeric keys in the JSON objects. The keys have to be turned into strings.
 */
json czarIdsToJson(map<qmeta::CzarId, string> const& ids) {
    json result = json::object();
    for (auto&& [id, name] : ids) {
        result[to_string(id)] = name;
    }
    return result;
}

/**
 * Create a MySQL configuration for connecting to the Czar's QMeta database.
 */
lsst::qserv::mysql::MySqlConfig czarQMetaConfig() {
    database::mysql::ConnectionParams const params = Configuration::qservCzarDbParams("qservMeta");
    string const noSocket;
    return lsst::qserv::mysql::MySqlConfig(params.user, params.password, params.host, params.port, noSocket,
                                           params.database);
}

}  // namespace

namespace lsst::qserv::replica {

void HttpQservMonitorModule::process(Controller::Ptr const& controller, string const& taskName,
                                     HttpProcessorConfig const& processorConfig,
                                     qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                     string const& subModuleName, http::AuthType const authType) {
    HttpQservMonitorModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

void HttpQservMonitorModule::_throwIfNotSucceeded(string const& func,
                                                  shared_ptr<QservMgtRequest> const& request) {
    if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) return;
    string const msg = "request id: " + request->id() + " of type: " + request->type() +
                       " failed, error: " + QservMgtRequest::state2string(request->extendedState());
    throw http::Error(func, msg);
}

HttpQservMonitorModule::HttpQservMonitorModule(Controller::Ptr const& controller, string const& taskName,
                                               HttpProcessorConfig const& processorConfig,
                                               qhttp::Request::Ptr const& req,
                                               qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpQservMonitorModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "WORKERS")
        return _workers();
    else if (subModuleName == "WORKER")
        return _worker();
    else if (subModuleName == "WORKER-CONFIG")
        return _workerConfig();
    else if (subModuleName == "WORKER-DB")
        return _workerDb();
    else if (subModuleName == "WORKER-FILES")
        return _workerFiles();
    else if (subModuleName == "CZAR")
        return _czar();
    else if (subModuleName == "CZAR-CONFIG")
        return _czarConfig();
    else if (subModuleName == "CZAR-DB")
        return _czarDb();
    else if (subModuleName == "QUERIES-ACTIVE")
        return _activeQueries();
    else if (subModuleName == "QUERIES-ACTIVE-PROGRESS")
        return _activeQueriesProgress();
    else if (subModuleName == "QUERIES-PAST")
        return _pastQueries();
    else if (subModuleName == "QUERY")
        return _userQuery();
    else if (subModuleName == "CSS")
        return _css();
    else if (subModuleName == "CSS-UPDATE")
        return _cssUpdate();
    else if (subModuleName == "INGEST-REQUESTS")
        return _userTables();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpQservMonitorModule::_workers() {
    debug(__func__);
    checkApiVersion(__func__, 19);

    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());
    bool const keepResources = query().optionalUInt("keep_resources", 0) != 0;
    wbase::TaskSelector const taskSelector = _translateTaskSelector(__func__);

    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    bool const allWorkers = true;
    string const noParentJobId;
    auto const job =
            QservStatusJob::create(timeoutSec, allWorkers, controller(), noParentJobId, taskSelector);
    job->start();
    job->wait();

    json result = json::object();
    result["status"] = json::object();

    map<string, set<int>> schedulers2chunks;
    set<int> chunks;

    QservStatus const& status = job->qservStatus();
    for (auto itr : status.workers) {
        string const& worker = itr.first;
        bool const success = itr.second;
        json const& info = success ? status.info.at(worker) : json();
        _processWorkerInfo(worker, keepResources, info, result["status"], schedulers2chunks, chunks);
    }
    result["schedulers_to_chunks"] = _schedulers2chunks2json(schedulers2chunks);
    result["chunks"] = _chunkInfo(chunks);
    return result;
}

json HttpQservMonitorModule::_worker() {
    debug(__func__);
    checkApiVersion(__func__, 19);

    auto const config = controller()->serviceProvider()->config();
    auto const worker = params().at("worker");
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());
    bool const keepResources = query().optionalUInt("keep_resources", 0) != 0;
    wbase::TaskSelector const taskSelector = _translateTaskSelector(__func__);

    debug(__func__, "worker=" + worker);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetStatusQservMgtRequest::CallbackType const onFinish = nullptr;
    auto const request = controller()->serviceProvider()->qservMgtServices()->status(
            worker, noParentJobId, taskSelector, onFinish, timeoutSec);
    request->wait();

    json result = json::object();
    result["status"] = json::object();

    map<string, set<int>> schedulers2chunks;
    set<int> chunks;

    bool const success = request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS;
    json const& info = success ? request->info() : json();
    _processWorkerInfo(worker, keepResources, info, result["status"], schedulers2chunks, chunks);
    result["schedulers_to_chunks"] = _schedulers2chunks2json(schedulers2chunks);
    result["chunks"] = _chunkInfo(chunks);
    result["czar_ids"] = ::czarIdsToJson(config->czarIds());
    return result;
}

json HttpQservMonitorModule::_workerConfig() {
    debug(__func__);
    checkApiVersion(__func__, 26);

    auto const worker = params().at("worker");
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());

    debug(__func__, "worker=" + worker);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetConfigQservMgtRequest::CallbackType const onFinish = nullptr;
    auto const request = controller()->serviceProvider()->qservMgtServices()->config(worker, noParentJobId,
                                                                                     onFinish, timeoutSec);
    request->wait();
    _throwIfNotSucceeded(__func__, request);

    return json::object({{"config", request->info()}});
}

json HttpQservMonitorModule::_workerDb() {
    debug(__func__);
    checkApiVersion(__func__, 24);

    auto const worker = params().at("worker");
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());

    debug(__func__, "worker=" + worker);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetDbStatusQservMgtRequest::CallbackType const onFinish = nullptr;
    auto const request = controller()->serviceProvider()->qservMgtServices()->databaseStatus(
            worker, noParentJobId, onFinish, timeoutSec);
    request->wait();
    _throwIfNotSucceeded(__func__, request);

    return json::object({{"status", request->info()}});
}

json HttpQservMonitorModule::_workerFiles() {
    debug(__func__);
    checkApiVersion(__func__, 28);

    auto const config = controller()->serviceProvider()->config();
    auto const worker = params().at("worker");
    auto const queryIds = query().optionalVectorUInt64("query_ids");
    auto const maxFiles = query().optionalUInt("max_files", 0);
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());

    debug(__func__, "worker=" + worker);
    debug(__func__, "query_ids=" + util::String::toString(queryIds));
    debug(__func__, "max_files=" + to_string(maxFiles));
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetResultFilesQservMgtRequest::CallbackType const onFinish = nullptr;
    auto const request = controller()->serviceProvider()->qservMgtServices()->resultFiles(
            worker, noParentJobId, queryIds, maxFiles, onFinish, timeoutSec);
    request->wait();
    _throwIfNotSucceeded(__func__, request);

    json result = json::object();
    result["status"] = request->info();
    result["czar_ids"] = ::czarIdsToJson(config->czarIds());
    return result;
}

json HttpQservMonitorModule::_czar() {
    debug(__func__);
    checkApiVersion(__func__, 29);

    auto const czar = params().at("czar");
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", czarResponseTimeoutSec());
    debug(__func__, "czar=" + czar);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetStatusQservCzarMgtRequest::CallbackType const onFinish = nullptr;
    auto const request = controller()->serviceProvider()->qservMgtServices()->czarStatus(
            czar, noParentJobId, onFinish, timeoutSec);
    request->wait();
    _throwIfNotSucceeded(__func__, request);

    return json::object({{"status", request->info()}});
}

json HttpQservMonitorModule::_czarConfig() {
    debug(__func__);
    checkApiVersion(__func__, 29);

    auto const czar = params().at("czar");
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", czarResponseTimeoutSec());
    debug(__func__, "czar=" + czar);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetConfigQservCzarMgtRequest::CallbackType const onFinish = nullptr;
    auto const request = controller()->serviceProvider()->qservMgtServices()->czarConfig(
            czar, noParentJobId, onFinish, timeoutSec);
    request->wait();
    _throwIfNotSucceeded(__func__, request);

    return json::object({{"config", request->info()}});
}

json HttpQservMonitorModule::_czarDb() {
    debug(__func__);
    checkApiVersion(__func__, 24);

    // Connect to the master database. Manage the new connection via the RAII-style
    // handler to ensure the transaction is automatically rolled-back in case of exceptions.
    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    bool const full = true;
    return json::object({{"status", database::mysql::processList(h.conn, full)}});
}

wbase::TaskSelector HttpQservMonitorModule::_translateTaskSelector(string const& func) const {
    wbase::TaskSelector selector;
    selector.includeTasks = query().optionalUInt("include_tasks", 0) != 0;
    selector.queryIds = query().optionalVectorUInt64("query_ids");
    string const taskStatesParam = "task_states";
    for (auto&& str : query().optionalVectorStr(taskStatesParam)) {
        try {
            auto const state = wbase::str2taskState(str);
            selector.taskStates.push_back(state);
            debug(func, "str='" + str + "', task state=" + wbase::taskState2str(state));
        } catch (exception const& ex) {
            string const msg =
                    "failed to parse query parameter '" + taskStatesParam + "', ex: " + string(ex.what());
            error(func, msg);
            throw invalid_argument(msg);
        }
    }
    selector.maxTasks = query().optionalUInt("max_tasks", 0);
    debug(func, "include_tasks=" + replica::bool2str(selector.includeTasks));
    debug(func, "query_ids=" + util::String::toString(selector.queryIds));
    debug(func, "task_states=" + util::String::toString(selector.taskStates));
    debug(func, "max_tasks=" + to_string(selector.maxTasks));
    return selector;
}

void HttpQservMonitorModule::_processWorkerInfo(string const& worker, bool keepResources,
                                                json const& inWorkerInfo, json& statusRef,
                                                map<string, set<int>>& schedulers2chunks,
                                                set<int>& chunks) const {
    statusRef[worker] = json::object();
    json& workerRef = statusRef[worker];
    workerRef["success"] = inWorkerInfo.is_null() ? 0 : 1;

    if (!inWorkerInfo.is_null()) {
        workerRef["info"] = inWorkerInfo;
        json& info = workerRef["info"];
        if (!keepResources) {
            info["resources"] = json::array();
        }
        workerRef["queries"] = _getQueries(info);
        for (json const& scheduler :
             info.at("processor").at("queries").at("blend_scheduler").at("schedulers")) {
            string const scheduerName = scheduler.at("name");
            for (json const& chunk2tasks : scheduler.at("chunk_to_num_tasks")) {
                int const chunk = chunk2tasks[0];
                schedulers2chunks[scheduerName].insert(chunk);
                chunks.insert(chunk);
            }
        }
    }
}

json HttpQservMonitorModule::_schedulers2chunks2json(map<string, set<int>> const& schedulers2chunks) const {
    json result;
    for (auto itr : schedulers2chunks) {
        string const& scheduerName = itr.first;
        for (int const chunk : itr.second) {
            result[scheduerName].push_back(chunk);
        }
    }
    return result;
}

json HttpQservMonitorModule::_activeQueries() {
    debug(__func__);
    checkApiVersion(__func__, 25);

    auto const config = controller()->serviceProvider()->config();
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    // Check which queries and in which schedulers are being executed
    // by Qserv workers.
    bool const allWorkers = true;
    auto const job = QservStatusJob::create(timeoutSec, allWorkers, controller());
    job->start();
    job->wait();

    map<QueryId, string> queryId2scheduler;
    auto&& status = job->qservStatus();
    for (auto&& entry : status.workers) {
        string const& worker = entry.first;
        bool success = entry.second;
        if (success) {
            auto info = status.info.at(worker);
            auto&& schedulers = info["processor"]["queries"]["blend_scheduler"]["schedulers"];
            for (auto&& scheduler : schedulers) {
                string const scheduerName = scheduler["name"];
                for (auto&& queryId2count : scheduler["query_id_to_count"]) {
                    QueryId const queryId = queryId2count[0];
                    // Keep the name of the "SchedSnail" scheduler to indicate the worst case scenario
                    // for the query.
                    if (queryId2scheduler[queryId] != "SchedSnail") {
                        queryId2scheduler[queryId] = scheduerName;
                    }
                }
            }
        }
    }

    // Connect to the master database. Manage the new connection via the RAII-style
    // handler to ensure the transaction is automatically rolled-back in case of exceptions.
    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    QueryGenerator const g(h.conn);

    // Get info on the ongoing queries
    json result;
    h.conn->executeInOwnTransaction(
            [&](auto conn) { result["queries"] = _currentUserQueries(conn, queryId2scheduler); });
    result["czar_ids"] = ::czarIdsToJson(config->czarIds());
    return result;
}

json HttpQservMonitorModule::_activeQueriesProgress() {
    debug(__func__);
    checkApiVersion(__func__, 29);

    auto const czar = params().at("czar");
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", czarResponseTimeoutSec());
    auto const queryIds = query().optionalVectorUInt64("query_ids");
    unsigned int const lastSeconds = query().optionalUInt("last_seconds", 0);
    string const queryStatus = query().optionalString("query_status");

    debug(__func__, "czar=" + czar);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));
    debug(__func__, "query_ids=" + util::String::toString(queryIds));
    debug(__func__, "last_seconds=" + to_string(lastSeconds));
    debug(__func__, "query_status=" + queryStatus);

    string const noParentJobId;
    GetQueryProgressQservCzarMgtRequest::CallbackType const onFinish = nullptr;
    auto const request = controller()->serviceProvider()->qservMgtServices()->czarQueryProgress(
            czar, noParentJobId, queryIds, lastSeconds, queryStatus, onFinish, timeoutSec);
    request->wait();
    _throwIfNotSucceeded(__func__, request);

    return request->info();
}

json HttpQservMonitorModule::_pastQueries() {
    debug(__func__);
    checkApiVersion(__func__, 36);

    auto const config = controller()->serviceProvider()->config();
    string const queryStatus = query().optionalString("query_status", string());
    string const queryType = query().optionalString("query_type", string());
    unsigned int const queryAgeSec = query().optionalUInt("query_age", 0);
    unsigned int const minElapsedSec = query().optionalUInt("min_elapsed_sec", 0);
    unsigned int const minNumChunks = query().optionalUInt("min_num_chunks", 0);
    unsigned int const minCollectedBytes = query().optionalUInt("min_collected_bytes", 0);
    unsigned int const minFinalRows = query().optionalUInt("min_final_rows", 0);
    unsigned int const limit4past = query().optionalUInt("limit4past", 1);
    string const searchPattern = query().optionalString("search_pattern", string());
    bool const searchRegexpMode = query().optionalUInt("search_regexp_mode", 0) != 0;
    bool const includeMessages = query().optionalUInt("include_messages", 0) != 0;

    debug(__func__, "query_status=" + queryStatus);
    debug(__func__, "query_type=" + queryType);
    debug(__func__, "query_age=" + to_string(queryAgeSec));
    debug(__func__, "min_elapsed_sec=" + to_string(minElapsedSec));
    debug(__func__, "min_num_chunks=" + to_string(minNumChunks));
    debug(__func__, "min_collected_bytes=" + to_string(minCollectedBytes));
    debug(__func__, "min_final_rows=" + to_string(minFinalRows));
    debug(__func__, "limit4past=" + to_string(limit4past));
    debug(__func__, "search_pattern=" + searchPattern);
    debug(__func__, "search_regexp_mode=" + bool2str(searchRegexpMode));
    debug(__func__, "include_messages=" + bool2str(includeMessages));

    // Connect to the master database. Manage the new connection via the RAII-style
    // handler to ensure the transaction is automatically rolled-back in case of exceptions.
    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    QueryGenerator const g(h.conn);

    // Get info on the past queries matching the specified criteria.
    string constraints;
    if (queryStatus.empty()) {
        g.packCond(constraints, g.neq("status", "EXECUTING"));
    } else {
        g.packCond(constraints, g.eq("status", queryStatus));
    }
    if (!queryType.empty()) {
        g.packCond(constraints, g.eq("qType", queryType));
    }
    if (queryAgeSec > 0) {
        string const cond = g.gt(g.TIMESTAMPDIFF("SECOND", "submitted", Sql::NOW), queryAgeSec);
        g.packCond(constraints, cond);
    }
    if (minElapsedSec > 0) {
        string const cond = g.gt(g.TIMESTAMPDIFF("SECOND", "submitted", "completed"), minElapsedSec);
        g.packCond(constraints, cond);
    }
    if (minNumChunks > 0) {
        string const cond = g.gt("chunkCount", minNumChunks);
        g.packCond(constraints, cond);
    }
    if (minCollectedBytes > 0) {
        string const cond = g.gt("collectedBytes", minCollectedBytes);
        g.packCond(constraints, cond);
    }
    if (minFinalRows > 0) {
        string const cond = g.gt("finalRows", minFinalRows);
        g.packCond(constraints, cond);
    }
    if (!searchPattern.empty()) {
        if (searchRegexpMode) {
            g.packCond(constraints, g.regexp("query", searchPattern));
        } else {
            g.packCond(constraints, g.like("query", "%" + searchPattern + "%"));
        }
    }
    json result;
    h.conn->executeInOwnTransaction([&](auto conn) {
        result["queries_past"] = _pastUserQueries(conn, constraints, limit4past, includeMessages);
    });
    result["czar_ids"] = ::czarIdsToJson(config->czarIds());
    return result;
}

json HttpQservMonitorModule::_userQuery() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const queryId = stoull(params().at("id"));
    bool const includeMessages = query().optionalUInt("include_messages", 0) != 0;
    debug(__func__, "id=" + to_string(queryId));
    debug(__func__, "include_messages=" + bool2str(includeMessages));

    // Connect to the master database
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    QueryGenerator const g(h.conn);

    json result;
    h.conn->executeInOwnTransaction([&](auto conn) {
        unsigned int const limit4past = 0;
        result["queries_past"] =
                _pastUserQueries(conn, g.eq("queryId", queryId), limit4past, includeMessages);
    });
    result["czar_ids"] = ::czarIdsToJson(config->czarIds());
    return result;
}

json HttpQservMonitorModule::_currentUserQueries(Connection::Ptr& conn,
                                                 map<QueryId, string> const& queryId2scheduler) {
    QueryGenerator const g(conn);
    string const query =
            g.select(g.id("QProgress", Sql::STAR), g.as(g.UNIX_TIMESTAMP("queryBegin"), "queryBegin_sec"),
                     g.as(g.UNIX_TIMESTAMP("lastUpdate"), "lastUpdate_sec"), g.as(Sql::NOW, "samplingTime"),
                     g.as(g.UNIX_TIMESTAMP(Sql::NOW), "samplingTime_sec"), g.id("QInfo", "query"),
                     g.id("QInfo", "czarId"), g.id("QInfo", "qType")) +
            g.from("QProgress", "QInfo") +
            g.where(g.eq(g.id("QProgress", "queryId"), g.id("QInfo", "queryId")),
                    g.eq(g.id("QInfo", "status"), "EXECUTING")) +
            g.orderBy(make_pair(g.id("QProgress", "queryBegin"), "DESC"));
    conn->execute(query);

    json result = json::array();
    if (conn->hasResult()) {
        Row row;
        while (conn->next(row)) {
            json resultRow;
            ::parseFieldIntoJson<QueryId>(__func__, row, "queryId", resultRow);
            ::parseFieldIntoJson<int>(__func__, row, "totalChunks", resultRow);
            ::parseFieldIntoJson<int>(__func__, row, "completedChunks", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "queryBegin", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "queryBegin_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "lastUpdate", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "lastUpdate_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "samplingTime", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "samplingTime_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "query", resultRow);
            ::parseFieldIntoJson<qmeta::CzarId>(__func__, row, "czarId", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "qType", resultRow);

            // Optionally, add the name of corresponding worker scheduler
            // if the one was already known for the query.

            QueryId const queryId = resultRow["queryId"];
            auto itr = queryId2scheduler.find(queryId);
            if (itr != queryId2scheduler.end()) {
                resultRow["scheduler"] = itr->second;
            }
            result.push_back(resultRow);
        }
    }
    return result;
}

json HttpQservMonitorModule::_pastUserQueries(Connection::Ptr& conn, string const& constraint,
                                              unsigned int limit4past, bool includeMessages) {
    json result = json::array();
    QueryGenerator const g(conn);
    string const query = g.select(Sql::STAR, g.as(g.UNIX_TIMESTAMP("submitted"), "submitted_sec"),
                                  g.as(g.UNIX_TIMESTAMP("completed"), "completed_sec"),
                                  g.as(g.UNIX_TIMESTAMP("returned"), "returned_sec")) +
                         g.from("QInfo") + g.where(constraint) + g.orderBy(make_pair("submitted", "DESC")) +
                         g.limit(limit4past);

    conn->execute(query);
    if (conn->hasResult()) {
        Row row;
        while (conn->next(row)) {
            json resultRow;
            ::parseFieldIntoJson<QueryId>(__func__, row, "queryId", resultRow);
            ::parseFieldIntoJson<qmeta::CzarId>(__func__, row, "czarId", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "qType", resultRow);
            ::parseFieldIntoJson<int>(__func__, row, "czarId", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "user", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "query", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "qTemplate", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "qMerge", resultRow, "");
            ::parseFieldIntoJson<string>(__func__, row, "status", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "submitted", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "submitted_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "completed", resultRow, "");
            ::parseFieldIntoJson<long>(__func__, row, "completed_sec", resultRow, 0);
            ::parseFieldIntoJson<string>(__func__, row, "returned", resultRow, "");
            ::parseFieldIntoJson<long>(__func__, row, "returned_sec", resultRow, 0);
            ::parseFieldIntoJson<string>(__func__, row, "messageTable", resultRow, "");
            ::parseFieldIntoJson<string>(__func__, row, "resultLocation", resultRow, "");
            ::parseFieldIntoJson<string>(__func__, row, "resultQuery", resultRow, "");
            ::parseFieldIntoJson<long>(__func__, row, "chunkCount", resultRow, 0);
            ::parseFieldIntoJson<uint64_t>(__func__, row, "collectedBytes", resultRow, 0);
            ::parseFieldIntoJson<uint64_t>(__func__, row, "collectedRows", resultRow, 0);
            ::parseFieldIntoJson<uint64_t>(__func__, row, "finalRows", resultRow, 0);
            resultRow["messages"] = json::array();
            result.push_back(resultRow);
        }
        if (includeMessages) {
            for (auto&& queryInfo : result) {
                string const query = g.select(Sql::STAR) + g.from("QMessages") +
                                     g.where(g.eq("queryId", queryInfo["queryId"].get<QueryId>())) +
                                     g.orderBy(make_pair("timestamp", "ASC"));
                conn->execute(query);
                if (conn->hasResult()) {
                    while (conn->next(row)) {
                        json resultRow;
                        ::parseFieldIntoJson<QueryId>(__func__, row, "queryId", resultRow);
                        ::parseFieldIntoJson<string>(__func__, row, "msgSource", resultRow);
                        ::parseFieldIntoJson<int>(__func__, row, "chunkId", resultRow);
                        ::parseFieldIntoJson<int>(__func__, row, "code", resultRow);
                        ::parseFieldIntoJson<string>(__func__, row, "message", resultRow);
                        ::parseFieldIntoJson<string>(__func__, row, "severity", resultRow);
                        ::parseFieldIntoJson<uint64_t>(__func__, row, "timestamp", resultRow);
                        queryInfo["messages"].push_back(resultRow);
                    }
                }
            }
        }
    }
    return result;
}

json HttpQservMonitorModule::_getQueries(json const& workerInfo) const {
    // Find identifiers of all queries in the wait queues of all schedulers
    set<QueryId> qids;
    for (json const& scheduler :
         workerInfo.at("processor").at("queries").at("blend_scheduler").at("schedulers")) {
        for (json const& entry : scheduler.at("query_id_to_count")) {
            qids.insert(entry[0].get<QueryId>());
        }
    }

    // Connect to the master database
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    auto const config = controller()->serviceProvider()->config();
    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    QueryGenerator const g(h.conn);

    // Extract descriptions of those queries from qservMeta
    json result;
    if (!qids.empty()) {
        string const query = g.select(Sql::STAR) + g.from("QInfo") + g.where(g.in("queryId", qids));
        h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
            conn->execute(query);
            ::extractQInfo(conn, result);
        });
    }
    return result;
}

json HttpQservMonitorModule::_css() {
    debug(__func__);
    checkApiVersion(__func__, 45);
    bool const readOnly = true;
    auto const cssAccess = qservCssAccess(readOnly);
    return _cssSharedScanParams(controller()->serviceProvider()->config(), cssAccess);
}

json HttpQservMonitorModule::_cssUpdate() {
    debug(__func__);
    checkApiVersion(__func__, 45);

    auto const databaseName = params().at("database");
    auto const tableName = params().at("table");

    css::ScanTableParams params;
    params.scanRating = body().required<int>("scanRating");

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    debug(__func__, "scanRating=" + to_string(params.scanRating));

    auto const config = controller()->serviceProvider()->config();

    // These methods will throw exceptions if the database or the table are not found.
    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(tableName);
    if (!(table.isPartitioned && !table.isRefMatch())) {
        throw invalid_argument(context() + "::" + string(__func__) +
                               "  the table must be partitioned and not a reference match: " + databaseName +
                               "." + tableName);
    }

    auto const cssAccess = qservCssAccess();
    cssAccess->setScanTableParams(databaseName, tableName, params);
    return _cssSharedScanParams(config, cssAccess);
}

json HttpQservMonitorModule::_userTables() {
    debug(__func__);
    checkApiVersion(__func__, 50);

    qmeta::UserTables userTables(::czarQMetaConfig());

    // The implementation of the API supports two modes of operation:
    // 1) if the 'id' parameter is specified then the request is for a
    //    specific request.  In this mode all other parameters are ignored.
    // 2) if the 'id' parameter is omitted or set to zero then the
    //    request is for a list of requests matching the specified criteria.

    uint64_t const id = query().optionalUInt64("id", 0);
    bool const extended = query().optionalUInt64("extended", 0) != 0;

    debug(__func__, "id=" + to_string(id));
    debug(__func__, "extended=" + bool2str(extended));

    if (id != 0) {
        json requests = json::array();
        requests.push_back(userTables.findRequest(id, extended).toJson());
        return json::object({{"requests", requests}});
    }

    auto const databaseName = query().optionalString("database");
    auto const tableName = query().optionalString("table");
    bool filterByStatus = false;
    qmeta::UserTableIngestRequest::Status status = qmeta::UserTableIngestRequest::Status::IN_PROGRESS;
    auto const statusStr = query().optionalString("status");
    if (!statusStr.empty()) {
        filterByStatus = true;
        status = qmeta::UserTableIngestRequest::str2status(statusStr);
    }
    uint64_t const beginTimeSec = query().optionalUInt64("begin_time_sec", 0);
    uint64_t const endTimeSec = query().optionalUInt64("end_time_sec", 0);
    uint64_t const limit = query().optionalUInt64("limit", 1);

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    debug(__func__,
          "status=" + (filterByStatus ? qmeta::UserTableIngestRequest::status2str(status) : string()));
    debug(__func__, "begin_time_sec=" + to_string(beginTimeSec));
    debug(__func__, "end_time_sec=" + to_string(endTimeSec));
    debug(__func__, "limit=" + to_string(limit));

    if (tableName.empty() && !databaseName.empty()) {
        throw invalid_argument(context() + "::" + string(__func__) +
                               "  the parameter 'table' is required if 'database' is specified");
    }
    if (endTimeSec > 0 && beginTimeSec >= endTimeSec) {
        throw invalid_argument(context() + "::" + string(__func__) +
                               "  the value of parameter 'begin_time_sec' must be < 'end_time_sec'");
    }

    json requests = json::array();
    for (auto&& entry : userTables.findRequests(databaseName, tableName, filterByStatus, status, beginTimeSec,
                                                endTimeSec, limit, extended)) {
        requests.push_back(entry.toJson());
    }
    return json::object({{"requests", requests}});
}

json HttpQservMonitorModule::_cssSharedScanParams(shared_ptr<Configuration> const& config,
                                                  shared_ptr<css::CssAccess> const& cssAccess) const {
    json resultSharedScan;
    for (string const& familyName : config->databaseFamilies()) {
        bool const allDatabases = true;
        for (string const& databaseName : config->databases(familyName, allDatabases)) {
            auto const database = config->databaseInfo(databaseName);
            // Do not include special tables into the report.
            vector<string> sharedScanTables;
            for (string const& tableName : database.tables()) {
                auto const table = database.findTable(tableName);
                if (table.isPartitioned && !table.isRefMatch()) {
                    sharedScanTables.emplace_back(table.name);
                    // Set the empty object as the default result for each table.
                    resultSharedScan[familyName][database.name][table.name] = json::object();
                }
            }
            // Override the default values for tables for which the shared scan
            // parameters were explicitly set.
            if (cssAccess->containsDb(database.name)) {
                for (string const& tableName : sharedScanTables) {
                    if (cssAccess->containsTable(database.name, tableName)) {
                        try {
                            css::ScanTableParams const params =
                                    cssAccess->getScanTableParams(database.name, tableName);
                            resultSharedScan[familyName][database.name][tableName] =
                                    json::object({{"scanRating", params.scanRating}});
                        } catch (css::NoSuchTable const&) {
                            // CSS key for the shared scans may not exist yet even if the table
                            // is known to CSS.
                            ;
                        }
                    }
                }
            }
        }
    }
    json result = json::object();
    result["css"]["shared_scan"] = resultSharedScan;
    return result;
}

json HttpQservMonitorModule::_chunkInfo(set<int> const& chunks) const {
    json result;
    auto const config = controller()->serviceProvider()->config();
    for (string const& familyName : config->databaseFamilies()) {
        auto&& familyInfo = config->databaseFamilyInfo(familyName);
        /*
         * TODO: both versions of the 'Chunker' class need to be used due to non-overlapping
         * functionality and the interface.  The one from the spherical geometry packages
         * provides a simple interface for validating chunk numbers, meanwhile the other
         * one allows to extract spatial parameters of chunks. This duality will be
         * addressed later after migrating package 'partition' to use geometry utilities
         * of package 'sphgeom'.
         */
        lsst::sphgeom::Chunker const sphgeomChunker(familyInfo.numStripes, familyInfo.numSubStripes);
        lsst::partition::Chunker const partitionChunker(familyInfo.overlap, familyInfo.numStripes,
                                                        familyInfo.numSubStripes);
        for (int const chunk : chunks) {
            if (sphgeomChunker.valid(chunk)) {
                json chunkGeometry;
                auto box = partitionChunker.getChunkBounds(chunk);
                chunkGeometry["lat_min"] = box.getLatMin();
                chunkGeometry["lat_max"] = box.getLatMax();
                chunkGeometry["lon_min"] = box.getLonMin();
                chunkGeometry["lon_max"] = box.getLonMax();
                result[to_string(chunk)][familyInfo.name] = chunkGeometry;
            }
        }
    }
    return result;
}

}  // namespace lsst::qserv::replica
