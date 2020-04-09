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
#include "replica/HttpQservMonitorModule.h"

// System headers
#include <map>
#include <stdexcept>

// Third party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "replica/Configuration.h"
#include "replica/ConfigurationTypes.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpRequestQuery.h"
#include "replica/QservMgtServices.h"
#include "replica/QservStatusJob.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/partition/Chunker.h"
#include "lsst/partition/Geometry.h"
#include "lsst/sphgeom/Chunker.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv;
using namespace lsst::qserv::replica;

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
template<typename T>
void parseFieldIntoJson(string const& context,
                        database::mysql::Row const& row,
                        string const& column,
                        json& obj) {
    T val;
    if (not row.get(column, val)) {
        throw invalid_argument(context + " no column '" + column + "' found in the result set");
    }
    obj[column] = val;
}

/**
 * The complementary version of the above defined function which replaces
 * 'NULL' found in a field with the specified default value.
 */
template<typename T>
void parseFieldIntoJson(string const& context,
                        database::mysql::Row const& row,
                        string const& column,
                        json& obj,
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
void extractQInfo(database::mysql::Connection::Ptr const& conn,
                  json& result) {

    if (not conn->hasResult()) return;

    database::mysql::Row row;
    while (conn->next(row)) {

        QueryId queryId;
        if (not row.get("queryId", queryId)) continue;
        
        string query, status, submitted, completed;
        row.get("query",     query);
        row.get("status",    status);
        row.get("submitted", submitted);
        row.get("completed", completed);

        string const queryIdStr = to_string(queryId);
        result[queryIdStr]["query"]     = query;
        result[queryIdStr]["status"]    = status;
        result[queryIdStr]["submitted"] = submitted;
        result[queryIdStr]["completed"] = completed;
    }
}
}

namespace lsst {
namespace qserv {
namespace replica {

HttpQservMonitorModule::Ptr HttpQservMonitorModule::create(
        Controller::Ptr const& controller,
        string const& taskName,
        HttpProcessorConfig const& processorConfig) {
    return Ptr(new HttpQservMonitorModule(
        controller, taskName, processorConfig));
}


HttpQservMonitorModule::HttpQservMonitorModule(Controller::Ptr const& controller,
                                               string const& taskName,
                                               HttpProcessorConfig const& processorConfig)
    :   HttpModule(controller, taskName, processorConfig) {
}


void HttpQservMonitorModule::executeImpl(string const& subModuleName) {

    if (subModuleName == "WORKERS") {
        _workers();
    } else if (subModuleName == "SELECT-WORKER-BY-NAME") {
        _worker();
    } else if (subModuleName == "QUERIES") {
        _userQueries();
    } else if (subModuleName == "SELECT-QUERY-BY-ID") {
        _userQuery();
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpQservMonitorModule::_workers() {
    debug(__func__);

    HttpRequestQuery const query(req()->query);
    unsigned int const timeoutSec    = query.optionalUInt("timeout_sec", workerResponseTimeoutSec());
    bool         const keepResources = query.optionalUInt("keep_resources", 0) != 0;

    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    bool const allWorkers = true;
    auto const job = QservStatusJob::create(timeoutSec, allWorkers, controller());
    job->start();
    job->wait();

    json result;
    map<string, set<int>> schedulers2chunks;
    set<int> chunks;
    auto&& status = job->qservStatus();
    for (auto&& entry: status.workers) {
        auto&& worker = entry.first;
        bool success = entry.second;
        if (success) {
            auto info = status.info.at(worker);
            if (not keepResources) {
                info["resources"] = json::array();
            }
            result["status"][worker]["success"] = 1;
            result["status"][worker]["info"] = info;
            result["status"][worker]["queries"] = _getQueries(info);
            auto&& schedulers = info["processor"]["queries"]["blend_scheduler"]["schedulers"];
            for (auto&& scheduler: schedulers) {
                string const scheduerName = scheduler["name"];
                for (auto&& chunk2tasks: scheduler["chunk_to_num_tasks"]) {
                    int const chunk = chunk2tasks[0];
                    schedulers2chunks[scheduerName].insert(chunk);
                    chunks.insert(chunk);
                }
            }
        } else {
            result["status"][worker]["success"] = 0;
        }
    }
    json resultSchedulers2chunks;
    for (auto&& entry: schedulers2chunks) {
        auto&& scheduerName = entry.first;
        for (auto&& chunk: entry.second) {
            resultSchedulers2chunks[scheduerName].push_back(chunk);
        }
    }
    result["schedulers_to_chunks"] = resultSchedulers2chunks;
    result["chunks"] = _chunkInfo(chunks);
    sendData(result);
}


void HttpQservMonitorModule::_worker() {
    debug(__func__);

    auto const worker = req()->params.at("name");

    HttpRequestQuery const query(req()->query);
    unsigned int const timeoutSec = query.optionalUInt("timeout_sec", workerResponseTimeoutSec());

    debug(__func__, "worker=" + worker);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetStatusQservMgtRequest::CallbackType const onFinish = nullptr;

    auto const request =
        controller()->serviceProvider()->qservMgtServices()->status(
            worker,
            noParentJobId,
            onFinish,
            timeoutSec);
    request->wait();

    json result;

    if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
        auto info = request->info();
        result["status"][worker]["success"] = 1;
        result["status"][worker]["info"] = info;
        result["status"][worker]["queries"] = _getQueries(info);
    } else {
        result["status"][worker]["success"] = 0;
    }        
    sendData(result);
}


void HttpQservMonitorModule::_userQueries() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    HttpRequestQuery const query(req()->query);
    unsigned int const timeoutSec = query.optionalUInt("timeout_sec", workerResponseTimeoutSec());
    unsigned int const limit4past = query.optionalUInt("limit4past", 1);

    debug(__func__, "timeout_sec=" + to_string(timeoutSec));
    debug(__func__, "limit4past=" + to_string(limit4past));

    // Check which queries and in which schedulers are being executed
    // by Qseev workers.

    bool const allWorkers = true;
    auto const job = QservStatusJob::create(timeoutSec, allWorkers, controller());
    job->start();
    job->wait();

    map<QueryId, string> queryId2scheduler;
    auto&& status = job->qservStatus();
    for (auto&& entry: status.workers) {
        auto&& worker = entry.first;
        bool success = entry.second;
        if (success) {
            auto info = status.info.at(worker);
            auto&& schedulers = info["processor"]["queries"]["blend_scheduler"]["schedulers"];
            for (auto&& scheduler: schedulers) {
                string const scheduerName = scheduler["name"];
                for (auto&& queryId2count: scheduler["query_id_to_count"]) {
                    QueryId const queryId = queryId2count[0];
                    queryId2scheduler[queryId] = scheduerName;
                }
            }
        }
    }

    json result;
    result["queries"] = json::array();
    result["queries_past"] = json::array();

    // Connect to the master database
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(
        database::mysql::Connection::open(
            database::mysql::ConnectionParams(
                config->qservMasterDatabaseHost(),
                config->qservMasterDatabasePort(),
                "root",
                Configuration::qservMasterDatabasePassword(),
                "qservMeta"
            )
        )
    );

    // NOTE: the roll-back for this transaction will happen automatically. It will
    // be done by the connection handler.
    h.conn->begin();
    h.conn->execute(
        "SELECT " + h.conn->sqlId("QStatsTmp") + ".*,"
        "UNIX_TIMESTAMP(" + h.conn->sqlId("queryBegin") + ") AS " + h.conn->sqlId("queryBegin_sec") + ","
        "UNIX_TIMESTAMP(" + h.conn->sqlId("lastUpdate") + ") AS " + h.conn->sqlId("lastUpdate_sec") + ","
        "NOW() AS "       + h.conn->sqlId("samplingTime") + ","
        "UNIX_TIMESTAMP(NOW()) AS " + h.conn->sqlId("samplingTime_sec") + "," +
        h.conn->sqlId("QInfo") + "." + h.conn->sqlId("query") +
        " FROM " + h.conn->sqlId("QStatsTmp") + "," + h.conn->sqlId("QInfo") +
        " WHERE " +
        h.conn->sqlId("QStatsTmp") + "." + h.conn->sqlId("queryId") + "=" +
        h.conn->sqlId("QInfo")     + "." + h.conn->sqlId("queryId") +
        " ORDER BY " + h.conn->sqlId("QStatsTmp") + "." + h.conn->sqlId("queryBegin") + " DESC"
    );
    if (h.conn->hasResult()) {
        database::mysql::Row row;
        while (h.conn->next(row)) {
            json resultRow;
            ::parseFieldIntoJson<QueryId>(__func__, row, "queryId",          resultRow);
            ::parseFieldIntoJson<int>(    __func__, row, "totalChunks",      resultRow);
            ::parseFieldIntoJson<int>(    __func__, row, "completedChunks",  resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "queryBegin",       resultRow);
            ::parseFieldIntoJson<long>(   __func__, row, "queryBegin_sec",   resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "lastUpdate",       resultRow);
            ::parseFieldIntoJson<long>(   __func__, row, "lastUpdate_sec",   resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "samplingTime",     resultRow);
            ::parseFieldIntoJson<long>(   __func__, row, "samplingTime_sec", resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "query",            resultRow);

            // Optionally, add the name of corresponding worker scheduler
            // if the one was already known for the query.

            QueryId const queryId = resultRow["queryId"];
            auto itr = queryId2scheduler.find(queryId);
            if (itr != queryId2scheduler.end()) {
                resultRow["scheduler"] = itr->second;
            }
            result["queries"].push_back(resultRow);
        }
    }
    h.conn->execute(
        "SELECT *,"
        "UNIX_TIMESTAMP(" + h.conn->sqlId("submitted") + ") AS " + h.conn->sqlId("submitted_sec") + "," +
        "UNIX_TIMESTAMP(" + h.conn->sqlId("completed") + ") AS " + h.conn->sqlId("completed_sec") + ","
        "UNIX_TIMESTAMP(" + h.conn->sqlId("returned")  + ") AS " + h.conn->sqlId("returned_sec") +
        " FROM "  + h.conn->sqlId("QInfo") +
        " WHERE " + h.conn->sqlNotEqual("status", "EXECUTING") +
        " ORDER BY " + h.conn->sqlId("submitted") + " DESC" +
        (limit4past == 0 ? "" : " LIMIT " + to_string(limit4past))
    );
    if (h.conn->hasResult()) {
        database::mysql::Row row;
        while (h.conn->next(row)) {
            json resultRow;
            ::parseFieldIntoJson<QueryId>(__func__, row, "queryId",        resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "qType",          resultRow);
            ::parseFieldIntoJson<int>(    __func__, row, "czarId",         resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "user",           resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "query",          resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "qTemplate",      resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "qMerge",         resultRow, "");
            ::parseFieldIntoJson<string>( __func__, row, "status",         resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "submitted",      resultRow);
            ::parseFieldIntoJson<long>(   __func__, row, "submitted_sec",  resultRow);
            ::parseFieldIntoJson<string>( __func__, row, "completed",      resultRow, "");
            ::parseFieldIntoJson<long>(   __func__, row, "completed_sec",  resultRow, 0);
            ::parseFieldIntoJson<string>( __func__, row, "returned",       resultRow, "");
            ::parseFieldIntoJson<long>(   __func__, row, "returned_sec",   resultRow, 0);
            ::parseFieldIntoJson<string>( __func__, row, "messageTable",   resultRow, "");
            ::parseFieldIntoJson<string>( __func__, row, "resultLocation", resultRow, "");
            ::parseFieldIntoJson<string>( __func__, row, "resultQuery",    resultRow, "");
            result["queries_past"].push_back(resultRow);
        }
    }
    sendData(result);
}


void HttpQservMonitorModule::_userQuery() {
    debug(__func__);

    auto const id = stoull(req()->params.at("id"));

    debug(__func__, " id=" + to_string(id));

    json result;
    sendData(result);
}


json HttpQservMonitorModule::_getQueries(json& workerInfo) const {

    // Find identifiers of all queries in the wait queues of all schedulers
    set<QueryId> qids;
    for (auto&& scheduler: workerInfo.at("processor").at("queries").at("blend_scheduler").at("schedulers")) {
        for (auto&& entry: scheduler.at("query_id_to_count")) {
            qids.insert(entry[0].get<QueryId>());
        }
    }

    // Connect to the master database
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    auto const config = controller()->serviceProvider()->config();
    database::mysql::ConnectionHandler const h(
        database::mysql::Connection::open(
            database::mysql::ConnectionParams(
                config->qservMasterDatabaseHost(),
                config->qservMasterDatabasePort(),
                "root",
                Configuration::qservMasterDatabasePassword(),
                "qservMeta"
            )
        )
    );

    // Extract descriptions of those queries from qservMeta

    json result;
    if (not qids.empty()) {
        h.conn->execute([&](decltype(h.conn) conn) {
            conn->begin();
            conn->execute("SELECT * FROM " + h.conn->sqlId("QInfo") +
                          " WHERE " + h.conn->sqlIn("queryId", qids));
            ::extractQInfo(conn, result);
            conn->commit();
        });
    }
    return result;
}


json HttpQservMonitorModule::_chunkInfo(set<int> const& chunks) const {
    json result;
    auto const config = controller()->serviceProvider()->config();
    for (auto&& familyName: config->databaseFamilies()) {
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
        for (auto&& chunk: chunks) {
            if (sphgeomChunker.valid(chunk)) {
                json chunkGeometry;
                auto&& box = partitionChunker.getChunkBounds(chunk);
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

}}}  // namespace lsst::qserv::replica
