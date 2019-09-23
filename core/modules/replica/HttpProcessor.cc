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
#include "replica/HttpProcessor.h"

// System headers
#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <iomanip>
#include <set>
#include <stdexcept>
#include <sstream>

// Third party headers
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

// Qserv headers
#include "css/CssAccess.h"
#include "global/intTypes.h"
#include "qhttp/Server.h"
#include "replica/AbortTransactionJob.h"
#include "replica/ChunkNumber.h"
#include "replica/ConfigurationTypes.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpCatalogsModule.h"
#include "replica/HttpConfigurationModule.h"
#include "replica/HttpControllersModule.h"
#include "replica/HttpJobsModule.h"
#include "replica/HttpRequestQuery.h"
#include "replica/HttpRequestsModule.h"
#include "replica/HttpReplicationLevelsModule.h"
#include "replica/HttpRequestBody.h"
#include "replica/HttpWorkerStatusModule.h"
#include "replica/IndexJob.h"
#include "replica/Performance.h"
#include "replica/QservMgtServices.h"
#include "replica/QservStatusJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceManagementJob.h"
#include "replica/SqlJob.h"
#include "replica/SqlRequest.h"

// LSST headers
#include "lsst/partition/Chunker.h"
#include "lsst/partition/Geometry.h"
#include "lsst/sphgeom/Chunker.h"


using namespace std;
using namespace std::placeholders;
namespace fs = boost::filesystem;
using json = nlohmann::json;
namespace qhttp = lsst::qserv::qhttp;
using namespace lsst::qserv::replica;

namespace {

string const taskName = "HTTP-PROCESSOR";


/**
 * @return the name of a worker which has the least number of replicas
 * among workers mentioned in the input collection of workrs.
 */
template<typename COLLECTION_OF_WORKERS>
string leastLoadedWorker(DatabaseServices::Ptr const& databaseServices,
                         COLLECTION_OF_WORKERS const& workers) {
    string worker;
    string const noSpecificDatabase;
    bool   const allDatabases = true;
    size_t numReplicas = numeric_limits<size_t>::max();
    for (auto&& candidateWorker: workers) {
        size_t const num =
            databaseServices->numWorkerReplicas(candidateWorker,
                                                noSpecificDatabase,
                                                allDatabases);
        if (num < numReplicas) {
            numReplicas = num;
            worker = candidateWorker;
        }
    }
    return worker;
}


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

}  // namespace


namespace lsst {
namespace qserv {
namespace replica {

string const HttpProcessor::_partitionByColumn = "qserv_trans_id";
string const HttpProcessor::_partitionByColumnType = "INT NOT NULL";


HttpProcessor::Ptr HttpProcessor::create(
                        Controller::Ptr const& controller,
                        unsigned int workerResponseTimeoutSec,
                        HealthMonitorTask::Ptr const& healthMonitorTask) {

    auto ptr = Ptr(new HttpProcessor(
        controller,
        workerResponseTimeoutSec,
        healthMonitorTask
    ));
    ptr->_initialize();
    return ptr;
}


HttpProcessor::HttpProcessor(Controller::Ptr const& controller,
                             unsigned int workerResponseTimeoutSec,
                             HealthMonitorTask::Ptr const& healthMonitorTask)
    :   EventLogger(controller,
                    taskName),
        _workerResponseTimeoutSec(workerResponseTimeoutSec),
        _catalogsModule(
            HttpCatalogsModule::create(
                controller,
                taskName,
                workerResponseTimeoutSec
            )
        ),
        _replicationLevelsModule(
            HttpReplicationLevelsModule::create(
                controller,
                taskName,
                workerResponseTimeoutSec,
                healthMonitorTask
            )
        ),
        _workerStatusModule(
            HttpWorkerStatusModule::create(
                controller,
                taskName,
                workerResponseTimeoutSec,
                healthMonitorTask
            )
        ),
        _controllersModule(
            HttpControllersModule::create(
                controller,
                taskName,
                workerResponseTimeoutSec
            )
        ),
        _requestsModule(
            HttpRequestsModule::create(
                controller,
                taskName,
                workerResponseTimeoutSec
            )
        ),
        _jobsModule(
            HttpJobsModule::create(
                controller,
                taskName,
                workerResponseTimeoutSec
            )
        ),
        _configurationModule(
            HttpConfigurationModule::create(
                controller,
                taskName,
                workerResponseTimeoutSec
            )
        ),
        _log(LOG_GET("lsst.qserv.replica.HttpProcessor")) {
}


HttpProcessor::~HttpProcessor() {
    logOnStopEvent();
    controller()->serviceProvider()->httpServer()->stop();
}


void HttpProcessor::_initialize() {

    logOnStartEvent();

    auto self = shared_from_this();

    controller()->serviceProvider()->httpServer()->addHandlers({
        {"GET", "/replication/v1/catalogs",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _catalogsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/level",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _replicationLevelsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/worker",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _workerStatusModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/controller",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _controllersModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/controller/:id",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _controllersModule->execute(req, resp, "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/v1/request",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _requestsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/request/:id",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _requestsModule->execute(req, resp, "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/v1/job",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _jobsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/job/:id",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _jobsModule->execute(req, resp, "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/v1/config",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp);
            }
        },
        {"PUT", "/replication/v1/config/general",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "UPDATE-GENERAL");
            }
        },
        {"PUT", "/replication/v1/config/worker/:name",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "UPDATE-WORKER");
            }
        },

        {"DELETE", "/replication/v1/config/worker/:name",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "DELETE-WORKER");
            }
        },
        {"POST", "/replication/v1/config/worker",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "ADD-WORKER");
            }
        },
        {"DELETE", "/replication/v1/config/family/:name",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "DELETE-DATABASE-FAMILY");
            }
        },
        {"POST", "/replication/v1/config/family",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "ADD-DATABASE-FAMILY");
            }
        },
        {"DELETE", "/replication/v1/config/database/:name",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "DELETE-DATABASE");
            }
        },
        {"POST", "/replication/v1/config/database",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "ADD-DATABASE");
            }
        },
        {"DELETE", "/replication/v1/config/table/:name",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "DELETE-TABLE");
            }
        },
        {"POST", "/replication/v1/config/table",
            [&](qhttp::Request::Ptr  const& req,
                qhttp::Response::Ptr const& resp) {
                _configurationModule->execute(req, resp, "ADD-TABLE");
            }
        },

        {"POST",   "/replication/v1/sql/query", bind(&HttpProcessor::_sqlQuery, self, _1, _2)},

        {"GET",    "/replication/v1/qserv/worker/status", bind(&HttpProcessor::_getQservManyWorkersStatus, self, _1, _2)},
        {"GET",    "/replication/v1/qserv/worker/status/:name", bind(&HttpProcessor::_getQservWorkerStatus, self, _1, _2)},
        {"GET",    "/replication/v1/qserv/master/query", bind(&HttpProcessor::_getQservManyUserQuery, self, _1, _2)},
        {"GET",    "/replication/v1/qserv/master/query/:id", bind(&HttpProcessor::_getQservUserQuery, self, _1, _2)},

        {"GET",    "/ingest/v1/trans", bind(&HttpProcessor::_getTransactions, self, _1, _2)},
        {"GET",    "/ingest/v1/trans/:id", bind(&HttpProcessor::_getTransaction, self, _1, _2)},
        {"POST",   "/ingest/v1/trans", bind(&HttpProcessor::_beginTransaction, self, _1, _2)},
        {"PUT",    "/ingest/v1/trans/:id", bind(&HttpProcessor::_endTransaction, self, _1, _2)},
        {"POST",   "/ingest/v1/database", bind(&HttpProcessor::_addDatabase, self, _1, _2)},
        {"PUT",    "/ingest/v1/database/:name", bind(&HttpProcessor::_publishDatabase, self, _1, _2)},
        {"DELETE", "/ingest/v1/database/:name", bind(&HttpProcessor::_deleteDatabase, self, _1, _2)},
        {"POST",   "/ingest/v1/table", bind(&HttpProcessor::_addTable, self, _1, _2)},
        {"POST",   "/ingest/v1/chunk", bind(&HttpProcessor::_addChunk, self, _1, _2)},
        {"POST",   "/ingest/v1/chunk/empty", bind(&HttpProcessor::_buildEmptyChunksList, self, _1, _2)}
    });
    controller()->serviceProvider()->httpServer()->start();
}


string HttpProcessor::_context() const {
    return taskName + " ";
}


void HttpProcessor::_info(string const& msg) const {
    LOGS(_log, LOG_LVL_INFO, _context() << msg);
}


void HttpProcessor::_debug(string const& msg) const {
    LOGS(_log, LOG_LVL_DEBUG, _context() << msg);
}


void HttpProcessor::_error(string const& msg) const {
    LOGS(_log, LOG_LVL_ERROR, _context() << msg);
}


void HttpProcessor::_sqlQuery(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        // All parameters must be provided via the body of the request

        HttpRequestBody body(req);
        auto const worker   = body.required<string>("worker");
        auto const query    = body.required<string>("query");
        auto const user     = body.required<string>("user");
        auto const password = body.required<string>("password");
        auto const maxRows  = body.optional<uint64_t>("max_rows", 0);

        _debug(string(__func__) + " worker="   + worker);
        _debug(string(__func__) + " query="    + query);
        _debug(string(__func__) + " user="     + user);
        _debug(string(__func__) + " maxRows="  + to_string(maxRows));

        auto const request = controller()->sqlQuery(
            worker,
            query,
            user,
            password,
            maxRows
        );
        request->wait();

        json result;
        result["result_set"] = request->responseData().toJson();

        bool const success = request->extendedState() == Request::SUCCESS ? 1 : 0;
        _sendData(resp, result, success);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}

void HttpProcessor::_getQservManyWorkersStatus(qhttp::Request::Ptr const& req,
                                               qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        HttpRequestQuery const query(req->query);
        unsigned int const timeoutSec    = query.optionalUInt("timeout_sec",    _workerResponseTimeoutSec);
        bool         const keepResources = query.optionalUInt("keep_resources", 0) != 0;

        _debug(string(__func__) + " timeout_sec=" + to_string(timeoutSec));

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
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getQservWorkerStatus(qhttp::Request::Ptr const& req,
                                          qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const worker = req->params.at("name");

        HttpRequestQuery const query(req->query);
        unsigned int const timeoutSec = query.optionalUInt("timeout_sec", _workerResponseTimeoutSec);

        _debug(string(__func__) + " worker=" + worker);
        _debug(string(__func__) + " timeout_sec=" + to_string(timeoutSec));

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
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getQservManyUserQuery(qhttp::Request::Ptr const& req,
                                           qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();

        HttpRequestQuery const query(req->query);
        unsigned int const timeoutSec = query.optionalUInt("timeout_sec", _workerResponseTimeoutSec);
        unsigned int const limit4past = query.optionalUInt("limit4past", 1);

        _debug(string(__func__) + " timeout_sec=" + to_string(timeoutSec));
        _debug(string(__func__) + " limit4past=" + to_string(limit4past));

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
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getQservUserQuery(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const id = stoull(req->params.at("id"));

        _debug(string(__func__) + " id=" + to_string(id));

        json result;
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


json HttpProcessor::_getQueries(json& workerInfo) const {

    // Find identifiers of all queries in the wait queues of all schedulers
    set<QueryId> qids;
    for (auto&& scheduler: workerInfo.at("processor").at("queries").at("blend_scheduler").at("schedulers")) {
        for (auto&& entry: scheduler.at("query_id_to_count")) {
            qids.insert(entry[0].get<QueryId>());
        }
    }

    // Connect to the database service of the Qserv Master
    auto const config = controller()->serviceProvider()->config();
    database::mysql::ConnectionParams const connectionParams(
        config->qservMasterDatabaseHost(),
        config->qservMasterDatabasePort(),
        "root",
        Configuration::qservMasterDatabasePassword(),
        "qservMeta"
    );
    auto const conn = database::mysql::Connection::open(connectionParams);

    // Extract descriptions of those queries from qservMeta

    json result;

    if (not qids.empty()) {
        conn->execute(
            "SELECT * FROM " + conn->sqlId("QInfo") +
            "  WHERE "       + conn->sqlIn("queryId", qids)
        );
        if (conn->hasResult()) {

            database::mysql::Row row;
            while (conn->next(row)) {

                QueryId queryId;
                if (not row.get("queryId", queryId)) continue;

                string query;
                string status;
                string submitted;
                string completed;

                row.get("query",     query);
                row.get("status",    status);
                row.get("submitted", submitted);
                row.get("completed", completed);

                string queryIdStr = to_string(queryId);
                result[queryIdStr]["query"]     = query;
                result[queryIdStr]["status"]    = status;
                result[queryIdStr]["submitted"] = submitted;
                result[queryIdStr]["completed"] = completed;
            }
        }
    }
    return result;
}


void HttpProcessor::_getTransactions(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);


    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        HttpRequestQuery const query(req->query);
        auto const database     = query.optionalString("database");
        auto const family       = query.optionalString("family");
        auto const allDatabases = query.optionalUInt64("all_databases", 0) != 0;
        auto const isPublished  = query.optionalUInt64("is_published",  0) != 0;

        _debug(string(__func__) + " database=" + database);
        _debug(string(__func__) + " family=" + family);
        _debug(string(__func__) + " all_databases=" + string(allDatabases ? "1": "0"));
        _debug(string(__func__) + " is_published=" + string(isPublished ? "1": "0"));

        vector<string> databases;
        if (database.empty()) {
            databases = config->databases(family, allDatabases, isPublished);
        } else {
            databases.push_back(database);
        }

        json result;
        result["databases"] = json::object();
        for (auto&& database: databases) {

            bool const allWorkers = true;
            vector<unsigned int> chunks;
            databaseServices->findDatabaseChunks(chunks, database, allWorkers);

            result["databases"][database]["info"] = config->databaseInfo(database).toJson();
            result["databases"][database]["num_chunks"] = chunks.size();

            result["databases"][database]["transactions"] = json::array();
            for (auto&& transaction: databaseServices->transactions(database)) {
                result["databases"][database]["transactions"].push_back(transaction.toJson());
            }
        }
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getTransaction(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const id = stoul(req->params.at("id"));

        _debug(__func__, "id=" + to_string(id));

        auto const transaction = databaseServices->transaction(id);

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, transaction.database, allWorkers);

        json result;
        result["databases"][transaction.database]["info"] = config->databaseInfo(transaction.database).toJson();
        result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
        result["databases"][transaction.database]["num_chunks"] = chunks.size();

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_beginTransaction(qhttp::Request::Ptr const& req,
                                      qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    uint32_t id = 0;
    string database;

    auto const logBeginTransaction = [&](string const& status, string const& msg=string()) {
        ControllerEvent event;
        event.operation = "BEGIN TRANSACTION";
        event.status = status;
        event.kvInfo.emplace_back("id", to_string(id));
        event.kvInfo.emplace_back("database", database);
        if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
        logEvent(event);
    };
    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        HttpRequestBody body(req);

        auto const database = body.required<string>("database");

        _debug(__func__, "database=" + database);

        auto const databaseInfo = config->databaseInfo(database);
        if (databaseInfo.isPublished) {
            _sendError(resp, __func__, "the database is already published");
            return;
        }
        auto const transaction = databaseServices->beginTransaction(databaseInfo.name);

        _addPartitionToSecondaryIndex(databaseInfo, transaction.id);

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, databaseInfo.name, allWorkers);

        json result;
        result["databases"][transaction.database]["info"] = config->databaseInfo(databaseInfo.name).toJson();
        result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
        result["databases"][transaction.database]["num_chunks"] = chunks.size();

        _sendData(resp, result);
        logBeginTransaction("SUCCESS");

    } catch (invalid_argument const& ex) {
        auto const msg = "invalid parameters of the request, ex: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logBeginTransaction("FAILED", msg);
    } catch (exception const& ex) {
        auto const msg = "operation failed due to: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logBeginTransaction("FAILED", msg);
    }
}


void HttpProcessor::_endTransaction(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    uint32_t id = 0;
    string database;
    bool abort = false;
    bool buildSecondaryIndex = false;

    auto const logEndTransaction = [&](string const& status, string const& msg=string()) {
        ControllerEvent event;
        event.operation = "END TRANSACTION";
        event.status = status;
        event.kvInfo.emplace_back("id", to_string(id));
        event.kvInfo.emplace_back("database", database);
        event.kvInfo.emplace_back("abort", abort ? "true" : "false");
        event.kvInfo.emplace_back("build-secondary-index", buildSecondaryIndex ? "true" : "false");
        if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
        logEvent(event);
    };
    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        id = stoul(req->params.at("id"));

        HttpRequestQuery const query(req->query);
        abort               = query.requiredBool("abort");
        buildSecondaryIndex = query.optionalBool("build-secondary-index");

        _debug(__func__, "id="    + to_string(id));
        _debug(__func__, "abort=" + to_string(abort ? 1 : 0));
        _debug(__func__, "build-secondary-index=" + to_string(abort ? 1 : 0));

        auto const transaction = databaseServices->endTransaction(id, abort);
        auto const databaseInfo = config->databaseInfo(transaction.database);
        database = transaction.database;

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, transaction.database, allWorkers);

        json result;
        result["databases"][transaction.database]["info"] = config->databaseInfo(transaction.database).toJson();
        result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
        result["databases"][transaction.database]["num_chunks"] = chunks.size();
        result["secondary-index-build-success"] = 0;

        if (abort) {

            // Drop the transaction-specific MySQL partition from the relevant tables
            auto const job = AbortTransactionJob::create(transaction.id, allWorkers, controller());
            job->start();
            logJobStartedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            job->wait();
            logJobFinishedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            result["data"] = job->getResultData().toJson();

            _removePartitionFromSecondaryIndex(databaseInfo, transaction.id);

        } else {

            // Make the best attempt to build a layer at the "secondary index"
            // if requested.
            if (buildSecondaryIndex) {
                bool const hasTransactions = true;
                string const destinationPath = transaction.database + "__" + databaseInfo.directorTable;
                auto const job = IndexJob::create(
                    transaction.database,
                    hasTransactions,
                    transaction.id,
                    allWorkers,
                    IndexJob::TABLE,
                    destinationPath,
                    controller()
                );
                job->start();
                logJobStartedEvent(IndexJob::typeName(), job, databaseInfo.family);
                job->wait();
                logJobFinishedEvent(IndexJob::typeName(), job, databaseInfo.family);
                result["secondary-index-build-success"] = job->extendedState() == Job::SUCCESS ? 1 : 0;
            }

            // TODO: replicate MySQL partition associated with the transaction
            _error(__func__, "replication stage is not implemented");
        }
        _sendData(resp, result);
        logEndTransaction("SUCCESS");

    } catch (invalid_argument const& ex) {
        auto const msg = "invalid parameters of the request, ex: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logEndTransaction("FAILED", msg);
    } catch (exception const& ex) {
        auto const msg = "operation failed due to: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logEndTransaction("FAILED", msg);
    }
}


void HttpProcessor::_addDatabase(qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();

        HttpRequestBody body(req);

        DatabaseInfo databaseInfo;
        databaseInfo.name = body.required<string>("database");

        auto const numStripes    = body.required<unsigned int>("num_stripes");
        auto const numSubStripes = body.required<unsigned int>("num_sub_stripes");
        auto const overlap       = body.required<double>("overlap");

        _debug(string(__func__) + " database="      + databaseInfo.name);
        _debug(string(__func__) + " numStripes="    + to_string(numStripes));
        _debug(string(__func__) + " numSubStripes=" + to_string(numSubStripes));
        _debug(string(__func__) + " overlap="       + to_string(overlap));

        if (overlap < 0) {
            _sendError(resp, __func__, "overlap can't have a negative value");
            return;
        }

        // Find an appropriate database family for the database. If none
        // found then create a new one named after the database.

        string familyName;
        for (auto&& candidateFamilyName: config->databaseFamilies()) {
            auto const familyInfo = config->databaseFamilyInfo(candidateFamilyName);
            if ((familyInfo.numStripes == numStripes) and (familyInfo.numSubStripes == numSubStripes)
                and (abs(familyInfo.overlap - overlap) <= numeric_limits<double>::epsilon())) {
                familyName = candidateFamilyName;
            }
        }
        if (familyName.empty()) {

            // When creating the family use partitioning attributes as the name of the family
            // as shown below:
            //
            //   layout_<numStripes>_<numSubStripes>

            familyName = "layout_" + to_string(numStripes) + "_" + to_string(numSubStripes);
            DatabaseFamilyInfo familyInfo;
            familyInfo.name = familyName;
            familyInfo.replicationLevel = 1;
            familyInfo.numStripes = numStripes;
            familyInfo.numSubStripes = numSubStripes;
            familyInfo.overlap = overlap;
            config->addDatabaseFamily(familyInfo);
        }
        
        // Create the database at all QServ workers

        bool const allWorkers = true;
        auto const job = SqlCreateDbJob::create(
            databaseInfo.name,
            allWorkers,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlCreateDbJob::typeName(), job, familyName);
        job->wait();
        logJobFinishedEvent(SqlCreateDbJob::typeName(), job, familyName);

        string error;
        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            auto&& workerResultSet = itr.second;
            for (auto&& resultSet: workerResultSet) {
                if (not resultSet.error.empty()) {
                    error += "database creation failed on worker: " + worker + ",  error: " +
                             resultSet.error + " ";
                }
            }
        }
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        // Register the new database in the Configuration.
        // Note, this operation will fail if the database with the name
        // already exists. Also, the new database won't have any tables
        // until they will be added as a separate step.

        databaseInfo.family = familyName;
        databaseInfo.isPublished = false;

        databaseInfo = config->addDatabase(databaseInfo);

        // Tell workers to reload their configurations

        unsigned int const workerResponseTimeoutSec = 60;
        error = _reconfigureWorkers(databaseInfo, allWorkers, workerResponseTimeoutSec);
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        json result;
        result["database"] = databaseInfo.toJson();
    
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_publishDatabase(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        bool const allWorkers = true;
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const config = controller()->serviceProvider()->config();

        auto const database = req->params.at("name");

        HttpRequestQuery const query(req->query);
        bool const consolidateSecondayIndex = query.optionalBool("consolidate_secondary_index", false);

        _debug(__func__, "database=" + database);
        _debug(__func__, "consolidate_secondary_index=" + to_string(consolidateSecondayIndex ? 1 : 0));

        auto const databaseInfo = config->databaseInfo(database);
        if (databaseInfo.isPublished) {
            _sendError(resp, __func__, "the database is already published");
            return;
        }

        // Scan super-transactions to make sure none is still open
        for (auto&& t: databaseServices->transactions(databaseInfo.name)) {
            if (t.state == "STARTED") {
                _sendError(resp, __func__, "database has uncommitted transactions");
                return;
            }
        }
        
        // ATTENTION: this operation may take a while if the table has
        // a large number of entries
        if (consolidateSecondayIndex) _consolidateSecondaryIndex(databaseInfo);

        // Grant SELECT authorizations for the new database to Qserv
        // MySQL account(s) at all workers and the master(s)
        if (not _grantDatabaseAccess(resp, databaseInfo, allWorkers)) return;

        // Enable this database in Qserv workers by adding an entry
        // to table 'qservw_worker.Dbs'
        if (not _enableDatabase(resp, databaseInfo, allWorkers)) return;

        // Consolidate MySQL-partitioned tables at workers
        if (not _removeMySQLPartitions(resp, databaseInfo, allWorkers)) return;

        // Tell workers to reload their configurations

        unsigned int const workerResponseTimeoutSec = 60;
        auto const error = _reconfigureWorkers(databaseInfo, allWorkers, workerResponseTimeoutSec);
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        // Finalize setting the database in Qserv master
        //
        // NOTE: the rest should be taken care of by the Replication system.
        // This includes registering chunks in the persistent store of the Replication
        // system, synchronizing with Qserv workers, fixing, re-balancing,
        // replicating, etc.

        _publishDatabaseInMaster(databaseInfo);

        ControllerEvent event;
        event.status = "PUBLISH DATABASE";
        event.kvInfo.emplace_back("database", database);
        logEvent(event);

        json result;
        result["database"] = config->publishDatabase(database).toJson();
    
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_deleteDatabase(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        bool const allWorkers = true;
        auto const database = req->params.at("name");

        HttpRequestQuery const query(req->query);
        bool const deleteSecondaryIndex = query.optionalBool("delete_secondary_index", false);

        _debug(__func__, "database=" + database);
        _debug(__func__, "delete_secondary_index=" + to_string(deleteSecondaryIndex ? 1 : 0));

        auto const databaseInfo = config->databaseInfo(database);
        if (databaseInfo.isPublished) {
            _sendError(resp, __func__, "unable to delete the database which is already published");
            return;
        }

        // Eliminate the secondary index
        if (deleteSecondaryIndex) _deleteSecondaryIndex(databaseInfo);

        // Delete database entries at workers
        auto const job = SqlDeleteDbJob::create(databaseInfo.name, allWorkers, controller());
        job->start();
        logJobStartedEvent(SqlDeleteDbJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlDeleteDbJob::typeName(), job, databaseInfo.family);

        string error;
        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            auto&& workerResultSet = itr.second;
            for (auto&& resultSet: workerResultSet) {
                if (not resultSet.error.empty()) {
                    error += "table creation failed on worker: " + worker + ",  error: " +
                             resultSet.error + " ";
                }
            }
        }
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        // Remove database entry from the Configuration. This will also eliminate all
        // dependent metadata, such as replicas info
        config->deleteDatabase(databaseInfo.name);

        // Ask all workers to reload their configurations
        unsigned int const workerResponseTimeoutSec = 60;
        error = _reconfigureWorkers(databaseInfo, allWorkers, workerResponseTimeoutSec);
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        json result;
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addTable(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();

        HttpRequestBody body(req);

        auto const database      = body.required<string>("database");
        auto const table         = body.required<string>("table");
        auto const isPartitioned = (bool)body.required<int>("is_partitioned");
        auto const schema        = body.required<json>("schema");
        auto const isDirector    = (bool)body.required<int>("is_director");
        auto const directorKey   = body.optional<string>("director_key", "");
        auto const chunkIdKey    = body.optional<string>("chunk_id_key", "");
        auto const subChunkIdKey = body.optional<string>("sub_chunk_id_key", "");
        auto const latitudeColName  = body.optional<string>("latitude_key",  "");
        auto const longitudeColName = body.optional<string>("longitude_key", "");

        _debug(string(__func__) + " database="      + database);
        _debug(string(__func__) + " table="         + table);
        _debug(string(__func__) + " isPartitioned=" + (isPartitioned ? "1" : "0"));
        _debug(string(__func__) + " schema="        + schema.dump());
        _debug(string(__func__) + " isDirector="    + (isDirector ? "1" : "0"));
        _debug(string(__func__) + " directorKey="   + directorKey);
        _debug(string(__func__) + " chunkIdKey="    + chunkIdKey);
        _debug(string(__func__) + " subChunkIdKey=" + subChunkIdKey);
        _debug(string(__func__) + " latitudeColName="  + latitudeColName);
        _debug(string(__func__) + " longitudeColName=" + longitudeColName);

        // Make sure the database is known and it's not PUBLISHED yet

        auto databaseInfo = config->databaseInfo(database);
        if (databaseInfo.isPublished) {
           _sendError(resp, __func__, "the database is already published");
            return;
        }

        // Make sure the table doesn't exist in the Configuration

        for (auto&& existingTable: databaseInfo.tables()) {
            if (table == existingTable) {
                _sendError(resp, __func__, "table already exists");
                return;
            }
        }

        // Translate table schema

        if (schema.is_null()) {
            _sendError(resp, __func__, "table schema is empty");
            return;
        }
        if (not schema.is_array()) {
            _sendError(resp, __func__, "table schema is not defined as an array");
            return;
        }

        list<pair<string,string>> columns;

        // The name of a special column for the super-transaction-based ingest.
        // Always insert this column as the very first one into the schema.
        columns.emplace_front(_partitionByColumn, _partitionByColumnType);

        for (auto&& coldef: schema) {
            if (not coldef.is_object()) {
                _sendError(resp, __func__,
                        "columns definitions in table schema are not JSON objects");
                return;
            }
            if (0 == coldef.count("name")) {
                _sendError(resp, __func__,
                        "column attribute 'name' is missing in table schema for "
                        "column number: " + to_string(columns.size() + 1));
                return;
            }
            string colName = coldef["name"];
            if (0 == coldef.count("type")) {
                _sendError(resp, __func__,
                        "column attribute 'type' is missing in table schema for "
                        "column number: " + to_string(columns.size() + 1));
                return;
            }
            string colType = coldef["type"];
            
            if (_partitionByColumn == colName) {
                _sendError(resp, __func__,
                        "reserved column '" + _partitionByColumn + "' is not allowed");
                return;
            }
            columns.emplace_back(colName, colType);
        }

        // TODO: if this is a partitioned table then add columns for
        //       chunk and sub-chunk numbers provided with the request.
        //       Check if these columns aren't present in the schema.
        //       Make sure they're provided for the partitioned table.

        // Create template tables on all workers. These tables will be used
        // to create chunk-specific tables before loading data.

        bool const allWorkers = true;
        string const engine = "MyISAM";

        auto const job = SqlCreateTableJob::create(
            database,
            table,
            engine,
            _partitionByColumn,
            columns,
            allWorkers,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);

        string error;
        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            auto&& workerResultSet = itr.second;
            for (auto&& resultSet: workerResultSet) {
                if (not resultSet.error.empty()) {
                    error += "table creation failed on worker: " + worker + ",  error: " +
                             resultSet.error + " ";
                }
            }
        }
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        // Register table in the Configuration

        json result;
        result["database"] = config->addTable(
            database, table, isPartitioned, columns, isDirector,
            directorKey, chunkIdKey, subChunkIdKey,
            latitudeColName, longitudeColName
        ).toJson();

        // Create the secondary index table using an updated version of
        // the database descriptor.

        if (isPartitioned and isDirector) _createSecondaryIndex(config->databaseInfo(database));

        // Ask all workers to reload their configurations

        unsigned int const workerResponseTimeoutSec = 60;
        error = _reconfigureWorkers(databaseInfo, allWorkers, workerResponseTimeoutSec);
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__,  "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addChunk(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        HttpRequestBody body(req);

        uint32_t const transactionId = body.required<uint32_t>("transaction_id");
        unsigned int const chunk = body.required<unsigned int>("chunk");

        _debug(string(__func__) + " transactionId=" + to_string(transactionId));
        _debug(string(__func__) + " chunk=" + to_string(chunk));

        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const config = controller()->serviceProvider()->config();

        auto const transactionInfo = databaseServices->transaction(transactionId);
        if (transactionInfo.state != "STARTED") {
            _sendError(resp, __func__, "this transaction is already over");
            return;
        }
        auto const databaseInfo = config->databaseInfo(transactionInfo.database);
        auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

        ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                                  databaseFamilyInfo.numSubStripes);
        if (not validator.valid(chunk)) {
            _sendError(resp, __func__, "this chunk number is not valid");
            return;
        }

        // This locks prevents other invocations of the method from making different
        // decisions on a chunk placement.
        util::Lock lock(_ingestManagementMtx, "HttpProcessor::" + string(__func__));

        // Decide on a worker where the chunk is best to be located.
        // If the chunk is already there then use it. Otherwise register an empty chunk
        // at some least loaded worker.
        //
        // ATTENTION: the current implementation of the algorithm assumes that
        // newly ingested chunks won't have replicas. This will change later
        // when the Replication system will be enhanced to allow creating replicas
        // of chunks within UNPUBLISHED databases.
        
        string worker;

        vector<ReplicaInfo> replicas;
        databaseServices->findReplicas(replicas, chunk, transactionInfo.database);
        if (replicas.size() > 1) {
            _sendError(resp, __func__, "this chunk has too many replicas");
            return;
        }
        if (replicas.size() == 1) {
            worker = replicas[0].worker();
        } else {

            // Search chunk in all databases of the same family to see
            // which workers may have replicas of the same chunk.
            // The idea here is to ensure the 'chunk colocation' requirements
            // is met, so that no unnecessary replica migration will be needed
            // when the database will be being published.
 
            bool const allDatabases = true;

            set<string> candidateWorkers;
            for (auto&& database: config->databases(databaseInfo.family, allDatabases)) {
                vector<ReplicaInfo> replicas;
                databaseServices->findReplicas(replicas, chunk, database);
                for (auto&& replica: replicas) {
                    candidateWorkers.insert(replica.worker());
                }
            }
            if (not candidateWorkers.empty()) {

                // Among those workers which have been found to have replicas with
                // the same chunk pick the one which has the least number of replicas
                // (of any chunks in any databases). The goal here is to ensure all
                // workers are equally loaded with data.
                //
                // NOTE: a decision of which worker is 'least loaded' is based
                // purely on the replica count, not on the amount of data residing
                // in the workers databases.

                worker = ::leastLoadedWorker(databaseServices, candidateWorkers);

            } else {

                // We got here because no database within the family has a chunk
                // with this number. Hence we need to pick some least loaded worker
                // among all known workers. 

                worker = ::leastLoadedWorker(databaseServices, config->workers());
            }

            // Register the new chunk
            //
            // TODO: Use status COMPLETE for now. Consider extending schema
            // of table 'replica' to store the status as well. This will allow
            // to differentiate between the 'INGEST_PRIMARY' and 'INGEST_SECONDARY' replicas,
            // which will be used for making the second replica of a chunk and selecting
            // the right version for further ingests.

            auto const verifyTime = PerformanceUtils::now();
            ReplicaInfo const newReplica(ReplicaInfo::Status::COMPLETE,
                                         worker,
                                         transactionInfo.database,
                                         chunk,
                                         verifyTime);
            databaseServices->saveReplicaInfo(newReplica);
        }
        
        // The sanity check, just to make sure we've found a worker
        if (worker.empty()) {
            _sendError(resp, __func__, "no suitable worker found");
            return;
        }
        ControllerEvent event;
        event.status = "ADD CHUNK";
        event.kvInfo.emplace_back("transaction", to_string(transactionInfo.id));
        event.kvInfo.emplace_back("database", transactionInfo.database);
        event.kvInfo.emplace_back("worker", worker);
        event.kvInfo.emplace_back("chunk", to_string(chunk));
        logEvent(event);

        // Pull connection parameters of the loader for the worker

        auto const workerInfo = config->workerInfo(worker);

        json result;
        result["location"]["worker"] = workerInfo.name;
        result["location"]["host"]   = workerInfo.loaderHost;
        result["location"]["port"]   = workerInfo.loaderPort;

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_buildEmptyChunksList(qhttp::Request::Ptr const& req,
                                          qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        HttpRequestBody body(req);

        string const database = body.required<string>("database");
        bool const force = (bool)body.optional<int>("force", 0);

        _debug(string(__func__) + " database=" + database);
        _debug(string(__func__) + " force=" + string(force ? "1" : "0"));

        auto const emptyListInfo = _buildEmptyChunksListImpl(database, force);

        json result;
        result["file"] = emptyListInfo.first;
        result["num_chunks"] = emptyListInfo.second;

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


bool HttpProcessor::_grantDatabaseAccess(qhttp::Response::Ptr const& resp,
                                         DatabaseInfo const& databaseInfo,
                                         bool allWorkers) const {
    _debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlGrantAccessJob::create(
        databaseInfo.name,
        config->qservMasterDatabaseUser(),
        allWorkers,
        controller()
    );
    job->start();
    logJobStartedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);

    string error;
    auto const& resultData = job->getResultData();
    for (auto&& itr: resultData.resultSets) {
        auto&& worker = itr.first;
        auto&& workerResultSet = itr.second;
        for (auto&& resultSet: workerResultSet) {
            if (not resultSet.error.empty()) {
                error +=
                        "grant access to a database failed on worker: " + worker +
                        ",  error: " + resultSet.error + " ";
            }
        }
    }
    if (not error.empty()) {
        _sendError(resp, __func__, error);
        return false;
    }
    return true;
}


bool HttpProcessor::_enableDatabase(qhttp::Response::Ptr const& resp,
                                    DatabaseInfo const& databaseInfo,
                                    bool allWorkers) const {
    _debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlEnableDbJob::create(
        databaseInfo.name,
        allWorkers,
        controller()
    );
    job->start();
    logJobStartedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);

    string error;
    auto const& resultData = job->getResultData();
    for (auto&& itr: resultData.resultSets) {
        auto&& worker = itr.first;
        auto&& workerResultSet = itr.second;
        for (auto&& resultSet: workerResultSet) {
            if (not resultSet.error.empty()) {
                error +=
                        "enabling database failed on worker: " + worker + ",  error: " +
                        resultSet.error + " ";
            }
        }
    }
    if (not error.empty()) {
        _sendError(resp, __func__, error);
        return false;
    }
    return true;
}


bool HttpProcessor::_removeMySQLPartitions(qhttp::Response::Ptr const& resp,
                                           DatabaseInfo const& databaseInfo,
                                           bool allWorkers) const {
    _debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    string error;
    for (auto const table: databaseInfo.tables()) {
        auto const job = SqlRemoveTablePartitionsJob::create(
            databaseInfo.name,
            table,
            allWorkers,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);

        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            auto&& workerResultSet = itr.second;
            for (auto&& resultSet: workerResultSet) {
                if (not resultSet.error.empty()) {
                    error +=
                            "MySQL partitions removal failed on worker: " + worker +
                            " for database: " + databaseInfo.name + " and table: " + table +
                            ",  error: " + resultSet.error + " ";
                }
            }
        }
    }
    if (not error.empty()) {
        _sendError(resp, __func__, error);
        return false;
    }
    return true;
}


void HttpProcessor::_publishDatabaseInMaster(DatabaseInfo const& databaseInfo) const {

    auto const config = controller()->serviceProvider()->config();
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    // Connect to the master database as user "root".
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    {
        database::mysql::ConnectionHandler const h(
            database::mysql::Connection::open(
                database::mysql::ConnectionParams(
                    config->qservMasterDatabaseHost(),
                    config->qservMasterDatabasePort(),
                    "root",
                    Configuration::qservMasterDatabasePassword(),
                    ""
                )
            )
        );

        // SQL statements to be executed
        vector<string> statements;

        // Statements for creating the database & table entries

        statements.push_back(
            "CREATE DATABASE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name)
        );
        for (auto const& table: databaseInfo.tables()) {
            string sql = "CREATE TABLE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name) +
                    "." + h.conn->sqlId(table) + " (";
            bool first = true;
            for (auto const& coldef: databaseInfo.columns.at(table)) {
                if (first) {
                    first = false;
                } else {
                    sql += ",";
                }
                sql += h.conn->sqlId(coldef.first) + " " + coldef.second;
            }
            sql += ") ENGINE=InnoDB";
            statements.push_back(sql);
        }

        // Statements for granting SELECT authorizations for the new database
        // to the Qserv account.

        statements.push_back(
            "GRANT ALL ON " + h.conn->sqlId(databaseInfo.name) + ".* TO " +
            h.conn->sqlValue(config->qservMasterDatabaseUser()) + "@" +
            h.conn->sqlValue(config->qservMasterDatabaseHost()));

        // Execute the statements
        //
        // TODO: switch to the more reliable way of executing queries
        // which would also reconnect to the server.

        h.conn->begin();
        for (auto const& query: statements) {
            h.conn->execute(query);
        }
        h.conn->commit();
    }

    // Register the database, tables and the partitioning scheme at CSS

    map<string, string> cssConfig;
    cssConfig["technology"] = "mysql";
    // FIXME: Address translation because CSS MySQL connector doesn't set the TCP protocol
    // option for 'localhost' and tries to connect via UNIX socket.
    cssConfig["hostname"] =
            config->qservMasterDatabaseHost() == "localhost" ?
                "127.0.0.1" :
                config->qservMasterDatabaseHost(),
    cssConfig["port"] = to_string(config->qservMasterDatabasePort());
    cssConfig["username"] = "root";
    cssConfig["password"] = Configuration::qservMasterDatabasePassword();
    cssConfig["database"] = "qservCssData";

    auto const cssAccess =
            css::CssAccess::createFromConfig(cssConfig, config->controllerEmptyChunksDir());
    if (not cssAccess->containsDb(databaseInfo.name)) {

        // First, try to find another database within the same family which
        // has already been published, and the one is found then use it
        // as a template when registering the database in CSS.
        //
        // Otherwise, create a new database using an extended CSS API which
        // will allocate a new partitioning identifier.

        bool const allDatabases = false;
        bool const isPublished = true;
        auto const databases = config->databases(databaseFamilyInfo.name, allDatabases, isPublished);
        if (not databases.empty()) {
            auto const templateDatabase = databases.front();
            cssAccess->createDbLike(databaseInfo.name, templateDatabase);
        } else {

            // This parameter is not used by the current implementation of the CSS API.
            // However, we should give it some meaningless value in case of the implementation
            // will change. (Hopefully) this would trigger an exception.
            int const unusedPartitioningId = -1;

            css::StripingParams const stripingParams(
                    databaseFamilyInfo.numStripes,
                    databaseFamilyInfo.numSubStripes,
                    unusedPartitioningId,
                    databaseFamilyInfo.overlap
            );
            string const storageClass = "L2";
            string const releaseStatus = "RELEASED";
            cssAccess->createDb(databaseInfo.name, stripingParams, storageClass, releaseStatus);
        }
    }

    // Register tables which still hasn't been registered in CSS
    
    for (auto const& table: databaseInfo.regularTables) {
        if (not cssAccess->containsTable(databaseInfo.name, table)) {

            // Neither of those groups of parameters apply to the regular tables,
            // so we're leaving them default constructed. 
            css::PartTableParams const partParams;
            css::ScanTableParams const scanParams;

            cssAccess->createTable(
                databaseInfo.name,
                table,
                databaseInfo.schema4css(table),
                partParams,
                scanParams
            );
        }
    }
    for (auto const& table: databaseInfo.partitionedTables) {
        if (not cssAccess->containsTable(databaseInfo.name, table)) {

            bool const isPartitioned = true;
            bool const hasSubChunks = true;
            css::PartTableParams const partParams(
                databaseInfo.name,
                databaseInfo.directorTable,
                databaseInfo.directorTableKey,
                databaseInfo.latitudeColName.at(table),
                databaseInfo.longitudeColName.at(table),
                databaseFamilyInfo.overlap,     /* same as for other tables of the database family*/
                isPartitioned,
                hasSubChunks
            );

            bool const lockInMem = true;
            int const scanRating = 1;
            css::ScanTableParams const scanParams(lockInMem, scanRating);

            cssAccess->createTable(
                databaseInfo.name,
                table,
                databaseInfo.schema4css(table),
                partParams,
                scanParams
            );
        }
    }
    
    bool const forceRebuild = true;
    _buildEmptyChunksListImpl(databaseInfo.name, forceRebuild);
}


pair<string,size_t> HttpProcessor::_buildEmptyChunksListImpl(string const& database,
                                                             bool force) const {
    _debug(__func__);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
        throw invalid_argument("database is already published");
    }

    bool const enabledWorkersOnly = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database, enabledWorkersOnly);

    set<unsigned int> uniqueChunks;
    for (auto chunk: chunks) uniqueChunks.insert(chunk);

    auto const file = "empty_" + database + ".txt";
    auto const filePath = fs::path(config->controllerEmptyChunksDir()) / file;

    if (not force) {
        boost::system::error_code ec;
        fs::file_status const stat = fs::status(filePath, ec);
        if (stat.type() == fs::status_error) {
            throw runtime_error("failed to check the status of file: " + filePath.string());
        }
        if (fs::exists(stat)) {
            throw runtime_error("'force' is required to overwrite existing file: " + filePath.string());
        }
    }

    _debug(__func__, "creating/opening file: " + filePath.string());
    ofstream ofs(filePath.string());
    if (not ofs.good()) {
        throw runtime_error("failed to create/open file: " + filePath.string());
    }
    unsigned int const maxChunkAllowed = 1000000;
    for (unsigned int chunk = 0; chunk < maxChunkAllowed; ++chunk) {
        if (not uniqueChunks.count(chunk)) {
            ofs << chunk << "\n";
        }
    }
    ofs.flush();
    ofs.close();
    
    return make_pair(file, chunks.size());
}


json HttpProcessor::_chunkInfo(set<int> const& chunks) const {
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


string HttpProcessor::_reconfigureWorkers(DatabaseInfo const& databaseInfo,
                                          bool allWorkers,
                                          unsigned int workerResponseTimeoutSec) const {

    auto const job = ServiceReconfigJob::create(
        allWorkers,
        workerResponseTimeoutSec,
        controller()
    );
    job->start();
    logJobStartedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);

    string error;
    auto const& resultData = job->getResultData();
    for (auto&& itr: resultData.workers) {
        auto&& worker = itr.first;
        auto&& success = itr.second;
        if (not success) {
            error += "reconfiguration failed on worker: " + worker + " ";
        }
    }
    return error;
}


void HttpProcessor::_createSecondaryIndex(DatabaseInfo const& databaseInfo) const {

    if (databaseInfo.directorTable.empty() or databaseInfo.directorTableKey.empty() or
        databaseInfo.chunkIdKey.empty() or databaseInfo.subChunkIdKey.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }
    if (0 == databaseInfo.columns.count(databaseInfo.directorTable)) {
        throw logic_error(
                "no schema found for director table '" + databaseInfo.directorTable +
                "' of database '" + databaseInfo.name + "'");
    }

    // Find types of the secondary index table's columns

    string directorTableKeyType;
    string chunkIdKeyType;
    string subChunkIdKeyType;

    for (auto&& colDef: databaseInfo.columns.at(databaseInfo.directorTable)) {
        auto&& colName = colDef.first;
        auto&& colType = colDef.second;
        if      (colName == databaseInfo.directorTableKey) directorTableKeyType = colType;
        else if (colName == databaseInfo.chunkIdKey)       chunkIdKeyType = colType;
        else if (colName == databaseInfo.subChunkIdKey)    subChunkIdKeyType = colType;
    }
    if (directorTableKeyType.empty() or chunkIdKeyType.empty() or subChunkIdKeyType.empty()) {
        throw logic_error(
                "column definitions for the Object identifier or chunk/sub-chunk identifier"
                " columns are missing in the director table schema for table '" +
                databaseInfo.directorTable + "' of database '" + databaseInfo.name + "'");
    }
    
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
    auto const escapedTableName = h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable);

    vector<string> queries;
    queries.push_back(
        "DROP TABLE IF EXISTS " + escapedTableName
    );
    queries.push_back(
        "CREATE TABLE IF NOT EXISTS " + escapedTableName +
        " (" + h.conn->sqlId(_partitionByColumn)            + " " + _partitionByColumnType + "," +
               h.conn->sqlId(databaseInfo.directorTableKey) + " " + directorTableKeyType   + "," +
               h.conn->sqlId(databaseInfo.chunkIdKey)       + " " + chunkIdKeyType         + "," +
               h.conn->sqlId(databaseInfo.subChunkIdKey)    + " " + subChunkIdKeyType      + ","
               " UNIQUE KEY (" + h.conn->sqlId(_partitionByColumn) + "," + h.conn->sqlId(databaseInfo.directorTableKey) + "),"
               " KEY (" + h.conn->sqlId(databaseInfo.directorTableKey) + ")"
        ") ENGINE=InnoDB PARTITION BY LIST (" + h.conn->sqlId(_partitionByColumn) +
        ") (PARTITION `p0` VALUES IN (0) ENGINE=InnoDB)"
    );

    // Execute the statement
    //
    // TODO: switch to the more reliable way of executing queries
    // which would also reconnect to the server.

    for (auto&& query: queries) {
        _debug(__func__, query);

        h.conn->begin();
        h.conn->execute(query);
        h.conn->commit();
    }
}


void HttpProcessor::_addPartitionToSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                  uint32_t transactionId) const {
    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

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
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " ADD PARTITION (PARTITION `p" + to_string(transactionId) + "` VALUES IN (" + to_string(transactionId) +
        ") ENGINE=InnoDB)";

    // Execute the statement
    //
    // TODO: switch to the more reliable way of executing queries
    // which would also reconnect to the server.

    _debug(__func__, query);

    h.conn->begin();
    h.conn->execute(query);
    h.conn->commit();
}


void HttpProcessor::_removePartitionFromSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                       uint32_t transactionId) const {
    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

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
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " DROP PARTITION `p" + to_string(transactionId) + "`";

    // Execute the statement
    //
    // TODO: switch to the more reliable way of executing queries
    // which would also reconnect to the server.

    _debug(__func__, query);

    h.conn->begin();
    h.conn->execute(query);
    h.conn->commit();
}


void HttpProcessor::_consolidateSecondaryIndex(DatabaseInfo const& databaseInfo) const {

    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

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
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " REMOVE PARTITIONING";

    // Execute the statement
    //
    // TODO: switch to the more reliable way of executing queries
    // which would also reconnect to the server.

    _debug(__func__, query);

    h.conn->begin();
    h.conn->execute(query);
    h.conn->commit();
}


void HttpProcessor::_deleteSecondaryIndex(DatabaseInfo const& databaseInfo) const {

    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

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
    string const query =
        "DROP TABLE IF EXISTS " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable);

    // Execute the statement
    //
    // TODO: switch to the more reliable way of executing queries
    // which would also reconnect to the server.

    _debug(__func__, query);

    h.conn->begin();
    h.conn->execute(query);
    h.conn->commit();
}


void HttpProcessor::_sendError(qhttp::Response::Ptr const& resp,
                               string const& func,
                               string const& error) const {
    _error(func, error);

    json result;
    result["success"] = 0;
    result["error"] = error;

    resp->send(result.dump(), "application/json");
}


void HttpProcessor::_sendData(qhttp::Response::Ptr const& resp,
                              json& result,
                              bool success) {
    result["success"] = success ? 1 : 0;
    result["error"] = "";

    resp->send(result.dump(), "application/json");
}

}}} // namespace lsst::qserv::replica
