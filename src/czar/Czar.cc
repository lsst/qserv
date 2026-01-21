/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "czar/Czar.h"

// System headers
#include <stdexcept>
#include <sys/time.h>
#include <thread>
#include <vector>

// Third-party headers
#include "boost/format.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "ccontrol/ConfigMap.h"
#include "ccontrol/UserQueryResources.h"
#include "ccontrol/UserQuerySelect.h"
#include "ccontrol/UserQueryType.h"
#include "czar/CzarErrors.h"
#include "czar/CzarThreads.h"
#include "czar/HttpSvc.h"
#include "czar/MessageTable.h"
#include "global/LogContext.h"
#include "proto/worker.pb.h"
#include "qdisp/CzarStats.h"
#include "qdisp/QdispPool.h"
#include "qdisp/SharedResources.h"
#include "qproc/DatabaseModels.h"
#include "rproc/InfileMerger.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"
#include "util/common.h"
#include "util/FileMonitor.h"
#include "util/IterableFormatter.h"
#include "util/String.h"
#include "xrdreq/QueryManagementAction.h"
#include "XrdSsi/XrdSsiProvider.hh"

using namespace std;

extern XrdSsiProvider* XrdSsiProviderClient;

// This macro is used to convert empty strings into "0" in order to avoid
// problems with calling std::atoi() when the string is empty.
#define ZERO_IF_EMPTY_STR(x) ((x.empty()) ? "0" : (x))

namespace {

string const createAsyncResultTmpl(
        "CREATE TABLE IF NOT EXISTS %1% "
        "(jobId BIGINT, resultLocation VARCHAR(1024))"
        "ENGINE=MEMORY;"
        "INSERT INTO %1% (jobId, resultLocation) "
        "VALUES (%2%, '%3%')");

LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.Czar");

}  // anonymous namespace

namespace lsst::qserv::czar {

Czar::Ptr Czar::_czar;

Czar::Ptr Czar::createCzar(string const& configFilePath, string const& czarName) {
    _czar.reset(new Czar(configFilePath, czarName));
    return _czar;
}

// Constructors
Czar::Czar(string const& configFilePath, string const& czarName)
        : _czarName(czarName), _czarConfig(cconfig::CzarConfig::create(configFilePath, czarName)) {
    // set id counter to milliseconds since the epoch, mod 1 year.
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    const int year = 60 * 60 * 24 * 365;
    _idCounter = uint64_t(tv.tv_sec % year) * 1000 + tv.tv_usec / 1000;

    auto databaseModels = qproc::DatabaseModels::create(_czarConfig->getCssConfigMap(),
                                                        _czarConfig->getMySqlResultConfig());

    // This should to done first as it adds logging context for new threads
    _uqFactory.reset(new ccontrol::UserQueryFactory(databaseModels, _czarName));

    // NOTE: This steps should be done after constructing the query factory where
    //       the name of the Czar gets translated into a numeric identifier.
    _czarConfig->setId(_uqFactory->userQuerySharedResources()->czarId);

    // Tell workers to cancel any queries that were submitted before this restart of Czar.
    // Figure out which query (if any) was recorded in Czar database before the restart.
    // The id will be used as the high-watermark for queries that need to be cancelled.
    // All queries that have identifiers that are strictly less than this one will
    // be affected by the operation.
    //
    if (_czarConfig->notifyWorkersOnCzarRestart()) {
        try {
            xrdreq::QueryManagementAction::notifyAllWorkers(_czarConfig->getXrootdFrontendUrl(),
                                                            proto::QueryManagement::CANCEL_AFTER_RESTART,
                                                            _czarConfig->id(), _lastQueryIdBeforeRestart());
        } catch (std::exception const& ex) {
            LOGS(_log, LOG_LVL_WARN, ex.what());
        }
    }

    int qPoolSize = _czarConfig->getQdispPoolSize();
    int maxPriority = std::max(0, _czarConfig->getQdispMaxPriority());
    string vectRunSizesStr = _czarConfig->getQdispVectRunSizes();
    vector<int> vectRunSizes = util::String::parseToVectInt(vectRunSizesStr, ":", 1);
    string vectMinRunningSizesStr = _czarConfig->getQdispVectMinRunningSizes();
    vector<int> vectMinRunningSizes = util::String::parseToVectInt(vectMinRunningSizesStr, ":", 0);
    LOGS(_log, LOG_LVL_INFO,
         "INFO qdisp config qPoolSize=" << qPoolSize << " maxPriority=" << maxPriority << " vectRunSizes="
                                        << vectRunSizesStr << " -> " << util::prettyCharList(vectRunSizes)
                                        << " vectMinRunningSizes=" << vectMinRunningSizesStr << " -> "
                                        << util::prettyCharList(vectMinRunningSizes));
    qdisp::QdispPool::Ptr qdispPool =
            make_shared<qdisp::QdispPool>(qPoolSize, maxPriority, vectRunSizes, vectMinRunningSizes);
    qdisp::CzarStats::setup(qdispPool);

    _qdispSharedResources = qdisp::SharedResources::create(qdispPool);

    int xrootdCBThreadsMax = _czarConfig->getXrootdCBThreadsMax();
    int xrootdCBThreadsInit = _czarConfig->getXrootdCBThreadsInit();
    LOGS(_log, LOG_LVL_INFO, "config xrootdCBThreadsMax=" << xrootdCBThreadsMax);
    LOGS(_log, LOG_LVL_INFO, "config xrootdCBThreadsInit=" << xrootdCBThreadsInit);
    XrdSsiProviderClient->SetCBThreads(xrootdCBThreadsMax, xrootdCBThreadsInit);
    int const xrootdSpread = _czarConfig->getXrootdSpread();
    LOGS(_log, LOG_LVL_INFO, "config xrootdSpread=" << xrootdSpread);
    XrdSsiProviderClient->SetSpread(xrootdSpread);
    _queryDistributionTestVer = _czarConfig->getQueryDistributionTestVer();

    LOGS(_log, LOG_LVL_INFO, "Creating czar instance with name " << czarName);
    LOGS(_log, LOG_LVL_INFO, "Czar config: " << *_czarConfig);

    // Watch to see if the log configuration is changed.
    // If LSST_LOG_CONFIG is not defined, there's no good way to know what log
    // configuration file is in use.
    string logConfigFile = std::getenv("LSST_LOG_CONFIG");
    if (logConfigFile.empty()) {
        LOGS(_log, LOG_LVL_ERROR,
             "FileMonitor LSST_LOG_CONFIG was blank, no log configuration file to watch.");
    } else {
        LOGS(_log, LOG_LVL_WARN, "logConfigFile=" << logConfigFile);
        _logFileMonitor = make_shared<util::FileMonitor>(logConfigFile);
    }

    // Start the control server for processing Czar management requests sent
    // by the Replication System. Update the port number in the configuration
    // in case if the server is run on the dynamically allocated port.
    _controlHttpSvc =
            HttpSvc::create(_czarConfig->replicationHttpPort(), _czarConfig->replicationNumHttpThreads());
    auto const port = _controlHttpSvc->start();
    _czarConfig->setReplicationHttpPort(port);

    // Start special threads.
    startRegistryUpdate(_czarConfig);
    startGarbageCollect(_czarConfig);
    startGarbageCollectAsync(_czarConfig);
    startGarbageCollectInProgress(_czarConfig, _uqFactory->userQuerySharedResources()->czarId,
                                  _uqFactory->userQuerySharedResources()->queryMetadata);
}

SubmitResult Czar::submitQuery(string const& query, map<string, string> const& hints) {
    LOGS(_log, LOG_LVL_DEBUG, "New query: " << query << ", hints: " << util::printable(hints));

    util::ConfigStore hintsConfigStore(hints);

    // Analyze query hints
    string clientId = hintsConfigStore.get("client_dst_name");

    // Not being able to get thread id is not fatal,
    // it just means query cannot be associate with particular
    // client/thread and will not be able to be killed later
    int threadId = hintsConfigStore.getInt("server_thread_id", -1);

    string defaultDb = hintsConfigStore.get("db");
    LOGS(_log, LOG_LVL_DEBUG, "Default database is \"" << defaultDb << "\"");

    // make message table name
    string userQueryId = to_string(_idCounter++);
    LOGS(_log, LOG_LVL_DEBUG, "userQueryId: " << userQueryId);
    string resultDb = _czarConfig->getMySqlResultConfig().dbName;
    string const msgTableName = "message_" + userQueryId;
    string const lockName = resultDb + "." + msgTableName;

    // Add logging context with user query ID
    LOG_MDC_SCOPE("TID", userQueryId);

    SubmitResult result;

    // instantiate message table manager
    MessageTable msgTable(lockName, _czarConfig->getMySqlResultConfig());
    try {
        msgTable.lock();
    } catch (std::exception const& exc) {
        result.errorMessage = exc.what();
        return result;
    }

    // make new UserQuery
    // this is atomic
    ccontrol::UserQuery::Ptr uq = _uqFactory->newUserQuery(query, defaultDb, getQdispSharedResources(),
                                                           userQueryId, msgTableName, resultDb);

    // Add logging context with query ID
    QSERV_LOGCONTEXT_QUERY(uq->getQueryId());
    // Generate a log message with the QueryId and the full user query so that problems in the log
    // can be traced back to the source query without accessing the database.
    LOGS(_log, LOG_LVL_WARN,
         "New query:" << query << ", hints:" << util::printable(hints) << " defaultDb:" << defaultDb
                      << " message_table:" << msgTableName);

    // check for errors
    auto error = uq->getError();
    if (not error.empty()) {
        result.errorMessage = uq->getQueryIdString() + " Failed to instantiate query: " + error;
        return result;
    }

    auto resultQuery = uq->getResultQuery();
    result.queryId = uq->getQueryId();

    // spawn background thread to wait until query finishes to unlock,
    // note that lambda stores copies of uq and msgTable.
    auto finalizer = [uq, msgTable]() mutable {
        // Add logging context with query ID
        QSERV_LOGCONTEXT_QUERY(uq->getQueryId());
        LOGS(_log, LOG_LVL_DEBUG, "submitting new query");
        uq->submit();
        uq->join();
        try {
            msgTable.unlock(uq);
            if (uq) uq->discard();
        } catch (std::exception const& exc) {
            // TODO? if this fails there is no way to notify client, and client
            // will likely hang because table may still be locked.
            LOGS(_log, LOG_LVL_ERROR, "Query finalization failed (client likely hangs): " << exc.what());
        }
    };
    LOGS(_log, LOG_LVL_DEBUG, "starting finalizer thread for query");
    thread finalThread(finalizer);
    finalThread.detach();

    // update/cleanup query map
    _updateQueryHistory(clientId, threadId, uq);

    // return all info to caller
    if (uq->isAsync()) {
        // make separate message and result tables to return info about ASYNC query,
        // we do not need to lock message because result is ready before we return
        string const resultTableName = resultDb + ".result_async_" + userQueryId;
        string const asyncLockName = resultDb + ".message_async_" + userQueryId;
        MessageTable msgTable(asyncLockName, _czarConfig->getMySqlResultConfig());
        try {
            _makeAsyncResult(resultTableName, uq->getQueryId(), uq->getResultLocation());
            msgTable.create();
        } catch (std::exception const& exc) {
            result.errorMessage = exc.what();
            return result;
        }

        result.resultTable = resultTableName;
        result.messageTable = asyncLockName;
        if (not resultTableName.empty()) {
            // respond with info about the results table.
            result.resultQuery = "SELECT * FROM " + resultTableName;
        }
    } else {
        result.messageTable = lockName;
        if (not resultQuery.empty()) {
            result.resultTable = resultDb + "." + uq->getResultTableName();
            result.resultQuery = resultQuery;
        }
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "returning result to proxy: resultTable=" << result.resultTable
                                                   << " messageTable=" << result.messageTable
                                                   << " resultQuery=" << result.resultQuery);
    return result;
}

void Czar::killQuery(string const& query, string const& clientId) {
    LOGS(_log, LOG_LVL_INFO, "KILL query: " << query << ", clientId: " << clientId);

    // the query can be one of:
    //   "KILL QUERY NNN" - kills currently running query in thread NNN
    //   "KILL CONNECTION NNN" - kills connection associated with thread NNN
    //                           and all queries in that connection
    //   "KILL NNN" - same as "KILL CONNECTION NNN"
    //   "CANCEL NNN" - kill query with ID=NNN

    // Clean query maps from expired entries
    _cleanupQueryHistory();

    ccontrol::UserQuery::Ptr uq;
    int threadId;
    QueryId queryId;
    if (ccontrol::UserQueryType::isKill(query, threadId)) {
        LOGS(_log, LOG_LVL_DEBUG, "thread ID: " << threadId);
        lock_guard<mutex> lock(_mutex);

        // find it in the client map based in client/thread id
        ClientThreadId ctId(clientId, threadId);
        auto iter = _clientToQuery.find(ctId);
        if (iter == _clientToQuery.end()) {
            LOGS(_log, LOG_LVL_INFO, "Cannot find client thread id: " << threadId);
            throw std::runtime_error("Unknown thread ID: " + query);
        }
        uq = iter->second.lock();
    } else if (ccontrol::UserQueryType::isCancel(query, queryId)) {
        LOGS(_log, LOG_LVL_DEBUG, "query ID: " << queryId);
        lock_guard<mutex> lock(_mutex);

        // find it in the client map based in client/thread id
        auto iter = _idToQuery.find(queryId);
        if (iter == _idToQuery.end()) {
            LOGS(_log, LOG_LVL_INFO, "Cannot find query id: " << queryId);
            throw std::runtime_error("Unknown or finished query ID: " + query);
        }
        uq = iter->second.lock();
    } else {
        throw std::runtime_error("Failed to parse query: " + query);
    }

    // assume this cannot fail or throw
    if (uq) {
        LOGS(_log, LOG_LVL_DEBUG, "Killing query: " << uq->getQueryId());
        // query killing can potentially take very long and we do now want to block
        // proxy from serving other requests so run it in a detached thread
        thread killThread([uq]() {
            uq->kill();
            LOGS(_log, LOG_LVL_DEBUG, "Finished killing query: " << uq->getQueryId());
        });
        killThread.detach();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Query has expired/finished: " << query);
        throw std::runtime_error("Query has already finished: " + query);
    }
}

void Czar::_cleanupQueryHistoryLocked() {
    // _mutex must be locked

    // first cleanup client query maps from completed queries
    for (auto iter = _clientToQuery.begin(); iter != _clientToQuery.end();) {
        if (iter->second.expired()) {
            iter = _clientToQuery.erase(iter);
        } else {
            ++iter;
        }
    }
    for (auto iter = _idToQuery.begin(); iter != _idToQuery.end();) {
        if (iter->second.expired()) {
            iter = _idToQuery.erase(iter);
        } else {
            ++iter;
        }
    }
}

void Czar::_cleanupQueryHistory() {
    lock_guard<mutex> lock(_mutex);
    _cleanupQueryHistoryLocked();
}

void Czar::_updateQueryHistory(string const& clientId, int threadId, ccontrol::UserQuery::Ptr const& uq) {
    lock_guard<mutex> lock(_mutex);

    // first cleanup client query maps from completed queries
    _cleanupQueryHistoryLocked();

    // remember query (weak pointer) in case we want to kill query
    if (uq->getQueryId() != QueryId(0)) {
        _idToQuery.insert(make_pair(uq->getQueryId(), uq));
        LOGS(_log, LOG_LVL_DEBUG,
             "Remembering query ID: " << uq->getQueryId() << " (new map size: " << _idToQuery.size() << ")");
    }
    if (not clientId.empty() and threadId >= 0) {
        ClientThreadId ctId(clientId, threadId);
        _clientToQuery.insert(make_pair(ctId, uq));
        LOGS(_log, LOG_LVL_DEBUG,
             "Remembering query: (" << clientId << ", " << threadId
                                    << ") (new map size: " << _clientToQuery.size() << ")");
    }
}

void Czar::_makeAsyncResult(string const& asyncResultTable, QueryId queryId, string const& resultLoc) {
    auto sqlConn = sql::SqlConnectionFactory::make(_czarConfig->getMySqlResultConfig());
    LOGS(_log, LOG_LVL_DEBUG, "creating async result table " << asyncResultTable);

    sql::SqlErrorObject sqlErr;
    string resultLocEscaped;
    if (not sqlConn->escapeString(resultLoc, resultLocEscaped, sqlErr)) {
        SqlError exc(ERR_LOC, "Failure in escapString", sqlErr);
        LOGS(_log, LOG_LVL_ERROR, exc.message());
        throw exc;
    }

    string query =
            (boost::format(::createAsyncResultTmpl) % asyncResultTable % queryId % resultLocEscaped).str();

    if (not sqlConn->runQuery(query, sqlErr)) {
        SqlError exc(ERR_LOC, "Failure creating async result table", sqlErr);
        LOGS(_log, LOG_LVL_ERROR, exc.message());
        throw exc;
    }
}

SubmitResult Czar::getQueryInfo(QueryId queryId) const {
    string const context = "Czar::" + string(__func__) + " ";
    auto sqlConn = sql::SqlConnectionFactory::make(_czarConfig->getMySqlQmetaConfig());
    string sql =
            "SELECT "
            "qi.status,qc.czarId,qc.czar,UNIX_TIMESTAMP(qi.submitted),UNIX_TIMESTAMP(qi.completed),qi."
            "chunkCount,"
            "qi.collectedBytes,qi.collectedRows,qi.finalRows,qi.messageTable,qi.resultQuery,qi.query,"
            "TRIM(BOTH ',' FROM GROUP_CONCAT(IF(qm.severity='ERROR',qm.message,''))) "
            "FROM QInfo qi "
            "JOIN QCzar qc ON qi.czarId=qc.czarId "
            "LEFT OUTER JOIN QMessages AS qm ON qi.queryId=qm.queryId "
            "WHERE qi.queryId=" +
            to_string(queryId);
    sql::SqlResults results;
    sql::SqlErrorObject err;
    if (!sqlConn->runQuery(sql, results, err)) {
        string const msg = context +
                           "Query to find info for the user query failed, err=" + err.printErrMsg() +
                           ", sql=" + sql;
        throw runtime_error(msg);
    }
    vector<string> colStatus;
    vector<string> colCzarId;
    vector<string> colCzarType;
    vector<string> colSubmitted;
    vector<string> colCompleted;
    vector<string> colChunkCount;
    vector<string> colCollectedBytes;
    vector<string> colCollectedRows;
    vector<string> colFinalRows;
    vector<string> colMessageTable;
    vector<string> colResultQuery;
    vector<string> colQuery;
    vector<string> colError;
    if (!results.extractFirstColumns(err, colStatus, colCzarId, colCzarType, colSubmitted, colCompleted,
                                     colChunkCount, colCollectedBytes, colCollectedRows, colFinalRows,
                                     colMessageTable, colResultQuery, colQuery, colError)) {
        string const msg = context + "Failed to extract info for the user query, err=" + err.printErrMsg() +
                           ", sql=" + sql;
        throw runtime_error(msg);
    }
    if (colStatus.size() != 1) {
        string const msg = context + "Unknown user query, err=" + err.printErrMsg() + ", sql=" + sql;
        throw runtime_error(msg);
    }

    SubmitResult result;
    result.errorMessage = colError[0];
    result.resultTable = "result_" + to_string(queryId);
    result.messageTable = colMessageTable[0];
    result.resultQuery = colResultQuery[0];
    result.query = colQuery[0];
    result.queryId = queryId;
    result.status = colStatus[0];
    result.czarId = stoul(colCzarId[0]);
    result.czarType = colCzarType[0];
    result.collectedBytes = stoull(ZERO_IF_EMPTY_STR(colCollectedBytes[0]));
    result.collectedRows = stoull(ZERO_IF_EMPTY_STR(colCollectedRows[0]));
    result.finalRows = stoull(ZERO_IF_EMPTY_STR(colFinalRows[0]));

    // Pull the progress info on the ongoing query processing if this information is still available.
    // This is a transient information that is only available for queries that are still in progress.
    sql = "SELECT totalChunks,completedChunks,UNIX_TIMESTAMP(queryBegin),UNIX_TIMESTAMP(lastUpdate) FROM "
          "QProgress WHERE queryId=" +
          to_string(queryId);
    if (!sqlConn->runQuery(sql, results, err)) {
        string const msg = context +
                           "Query to find stats for the user query failed, err=" + err.printErrMsg() +
                           ", sql=" + sql;
        throw runtime_error(msg);
    }
    vector<string> colTotalChunks;
    vector<string> colCompletedChunks;
    vector<string> colQueryBeginEpoch;
    vector<string> colLastUpdateEpoch;
    if (!results.extractFirst4Columns(colTotalChunks, colCompletedChunks, colQueryBeginEpoch,
                                      colLastUpdateEpoch, err)) {
        string const msg = context + "Failed to extract stats for the user query, err=" + err.printErrMsg() +
                           ", sql=" + sql;
        throw runtime_error(msg);
    }
    switch (colTotalChunks.size()) {
        case 0:
            // No stats means the query is over. Pull the final stats from the main table.
            result.totalChunks = stoi(ZERO_IF_EMPTY_STR(colChunkCount[0]));
            result.completedChunks = result.totalChunks;
            result.queryBeginEpoch = stoi(ZERO_IF_EMPTY_STR(colSubmitted[0]));
            result.lastUpdateEpoch = stoi(ZERO_IF_EMPTY_STR(colCompleted[0]));
            break;
        case 1:
            // The query might be still in progress
            result.totalChunks = stoi(ZERO_IF_EMPTY_STR(colTotalChunks[0]));
            result.completedChunks = stoi(ZERO_IF_EMPTY_STR(colCompletedChunks[0]));
            result.queryBeginEpoch = stoi(ZERO_IF_EMPTY_STR(colQueryBeginEpoch[0]));
            result.lastUpdateEpoch = stoi(ZERO_IF_EMPTY_STR(colLastUpdateEpoch[0]));
            break;
        default:
            // Should never be here.
            string const msg = context +
                               "Inconsistent stats returned for the user query, err=" + err.printErrMsg() +
                               ", sql=" + sql;
            throw runtime_error(msg);
    }
    return result;
}

QueryId Czar::_lastQueryIdBeforeRestart() const {
    string const context = "Czar::" + string(__func__) + " ";
    auto sqlConn = sql::SqlConnectionFactory::make(_czarConfig->getMySqlQmetaConfig());
    string const sql = "SELECT MAX(queryId) FROM QInfo";
    sql::SqlResults results;
    sql::SqlErrorObject err;
    if (!sqlConn->runQuery(sql, results, err)) {
        string const msg =
                context + "Query to find the last query id failed, err=" + err.printErrMsg() + ", sql=" + sql;
        throw runtime_error(msg);
    }
    string queryIdStr;
    if (!results.extractFirstValue(queryIdStr, err)) {
        string const msg = context + "Failed to extract the last query id from the result set, err=" +
                           err.printErrMsg() + ", sql=" + sql;
        throw runtime_error(msg);
    }
    return stoull(queryIdStr);
}

}  // namespace lsst::qserv::czar
