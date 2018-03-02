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
#include <sys/time.h>
#include <thread>

// Third-party headers
#include "boost/format.hpp"
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/UserQueryType.h"
#include "czar/CzarErrors.h"
#include "czar/MessageTable.h"
#include "rproc/InfileMerger.h"
#include "sql/SqlConnection.h"
#include "util/IterableFormatter.h"
#include "XrdSsi/XrdSsiProvider.hh"


extern XrdSsiProvider *XrdSsiProviderClient;


namespace {

std::string const createAsyncResultTmpl("CREATE TABLE IF NOT EXISTS %1% "
    "(jobId BIGINT, resultLocation VARCHAR(1024))"
    "ENGINE=MEMORY;"
    "INSERT INTO %1% (jobId, resultLocation) "
    "VALUES (%2%, '%3%')");


LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.Czar");

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace czar {

Czar::Ptr Czar::_czar;

Czar::Ptr Czar::createCzar(std::string const& configPath, std::string const& czarName) {
    _czar.reset(new Czar(configPath, czarName));
    return _czar;
}

// Constructors
Czar::Czar(std::string const& configPath, std::string const& czarName)
    : _czarName(czarName), _czarConfig(configPath),
      _idCounter(), _uqFactory(), _clientToQuery(), _mutex() {

    // set id counter to milliseconds since the epoch, mod 1 year.
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    const int year = 60*60*24*365;
    _idCounter = uint64_t(tv.tv_sec % year)*1000 + tv.tv_usec/1000;

    std::string logConfig = _czarConfig.getLogConfig();
    if (not logConfig.empty()) {
        LOG_CONFIG(logConfig);
    }

    int largeResultConcurrent = _czarConfig.getLargeResultConcurrentMerges();
    // TODO:DM-10273 - remove largeResults from configuration
    LOGS(_log, LOG_LVL_INFO, "config largeResultConcurrent=" << largeResultConcurrent);
    _qdispPool = std::make_shared<qdisp::QdispPool>(); // TODO:configuration add to configuration

    int xrootdCBThreadsMax = _czarConfig.getXrootdCBThreadsMax();
    int xrootdCBThreadsInit = _czarConfig.getXrootdCBThreadsInit();
    LOGS(_log, LOG_LVL_INFO, "config xrootdCBThreadsMax=" << xrootdCBThreadsMax);
    LOGS(_log, LOG_LVL_INFO, "config xrootdCBThreadsInit=" << xrootdCBThreadsInit);
    XrdSsiProviderClient->SetCBThreads(xrootdCBThreadsMax, xrootdCBThreadsInit);

    LOGS(_log, LOG_LVL_INFO, "Creating czar instance with name " << czarName);
    LOGS(_log, LOG_LVL_DEBUG, "Czar config: " << _czarConfig);

    _uqFactory.reset(new ccontrol::UserQueryFactory(_czarConfig, _czarName));
}

SubmitResult
Czar::submitQuery(std::string const& query,
                  std::map<std::string, std::string> const& hints) {

    LOGS(_log, LOG_LVL_INFO, "New query: " << query
         << ", hints: " << util::printable(hints));

    util::ConfigStore hintsConfigStore(hints);

    // Analyze query hints
    std::string clientId = hintsConfigStore.get("client_dst_name");

    // Not being able to get thread id is not fatal,
    // it just means query cannot be associate with particular
    // client/thread and will not be able to be killed later
    int threadId = hintsConfigStore.getInt("server_thread_id", -1);

    std::string defaultDb = hintsConfigStore.get("db");
    LOGS(_log, LOG_LVL_INFO, "Default database is \"" << defaultDb <<"\"");

    // make message table name
    std::string userQueryId = std::to_string(_idCounter++);
    LOGS(_log, LOG_LVL_DEBUG, "userQueryId: " << userQueryId);
    std::string resultDb = _czarConfig.getMySqlResultConfig().dbName;
    std::string const msgTableName = "message_" + userQueryId;
    std::string const lockName = resultDb + "." + msgTableName;

    SubmitResult result;

    // instantiate message table manager
    MessageTable msgTable(lockName, _czarConfig.getMySqlResultConfig());
    try {
        msgTable.lock();
    } catch (std::exception const& exc) {
        result.errorMessage = exc.what();
        return result;
    }


    // make new UserQuery
    // this is atomic
    ccontrol::UserQuery::Ptr uq;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        uq = _uqFactory->newUserQuery(query, defaultDb, getQdispPool(), userQueryId, msgTableName);
    }
    auto queryIdStr = uq->getQueryIdString();

    // check for errors
    auto error = uq->getError();
    if (not error.empty()) {
        result.errorMessage = queryIdStr + " Failed to instantiate query: " + error;
        return result;
    }

    // spawn background thread to wait until query finishes to unlock,
    // note that lambda stores copies of uq and msgTable.
    auto finalizer = [uq, msgTable]() mutable {
        LOGS(_log, LOG_LVL_DEBUG, uq->getQueryIdString() << " submitting new query");
        uq->submit();
        uq->join();
        try {
            msgTable.unlock(uq);
            if (uq) uq->discard();
        } catch (std::exception const& exc) {
            // TODO? if this fails there is no way to notify client, and client
            // will likely hang because table may still be locked.
            LOGS(_log, LOG_LVL_ERROR, uq->getQueryIdString()
                 << " Query finalization failed (client likely hangs): " << exc.what());
        }
    };
    LOGS(_log, LOG_LVL_DEBUG, queryIdStr << " starting finalizer thread for query");
    std::thread finalThread(finalizer);
    finalThread.detach();

    // update/cleanup query map
    _updateQueryHistory(clientId, threadId, uq);

    // return all info to caller
    if (uq->isAsync()) {

        // make separate message and result tables to return info about ASYNC query,
        // we do not need to lock message because result is ready before we return
        std::string const resultTableName = resultDb + ".result_async_" + userQueryId;
        std::string const asyncLockName = resultDb + ".message_async_" + userQueryId;
        MessageTable msgTable(asyncLockName, _czarConfig.getMySqlResultConfig());
        try {
            _makeAsyncResult(resultTableName, uq->getQueryId(), uq->getResultLocation());
            msgTable.create();
        } catch (std::exception const& exc) {
            result.errorMessage = exc.what();
            return result;
        }

        result.resultTable = resultTableName;
        result.messageTable = asyncLockName;

    } else {

        if (not uq->getResultTableName().empty()) {
            result.resultTable = resultDb + "." + uq->getResultTableName();
        }
        result.messageTable = lockName;
        result.orderBy = uq->getProxyOrderBy();

    }
    LOGS(_log, LOG_LVL_DEBUG, queryIdStr << " returning result to proxy: resultTable="
         << result.resultTable << " messageTable=" << result.messageTable
         << " orderBy=" << result.orderBy);

    return result;
}

void
Czar::killQuery(std::string const& query, std::string const& clientId) {

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
        std::lock_guard<std::mutex> lock(_mutex);

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
        std::lock_guard<std::mutex> lock(_mutex);

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
        std::thread killThread([uq, threadId]() {
            uq->kill();
            LOGS(_log, LOG_LVL_DEBUG, "Finished killing query: " << uq->getQueryId());
        });
        killThread.detach();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Query has expired/finished: " << query);
        throw std::runtime_error("Query has already finished: " + query);
    }
}

void
Czar::_cleanupQueryHistoryLocked() {
    // _mutex must be locked

    // first cleanup client query maps from completed queries
    for (auto iter = _clientToQuery.begin(); iter != _clientToQuery.end(); ) {
        if (iter->second.expired()) {
            iter = _clientToQuery.erase(iter);
        } else {
            ++ iter;
        }
    }
    for (auto iter = _idToQuery.begin(); iter != _idToQuery.end(); ) {
        if (iter->second.expired()) {
            iter = _idToQuery.erase(iter);
        } else {
            ++ iter;
        }
    }
}

void
Czar::_cleanupQueryHistory() {
    std::lock_guard<std::mutex> lock(_mutex);
    _cleanupQueryHistoryLocked();
}

void
Czar::_updateQueryHistory(std::string const& clientId,
                          int threadId,
                          ccontrol::UserQuery::Ptr const& uq) {

    std::lock_guard<std::mutex> lock(_mutex);

    // first cleanup client query maps from completed queries
    _cleanupQueryHistoryLocked();

    // remember query (weak pointer) in case we want to kill query
    if (uq->getQueryId() != QueryId(0)) {
        _idToQuery.insert(std::make_pair(uq->getQueryId(), uq));
        LOGS(_log, LOG_LVL_DEBUG, uq->getQueryIdString() << " Remembering query ID: "
             << uq->getQueryId() << " (new map size: "
             << _idToQuery.size() << ")");
    }
    if (not clientId.empty() and threadId >= 0) {
        ClientThreadId ctId(clientId, threadId);
        _clientToQuery.insert(std::make_pair(ctId, uq));
        LOGS(_log, LOG_LVL_DEBUG, uq->getQueryIdString() << " Remembering query: ("
             << clientId << ", " << threadId << ") (new map size: "
             << _clientToQuery.size() << ")");
    }
}

void
Czar::_makeAsyncResult(std::string const& asyncResultTable,
                       QueryId queryId,
                       std::string const& resultLoc) {

    sql::SqlConnection sqlConn(_czarConfig.getMySqlResultConfig());
    LOGS(_log, LOG_LVL_DEBUG, "creating async result table " << asyncResultTable);

    sql::SqlErrorObject sqlErr;
    std::string resultLocEscaped;
    if (not sqlConn.escapeString(resultLoc, resultLocEscaped,sqlErr)) {
        SqlError exc(ERR_LOC, "Failure in escapString", sqlErr);
        LOGS(_log, LOG_LVL_ERROR, exc.message());
        throw exc;
    }

    std::string query = (boost::format(::createAsyncResultTmpl)
                    % asyncResultTable % queryId % resultLocEscaped).str();

    if (not sqlConn.runQuery(query, sqlErr)) {
        SqlError exc(ERR_LOC, "Failure creating async result table", sqlErr);
        LOGS(_log, LOG_LVL_ERROR, exc.message());
        throw exc;
    }
}

}}} // namespace lsst::qserv::czar
