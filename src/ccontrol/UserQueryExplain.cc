// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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
#include "ccontrol/UserQueryExplain.h"

// System headers
#include <sstream>

// Third-party headers
#include "nlohmann/json.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "czar/Czar.h"
#include "czar/CzarFamilyMap.h"
#include "czar/CzarRegistry.h"
#include "http/Client.h"
#include "http/ClientConfig.h"
#include "http/MetaModule.h"
#include "http/Method.h"
#include "protojson/QueryExplainMsg.h"
#include "query/ScanTableInfo.h"
#include "qmeta/MessageStore.h"
#include "qproc/QuerySession.h"
#include "query/AreaRestrictor.h"
#include "query/QueryTemplate.h"
#include "query/SecIdxRestrictor.h"
#include "query/SelectStmt.h"
#include "query/typedefs.h"
#include "sql/SqlBulkInsert.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlErrorObject.h"
#include "util/Error.h"
#include "util/IterableFormatter.h"
#include "util/MultiError.h"

using namespace lsst::qserv;
using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryExplain");

/// Concatenate restrictors for display
template <typename VecPtr>
std::string restrictorsToString(VecPtr const& restrictors) {
    if (restrictors == nullptr || restrictors->empty()) return "none";
    std::ostringstream os;
    os << util::ptrPrintable(restrictors, "", "");
    return os.str();
}

/// Convert JSON values for use in MariaDB-style EXPLAIN table
std::string jsonValueToString(json const& value) {
    if (value.is_string()) return value.get<std::string>();
    if (value.is_boolean()) return value.get<bool>() ? "yes" : "no";
    if (value.is_number()) return value.dump();
    return value.dump(2);
}

}  // namespace

namespace lsst::qserv::ccontrol {

UserQueryExplain::UserQueryExplain(std::shared_ptr<qproc::QuerySession> const& qSession,
                                   std::string const& userQueryId, std::string const& resultDb,
                                   bool jsonFormat, CzarId czarId)
        : _qSession(qSession),
          _jsonFormat(jsonFormat),
          _messageStore(std::make_shared<qmeta::MessageStore>()),
          _resultTableName("qserv_result_explain_" + userQueryId),
          _resultDb(resultDb),
          _czarId(czarId) {}

std::string UserQueryExplain::getError() const { return _qSession->getError(); }

json UserQueryExplain::_getQueryInfo() const {
    auto const& qs = *_qSession;
    json qInfo = json::object();

    qInfo["parsed_sql"] = qs.getStmt().getQueryTemplate().sqlFragment();

    std::ostringstream irStream;
    irStream << qs.getStmt();
    qInfo["parsed_ir"] = irStream.str();

    qInfo["needs_merge"] = qs.needsMerge();
    auto const mergeStmt = qs.getMergeStmt();
    qInfo["merge_sql"] = mergeStmt != nullptr ? mergeStmt->getQueryTemplate().sqlFragment() : "none";

    qInfo["is_chunked"] = qs.hasChunks();
    qInfo["chunks_matched"] = qs.isDummy() ? 0 : qs.getChunksSize();

    qInfo["area_restrictors"] = restrictorsToString(qs.getAreaRestrictors());
    qInfo["secidx_restrictors"] = restrictorsToString(qs.getSecIdxRestrictors());

    qInfo["scan_interactive"] = qs.getScanInteractive();
    auto const scanInfo = qs.getScanInfo();
    qInfo["scan_rating"] = scanInfo->scanRating;
    json scanTables = json::array();
    for (auto const& tbl : scanInfo->infoTables) {
        scanTables.push_back(json::object({{"db", tbl.db},
                                           {"table", tbl.table},
                                           {"rating", tbl.scanRating},
                                           {"lock_in_memory", tbl.lockInMemory}}));
    }
    qInfo["scan_tables"] = scanTables;

    std::ostringstream workerStream;
    bool first = true;
    for (auto const& stmt : qs.getStmtParallel()) {
        if (stmt == nullptr) continue;
        if (!first) workerStream << " /*QSEPARATOR*/; ";
        workerStream << stmt->getQueryTemplate().sqlFragment();
        first = false;
    }
    qInfo["worker_sql"] = first ? "none" : workerStream.str();

    return qInfo;
}

std::string UserQueryExplain::getResultQuery() const {
    return "SELECT * FROM " + _resultDb + "." + _resultTableName + " ORDER BY 1";
}

string UserQueryExplain::cName(const char* fName) const { return "UserQueryExplain::" + string(fName) + " "; }

protojson::QueryExplainResultMsg::Ptr UserQueryExplain::_sendExplainQueryToWorker() {
    util::MultiError explainError;
    auto respMsg = protojson::QueryExplainResultMsg::create();
    static atomic<QueryId> explainQueryIdSeq{1};
    QueryId queryId = explainQueryIdSeq++;  // not ideal, but it can help with debugging.
    qproc::ChunkQuerySpec::Ptr cs;
    // Get query templates,
    auto queryTemplates = _qSession->makeQueryTemplates();
    auto chunks = _qSession->getChunksCopy();
    qproc::ChunkSpec chunkSpec;
    string dbName("");
    if (!chunks.empty()) {
        // Pick a chunk from the middle of the list.
        chunkSpec = chunks[chunks.size() / 2];

        bool fillInChunkIdTag = false;  // do not fill in the chunkId
        cs = _qSession->buildChunkQuerySpec(queryTemplates, chunkSpec, fillInChunkIdTag);
        dbName = cs->db;
    } else {
        string const eMsg = "No chunks available for EXPLAIN query " + queryTemplates.front().sqlFragment();
        LOGS(_log, LOG_LVL_WARN, eMsg);
        util::Error err(util::Error::INTERNAL, util::Error::NONE, eMsg);
        explainError.insert(err);
        respMsg->note = explainError.toOneLineString();
        return respMsg;
    }
    int subchunkId = -1;
    if (!chunkSpec.subChunks.empty()) {
        subchunkId = chunkSpec.subChunks[chunkSpec.subChunks.size() / 2];
    }

    // Get czar info and the worker contactMap.
    auto czarPtr = czar::Czar::getCzar();
    auto czFamilyMap = czarPtr->getCzarFamilyMap();
    auto czChunkMap = czFamilyMap->getChunkMap(dbName);
    auto czRegistry = czarPtr->getCzarRegistry();
    // wContactMap is constant and safe to access without mutex lock.
    auto const wContactMap = czRegistry->waitForWorkerContactMap();

    // This is similar to code in Executive::buildAndSendUberJobs()
    // that determines the target worker, but different enough that it's
    // hard to combine to reduce code duplication.
    if (czChunkMap == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " no map found for queryDbName=" << dbName);
        // Make an empty chunk map so all jobs are flagged as needing to be reassigned.
        // There's a chance that a family will be replicated by the registry.
        czChunkMap = czar::CzarChunkMap::create();
    }
    auto const [chunkMapPtr, workerChunkMapPtr] = czChunkMap->getMaps();
    auto spIter = chunkMapPtr->find(chunkSpec.chunkId);
    if (spIter == chunkMapPtr->end()) {
        string const eMsg = "No worker info for " + queryTemplates.front().sqlFragment() +
                            " chunkId=" + std::to_string(chunkSpec.chunkId) + " db=" + dbName;
        LOGS(_log, LOG_LVL_WARN, eMsg);
        util::Error err(util::Error::INTERNAL, util::Error::NONE, eMsg);
        explainError.insert(err);
        respMsg->note = explainError.toOneLineString();
        return respMsg;
    }

    czar::CzarChunkMap::ChunkData::Ptr chunkData = spIter->second;
    // This may not be the ideal worker to use for this
    auto targetWorker = chunkData->getPrimaryScanWorker().lock();
    if (targetWorker != nullptr && targetWorker->isDead()) targetWorker = nullptr;
    if (targetWorker == nullptr) {
        // Try to assign a different worker to this job
        auto workerHasThisChunkMap = chunkData->getWorkerHasThisMapCopy();
        bool found = false;
        for (auto wIter = workerHasThisChunkMap.begin(); wIter != workerHasThisChunkMap.end() && !found;
             ++wIter) {
            auto maybeTarg = wIter->second.lock();
            if (maybeTarg != nullptr && !maybeTarg->isDead()) {
                targetWorker = maybeTarg;
                found = true;
                LOGS(_log, LOG_LVL_WARN,
                     cName(__func__) << " Alternate worker=" << targetWorker->getWorkerId()
                                     << " found for chunk=" << chunkData->dump());
            }
        }
    }

    if (targetWorker == nullptr) {
        string const eMsg = "No live worker found for " + queryTemplates.front().sqlFragment() +
                            " chunkId=" + std::to_string(chunkSpec.chunkId) + " db=" + dbName;
        LOGS(_log, LOG_LVL_WARN, eMsg);
        util::Error err(util::Error::INTERNAL, util::Error::NONE, eMsg);
        explainError.insert(err);
        respMsg->note = explainError.toOneLineString();
        return respMsg;
    }
    string workerId = targetWorker->getWorkerId();
    auto iterWcm = wContactMap->find(workerId);
    if (iterWcm == wContactMap->end()) {
        string const eMsg = "No contact info for worker=" + workerId +
                            " for query=" + queryTemplates.front().sqlFragment();
        LOGS(_log, LOG_LVL_WARN, eMsg);
        util::Error err(util::Error::INTERNAL, util::Error::NONE, eMsg);
        explainError.insert(err);
        respMsg->note = explainError.toOneLineString();
        return respMsg;
    }
    protojson::WorkerContactInfo::Ptr wInfo = iterWcm->second;

    // Send explain request to the worker
    auto const method = http::Method::POST;
    auto [ciwId, ciwHost, ciwManagment, ciwPort] = wInfo->getAll();
    string const url = "http://" + ciwHost + ":" + to_string(ciwPort) + "/queryexplain";
    vector<string> const headers = {"Content-Type: application/json"};
    auto const& czarConfig = cconfig::CzarConfig::instance();

    protojson::AuthContext const authC(cconfig::CzarConfig::instance()->replicationInstanceId(),
                                       cconfig::CzarConfig::instance()->replicationAuthKey());

    vector<string> sqlFragments;
    for (auto const& qt : queryTemplates) {
        sqlFragments.push_back(qt.sqlFragment());
    }
    protojson::QueryExplainMsg::Ptr queryExplainMsg = protojson::QueryExplainMsg::create(
            authC, http::MetaModule::version, ciwId, _czarId, queryId, dbName, chunkSpec.chunkId, subchunkId,
            sqlFragments, cs->subChunkTables);
    json request = queryExplainMsg->toJson();

    string const requestContext = "Czar: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_TRACE,
         "QueryExplain czarPost url=" << url << " request=" << request.dump() << " headers=" << headers[0]);
    auto commandHttpPool = czar::Czar::getCzar()->getCommandHttpPool();
    http::ClientConfig clientConfig;
    clientConfig.httpVersion = CURL_HTTP_VERSION_1_1;  // same as in qhttp
    clientConfig.bufferSize = CURL_MAX_READ_SIZE;      // 10 MB in the current version of libcurl
    clientConfig.tcpKeepAlive = true;
    clientConfig.tcpKeepIdle = 30;  // the default is 60 sec
    clientConfig.tcpKeepIntvl = 5;  // the default is 60 sec
    http::Client client(method, url, request.dump(), headers, clientConfig, commandHttpPool);
    bool transmitSuccess = false;
    string exceptionWhat;
    json response = json::object();
    try {
        LOGS(_log, LOG_LVL_TRACE, "QueryExplain sending");
        response = client.readAsJson();
        LOGS(_log, LOG_LVL_TRACE, "QueryExplain worker recv");
        respMsg = protojson::QueryExplainResultMsg::createFromJson(response);
        if (respMsg != nullptr && respMsg->success) {
            transmitSuccess = true;
        } else {
            LOGS(_log, LOG_LVL_WARN,
                 cName(__func__) << " parse failure respMsg="
                                 << (respMsg == nullptr ? "null" : respMsg->dump()));
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN,
             requestContext + "  failed, ex: " + ex.what() + " respMsg="
                     << (respMsg == nullptr ? "null" : respMsg->dump()));
        exceptionWhat = ex.what();
    }
    if (respMsg == nullptr) {
        // Create one for error reporting.
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " respMsg is nullptr");
        respMsg = protojson::QueryExplainResultMsg::create();
        respMsg->success = false;
        respMsg->note += " response had nullptr.";
    }
    respMsg->note += " workerAddr(" + ciwManagment + "," + ciwHost + ":" + std::to_string(ciwPort) + ")";
    if (!transmitSuccess) {
        string const eMsg = "QueryExplain worker request failed " + queryTemplates.front().sqlFragment() +
                            " ex:" + exceptionWhat;
        LOGS(_log, LOG_LVL_WARN, eMsg);
        util::Error err(util::Error::INTERNAL, util::Error::NONE, eMsg);
        explainError.insert(err);
        respMsg->note += explainError.toOneLineString();
        return respMsg;
    }
    return respMsg;
}

void UserQueryExplain::submit() {
    auto workerExplain = _sendExplainQueryToWorker();
    try {
        _populateResultTable(workerExplain);
    } catch (std::exception const& exc) {
        std::string const message = std::string("Internal failure, EXPLAIN failed: ") + exc.what();
        LOGS(_log, LOG_LVL_ERROR, message);
        _messageStore->addMessage(-1, "EXPLAIN", util::Error::INTERNAL, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
    }
}

void UserQueryExplain::_populateResultTable(protojson::QueryExplainResultMsg::Ptr const& workerExplain) {
    auto qInfo = _getQueryInfo();
    if (workerExplain != nullptr) {
        qInfo["worker_explain"] = workerExplain->toUserString();
    } else {
        qInfo["worker_explain"] = "none";
    }

    auto const czarConfig = cconfig::CzarConfig::instance();
    auto const resultDbConn = sql::SqlConnectionFactory::make(czarConfig->getMySqlResultConfig());
    sql::SqlErrorObject errObj;

    auto fail = [this](int code, std::string const& message) {
        LOGS(_log, LOG_LVL_ERROR, message);
        _messageStore->addMessage(-1, "EXPLAIN", code, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
    };

    std::string createTable;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;

    if (_jsonFormat) {
        // Single-column, single-row JSON document (like MariaDB EXPLAIN FORMAT=JSON).
        createTable = "CREATE TABLE " + _resultTableName + " (`EXPLAIN` MEDIUMTEXT)";
        columns = {"EXPLAIN"};
        rows.push_back({qInfo.dump(2)});
    } else {
        // Attribute/value table, one row per entry
        createTable = "CREATE TABLE " + _resultTableName + " (`attribute` VARCHAR(255), `value` MEDIUMTEXT)";
        columns = {"attribute", "value"};
        for (auto const& [key, value] : qInfo.items()) {
            rows.push_back({key, jsonValueToString(value)});
        }
    }

    LOGS(_log, LOG_LVL_DEBUG, "creating result table: " << createTable);
    if (!resultDbConn->runQuery(createTable, errObj)) {
        fail(util::Error::RESULT_CREATETABLE,
             "Internal failure, failed to create result table: " + errObj.errMsg());
        return;
    }

    sql::SqlBulkInsert bulkInsert(resultDbConn.get(), _resultTableName, columns);
    for (auto const& row : rows) {
        std::vector<std::string> values;
        values.reserve(row.size());
        for (auto const& value : row) {
            values.push_back("'" + resultDbConn->escapeString(value) + "'");
        }
        if (!bulkInsert.addRow(values, errObj)) {
            fail(util::Error::RESULT_SQL,
                 "Internal failure, error updating result table: " + errObj.errMsg());
            return;
        }
    }
    if (!bulkInsert.flush(errObj)) {
        fail(util::Error::RESULT_SQL, "Internal failure, error updating result table: " + errObj.errMsg());
        return;
    }

    _qState = SUCCESS;
}

QueryState UserQueryExplain::join() { return _qState; }

void UserQueryExplain::kill() {}

void UserQueryExplain::discard() {}

}  // namespace lsst::qserv::ccontrol
