// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 AURA/LSST.
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
/**
 * @file
 *
 * @brief Interface for managing the execution of user queries, that is,
 * queries as they are submitted by the user. The generation of smaller
 * chunk-level queries is handled here or by delegate classes.
 *
 * Basic usage:
 *
 * After constructing a UserQuery object ...
 *
 * getRestrictors() -- retrieve restrictors to be passed to spatial region selection code in another layer.
 *
 * getDbStriping() -- retrieve the striping parameters of the dominantDb.
 *
 * getError() -- See if there are errors
 *
 * getExecDesc() -- see how execution is progressing
 *
 * addChunk() -- Add a chunk number (and subchunks, as appropriate) to be
 * dispatched during submit(). The czar uses getRestrictors and getDbStriping
 * to query a region selector over a chunk number generator and an emptychunks
 * list to compute the relevant chunk numbers.
 *
 * submit() -- send the query (in generated fragments) to the cluster for
 * execution.
 *
 * join() -- block until query execution is complete (or encounters errors)
 *
 * kill() -- stop a query in progress
 *
 * discard() -- release resources for this query.
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "ccontrol/UserQuerySelect.h"

// System headers
#include <cassert>
#include <memory>
#include <stdexcept>

// Third-party headers
#include <boost/algorithm/string/replace.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "ccontrol/MergingHandler.h"
#include "ccontrol/UserQueryError.h"
#include "czar/Czar.h"
#include "czar/CzarFamilyMap.h"
#include "czar/CzarRegistry.h"
#include "global/constants.h"
#include "global/LogContext.h"
#include "qdisp/Executive.h"
#include "qdisp/JobQuery.h"
#include "qmeta/MessageStore.h"
#include "qmeta/QMeta.h"
#include "qmeta/Exceptions.h"
#include "qmeta/QMeta.h"
#include "qmeta/QProgress.h"
#include "qproc/QuerySession.h"
#include "qproc/IndexMap.h"
#include "query/ScanTableInfo.h"
#include "query/ColumnRef.h"
#include "query/FromList.h"
#include "query/JoinRef.h"
#include "query/QueryTemplate.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "qdisp/UberJob.h"
#include "query/ValueExpr.h"
#include "rproc/InfileMerger.h"
#include "sql/Schema.h"
#include "util/Bug.h"
#include "util/QdispPool.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQuerySelect");
}  // namespace

namespace lsst::qserv::ccontrol {

/// Constructor
UserQuerySelect::UserQuerySelect(shared_ptr<qproc::QuerySession> const& qs,
                                 shared_ptr<qmeta::MessageStore> const& messageStore,
                                 shared_ptr<qdisp::Executive> const& executive,
                                 shared_ptr<qproc::DatabaseModels> const& dbModels,
                                 shared_ptr<rproc::InfileMergerConfig> const& infileMergerConfig,
                                 shared_ptr<qproc::SecondaryIndex> const& secondaryIndex,
                                 shared_ptr<qmeta::QMeta> const& queryMetadata,
                                 shared_ptr<qmeta::QProgress> const& queryProgress, CzarId czarId,
                                 string const& errorExtra, bool async, string const& resultDb)
        : _qSession(qs),
          _messageStore(messageStore),
          _executive(executive),
          _databaseModels(dbModels),
          _infileMergerConfig(infileMergerConfig),
          _secondaryIndex(secondaryIndex),
          _queryMetadata(queryMetadata),
          _queryProgress(queryProgress),
          _czarId(czarId),
          _errorExtra(errorExtra),
          _resultDb(resultDb),
          _async(async) {}

string UserQuerySelect::getError() const {
    string div = (_errorExtra.size() && _qSession->getError().size()) ? " " : "";
    return _qSession->getError() + div + _errorExtra;
}

void UserQuerySelect::kill() {
    LOGS(_log, LOG_LVL_INFO, "UserQuerySelect KILL");
    lock_guard<mutex> lock(_killMutex);
    if (!_killed) {
        _killed = true;
        auto exec = _executive;
        int64_t collectedRows = (exec) ? exec->getTotalResultRows() : -1;
        size_t collectedBytes = _infileMerger->getTotalResultSize();
        try {
            if (exec != nullptr) {
                exec->squash("UserQuerySelect::kill");
            }
        } catch (UserQueryError const& e) {
            // Silence merger discarding errors, because this object is being
            // released. Client no longer cares about merger errors.
        }
        // Since this is being aborted, collectedRows and collectedBytes are going to
        // be off a bit as results were still coming in. A rough idea should be
        // good enough.
        _qMetaUpdateStatus(qmeta::QInfo::ABORTED, collectedRows, collectedBytes, 0);
    }
}

string UserQuerySelect::_getResultOrderBy() const { return _qSession->getResultOrderBy(); }

string UserQuerySelect::getResultQuery() const {
    query::SelectList selectList;
    auto const& valueExprList = *_qSession->getStmt().getSelectList().getValueExprList();
    for (auto const& valueExpr : valueExprList) {
        if (valueExpr->isStar()) {
            auto useSelectList = make_shared<query::SelectList>();
            useSelectList->addValueExpr(valueExpr);
            query::SelectStmt starStmt(useSelectList, _qSession->getStmt().getFromList().clone());
            sql::Schema schema;
            if (not _infileMerger->getSchemaForQueryResults(starStmt, schema)) {
                _errorExtra = "Internal error getting schema for query results:" +
                              _infileMerger->getError().getMsg();
            }
            for (auto const& column : schema.columns) {
                selectList.addValueExpr(query::ValueExpr::newColumnExpr(column.name));
            }
        } else {
            // Add a column that describes the top-level ValueExpr.
            // If the value is a column ref _and_ there was not a user defined alias, then the TablePlugin
            // will have assigned an alias that included the table name. We don't want that table name to
            // appear in the results in that case, so just assign the column. Otherwise, use the alias.
            shared_ptr<query::ValueExpr> newValueExpr;
            if (valueExpr->isColumnRef() && not valueExpr->getAliasIsUserDefined()) {
                newValueExpr = query::ValueExpr::newColumnExpr(valueExpr->getAlias());
                newValueExpr->setAlias(valueExpr->getColumnRef()->getColumn());
            } else {
                newValueExpr = query::ValueExpr::newColumnExpr(valueExpr->getAlias());
                newValueExpr->setAlias(valueExpr->getAlias());
            }
            selectList.addValueExpr(newValueExpr);
        }
    }

    // The SELECT list needs to define aliases in the result query, so that the columns we are selecting from
    // the result table that may be mangled by internal handling of the query are restored to the column name
    // that the user expects, by way of the alias defined here.
    query::QueryTemplate qt(query::QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS);
    selectList.renderTo(qt);

    string resultQuery = "SELECT " + qt.sqlFragment() + " FROM " + _resultDb + "." + getResultTableName();
    string orderBy = _getResultOrderBy();
    if (not orderBy.empty()) {
        resultQuery += " " + orderBy;
    }
    LOGS(_log, LOG_LVL_DEBUG, "made result query:" << resultQuery);
    return resultQuery;
}

void UserQuerySelect::submit() {
    auto submitTmStart = CLOCK::now();
    auto exec = _executive;
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "UserQuerySelect::submit() executive is null at start");
        return;
    }
    _qSession->finalize();

    // Using the QuerySession, generate query specs (text, db, chunkId) and then
    // create query messages and send them to the async query manager.
    LOGS(_log, LOG_LVL_DEBUG, "UserQuerySelect beginning submission");
    assert(_infileMerger);

    vector<int> chunks;
    mutex chunksMtx;
    JobId sequence = 0;

    auto queryTemplates = _qSession->makeQueryTemplates();
    LOGS(_log, LOG_LVL_DEBUG,
         "first query template:" << (queryTemplates.size() > 0 ? queryTemplates[0].sqlFragment()
                                                               : "none produced."));

    // Writing query for each chunk, stop if query is cancelled.

    // Add QProgress table entry
    try {
        _queryProgress->insert(_queryId, _qSession->getChunksSize());
    } catch (qmeta::SqlError const& e) {
        LOGS(_log, LOG_LVL_WARN, "Failed QProgress::insert, ex: " << e.what());
    }

    exec->setScanInteractive(_qSession->getScanInteractive());
    exec->setScanInfo(*_qSession->getScanInfo());

    string dbName("");
    bool dbNameSet = false;
    auto submitTmBuildJobsStart = CLOCK::now();
    for (auto i = _qSession->cQueryBegin(), e = _qSession->cQueryEnd(); i != e && !exec->getCancelled();
         ++i) {
        auto& chunkSpec = *i;

        QSERV_LOGCONTEXT_QUERY(_queryId);

        // TODO:UJTemplate The template(s) is generated here and later it is compared to other
        //         templates. It would be better to create the list of query templates here
        //         and just store the index into the list of templates in the `cs`.
        qproc::ChunkQuerySpec::Ptr cs;
        {
            lock_guard<mutex> lock(chunksMtx);
            bool fillInChunkIdTag = false;  // do not fill in the chunkId
            cs = _qSession->buildChunkQuerySpec(queryTemplates, chunkSpec, fillInChunkIdTag);
            chunks.push_back(cs->chunkId);
        }

        // This should only need to be set once as all jobs should have the same database name.
        if (cs->db != dbName) {
            if (dbNameSet) {
                LOGS(_log, LOG_LVL_ERROR, "dbName change from " << dbName << " to " << cs->db);
                return;
            }
            dbName = cs->db;
            exec->setQueryDbName(dbName);
            dbNameSet = true;
        }

        ResourceUnit ru;
        ru.setAsDbChunk(cs->db, cs->chunkId);
        qdisp::JobDescription::Ptr jobDesc =
                qdisp::JobDescription::create(_czarId, exec->getId(), sequence, ru, cs);
        auto job = exec->add(jobDesc);
        ++sequence;
    }
    auto submitTmBuildJobsEnd = CLOCK::now();

    /// At this point the executive has a map of all jobs with the chunkIds as the key.
    // This is needed to prevent Czar::_monitor from starting things before they are ready.
    exec->setAllJobsCreated();
    exec->buildAndSendUberJobs();
    auto submitTmUberJobsBuilt = CLOCK::now();

    LOGS(_log, LOG_LVL_DEBUG, "total jobs in query=" << sequence);
    // Waiting for all jobs to start seems to provide more consistent results.
    exec->waitForAllJobsToStart();
    auto submitTmEnd = CLOCK::now();

    LOGS(_log, LOG_LVL_INFO,
         "UserQuerySelect::submit() times ms QID="
                 << _queryId << " total="
                 << chrono::duration_cast<chrono::milliseconds>(submitTmEnd - submitTmStart).count()
                 << " setup="
                 << chrono::duration_cast<chrono::milliseconds>(submitTmBuildJobsStart - submitTmStart)
                            .count()
                 << " jobs="
                 << chrono::duration_cast<chrono::milliseconds>(submitTmBuildJobsEnd - submitTmBuildJobsStart)
                            .count()
                 << " UberJobs="
                 << chrono::duration_cast<chrono::milliseconds>(submitTmUberJobsBuilt - submitTmBuildJobsEnd)
                            .count()
                 << " allStarted="
                 << chrono::duration_cast<chrono::milliseconds>(submitTmEnd - submitTmUberJobsBuilt).count());
}

QueryState UserQuerySelect::join() {
    auto exec = _executive;
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "UserQuerySelect::join() called with null exec " << getQueryIdString());
        return ERROR;
    }
    bool successful = exec->join();  // Wait for all data
    // Since all data are in, run final SQL commands like GROUP BY.
    size_t collectedBytes = 0;
    int64_t finalRows = 0;
    if (!_infileMerger->finalize(collectedBytes, finalRows)) {
        successful = false;
        LOGS(_log, LOG_LVL_ERROR, "InfileMerger::finalize failed");
        // Error: 1105 SQLSTATE: HY000 (ER_UNKNOWN_ERROR) Message: Unknown error
        _messageStore->addMessage(-1, "MERGE", 1105, "Failure while merging result",
                                  MessageSeverity::MSG_ERROR);
    }
    exec->updateProxyMessages();

    try {
        // The lock is required to prevent the race condition if the query cancellation
        // happens at the same time as the merger is being discarded.
        _discardMerger(lock_guard<mutex>(_killMutex));
    } catch (exception const& exc) {
        // exception here means error in qserv logic, we do not want to leak
        // it or expose it to user, just dump it to log
        LOGS(_log, LOG_LVL_ERROR, "exception from _discardMerger: " << exc.what());
    }

    // Update the permanent message table.
    _qMetaUpdateMessages();

    int64_t collectedRows = exec->getTotalResultRows();
    // finalRows < 0 indicates there was no postprocessing, so collected rows and final rows should be the
    // same.
    if (finalRows < 0) finalRows = collectedRows;

    QueryState state = SUCCESS;
    if (successful) {
        _qMetaUpdateStatus(qmeta::QInfo::COMPLETED, collectedRows, collectedBytes, finalRows);
        LOGS(_log, LOG_LVL_INFO, "Joined everything (success) QID=" << getQueryId());
    } else if (_killed) {
        // status is already set to ABORTED
        LOGS(_log, LOG_LVL_ERROR, "Joined everything (killed) QID=" << getQueryId());
        state = ERROR;
    } else {
        auto status = qmeta::QInfo::FAILED;
        size_t errCollectedBytes = collectedBytes;
        if (exec->isResultSizeLimitExceeded()) {
            _errorExtra = "Query result size limit exceeded.";
            status = qmeta::QInfo::FAILED_LR;
            errCollectedBytes = exec->getResultFileSizeErr();
        }
        _qMetaUpdateStatus(status, collectedRows, errCollectedBytes, finalRows);
        LOGS(_log, LOG_LVL_ERROR,
             "Joined everything (failure!) QID=" << getQueryId() << " status=" << status);
        state = ERROR;
    }
    auto const czarConfig = cconfig::CzarConfig::instance();

    // Notify workers on the query completion/cancellation to ensure
    // resources are properly cleaned over there as well.
    czar::Czar::getCzar()->getActiveWorkerMap()->addToDoneDeleteFiles(exec->getId());
    return state;
}

void UserQuerySelect::_discardMerger(lock_guard<mutex> const& lock) {
    if (_infileMerger && !_infileMerger->isFinished()) {
        throw UserQueryError(getQueryIdString() + " merger unfinished, cannot discard");
    }
    _infileMergerConfig.reset();
}

void UserQuerySelect::discard() {
    // The lock must be held for the entire discard operation to prevent
    // the race condition if the query cancellation happens at the same time
    // the query resources are being discarded.
    lock_guard<mutex> lock(_killMutex);
    if (_killed) return;

    auto exec = _executive;
    if (exec == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "UserQuerySelect::discard called with null exec " << getQueryIdString());
        return;
    }

    // Make sure resources are released.
    if (exec->getNumInflight() > 0) {
        throw UserQueryError(getQueryIdString() + " Executive unfinished, cannot discard");
    }

    // Deleting the executive may save some time if results were found early.
    _executive.reset();

    try {
        _discardMerger(lock);
    } catch (UserQueryError const& e) {
        // Silence merger discarding errors, because this object is being released.
        // client no longer cares about merger errors.
    }
    LOGS(_log, LOG_LVL_INFO, "Discarded UserQuerySelect");
}

void UserQuerySelect::setupMerger() {
    LOGS(_log, LOG_LVL_TRACE, "Setup merger");
    _infileMergerConfig->targetTable = _resultTable;
    _infileMergerConfig->mergeStmt = _qSession->getMergeStmt();
    LOGS(_log, LOG_LVL_DEBUG,
         "setting mergeStmt:" << (_infileMergerConfig->mergeStmt != nullptr
                                          ? _infileMergerConfig->mergeStmt->getQueryTemplate().sqlFragment()
                                          : "nullptr"));
    _infileMerger = make_shared<rproc::InfileMerger>(*_infileMergerConfig, _databaseModels);

    auto&& preFlightStmt = _qSession->getPreFlightStmt();
    if (preFlightStmt == nullptr) {
        _qMetaUpdateStatus(qmeta::QInfo::FAILED);
        _errorExtra = "Could not create results table for query (no worker queries).";
        return;
    }
    if (not _infileMerger->makeResultsTableForQuery(*preFlightStmt)) {
        _errorExtra = _infileMerger->getError().getMsg();
        _qMetaUpdateStatus(qmeta::QInfo::FAILED);
    }

    _expandSelectStarInMergeStatment(_infileMergerConfig->mergeStmt);

    _infileMerger->setMergeStmtFromList(_infileMergerConfig->mergeStmt);
}

void UserQuerySelect::_expandSelectStarInMergeStatment(shared_ptr<query::SelectStmt> const& mergeStmt) {
    if (nullptr != mergeStmt) {
        auto& selectList = *(mergeStmt->getSelectList().getValueExprList());
        for (auto valueExprItr = selectList.begin(); valueExprItr != selectList.end(); ++valueExprItr) {
            auto& valueExpr = *valueExprItr;
            if (valueExpr->isStar()) {
                auto valueExprVec = make_shared<query::ValueExprPtrVector>();
                valueExprVec->push_back(valueExpr);
                auto starStmt = query::SelectStmt(make_shared<query::SelectList>(valueExprVec),
                                                  mergeStmt->getFromListPtr());
                sql::Schema schema;
                if (not _infileMerger->getSchemaForQueryResults(starStmt, schema)) {
                    throw UserQueryError(getQueryIdString() + " Couldn't get schema for merge query.");
                }
                // Only use the column names retured by the SELECT* (not the database or table name) because
                // when performing the merge the columns will be in the merge table (not the table that was
                // originally queried).
                query::ValueExprPtrVector starColumns;
                for (auto const& column : schema.columns) {
                    starColumns.push_back(query::ValueExpr::newColumnExpr("", "", "", column.name));
                }
                valueExprItr = selectList.insert(valueExprItr, starColumns.begin(), starColumns.end());
                advance(valueExprItr, starColumns.size());
                // erase the STAR ValueExpr becasue it's been replaced with named columns.
                valueExprItr = selectList.erase(valueExprItr);
                if (valueExprItr == selectList.end()) break;
            }
        }
    }
}

void UserQuerySelect::saveResultQuery() { _queryMetadata->saveResultQuery(_queryId, getResultQuery()); }

void UserQuerySelect::_setupChunking() { _qSession->setupChunking(_secondaryIndex); }

void UserQuerySelect::qMetaRegister(string const& resultLocation, string const& msgTableName) {
    qmeta::QInfo::QType qType = _async ? qmeta::QInfo::ASYNC : qmeta::QInfo::SYNC;
    string user = "anonymous";  // we do not have access to that info yet

    /// Chunking information is required before registering the query.
    _setupChunking();

    string qTemplate;
    auto const& stmtVector = _qSession->getStmtParallel();
    for (auto itr = stmtVector.begin(); itr != stmtVector.end(); ++itr) {
        auto stmt = *itr;
        if (stmt) {
            if (not qTemplate.empty()) {
                // if there is more than one statement separate them by
                // special token
                qTemplate += " /*QSEPARATOR*/; ";
            }
            qTemplate += stmt->getQueryTemplate().sqlFragment();
        }
    }

    string qMerge;
    auto mergeStmt = _qSession->getMergeStmt();
    if (mergeStmt) {
        qMerge = mergeStmt->getQueryTemplate().sqlFragment();
    }
    _resultLoc = resultLocation;
    if (_resultLoc.empty()) {
        // Special token #QID# is replaced with query ID later.
        _resultLoc = "table:result_#QID#";
    }

    int const chunkCount = _qSession->getChunksSize();

    qmeta::QInfo qInfo(qType, _czarId, user, _qSession->getOriginal(), qTemplate, qMerge, _resultLoc,
                       msgTableName, "", chunkCount);

    // find all table names used by statement (which appear in FROM ... [JOIN ...])
    qmeta::QMeta::TableNames tableNames;
    const auto& tables = _qSession->getStmt().getFromList().getTableRefList();
    for (auto itr = tables.begin(); itr != tables.end(); ++itr) {
        // add table name
        tableNames.push_back(make_pair((*itr)->getDb(), (*itr)->getTable()));

        // add its joins if any
        const auto& joins = (*itr)->getJoins();
        for (auto jtr = joins.begin(); jtr != joins.end(); ++jtr) {
            const auto& right = (*jtr)->getRight();
            if (right) {
                tableNames.push_back(make_pair(right->getDb(), right->getTable()));
            }
        }
    }

    // register query, save its ID
    _queryId = _queryMetadata->registerQuery(qInfo, tableNames);
    _queryIdStr = QueryIdHelper::makeIdStr(_queryId);
    // Add logging context with query ID
    QSERV_LOGCONTEXT_QUERY(_queryId);
    LOGS(_log, LOG_LVL_DEBUG, "UserQuery registered " << _qSession->getOriginal());

    // update #QID# with actual query ID
    boost::replace_all(_resultLoc, "#QID#", to_string(_queryId));

    // guess query result location
    if (_resultLoc.compare(0, 6, "table:") == 0) {
        _resultTable = _resultLoc.substr(6);
    } else {
        // we only support results going to tables for now, abort for anything else
        string const msg = "Unexpected result location '" + _resultLoc + "'";
        _messageStore->addMessage(-1, "SYSTEM", 1146, msg, MessageSeverity::MSG_ERROR);
        throw UserQueryError(getQueryIdString() + _errorExtra);
    }

    auto exec = _executive;
    if (exec != nullptr) {
        exec->setQueryId(_queryId);
    } else {
        LOGS(_log, LOG_LVL_WARN, "No Executive, assuming invalid query");
    }

    // Note that ordering is important here, this check must happen after
    // query is registered in qmeta
    for (auto itr = tableNames.begin(); itr != tableNames.end(); ++itr) {
        if (not _qSession->containsTable(itr->first, itr->second)) {
            // table either does not exist or it is being deleted, we must stop
            // here but we must mark query as failed
            _qMetaUpdateStatus(qmeta::QInfo::FAILED);

            // Throwing exception stops submit() but it does not set any
            // error condition, only prints error message to the log. To communicate
            // error message to caller we need to set _errorExtra
            string const msg = "Table '" + itr->first + "." + itr->second + "' does not exist";
            _messageStore->addMessage(-1, "SYSTEM", 1146, msg, MessageSeverity::MSG_ERROR);
            throw UserQueryError(getQueryIdString() + _errorExtra);
        }
    }
}

void UserQuerySelect::_qMetaUpdateStatus(qmeta::QInfo::QStatus qStatus, size_t rows, size_t bytes,
                                         size_t finalRows) {
    _queryMetadata->completeQuery(_queryId, qStatus, rows, bytes, finalRows);
    // Remove the row for temporary query statistics.
    try {
        _queryProgress->remove(_queryId);
    } catch (qmeta::SqlError const&) {
        LOGS(_log, LOG_LVL_WARN, "QProgress::remove failed, queryId: " << _queryIdStr);
    }
}

void UserQuerySelect::_qMetaUpdateMessages() {
    try {
        _queryMetadata->addQueryMessages(_queryId, _messageStore);
    } catch (qmeta::SqlError const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "UserQuerySelect::_qMetaUpdateMessages failed, ex: " << ex.what());
    }
}

}  // namespace lsst::qserv::ccontrol
