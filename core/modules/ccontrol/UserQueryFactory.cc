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

// Class header
#include "ccontrol/UserQueryFactory.h"

// System headers
#include <cassert>
#include <cstdlib>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigError.h"
#include "ccontrol/ConfigMap.h"
#include "ccontrol/ParseRunner.h"
#include "ccontrol/UserQueryAsyncResult.h"
#include "ccontrol/UserQueryDrop.h"
#include "ccontrol/UserQueryFlushChunksCache.h"
#include "ccontrol/UserQueryInvalid.h"
#include "ccontrol/UserQueryProcessList.h"
#include "ccontrol/UserQueryResources.h"
#include "ccontrol/UserQuerySelect.h"
#include "ccontrol/UserQueryType.h"
#include "css/CssAccess.h"
#include "css/KvInterfaceImplMem.h"
#include "czar/CzarConfig.h"
#include "mysql/MySqlConfig.h"
#include "parser/ParseException.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qmeta/QMetaMysql.h"
#include "qmeta/QMetaSelect.h"
#include "qmeta/QStatusMysql.h"
#include "qproc/DatabaseModels.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "query/FromList.h"
#include "query/SelectStmt.h"
#include "rproc/InfileMerger.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryFactory");
}


namespace lsst {
namespace qserv {
namespace ccontrol {

std::shared_ptr<UserQuerySharedResources>
makeUserQuerySharedResources(czar::CzarConfig const& czarConfig,
                             std::shared_ptr<qproc::DatabaseModels> const& dbModels,
                             std::string const& czarName) {
    return std::make_shared<UserQuerySharedResources>(
        css::CssAccess::createFromConfig(czarConfig.getCssConfigMap(), czarConfig.getEmptyChunkPath()),
        czarConfig.getMySqlResultConfig(),
        std::make_shared<qproc::SecondaryIndex>(czarConfig.getMySqlQmetaConfig()),
        std::make_shared<qmeta::QMetaMysql>(czarConfig.getMySqlQmetaConfig()),
        std::make_shared<qmeta::QStatusMysql>(czarConfig.getMySqlQStatusDataConfig()),
        std::make_shared<qmeta::QMetaSelect>(czarConfig.getMySqlQmetaConfig()),
        sql::SqlConnectionFactory::make(czarConfig.getMySqlResultConfig()),
        dbModels,
        czarName);
}



////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(czar::CzarConfig const& czarConfig,
                                   qproc::DatabaseModels::Ptr const& dbModels,
                                   std::string const& czarName)
        :  _userQuerySharedResources(makeUserQuerySharedResources(czarConfig, dbModels, czarName)) {

    ::putenv((char*)"XRDDEBUG=1");

    _executiveConfig = std::make_shared<qdisp::ExecutiveConfig>(
            czarConfig.getXrootdFrontendUrl(), czarConfig.getQMetaSecondsBetweenChunkUpdates());

    // When czar crashes/exits while some queries are still in flight they
    // are left in EXECUTING state in QMeta. We want to cleanup that state
    // to avoid confusion. Note that when/if clean czar restart is implemented
    // we'll need a new logic to restart query processing.
    _userQuerySharedResources->queryMetadata->cleanup(_userQuerySharedResources->qMetaCzarId);

    // Add logging context with czar ID
    qmeta::CzarId qMetaCzarId = _userQuerySharedResources->qMetaCzarId;
    LOG_MDC_INIT([qMetaCzarId]() { LOG_MDC("CZID", std::to_string(qMetaCzarId)); });
}


UserQuery::Ptr
UserQueryFactory::newUserQuery(std::string const& aQuery,
                               std::string const& defaultDb,
                               qdisp::QdispPool::Ptr const& qdispPool,
                               std::string const& userQueryId,
                               std::string const& msgTableName,
                               std::string const& resultDb) {

    // result location could potentially be specified by SUBMIT command, for now
    // we keep it empty which means that UserQuerySelect uses default result table.
    std::string resultLocation;

    // First check for SUBMIT and strip it
    std::string query = aQuery;

    std::string stripped;
    bool async = false;
    if (UserQueryType::isSubmit(query, stripped)) {
        // SUBMIT is only allowed with SELECT for now, complain if anything else is there
        if (!UserQueryType::isSelect(stripped)) {
            auto uq = std::make_shared<UserQueryInvalid>("Invalid or unsupported query: " + query);
            return uq;
        }
        async = true;
        query = stripped;
    }

    std::string dbName, tableName;
    bool full = false;
    QueryId userJobId = 0;

    if (UserQueryType::isSelect(query)) {
        // Processing regular select query
        bool sessionValid = true;
        std::string errorExtra;

        // Parse SELECT

        ParseRunner::Ptr parser;
        try {
            parser = std::make_shared<ParseRunner>(query);
        } catch (parser::ParseException& e) {
            return std::make_shared<UserQueryInvalid>(std::string("ParseException:") + e.what());
        }
        auto stmt = parser->getSelectStmt();

        // handle special database/table names
        auto&& tblRefList = stmt->getFromList().getTableRefList();
        if (tblRefList.size() == 1) {
            auto&& tblRef = tblRefList[0];
            std::string const db = tblRef->getDb().empty() ? defaultDb : tblRef->getDb();
            if (UserQueryType::isProcessListTable(db, tblRef->getTable())) {
                if (async) {
                    // no point supporting async for these
                    auto uq = std::make_shared<UserQueryInvalid>("SUBMIT is not allowed with query: " + aQuery);
                    return uq;
                }
                LOGS(_log, LOG_LVL_DEBUG, "SELECT query is a PROCESSLIST");
                try {
                    return std::make_shared<UserQueryProcessList>(stmt, _userQuerySharedResources->resultDbConn.get(),
                            _userQuerySharedResources->qMetaSelect, _userQuerySharedResources->qMetaCzarId, userQueryId, resultDb);
                } catch(std::exception const& exc) {
                    return std::make_shared<UserQueryInvalid>(exc.what());
                }
            }
        }

        // This is a regular SELECT for qserv

        // Currently using the database for results to get schema information.
        auto qs = std::make_shared<qproc::QuerySession>(_userQuerySharedResources->css,
                                                        _userQuerySharedResources->databaseModels,
                                                        defaultDb);
        try {
            qs->analyzeQuery(query, stmt);
        } catch (...) {
            errorExtra = "Unknown failure occurred setting up QuerySession (query is invalid).";
            LOGS(_log, LOG_LVL_ERROR, errorExtra);
            sessionValid = false;
        }
        if (!qs->getError().empty()) {
            LOGS(_log, LOG_LVL_ERROR, "Invalid query: " << qs->getError());
            sessionValid = false;
        }

        auto messageStore = std::make_shared<qdisp::MessageStore>();
        std::shared_ptr<qdisp::Executive> executive;
        if (sessionValid) {
            executive = qdisp::Executive::create(*_executiveConfig, messageStore,
                                                 qdispPool, _userQuerySharedResources->queryStatsData);
        }
        auto uq = std::make_shared<UserQuerySelect>(qs, messageStore, executive, _userQuerySharedResources->databaseModels,
                                                    _userQuerySharedResources->mysqlResultConfig,
                                                    _userQuerySharedResources->secondaryIndex, _userQuerySharedResources->queryMetadata,
                                                    _userQuerySharedResources->queryStatsData, _userQuerySharedResources->qMetaCzarId,
                                                    qdispPool, errorExtra, async, resultDb);
        if (sessionValid) {
            uq->qMetaRegister(resultLocation, msgTableName);
            uq->setupChunking();
            uq->setupMerger();
            uq->saveResultQuery();
        }
        return uq;
    } else if (UserQueryType::isSelectResult(query, userJobId)) {
        auto uq = std::make_shared<UserQueryAsyncResult>(userJobId, _userQuerySharedResources->qMetaCzarId,
                                                         _userQuerySharedResources->queryMetadata,
                                                         _userQuerySharedResources->resultDbConn.get());
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryAsyncResult: userJobId=" << userJobId);
        return uq;
    } else if (UserQueryType::isDropTable(query, dbName, tableName)) {
        // processing DROP TABLE
        if (dbName.empty()) {
            dbName = defaultDb;
        }
        auto uq = std::make_shared<UserQueryDrop>(_userQuerySharedResources->css, dbName, tableName,
                                                  _userQuerySharedResources->resultDbConn.get(),
                                                  _userQuerySharedResources->queryMetadata, _userQuerySharedResources->qMetaCzarId);
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryDrop: " << dbName << "." << tableName);
        return uq;
    } else if (UserQueryType::isDropDb(query, dbName)) {
        // processing DROP DATABASE
        auto uq = std::make_shared<UserQueryDrop>(_userQuerySharedResources->css, dbName, std::string(),
                                                  _userQuerySharedResources->resultDbConn.get(),
                                                  _userQuerySharedResources->queryMetadata, _userQuerySharedResources->qMetaCzarId);
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryDrop: db=" << dbName);
        return uq;
    } else if (UserQueryType::isFlushChunksCache(query, dbName)) {
        auto uq = std::make_shared<UserQueryFlushChunksCache>(_userQuerySharedResources->css, dbName,
                                                              _userQuerySharedResources->resultDbConn.get());
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryFlushChunksCache: " << dbName);
        return uq;
    } else if (UserQueryType::isShowProcessList(query, full)) {
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryProcessList: full=" << (full ? 'y' : 'n'));
        try {
            return std::make_shared<UserQueryProcessList>(full, _userQuerySharedResources->resultDbConn.get(),
                    _userQuerySharedResources->qMetaSelect, _userQuerySharedResources->qMetaCzarId, userQueryId, resultDb);
        } catch(std::exception const& exc) {
            return std::make_shared<UserQueryInvalid>(exc.what());
        }
    } else if (UserQueryType::isCall(query)) {
        auto parser = std::make_shared<ParseRunner>(query,
            _userQuerySharedResources->makeUserQueryResources(userQueryId, resultDb));
        return parser->getUserQuery();
    } else {
        // something that we don't recognize
        auto uq = std::make_shared<UserQueryInvalid>("Invalid or unsupported query: " + query);
        return uq;
    }
}

}}} // lsst::qserv::ccontrol
