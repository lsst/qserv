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
#include "ccontrol/UserQueryAsyncResult.h"
#include "ccontrol/UserQueryDrop.h"
#include "ccontrol/UserQueryFlushChunksCache.h"
#include "ccontrol/UserQueryInvalid.h"
#include "ccontrol/UserQueryProcessList.h"
#include "ccontrol/UserQuerySelect.h"
#include "ccontrol/UserQueryType.h"
#include "css/CssAccess.h"
#include "css/KvInterfaceImplMem.h"
#include "czar/CzarConfig.h"
#include "mysql/MySqlConfig.h"
#include "parser/ParseException.h"
#include "parser/SelectParser.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qmeta/QMetaMysql.h"
#include "qmeta/QMetaSelect.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "query/FromList.h"
#include "query/SelectStmt.h"
#include "rproc/InfileMerger.h"
#include "sql/SqlConnection.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryFactory");
}

namespace lsst {
namespace qserv {
namespace ccontrol {


/// Implementation class (PIMPL-style) for UserQueryFactory.
class UserQueryFactory::Impl {
public:

    Impl(czar::CzarConfig const& czarConfig);

    /// State shared between UserQueries
    qdisp::Executive::Config::Ptr executiveConfig;
    std::shared_ptr<css::CssAccess> css;
    mysql::MySqlConfig const mysqlResultConfig;
    std::shared_ptr<qproc::SecondaryIndex> secondaryIndex;
    std::shared_ptr<qmeta::QMeta> queryMetadata;
    std::shared_ptr<qmeta::QMetaSelect> qMetaSelect;
    std::unique_ptr<sql::SqlConnection> resultDbConn;
    qmeta::CzarId qMetaCzarId = {0};   ///< Czar ID in QMeta database
};

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(czar::CzarConfig const& czarConfig,
                                   std::string const& czarName)
    :  _impl(std::make_shared<Impl>(czarConfig)) {

    ::putenv((char*)"XRDDEBUG=1");

    // register czar in QMeta
    // TODO: check that czar with the same name is not active already?
    _impl->qMetaCzarId = _impl->queryMetadata->registerCzar(czarName);
}

UserQuery::Ptr
UserQueryFactory::newUserQuery(std::string const& aQuery,
                               std::string const& defaultDb,
                               qdisp::LargeResultMgr::Ptr const& largeResultMgr,
                               std::string const& userQueryId,
                               std::string const& msgTableName) {

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
        std::shared_ptr<query::SelectStmt> stmt;
        try {
            auto parser = parser::SelectParser::newInstance(query);
            parser->setup();
            stmt = parser->getSelectStmt();
        } catch(parser::ParseException const& e) {
            return std::make_shared<UserQueryInvalid>(std::string("ParseException:") + e.what());
        }

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
                    return std::make_shared<UserQueryProcessList>(stmt, _impl->resultDbConn.get(),
                            _impl->qMetaSelect, _impl->qMetaCzarId, userQueryId);
                } catch(std::exception const& exc) {
                    return std::make_shared<UserQueryInvalid>(exc.what());
                }
            }
        }

        // This is a regular SELECT for qserv

        // Currently using the database for results to get schema information.
        auto qs = std::make_shared<qproc::QuerySession>(_impl->css,
                                                        _impl->mysqlResultConfig,
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
        std::shared_ptr<rproc::InfileMergerConfig> infileMergerConfig;
        if (sessionValid) {
            executive = qdisp::Executive::newExecutive(_impl->executiveConfig, messageStore,
                                                       largeResultMgr);
            infileMergerConfig = std::make_shared<rproc::InfileMergerConfig>(_impl->mysqlResultConfig);
        }
        auto uq = std::make_shared<UserQuerySelect>(qs, messageStore, executive, infileMergerConfig,
                                                    _impl->secondaryIndex, _impl->queryMetadata,
                                                    _impl->qMetaCzarId, largeResultMgr,
                                                    errorExtra, async);
        if (sessionValid) {
            uq->qMetaRegister(resultLocation, msgTableName);
            uq->setupChunking();
        }
        return uq;
    } else if (UserQueryType::isSelectResult(query, userJobId)) {
        auto uq = std::make_shared<UserQueryAsyncResult>(userJobId, _impl->qMetaCzarId,
                                                         _impl->queryMetadata,
                                                         _impl->resultDbConn.get());
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryAsyncResult: userJobId=" << userJobId);
        return uq;
    } else if (UserQueryType::isDropTable(query, dbName, tableName)) {
        // processing DROP TABLE
        if (dbName.empty()) {
            dbName = defaultDb;
        }
        auto uq = std::make_shared<UserQueryDrop>(_impl->css, dbName, tableName,
                                                  _impl->resultDbConn.get(),
                                                  _impl->queryMetadata, _impl->qMetaCzarId);
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryDrop: " << dbName << "." << tableName);
        return uq;
    } else if (UserQueryType::isDropDb(query, dbName)) {
        // processing DROP DATABASE
        auto uq = std::make_shared<UserQueryDrop>(_impl->css, dbName, std::string(),
                                                  _impl->resultDbConn.get(),
                                                  _impl->queryMetadata, _impl->qMetaCzarId);
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryDrop: db=" << dbName);
        return uq;
    } else if (UserQueryType::isFlushChunksCache(query, dbName)) {
        auto uq = std::make_shared<UserQueryFlushChunksCache>(_impl->css, dbName,
                                                              _impl->resultDbConn.get());
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryFlushChunksCache: " << dbName);
        return uq;
    } else if (UserQueryType::isShowProcessList(query, full)) {
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryProcessList: full=" << (full ? 'y' : 'n'));
        try {
            return std::make_shared<UserQueryProcessList>(full, _impl->resultDbConn.get(),
                    _impl->qMetaSelect, _impl->qMetaCzarId, userQueryId);
        } catch(std::exception const& exc) {
            return std::make_shared<UserQueryInvalid>(exc.what());
        }
    } else {
        // something that we don't recognize
        auto uq = std::make_shared<UserQueryInvalid>("Invalid or unsupported query: " + query);
        return uq;
    }
}

UserQueryFactory::Impl::Impl(czar::CzarConfig const& czarConfig)
    : mysqlResultConfig(czarConfig.getMySqlResultConfig()) {

    executiveConfig = std::make_shared<qdisp::Executive::Config>(czarConfig.getXrootdFrontendUrl());
    secondaryIndex = std::make_shared<qproc::SecondaryIndex>(mysqlResultConfig);

    // make one dedicated connection for results database
    resultDbConn.reset(new sql::SqlConnection(mysqlResultConfig));

    queryMetadata = std::make_shared<qmeta::QMetaMysql>(czarConfig.getMySqlQmetaConfig());
    qMetaSelect = std::make_shared<qmeta::QMetaSelect>(czarConfig.getMySqlQmetaConfig());

    // create CssAccess instance
    css = css::CssAccess::createFromConfig(czarConfig.getCssConfigMap(), czarConfig.getEmptyChunkPath());
}

}}} // lsst::qserv::ccontrol
