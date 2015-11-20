// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include <regex>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/ConfigError.h"
#include "ccontrol/UserQueryDropTable.h"
#include "ccontrol/UserQueryInvalid.h"
#include "ccontrol/UserQuerySelect.h"
#include "ccontrol/userQueryProxy.h"
#include "css/CssAccess.h"
#include "css/KvInterfaceImplMem.h"
#include "mysql/MySqlConfig.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qmeta/QMetaMysql.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "rproc/InfileMerger.h"
#include "sql/SqlConnection.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryFactory");

// Returns true if query is DROP TABLE
bool isDropTable(std::string const& query, std::string& dbName, std::string& tableName);

// Returns true if query is SELECT
bool isSelect(std::string const& query);

}

namespace lsst {
namespace qserv {
namespace ccontrol {


/// Implementation class (PIMPL-style) for UserQueryFactory.
class UserQueryFactory::Impl {
public:

    Impl(StringMap const& config);

    /// State shared between UserQueries
    qdisp::Executive::Config::Ptr executiveConfig;
    std::shared_ptr<css::CssAccess> css;
    rproc::InfileMergerConfig infileMergerConfigTemplate;
    std::shared_ptr<qproc::SecondaryIndex> secondaryIndex;
    std::shared_ptr<qmeta::QMeta> queryMetadata;
    std::unique_ptr<sql::SqlConnection> resultDbConn;
    qmeta::CzarId qMetaCzarId;   ///< Czar ID in QMeta database
};

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(StringMap const& m,
                                   std::string const& czarName)
    :  _impl(std::make_shared<Impl>(m)) {

    ::putenv((char*)"XRDDEBUG=1");

    // register czar in QMeta
    // TODO: check that czar with the same name is not active already?
    _impl->qMetaCzarId = _impl->queryMetadata->registerCzar(czarName);
}

std::pair<int,std::string>
UserQueryFactory::newUserQuery(std::string const& query,
                               std::string const& defaultDb,
                               std::string const& resultTable) {
    std::string dbName, tableName;

    if (::isSelect(query)) {
        // Processing regular select query
        bool sessionValid = true;
        std::string errorExtra;
        qproc::QuerySession::Ptr qs = std::make_shared<qproc::QuerySession>(_impl->css);
        try {
            qs->setResultTable(resultTable);
            qs->setDefaultDb(defaultDb);
            qs->analyzeQuery(query);
        } catch (...) {
            errorExtra = "Unknown failure occurred setting up QuerySession (query is invalid).";
            LOGF(_log, LOG_LVL_ERROR, errorExtra);
            sessionValid = false;
        }
        if(!qs->getError().empty()) {
            LOGF(_log, LOG_LVL_ERROR, "Invalid query: %1%" % qs->getError());
            sessionValid = false;
        }

        auto messageStore = std::make_shared<qdisp::MessageStore>();
        std::shared_ptr<qdisp::Executive> executive;
        std::shared_ptr<rproc::InfileMergerConfig> infileMergerConfig;
        if(sessionValid) {
            executive = std::make_shared<qdisp::Executive>(_impl->executiveConfig, messageStore);
            infileMergerConfig = std::make_shared<rproc::InfileMergerConfig>(_impl->infileMergerConfigTemplate);
            infileMergerConfig->targetTable = resultTable;
        }
        auto uq = new UserQuerySelect(qs, messageStore, executive, infileMergerConfig,
                                      _impl->secondaryIndex, _impl->queryMetadata,
                                      _impl->qMetaCzarId, errorExtra);
        int sessionId = UserQuery_takeOwnership(uq);
        uq->setSessionId(sessionId);
        if(sessionValid) {
            uq->setupChunking();
        }
        return std::make_pair(sessionId, qs->getProxyOrderBy());
    } else if (::isDropTable(query, dbName, tableName)) {
        // processing DROP TABLE
        if (dbName.empty()) {
            dbName = defaultDb;
        }
        auto uq = new UserQueryDropTable(_impl->css, dbName, tableName,
                                         _impl->resultDbConn.get(), resultTable,
                                         _impl->queryMetadata, _impl->qMetaCzarId);
        int sessionId = UserQuery_takeOwnership(uq);
        uq->setSessionId(sessionId);
        LOGF(_log, LOG_LVL_DEBUG, "make UserQueryDropTable: %s.%s -> %s" % dbName % tableName % sessionId);
        return std::make_pair(sessionId, std::string());
    } else {
        // something that we don't recognize
        auto uq = new UserQueryInvalid("Invalid or unsupported query: " + query);
        int sessionId = UserQuery_takeOwnership(uq);
        uq->setSessionId(sessionId);
        return std::make_pair(sessionId, std::string());
    }
}

UserQueryFactory::Impl::Impl(StringMap const& m) {

    ConfigMap cm(m);
    /// localhost:1094 is the most reasonable default, even though it is
    /// the wrong choice for all but small developer installations.
    std::string serviceUrl = cm.get(
        "frontend.xrootd", // czar.serviceUrl
        "WARNING! No xrootd spec. Using localhost:1094",
        "localhost:1094");
    executiveConfig = std::make_shared<qdisp::Executive::Config>(serviceUrl);
    // This should be overriden by the installer properly.
    infileMergerConfigTemplate.socket = cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using /u1/local/mysql.sock.",
        "/u1/local/mysql.sock");
    infileMergerConfigTemplate.user = cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    infileMergerConfigTemplate.targetDb = cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservResult.",
        "qservResult");
    mysql::MySqlConfig mc;
    mc.username = infileMergerConfigTemplate.user;
    mc.dbName = infileMergerConfigTemplate.targetDb; // any valid db is ok.
    mc.socket = infileMergerConfigTemplate.socket;
    secondaryIndex = std::make_shared<qproc::SecondaryIndex>(mc);

    // make one dedicated connection for results database
    resultDbConn.reset(new sql::SqlConnection(mc));

    // get config parameters for qmeta db
    mysql::MySqlConfig qmetaConfig;
    qmetaConfig.hostname = cm.get(
        "qmeta.host",
        "Error, qmeta.host not found. Using empty host name.",
        "");
    qmetaConfig.port = cm.getTyped<unsigned>(
        "qmeta.port",
        "Error, qmeta.port not found. Using 0 for port.",
        0U);
    qmetaConfig.username = cm.get(
        "qmeta.user",
        "Error, qmeta.user not found. Using qsmaster.",
        "qsmaster");
    qmetaConfig.password = cm.get(
        "qmeta.passwd",
        "Error, qmeta.passwd not found. Using empty string.",
        "");
    qmetaConfig.socket = cm.get(
        "qmeta.unix_socket",
        "Error, qmeta.unix_socket not found. Using empty string.",
        "");
    qmetaConfig.dbName = cm.get(
        "qmeta.db",
        "Error, qmeta.db not found. Using qservMeta.",
        "qservMeta");
    queryMetadata = std::make_shared<qmeta::QMetaMysql>(qmetaConfig);

    // empty chunk path
    std::string emptyChunkPath = cm.get(
        "partitioner.emptychunkpath",
        "Error, missing path for Empty chunk file, using '.'.",
        ".");

    // find all css.* parameters and copy to new map (dropping css.)
    StringMap cssConfig;
    for (auto& kv: m) {
        if (kv.first.compare(0, 4, "css.") == 0) {
            cssConfig.insert(std::make_pair(std::string(kv.first, 4), kv.second));
        }
    }

    // create CssAccess instance
    css = css::CssAccess::createFromConfig(cssConfig, emptyChunkPath);
}

}}} // lsst::qserv::ccontrol


namespace {

// regex for DROP TABLE [dbname.]table; both table and db names can be in quotes;
// db name will be in group 3, table name in group 5.
// Note that parens around whole string are not part of the regex but raw string literal
std::regex _dropTableRe(R"(^drop\s+table\s+((["`]?)(\w+)\2[.])?(["`]?)(\w+)\4\s*;?\s*$)",
                        std::regex::ECMAScript | std::regex::icase | std::regex::optimize);

bool isDropTable(std::string const& query, std::string& dbName, std::string& tableName) {
    LOGF(_log, LOG_LVL_DEBUG, "isDropTable: %s" % query);
    std::smatch sm;
    bool match = regex_match(query, sm, _dropTableRe);
    if (match) {
        dbName = sm.str(3);
        tableName = sm.str(5);
        LOGF(_log, LOG_LVL_DEBUG, "isDropTable: match: %s.%s" % dbName % tableName);
    }
    return match;
}

// regex for SELECT *
// Note that parens around whole string are not part of the regex but raw string literal
std::regex _selectRe(R"(^select\s+.+$)",
                     std::regex::ECMAScript | std::regex::icase | std::regex::optimize);

bool isSelect(std::string const& query) {
    LOGF(_log, LOG_LVL_DEBUG, "isSelect: %s" % query);
    std::smatch sm;
    bool match = regex_match(query, sm, _selectRe);
    if (match) {
        LOGF(_log, LOG_LVL_DEBUG, "isSelect: match");
    }
    return match;
}

}
