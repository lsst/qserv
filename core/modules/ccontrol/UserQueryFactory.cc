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
#include <stdlib.h>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/ConfigError.h"
#include "ccontrol/UserQuery.h"
#include "ccontrol/userQueryProxy.h"
#include "css/Facade.h"
#include "css/KvInterfaceImplMem.h"
#include "mysql/MySqlConfig.h"
#include "qdisp/Executive.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "rproc/InfileMerger.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

LOG_LOGGER UserQueryFactory::_log = LOG_GET("lsst.qserv.ccontrol.UserQueryFactory");

/// Implementation class (PIMPL-style) for UserQueryFactory.
class UserQueryFactory::Impl {
public:
    /// Import non-Facade-related config from caller
    void readConfig(StringMap const& m);

    /// Import facade config and construct Facade
    void readConfigFacade(StringMap const& m,
                          std::shared_ptr<css::KvInterface> kvi);

    void initFacade(std::string const& cssTech, std::string const& cssConn,
                    int timeout,
                    std::string const& emptyChunkPath);

    void initFacade(std::shared_ptr<css::KvInterface> kvi,
                    std::string const& emptyChunkPath);

    void initMergerTemplate(); ///< Construct template config for merger

    /// State shared between UserQueries
    qdisp::Executive::Config::Ptr executiveConfig;
    std::shared_ptr<css::Facade> facade;
    rproc::InfileMergerConfig infileMergerConfigTemplate;
    std::shared_ptr<qproc::SecondaryIndex> secondaryIndex;
};

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(StringMap const& m,
                                   std::shared_ptr<css::KvInterface> kvi)
    :  _impl(std::make_shared<Impl>()) {
    ::putenv((char*)"XRDDEBUG=1");
    assert(_impl);
    _impl->readConfig(m);
    _impl->readConfigFacade(m, kvi);
}

std::pair<int,std::string>
UserQueryFactory::newUserQuery(std::string const& query,
                               std::string const& defaultDb,
                               std::string const& resultTable) {
    bool sessionValid = true;
    std::string errorExtra;
    qproc::QuerySession::Ptr qs =
            std::make_shared<qproc::QuerySession>(_impl->facade);
    try {
        qs->setResultTable(resultTable);
        qs->setDefaultDb(defaultDb);
        qs->analyzeQuery(query);
    } catch (...) {
        errorExtra = "Unknown failure occured setting up QuerySession (query is invalid).";
        LOGF(_log, LOG_LVL_ERROR, errorExtra);
        sessionValid = false;
    }
    if(!qs->getError().empty()) {
        LOGF(_log, LOG_LVL_ERROR, "Invalid query: %1%" % qs->getError());
        sessionValid = false;
    }
    if(!qs->getError().empty()) {
        LOGF_INFO("Invalid query: %s" % qs->getError());
        sessionValid = false;
    }
    UserQuery* uq = new UserQuery(qs);
    int sessionId = UserQuery_takeOwnership(uq);
    uq->_sessionId = sessionId;
    uq->_secondaryIndex = _impl->secondaryIndex;
    if(sessionValid) {
        uq->_executive = std::make_shared<qdisp::Executive>(
                _impl->executiveConfig, uq->_messageStore);

        rproc::InfileMergerConfig* ict
            = new rproc::InfileMergerConfig(_impl->infileMergerConfigTemplate);
        ict->targetTable = resultTable;
        uq->_infileMergerConfig.reset(ict);
        uq->_setupChunking();
    } else {
        uq->_errorExtra += errorExtra;
    }
    return std::make_pair(sessionId, qs->getProxyOrderBy());
}

void UserQueryFactory::Impl::readConfig(StringMap const& m) {
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
}

void UserQueryFactory::Impl::readConfigFacade(
    StringMap const& m,
    std::shared_ptr<css::KvInterface> kvi) {
    ConfigMap cm(m);

    std::string emptyChunkPath = cm.get(
        "partitioner.emptychunkpath",
        "Error, missing path for Empty chunk file, using '.'.",
        ".");
    if (!kvi) {
        std::string cssTech = cm.get(
            "css.technology",
            "Error, css.technology not found.",
            "invalid");
        std::string cssConn = cm.get(
            "css.connection",
            "Error, css.connection not found.",
            "");
        int cssTimeout = cm.getTyped<int>(
            "css.timeout",
            "Error, css.timeout not found.",
            10000);

        initFacade(cssTech, cssConn, cssTimeout, emptyChunkPath);
    } else {
        initFacade(kvi, emptyChunkPath);
    }
}

void UserQueryFactory::Impl::initFacade(std::string const& cssTech,
                                        std::string const& cssConn,
                                        int timeout_msec,
                                        std::string const& emptyChunkPath) {
    if (cssTech == "mem") {
        LOGF(_log, LOG_LVL_INFO, "Initializing memory-based css, with %1%" % cssConn);
        facade = css::FacadeFactory::createMemFacade(cssConn, emptyChunkPath);
    } else {
        LOGF(_log, LOG_LVL_ERROR, "Unable to determine css technology, check config file.");
        throw ConfigError("Invalid css technology, check config file.");
    }
}

void UserQueryFactory::Impl::initFacade(
    std::shared_ptr<css::KvInterface> kvi,
    std::string const& emptyChunkPath) {
    facade = css::FacadeFactory::createCacheFacade(kvi, emptyChunkPath);
    LOGF(_log, LOG_LVL_INFO, "Initializing cache-based css facade");
}

}}} // lsst::qserv::ccontrol
