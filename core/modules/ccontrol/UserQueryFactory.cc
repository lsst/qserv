// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#include "ccontrol/UserQueryFactory.h"

// System headers
#include <cassert>
#include <stdlib.h>

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/ConfigError.h"
#include "ccontrol/UserQuery.h"
#include "ccontrol/userQueryProxy.h"
#include "css/Facade.h"
#include "qdisp/Executive.h"
#include "qproc/QuerySession.h"
#include "rproc/InfileMerger.h"
#include "rproc/TableMerger.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

/// Implementation class (PIMPL-style) for UserQueryFactory.
class UserQueryFactory::Impl {
public:
    void readConfig(StringMap const& m); ///< Import config from caller
    void initFacade(std::string const& cssTech, std::string const& cssConn,
                    int timeout_msec);
    void initMergerTemplate(); ///< Construct template config for merger

    /// State shared between UserQueries
    qdisp::Executive::Config::Ptr executiveConfig;
    boost::shared_ptr<css::Facade> facade;
    rproc::InfileMergerConfig infileMergerConfigTemplate;
};

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(StringMap const& m) {
    ::putenv((char*)"XRDDEBUG=1");
    _impl.reset(new Impl);
    assert(_impl);
    _impl->readConfig(m);
}

int
UserQueryFactory::newUserQuery(std::string const& query,
                               std::string const& defaultDb,
                               std::string const& resultTable) {
    bool sessionValid = true;
    qproc::QuerySession::Ptr qs(new qproc::QuerySession(_impl->facade));
    try {
        qs->setResultTable(resultTable);
        qs->setDefaultDb(defaultDb);
        qs->setQuery(query);
    } catch (...) {
        sessionValid = false;
    }
    UserQuery* uq = new UserQuery(qs);
    int sessionId = UserQuery_takeOwnership(uq);
    uq->_sessionId = sessionId;
    if(sessionValid) {
        uq->_executive.reset(new qdisp::Executive(
                                 _impl->executiveConfig,
                                 uq->_messageStore));

        rproc::InfileMergerConfig* ict
            = new rproc::InfileMergerConfig(_impl->infileMergerConfigTemplate);
        ict->targetTable = resultTable;
        uq->_infileMergerConfig.reset(ict);
    } else {
        uq->_errorExtra += "Unknown error setting QuerySession";
    }
    return sessionId;
}

void UserQueryFactory::Impl::readConfig(StringMap const& m) {
    ConfigMap cm(m);
    /// localhost:1094 is the most reasonable default, even though it is
    /// the wrong choice for all but small developer installations.
    std::string serviceUrl = cm.get(
        "frontend.xrootd", // czar.serviceUrl
        "WARNING! No xrootd spec. Using localhost:1094",
        "localhost:1094");
    executiveConfig.reset(new qdisp::Executive::Config(serviceUrl));
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
    std::string cssTech = cm.get(
        "css.technology",
        "Error, css.technology not found.",
        "invalid");
    std::string cssConn = cm.get(
        "css.connection",
        "Error, css.connection not found.",
        "");
    int cssTimeout = atoi(cm.get(
        "css.timeout",
        "Error, css.timeout not found.",
        "10000").c_str());
    initFacade(cssTech, cssConn, cssTimeout);
}

void UserQueryFactory::Impl::initFacade(std::string const& cssTech,
                                        std::string const& cssConn,
                                        int timeout_msec) {
    if (cssTech == "zoo") {
        LOGGER_INF << "Initializing zookeeper-based css, with "
                   << cssConn << ", " << timeout_msec << std::endl;
        facade = css::FacadeFactory::createZooFacade(cssConn, timeout_msec);
    } else if (cssTech == "mem") {
        LOGGER_INF << "Initializing memory-based css, with "
                   << cssConn << std::endl;
        facade = css::FacadeFactory::createMemFacade(cssConn);
    } else {
        LOGGER_ERR << "Unable to determine css technology, check config file."
                   << std::endl;
        throw ConfigError("Invalid css technology, check config file.");
    }
}

}}} // lsst::qserv::ccontrol
