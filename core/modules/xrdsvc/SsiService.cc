// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#include "xrdsvc/SsiService.h"

// System headers
#include <cassert>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <unistd.h>

// Third-party headers
#include "XProtocol/XProtocol.hh"
#include "XrdSsi/XrdSsiLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "sql/SqlConnection.h"
#include "wbase/Base.h"
#include "wconfig/Config.h"
#include "wconfig/ConfigError.h"
#include "wcontrol/Foreman.h"
#include "wpublish/ChunkInventory.h"
#include "wsched/BlendScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"
#include "xrdsvc/SsiSession.h"
#include "xrdsvc/XrdName.h"


class XrdPosixCallBack; // Forward.


namespace lsst {
namespace qserv {
namespace xrdsvc {

std::shared_ptr<sql::SqlConnection> makeSqlConnection() {
    std::shared_ptr<sql::SqlConnection> conn;
    mysql::MySqlConfig sqlConfig = wconfig::getConfig().getSqlConfig();
    sqlConfig.dbName = ""; // Force dbName empty to prevent accidental context
    conn = std::make_shared<sql::SqlConnection>(sqlConfig, true);
    return conn;
}


SsiService::SsiService(XrdSsiLogger* log) {
    LOG_INFO("SsiService starting...");

    _configure();
    _initInventory();
    _setupResultPath();

    if(!_setupScratchDb()) {
        throw wconfig::ConfigError("Couldn't setup scratch db");
    }

    _foreman = wcontrol::newForeman(
        std::make_shared<wsched::BlendScheduler>(
            std::make_shared<wsched::GroupScheduler>(),
            std::make_shared<wsched::ScanScheduler>()
        )
    );
}

SsiService::~SsiService() {
    LOG_INFO("SsiService dying.");
}

bool
SsiService::Provision(XrdSsiService::Resource* r,
                      unsigned short timeOut) { // Step 2
    LOGF_INFO("Got provision call where rName is: %1%" % r->rName);

    XrdSsiSession* session = new SsiSession(
        r->rName,
        _chunkInventory->newValidator(),
        _foreman->getProcessor());
    r->ProvisionDone(session); // Step 3: trigger client-side ProvisionDone()
    return true;
}

void SsiService::_initInventory() {
    XrdName x;
    std::shared_ptr<sql::SqlConnection> conn = makeSqlConnection();
    assert(conn);
    _chunkInventory =
            std::make_shared<wpublish::ChunkInventory>(x.getName(), conn);
    std::ostringstream os;
    os << "Paths exported: ";
    _chunkInventory->dbgPrint(os);
    LOGF_INFO(os.str());
}

void SsiService::_setupResultPath() {
    wbase::updateResultPath();
    wbase::clearResultPath();
}

void SsiService::_configure() {
    if(!wconfig::getConfig().getIsValid()) {
        std::string msg("Configuration invalid: "
                        + wconfig::getConfig().getError());
        LOG_FATAL(msg.c_str());
        throw wconfig::ConfigError(msg);
    }
}

/// Cleanup scratch space and scratch dbs.
/// This means that scratch db and scratch dirs CANNOT be shared among
/// qserv workers. Take heed.
/// @return true if cleanup was successful, false otherwise.
bool SsiService::_setupScratchDb() {
    std::shared_ptr<sql::SqlConnection> conn = makeSqlConnection();
    if(!conn) {
        return false;
    }
    sql::SqlErrorObject errObj;
    std::string dbName = wconfig::getConfig().getString("scratchDb");
    LOGF_INFO("Cleaning up scratchDb: %1%." % dbName);
    if(!conn->dropDb(dbName, errObj, false)) {
        LOGF_ERROR("Cfg error! couldn't drop scratchDb: %1% %2%." % dbName % errObj.errMsg());
        return false;
    }
    errObj.reset();
    if(!conn->createDb(dbName, errObj, true)) {
        LOGF_ERROR("Cfg error! couldn't create scratchDb: %1% %2%." % dbName % errObj.errMsg());
        return false;
    }
    return true;
}


}}} // lsst::qserv::xrdsvc
