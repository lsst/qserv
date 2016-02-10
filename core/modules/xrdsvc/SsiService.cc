// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
#include "memman/MemManNone.h"
#include "sql/SqlConnection.h"
#include "wbase/Base.h"
#include "wconfig/Config.h"
#include "wconfig/ConfigError.h"
#include "wcontrol/Foreman.h"
#include "wpublish/ChunkInventory.h"
#include "wsched/BlendScheduler.h"
#include "wsched/FifoScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"
#include "xrdsvc/SsiSession.h"
#include "xrdsvc/XrdName.h"


class XrdPosixCallBack; // Forward.

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.SsiService");

// add LWP to MDC in log messages
void initMDC() {
    LOG_MDC("LWP", std::to_string(lsst::log::lwpID()));
}
int dummyInitMDC = LOG_MDC_INIT(initMDC);

}

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
    LOGS(_log, LOG_LVL_DEBUG, "SsiService starting...");

    _configure();
    _initInventory();
    _setupResultPath();

    if (!_setupScratchDb()) {
        throw wconfig::ConfigError("Couldn't setup scratch db");
    }

    // TODO: DM-4943 use MemManReal
    // Memory available is meaningless for MemManNone
    memman::MemMan::Ptr memMan = std::make_shared<memman::MemManNone>(1, false);

    // TODO: set poolSize and all maxThreads values from config file.
    uint poolSize = std::max(static_cast<uint>(15), std::thread::hardware_concurrency());
    // TODO: set GroupScheduler group size from configuration file
    // TODO: Consider limiting the number of chunks being accessed at a time
    //       by GroupScheduler and ScanScheduler
    // poolSize should be greater than either GroupScheduler::maxThreads or ScanScheduler::maxThreads
    //uint maxThread = poolSize - 3;
    uint maxThread = poolSize;
    int maxReserve = 2;
    int priority = 1;
    auto group = std::make_shared<wsched::GroupScheduler>("SchedGroup", maxThread, maxReserve, 10, priority++);

    int const fastest = lsst::qserv::proto::ScanInfo::Rating::FASTEST;
    int const fast    = lsst::qserv::proto::ScanInfo::Rating::FAST;
    int const medium  = lsst::qserv::proto::ScanInfo::Rating::MEDIUM;
    int const slow    = lsst::qserv::proto::ScanInfo::Rating::SLOW;
    std::vector<wsched::ScanScheduler::Ptr> scanSchedulers{
        std::make_shared<wsched::ScanScheduler>("SchedFast", maxThread, maxReserve, priority++, memMan, fastest, fast),
        std::make_shared<wsched::ScanScheduler>("SchedMed", maxThread, maxReserve, priority++, memMan, fast+1, medium),
        std::make_shared<wsched::ScanScheduler>("SchedSlow", maxThread, maxReserve, priority++, memMan, medium+1, slow)
    };

    _foreman = wcontrol::Foreman::newForeman(
        std::make_shared<wsched::BlendScheduler>("BlendSched", maxThread, group, scanSchedulers),
        poolSize);
}

SsiService::~SsiService() {
    LOGS(_log, LOG_LVL_DEBUG, "SsiService dying.");
}

void SsiService::Provision(XrdSsiService::Resource* r,
                           unsigned short timeOut,
                           bool userConn) { // Step 2
    LOGS(_log, LOG_LVL_DEBUG, "Got provision call where rName is: " << r->rName);
    XrdSsiSession* session = new SsiSession(r->rName, _chunkInventory->newValidator(), _foreman);
    r->ProvisionDone(session); // Step 3: trigger client-side ProvisionDone()
}

void SsiService::_initInventory() {
    XrdName x;
    std::shared_ptr<sql::SqlConnection> conn = makeSqlConnection();
    assert(conn);
    _chunkInventory = std::make_shared<wpublish::ChunkInventory>(x.getName(), conn);
    std::ostringstream os;
    os << "Paths exported: ";
    _chunkInventory->dbgPrint(os);
    LOGS(_log, LOG_LVL_DEBUG, os.str());
}

void SsiService::_setupResultPath() {
    wbase::updateResultPath();
    wbase::clearResultPath();
}

void SsiService::_configure() {
    if (!wconfig::getConfig().getIsValid()) {
        std::string msg("Configuration invalid: "
                        + wconfig::getConfig().getError());
        LOGS(_log, LOG_LVL_FATAL, msg);
        throw wconfig::ConfigError(msg);
    }
}

/// Cleanup scratch space and scratch dbs.
/// This means that scratch db and scratch dirs CANNOT be shared among
/// qserv workers. Take heed.
/// @return true if cleanup was successful, false otherwise.
bool SsiService::_setupScratchDb() {
    std::shared_ptr<sql::SqlConnection> conn = makeSqlConnection();
    if (!conn) {
        return false;
    }
    sql::SqlErrorObject errObj;
    std::string dbName = wconfig::getConfig().getString("scratchDb");
    LOGS(_log, LOG_LVL_DEBUG, "Cleaning up scratchDb: " << dbName);
    if (!conn->dropDb(dbName, errObj, false)) {
        LOGS(_log, LOG_LVL_ERROR, "Cfg error! couldn't drop scratchDb: "
             << dbName << ", error: " << errObj.errMsg());
        return false;
    }
    errObj.reset();
    if (!conn->createDb(dbName, errObj, true)) {
        LOGS(_log, LOG_LVL_ERROR, "Cfg error! couldn't create scratchDb: "
             << dbName << ", error: " << errObj.errMsg());
        return false;
    }
    return true;
}


}}} // namespace
