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
#include "XrdSsi/XrdSsiLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "memman/MemMan.h"
#include "memman/MemManNone.h"
#include "mysql/MySqlConnection.h"
#include "sql/SqlConnection.h"
#include "wbase/Base.h"
#include "wconfig/WorkerConfig.h"
#include "wconfig/WorkerConfigError.h"
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

SsiService::SsiService(XrdSsiLogger* log, wconfig::WorkerConfig const& workerConfig)
    : _mySqlConfig(workerConfig.getMySqlConfig()) {
    LOGS(_log, LOG_LVL_DEBUG, "SsiService starting...");


    if (not mysql::MySqlConnection::checkConnection(_mySqlConfig)) {
        LOGS(_log, LOG_LVL_FATAL, "Unable to connect to MySQL using configuration:" << _mySqlConfig);
        throw wconfig::WorkerConfigError("Unable to connect to MySQL");
    }
    _initInventory();

    std::string cfgMemMan = workerConfig.getMemManClass();
    memman::MemMan::Ptr memMan;
    if (cfgMemMan  == "MemManReal") {
        // Default to 1 gigabyte
        uint64_t memManSize = workerConfig.getMemManSizeMb()*1000000;
        LOGS(_log, LOG_LVL_DEBUG, "Using MemManReal with memManSizeMb=" << workerConfig.getMemManSizeMb() 
            << " location=" <<  workerConfig.getMemManLocation());
        memMan = std::shared_ptr<memman::MemMan>(memman::MemMan::create(memManSize, workerConfig.getMemManLocation()));
    } else if (cfgMemMan == "MemManNone"){
        memMan = std::make_shared<memman::MemManNone>(1, false);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "Unrecognized memory manager " << cfgMemMan);
        throw wconfig::WorkerConfigError("Unrecognized memory manager.");
    }

    // Set thread pool size.
    uint poolSize = std::max(workerConfig.getThreadPoolSize(), std::thread::hardware_concurrency());

    // poolSize should be greater than either GroupScheduler::maxThreads or ScanScheduler::maxThreads
    uint maxThread = poolSize;
    int maxReserve = 2;
    auto group = std::make_shared<wsched::GroupScheduler>(
        "SchedGroup", maxThread, maxReserve,
        workerConfig.getMaxGroupSize(), wsched::SchedulerBase::getMaxPriority());

    int const fastest = lsst::qserv::proto::ScanInfo::Rating::FASTEST;
    int const fast    = lsst::qserv::proto::ScanInfo::Rating::FAST;
    int const medium  = lsst::qserv::proto::ScanInfo::Rating::MEDIUM;
    int const slow    = lsst::qserv::proto::ScanInfo::Rating::SLOW;
    int const slowest = lsst::qserv::proto::ScanInfo::Rating::SLOWEST;
    double fastScanMaxMinutes = (double)workerConfig.getScanMaxMinutesFast();
    double medScanMaxMinutes = (double)workerConfig.getScanMaxMinutesMed();
    double slowScanMaxMinutes = (double)workerConfig.getScanMaxMinutesSlow();
    double snailScanMaxMinutes = (double)workerConfig.getScanMaxMinutesSnail();
    int maxTasksBootedPerUserQuery = workerConfig.getMaxTasksBootedPerUserQuery();
    std::vector<wsched::ScanScheduler::Ptr> scanSchedulers{
        std::make_shared<wsched::ScanScheduler>(
            "SchedSlow", maxThread, workerConfig.getMaxReserveSlow(), workerConfig.getPrioritySlow(),
            workerConfig.getMaxActiveChunksSlow(), memMan, medium+1, slow, slowScanMaxMinutes),
        std::make_shared<wsched::ScanScheduler>(
            "SchedMed", maxThread, workerConfig.getMaxReserveMed(), workerConfig.getPriorityMed(),
            workerConfig.getMaxActiveChunksMed(), memMan, fast+1, medium, medScanMaxMinutes),
        std::make_shared<wsched::ScanScheduler>(
            "SchedFast", maxThread, workerConfig.getMaxReserveFast(), workerConfig.getPriorityFast(),
            workerConfig.getMaxActiveChunksFast(), memMan, fastest, fast, fastScanMaxMinutes),

    };

    auto snail = std::make_shared<wsched::ScanScheduler>(
        "SchedSnail", maxThread, workerConfig.getMaxReserveSnail(), workerConfig.getPrioritySnail(),
        workerConfig.getMaxActiveChunksSnail(), memMan, slow+1, slowest, snailScanMaxMinutes);

    wpublish::QueriesAndChunks::Ptr queries =
        std::make_shared<wpublish::QueriesAndChunks>(std::chrono::minutes(5), std::chrono::minutes(5),
                maxTasksBootedPerUserQuery);
    wsched::BlendScheduler::Ptr blendSched = std::make_shared<wsched::BlendScheduler>("BlendSched", queries,
            maxThread, group, snail, scanSchedulers);
    queries->setBlendScheduler(blendSched);

    unsigned int requiredTasksCompleted = workerConfig.getRequiredTasksCompleted();
    queries->setRequiredTasksCompleted(requiredTasksCompleted);

    _foreman = std::make_shared<wcontrol::Foreman>(
            blendSched, poolSize, workerConfig.getMySqlConfig(), queries);
}

SsiService::~SsiService() {
    LOGS(_log, LOG_LVL_DEBUG, "SsiService dying.");
}

void SsiService::ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef) {
    LOGS(_log, LOG_LVL_DEBUG, "Got request call where rName is: " << resRef.rName);
    auto session = SsiSession::newSsiSession(resRef.rName, _chunkInventory->newValidator(), _foreman);

    // Continue execution in the session object as SSI gave us a new thread.
    // Object deletes itself when finished is called.
    //
    session->execute(reqRef);
}

void SsiService::_initInventory() {
    XrdName x;
    if (not _mySqlConfig.dbName.empty()) {
        LOGS(_log, LOG_LVL_FATAL, "dbName must be empty to prevent accidental context");
        throw std::runtime_error("dbName must be empty to prevent accidental context");
    }
    auto conn = std::make_shared<sql::SqlConnection>(_mySqlConfig, true);
    assert(conn);
    _chunkInventory = std::make_shared<wpublish::ChunkInventory>(x.getName(), conn);
    std::ostringstream os;
    os << "Paths exported: ";
    _chunkInventory->dbgPrint(os);
    LOGS(_log, LOG_LVL_DEBUG, os.str());
}


}}} // namespace
