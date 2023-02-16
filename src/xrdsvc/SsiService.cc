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
#include <xrdsvc/SsiRequest.h>

// Third-party headers
#include "XrdSsi/XrdSsiLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "memman/MemMan.h"
#include "memman/MemManNone.h"
#include "mysql/MySqlConnection.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "util/FileMonitor.h"
#include "wbase/Base.h"
#include "wconfig/WorkerConfig.h"
#include "wconfig/WorkerConfigError.h"
#include "wcontrol/Foreman.h"
#include "wcontrol/SqlConnMgr.h"
#include "wcontrol/TransmitMgr.h"
#include "wpublish/ChunkInventory.h"
#include "wsched/BlendScheduler.h"
#include "wsched/FifoScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"
#include "xrdsvc/XrdName.h"

using namespace std;

class XrdPosixCallBack;  // Forward.

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.SsiService");

// add LWP to MDC in log messages
void initMDC() { LOG_MDC("LWP", to_string(lsst::log::lwpID())); }
int dummyInitMDC = LOG_MDC_INIT(initMDC);

}  // namespace

namespace lsst::qserv::xrdsvc {

SsiService::SsiService(XrdSsiLogger* log, wconfig::WorkerConfig const& workerConfig)
        : _mySqlConfig(workerConfig.getMySqlConfig()) {
    LOGS(_log, LOG_LVL_DEBUG, "SsiService starting...");

    if (not mysql::MySqlConnection::checkConnection(_mySqlConfig)) {
        LOGS(_log, LOG_LVL_FATAL, "Unable to connect to MySQL using configuration:" << _mySqlConfig);
        throw wconfig::WorkerConfigError("Unable to connect to MySQL");
    }
    _initInventory();

    string cfgMemMan = workerConfig.getMemManClass();
    memman::MemMan::Ptr memMan;
    if (cfgMemMan == "MemManReal") {
        // Default to 1 gigabyte
        uint64_t memManSize = workerConfig.getMemManSizeMb() * 1000000;
        LOGS(_log, LOG_LVL_DEBUG,
             "Using MemManReal with memManSizeMb=" << workerConfig.getMemManSizeMb()
                                                   << " location=" << workerConfig.getMemManLocation());
        memMan = shared_ptr<memman::MemMan>(
                memman::MemMan::create(memManSize, workerConfig.getMemManLocation()));
    } else if (cfgMemMan == "MemManNone") {
        memMan = make_shared<memman::MemManNone>(1, false);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "Unrecognized memory manager " << cfgMemMan);
        throw wconfig::WorkerConfigError("Unrecognized memory manager.");
    }

    int64_t bufferMaxTotalBytes = workerConfig.getBufferMaxTotalGB() * 1'000'000'000LL;
    StreamBuffer::setMaxTotalBytes(bufferMaxTotalBytes);

    // Set thread pool size.
    unsigned int poolSize = max(workerConfig.getThreadPoolSize(), thread::hardware_concurrency());
    unsigned int maxPoolThreads = max(workerConfig.getMaxPoolThreads(), poolSize);

    // poolSize should be greater than either GroupScheduler::maxThreads or ScanScheduler::maxThreads
    unsigned int maxThread = poolSize;
    int maxReserve = 2;
    auto group = make_shared<wsched::GroupScheduler>("SchedGroup", maxThread, maxReserve,
                                                     workerConfig.getMaxGroupSize(),
                                                     wsched::SchedulerBase::getMaxPriority());

    int const fastest = lsst::qserv::proto::ScanInfo::Rating::FASTEST;
    int const fast = lsst::qserv::proto::ScanInfo::Rating::FAST;
    int const medium = lsst::qserv::proto::ScanInfo::Rating::MEDIUM;
    int const slow = lsst::qserv::proto::ScanInfo::Rating::SLOW;
    int const slowest = lsst::qserv::proto::ScanInfo::Rating::SLOWEST;
    double fastScanMaxMinutes = (double)workerConfig.getScanMaxMinutesFast();
    double medScanMaxMinutes = (double)workerConfig.getScanMaxMinutesMed();
    double slowScanMaxMinutes = (double)workerConfig.getScanMaxMinutesSlow();
    double snailScanMaxMinutes = (double)workerConfig.getScanMaxMinutesSnail();
    int maxTasksBootedPerUserQuery = workerConfig.getMaxTasksBootedPerUserQuery();
    vector<wsched::ScanScheduler::Ptr> scanSchedulers{
            make_shared<wsched::ScanScheduler>(
                    "SchedSlow", maxThread, workerConfig.getMaxReserveSlow(), workerConfig.getPrioritySlow(),
                    workerConfig.getMaxActiveChunksSlow(), memMan, medium + 1, slow, slowScanMaxMinutes),
            make_shared<wsched::ScanScheduler>(
                    "SchedFast", maxThread, workerConfig.getMaxReserveFast(), workerConfig.getPriorityFast(),
                    workerConfig.getMaxActiveChunksFast(), memMan, fastest, fast, fastScanMaxMinutes),
            make_shared<wsched::ScanScheduler>(
                    "SchedMed", maxThread, workerConfig.getMaxReserveMed(), workerConfig.getPriorityMed(),
                    workerConfig.getMaxActiveChunksMed(), memMan, fast + 1, medium, medScanMaxMinutes),
    };

    auto snail = make_shared<wsched::ScanScheduler>(
            "SchedSnail", maxThread, workerConfig.getMaxReserveSnail(), workerConfig.getPrioritySnail(),
            workerConfig.getMaxActiveChunksSnail(), memMan, slow + 1, slowest, snailScanMaxMinutes);

    wpublish::QueriesAndChunks::Ptr queries = wpublish::QueriesAndChunks::setupGlobal(
            chrono::minutes(5), chrono::minutes(5), maxTasksBootedPerUserQuery);
    wsched::BlendScheduler::Ptr blendSched = make_shared<wsched::BlendScheduler>(
            "BlendSched", queries, maxThread, group, snail, scanSchedulers);
    blendSched->setPrioritizeByInFlight(false);  // TODO: set in configuration file.
    queries->setBlendScheduler(blendSched);

    unsigned int requiredTasksCompleted = workerConfig.getRequiredTasksCompleted();
    queries->setRequiredTasksCompleted(requiredTasksCompleted);

    int const maxSqlConn = workerConfig.getMaxSqlConnections();
    int const resvInteractiveSqlConn = workerConfig.getReservedInteractiveSqlConnections();
    auto sqlConnMgr = make_shared<wcontrol::SqlConnMgr>(maxSqlConn, maxSqlConn - resvInteractiveSqlConn);
    LOGS(_log, LOG_LVL_WARN, "config sqlConnMgr" << *sqlConnMgr);

    int const maxTransmits = workerConfig.getMaxTransmits();
    int const maxPerQid = workerConfig.getMaxPerQid();
    _transmitMgr = make_shared<wcontrol::TransmitMgr>(maxTransmits, maxPerQid);
    LOGS(_log, LOG_LVL_WARN, "config transmitMgr" << *_transmitMgr);
    LOGS(_log, LOG_LVL_WARN, "maxPoolThreads=" << maxPoolThreads);

    _foreman = make_shared<wcontrol::Foreman>(blendSched, poolSize, maxPoolThreads,
                                              workerConfig.getMySqlConfig(), queries, sqlConnMgr);

    // Watch to see if the log configuration is changed.
    // If LSST_LOG_CONFIG is not defined, there's no good way to know what log
    // configuration file is in use.
    string logConfigFile = std::getenv("LSST_LOG_CONFIG");
    if (logConfigFile == "") {
        LOGS(_log, LOG_LVL_ERROR,
             "FileMonitor LSST_LOG_CONFIG was blank, no log configuration file to watch.");
    } else {
        LOGS(_log, LOG_LVL_ERROR, "logConfigFile=" << logConfigFile);
        _logFileMonitor = make_shared<util::FileMonitor>(logConfigFile);
    }
}

SsiService::~SsiService() { LOGS(_log, LOG_LVL_DEBUG, "SsiService dying."); }

void SsiService::ProcessRequest(XrdSsiRequest& reqRef, XrdSsiResource& resRef) {
    LOGS(_log, LOG_LVL_DEBUG, "Got request call where rName is: " << resRef.rName);
    auto request =
            SsiRequest::newSsiRequest(resRef.rName, _chunkInventory, _foreman, _mySqlConfig, _transmitMgr);

    // Continue execution in the session object as SSI gave us a new thread.
    // Object deletes itself when finished is called.
    //
    request->execute(reqRef);
}

void SsiService::_initInventory() {
    XrdName x;
    if (not _mySqlConfig.dbName.empty()) {
        LOGS(_log, LOG_LVL_FATAL, "dbName must be empty to prevent accidental context");
        throw runtime_error("dbName must be empty to prevent accidental context");
    }
    auto conn = sql::SqlConnectionFactory::make(_mySqlConfig);
    assert(conn);
    _chunkInventory = make_shared<wpublish::ChunkInventory>(x.getName(), conn);
    ostringstream os;
    os << "Paths exported: ";
    _chunkInventory->dbgPrint(os);
    LOGS(_log, LOG_LVL_DEBUG, os.str());
}

}  // namespace lsst::qserv::xrdsvc
