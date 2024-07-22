// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 LSST Corporation.
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
 *
 * @brief Simple testing for class FifoScheduler
 *
 * @author Daniel L. Wang, SLAC
 */

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "proto/worker.pb.h"
#include "protojson/ScanTableInfo.h"
#include "util/Command.h"
#include "util/EventThread.h"
#include "wbase/FileChannelShared.h"
#include "wbase/Task.h"
#include "wbase/UberJobData.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/SqlConnMgr.h"
#include "wpublish/QueriesAndChunks.h"
#include "wsched/ChunkTasksQueue.h"
#include "wsched/BlendScheduler.h"
#include "wsched/FifoScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"

// Boost unit test header
#define BOOST_TEST_MODULE WorkerScheduler
#include "boost/test/unit_test.hpp"

namespace test = boost::test_tools;
namespace wsched = lsst::qserv::wsched;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.testSchedulers");
}

using namespace std;
using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::protojson::ScanInfo::Rating::FAST;
using lsst::qserv::protojson::ScanInfo::Rating::FASTEST;
using lsst::qserv::protojson::ScanInfo::Rating::MEDIUM;
using lsst::qserv::protojson::ScanInfo::Rating::SLOW;
using lsst::qserv::wbase::FileChannelShared;
using lsst::qserv::wbase::SendChannel;
using lsst::qserv::wbase::Task;
using lsst::qserv::wbase::UberJobData;
using lsst::qserv::wconfig::WorkerConfig;
using lsst::qserv::wcontrol::SqlConnMgr;
using lsst::qserv::wdb::ChunkResourceMgr;
using lsst::qserv::wpublish::QueriesAndChunks;

double const oneHr = 60.0;

bool const resetForTestingC = true;
int const maxBootedC = 5;
int const maxDarkTasksC = 25;

shared_ptr<ChunkResourceMgr> crm;  // not used in this test, required by Task::createTasks
MySqlConfig mySqlConfig;           // not used in this test, required by Task::createTasks
SqlConnMgr::Ptr sqlConnMgr;        // not used in this test, required by Task::createTasks

auto workerCfg = lsst::qserv::wconfig::WorkerConfig::create();

std::vector<FileChannelShared::Ptr> locSendSharedPtrs;

lsst::qserv::protojson::ScanInfo::Ptr makeScanInfoFastest() {
    auto info = lsst::qserv::protojson::ScanInfo::create();
    info->scanRating = FASTEST;
    int const sRating = FAST;
    string const db = "moose";
    string const table = "Object";
    bool const lockInMem = true;
    info->infoTables.emplace_back(db, table, lockInMem, sRating);
    return info;
}

lsst::qserv::protojson::ScanInfo::Ptr makeScanInfoFast(string const& slowestTableName = string("")) {
    auto info = lsst::qserv::protojson::ScanInfo::create();
    info->scanRating = FAST;
    int const sRating = FAST;
    string const db = "moose";
    string const table = "Object";
    bool const lockInMem = true;
    info->infoTables.emplace_back(db, table, lockInMem, sRating);
    info->sortTablesSlowestFirst();
    if (slowestTableName != string("")) {
        info->infoTables[0].table = slowestTableName;
    }
    return info;
}

lsst::qserv::protojson::ScanInfo::Ptr makeScanInfoMedium(string const& slowestTableName = string("")) {
    auto info = lsst::qserv::protojson::ScanInfo::create();
    info->scanRating = MEDIUM;
    string const db = "moose";
    bool const lockInMem = true;
    info->infoTables.emplace_back(db, "Object", lockInMem, FAST);
    info->infoTables.emplace_back(db, "Source", lockInMem, MEDIUM);
    info->sortTablesSlowestFirst();
    if (slowestTableName != string("")) {
        info->infoTables[0].table = slowestTableName;
    }
    return info;
}

lsst::qserv::protojson::ScanInfo::Ptr makeScanInfoSlow(string const& slowestTableName = string("")) {
    auto info = lsst::qserv::protojson::ScanInfo::create();
    info->scanRating = SLOW;
    string const db = "moose";
    bool const lockInMem = true;
    info->infoTables.emplace_back(db, "Object", lockInMem, FAST);
    info->infoTables.emplace_back(db, "Source", lockInMem, MEDIUM);
    info->infoTables.emplace_back(db, "ForcedSource", lockInMem, SLOW);
    info->sortTablesSlowestFirst();
    if (slowestTableName != string("")) {
        info->infoTables[0].table = slowestTableName;
    }
    return info;
}

UberJobData::Ptr makeUberJobData(uint64_t queryId,
                                 std::shared_ptr<lsst::qserv::protojson::ScanInfo> const& scanInfo,
                                 bool scanInteractive) {
    auto ujd = UberJobData::create(7,        // UberJobId
                                   "cz1",    // czarName
                                   11,       // czarId,
                                   "aHost",  // czarHost
                                   3333,     // czarPort
                                   queryId,
                                   0,           // rowLimit
                                   5000000000,  // maxTableSizeBytes
                                   scanInfo,
                                   scanInteractive,  //  scanInteractive
                                   "worker_13",      // workerId,
                                   nullptr,          // std::shared_ptr<wcontrol::Foreman> const& foreman
                                   "whatever"        // authKey
    );
    return ujd;
}

Task::Ptr makeTask(UberJobData::Ptr const& ujData, int jobId, int chunkId, int fragmentNumber,
                   size_t templateId, bool hasSubchunks, int subchunkId,
                   vector<lsst::qserv::wbase::TaskDbTbl> const& fragSubTables,
                   vector<int> const& fragSubchunkIds, shared_ptr<FileChannelShared> const& sc,
                   std::shared_ptr<lsst::qserv::wpublish::QueryStatistics> const& queryStats) {
    WorkerConfig::create();
    string const db = ujData->getScanInfo()->infoTables[0].db;
    int const attemptCount = 0;
    Task::Ptr task = shared_ptr<Task>(new Task(ujData, jobId, attemptCount, chunkId, fragmentNumber,
                                               templateId, hasSubchunks, subchunkId, db, fragSubTables,
                                               fragSubchunkIds, sc, queryStats));
    return task;
}
*/

struct SchedulerFixture {
    SchedulerFixture(void) { counter = 20; }
    ~SchedulerFixture(void) {}

    Task::Ptr makeUTask(int seq, int jobId, UberJobData::Ptr const& ujData,
                        shared_ptr<FileChannelShared> const& sc,
                        shared_ptr<QueriesAndChunks> const& queries) {
        ++counter;
        int const chunkId = seq;
        int const fragmentNumber = 0;
        size_t const templateId = 0;
        bool const hasSubchunks = false;
        int const subchunkId = 0;
        vector<lsst::qserv::wbase::TaskDbTbl> fragSubTables;
        vector<int> fragSubchunkIds;
        Task::Ptr t = makeTask(ujData, jobId, chunkId, fragmentNumber, templateId, hasSubchunks, subchunkId,
                               fragSubTables, fragSubchunkIds, sc, queries->getStats(ujData->getQueryId()));
        return t;
    }

    Task::Ptr queMsgWithChunkId(UberJobData::Ptr const& ujData, wsched::GroupScheduler& gs, int chunkId,
                                int jobId, shared_ptr<FileChannelShared> const& sc,
                                shared_ptr<QueriesAndChunks> const& queries) {
        int const fragmentNumber = 0;
        size_t const templateId = 0;
        bool const hasSubchunks = false;
        int const subchunkId = 0;
        vector<lsst::qserv::wbase::TaskDbTbl> fragSubTables;
        vector<int> fragSubchunkIds;
        Task::Ptr t = makeTask(ujData, jobId, chunkId, fragmentNumber, templateId, hasSubchunks, subchunkId,
                               fragSubTables, fragSubchunkIds, sc, queries->getStats(ujData->getQueryId()));
        gs.queCmd(t);
        return t;
    }
    */

    int counter;
};

BOOST_FIXTURE_TEST_SUITE(SchedulerSuite, SchedulerFixture)

/// examineAfter=0  Don't run _examineThread when 0
/// deadAfter=1     Consider queries dead if they finished more than 1 second ago.
lsst::qserv::wpublish::QueriesAndChunks::Ptr setupQueries(int maxTasksBooted, int maxDarkTasks,
                                                          bool resetForTesting, int deadAfter = 1,
                                                          int examineAfter = 0) {
    auto qac = lsst::qserv::wpublish::QueriesAndChunks::setupGlobal(
            chrono::seconds(deadAfter), chrono::seconds(examineAfter), maxTasksBooted, maxDarkTasks,
            resetForTesting);
    return qac;
}

struct SchedFixture {
    SchedFixture(double maxScanTimeFast, lsst::qserv::wpublish::QueriesAndChunks::Ptr const& queries_)
            : _maxScanTimeFast(maxScanTimeFast), queries(queries_) {
        setupQueriesBlend();
    }
    ~SchedFixture() {}

    void setupQueriesBlend() {
        blend = std::make_shared<wsched::BlendScheduler>("blendSched", queries, maxThreads, group, scanSlow,
                                                         scanSchedulers);
        group->setDefaultPosition(0);
        scanFast->setDefaultPosition(1);
        scanMed->setDefaultPosition(2);
        scanSlow->setDefaultPosition(3);
        queries->setBlendScheduler(blend);
        queries->setRequiredTasksCompleted(1);  // Make it easy to set a baseline.
    }

    lsst::qserv::QueryId qIdInc{1};
    int maxThreads{9};
    int maxActiveChunks{20};
    int priority{2};

private:
    double _maxScanTimeFast{oneHr};  ///< Don't hit time limit in tests.

public:
    wsched::GroupScheduler::Ptr group{
            std::make_shared<wsched::GroupScheduler>("GroupSched", maxThreads, 2, 3, priority++)};
    wsched::ScanScheduler::Ptr scanSlow{std::make_shared<wsched::ScanScheduler>(
            "ScanSlow", maxThreads, 2, priority++, maxActiveChunks, MEDIUM + 1, SLOW, oneHr)};
    wsched::ScanScheduler::Ptr scanMed{std::make_shared<wsched::ScanScheduler>(
            "ScanMed", maxThreads, 2, priority++, maxActiveChunks, FAST + 1, MEDIUM, oneHr)};
    wsched::ScanScheduler::Ptr scanFast{std::make_shared<wsched::ScanScheduler>(
            "ScanFast", maxThreads, 3, priority++, maxActiveChunks, FASTEST, FAST, _maxScanTimeFast)};
    std::vector<wsched::ScanScheduler::Ptr> scanSchedulers{scanFast, scanMed};

    lsst::qserv::wpublish::QueriesAndChunks::Ptr queries;
    wsched::BlendScheduler::Ptr blend;
};

void logCmd(lsst::qserv::util::Command::Ptr const& cmd, std::string const& note) {
    if (cmd == nullptr)
        LOGS(_log, LOG_LVL_WARN, note << " null");
    else
        LOGS(_log, LOG_LVL_WARN, note << ":" << cmd->dump());
}

// TODO: DM-33302 replace this test case
BOOST_AUTO_TEST_CASE(Grouping) {
#if 0   // &&& fix and re-enable
    SchedFixture f(60.0, 1);  // Values to keep QueriesAndChunk from triggering.

    LOGS(_log, LOG_LVL_DEBUG, "Test_case grouping");

    int const deadAfter = 1;
    int const examineAfter = 1;
    auto qac = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, deadAfter, examineAfter);
    SchedFixture fixt(60.0, qac);  // Values to keep QueriesAndChunk from triggering.

    // Test grouping by chunkId. Max entries added to a single group set to 3.
    wsched::GroupScheduler gs{"GroupSchedA", 100, 0, 3, 0};
    // chunk Ids
    int a = 50;
    int b = 11;
    int c = 75;
    int d = 4;

    BOOST_CHECK(gs.empty() == true);
    BOOST_CHECK(gs.ready() == false);

    lsst::qserv::QueryId qIdInc = 1;
    // Either FASTEST scan rating or scanInteractive = true should make the scan interactive.
    auto scanInfoFastest = makeScanInfoFastest();
    auto scanInfoFast = makeScanInfoFast();
    bool const scanInteractive = true;
    auto ujData_a1 = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    shared_ptr<FileChannelShared> sc = nullptr;
    Task::Ptr a1 = queMsgWithChunkId(ujData_a1, gs, a, 0, sc, fixt.queries);
    BOOST_CHECK(gs.empty() == false);
    BOOST_CHECK(gs.ready() == true);

    auto b1Ujd = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    Task::Ptr b1 = queMsgWithChunkId(b1Ujd, gs, b, 0, sc, fixt.queries);

    auto c1Ujd = makeUberJobData(qIdInc++, scanInfoFast, scanInteractive);
    Task::Ptr c1 = queMsgWithChunkId(c1Ujd, gs, c, 0, sc, fixt.queries);

    auto b2Ujd = makeUberJobData(qIdInc++, scanInfoFastest, false);
    Task::Ptr b2 = queMsgWithChunkId(b2Ujd, gs, b, 0, sc, fixt.queries);

    auto b3Ujd = makeUberJobData(qIdInc++, scanInfoFast, scanInteractive);
    Task::Ptr b3 = queMsgWithChunkId(b3Ujd, gs, b, 0, sc, fixt.queries);

    auto b4Ujd = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    Task::Ptr b4 = queMsgWithChunkId(b4Ujd, gs, b, 0, sc, fixt.queries);

    auto a2Ujd = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    Task::Ptr a2 = queMsgWithChunkId(a2Ujd, gs, a, 0, sc, fixt.queries);

    auto a3Ujd = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    Task::Ptr a3 = queMsgWithChunkId(a3Ujd, gs, a, 0, sc, fixt.queries);

    auto b5Ujd = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    Task::Ptr b5 = queMsgWithChunkId(b5Ujd, gs, b, 0, sc, fixt.queries);

    auto d1Ujd = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    Task::Ptr d1 = queMsgWithChunkId(d1Ujd, gs, d, 0, sc, fixt.queries);

    BOOST_CHECK(gs.getSize() == 5);
    BOOST_CHECK(gs.ready() == true);

    // Should get all the first 3 'a' commands in order
    auto aa1 = gs.getCmd(false);
    auto aa2 = gs.getCmd(false);
    auto a4Ujd = makeUberJobData(qIdInc++, scanInfoFastest, scanInteractive);
    Task::Ptr a4 = queMsgWithChunkId(a4Ujd, gs, a, 0, sc, fixt.queries);  // this should get its own group

    auto aa3 = gs.getCmd(false);
    BOOST_CHECK(a1.get() == aa1.get());
    BOOST_CHECK(a2.get() == aa2.get());
    BOOST_CHECK(a3.get() == aa3.get());
    BOOST_CHECK(gs.getInFlight() == 3);
    BOOST_CHECK(gs.ready() == true);

    // Should get the first 3 'b' commands in order
    auto bb1 = gs.getCmd(false);
    auto bb2 = gs.getCmd(false);
    auto bb3 = gs.getCmd(false);
    BOOST_CHECK(b1.get() == bb1.get());
    BOOST_CHECK(b2.get() == bb2.get());
    BOOST_CHECK(b3.get() == bb3.get());
    BOOST_CHECK(gs.getInFlight() == 6);
    BOOST_CHECK(gs.ready() == true);

    // Verify that commandFinish reduces in flight count.
    gs.commandFinish(a1);
    BOOST_CHECK(gs.getInFlight() == 5);

    // Should get the only 'c' command
    auto cc1 = gs.getCmd(false);
    BOOST_CHECK(c1.get() == cc1.get());
    BOOST_CHECK(gs.getInFlight() == 6);

    // Should get the last 2 'b' commands
    auto bb4 = gs.getCmd(false);
    auto bb5 = gs.getCmd(false);
    BOOST_CHECK(b4.get() == bb4.get());
    BOOST_CHECK(b5.get() == bb5.get());
    BOOST_CHECK(gs.getInFlight() == 8);
    BOOST_CHECK(gs.ready() == true);

    // Get the 'd' command
    auto dd1 = gs.getCmd(false);
    BOOST_CHECK(d1.get() == d1.get());
    BOOST_CHECK(gs.getInFlight() == 9);
    BOOST_CHECK(gs.ready() == true);

    // Get the last 'a' command
    auto aa4 = gs.getCmd(false);
    BOOST_CHECK(a4.get() == aa4.get());
    BOOST_CHECK(gs.getInFlight() == 10);
    BOOST_CHECK(gs.ready() == false);
    BOOST_CHECK(gs.empty() == true);
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(GroupMaxThread) {
#if 0   // &&& fix and re-enable
    // Test that maxThreads is meaningful.
    LOGS(_log, LOG_LVL_WARN, "Test_case GroupMaxThread");
    auto scanInfo = makeScanInfoFastest();
    bool const scanInteractive = true;
    shared_ptr<FileChannelShared> sc = nullptr;

    auto queries = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, 1, 300);
    wsched::GroupScheduler gs{"GroupSchedB", 3, 0, 100, 0};
    lsst::qserv::QueryId qIdInc = 1;

    int a = 42;
    auto a1Ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr a1 = queMsgWithChunkId(a1Ujd, gs, a, 0, sc, queries);

    auto a2Ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr a2 = queMsgWithChunkId(a2Ujd, gs, a, 0, sc, queries);

    auto a3Ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr a3 = queMsgWithChunkId(a3Ujd, gs, a, 0, sc, queries);

    auto a4Ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr a4 = queMsgWithChunkId(a4Ujd, gs, a, 0, sc, queries);

    BOOST_CHECK(gs.ready() == true);
    auto aa1 = gs.getCmd(false);
    BOOST_CHECK(a1.get() == aa1.get());

    BOOST_CHECK(gs.ready() == true);
    auto aa2 = gs.getCmd(false);
    BOOST_CHECK(a2.get() == aa2.get());

    BOOST_CHECK(gs.ready() == true);
    auto aa3 = gs.getCmd(false);
    BOOST_CHECK(a3.get() == aa3.get());
    BOOST_CHECK(gs.getInFlight() == 3);
    BOOST_CHECK(gs.ready() == false);

    gs.commandFinish(a3);
    BOOST_CHECK(gs.ready() == true);
    auto aa4 = gs.getCmd(false);
    BOOST_CHECK(a4.get() == aa4.get());
    BOOST_CHECK(gs.ready() == false);
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(ScanScheduleTest) {
#if 0   // &&& fix and re-enable
    LOGS(_log, LOG_LVL_DEBUG, "Test_case ScanScheduleTest");

    auto queries = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, 1, 300);
    wsched::ScanScheduler sched{"ScanSchedA", 2, 1, 0, 20, 0, 100, oneHr};
    auto scanInfo = makeScanInfoFast();
    bool const scanInteractive = true;
    shared_ptr<FileChannelShared> sc = nullptr;

    lsst::qserv::QueryId qIdInc = 1;
    // Test ready state as Tasks added and removed.
    BOOST_CHECK(sched.ready() == false);

    auto ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr a38 = makeUTask(38, 0, ujd, sc, queries);
    sched.queCmd(a38);
    // Calling read swaps active and pending heaps, putting a38 at the top of the active.
    BOOST_CHECK(sched.ready() == true);

    ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr a40 = makeUTask(40, 0, ujd, sc, queries);  // goes on active
    sched.queCmd(a40);

    ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr b41 = makeUTask(41, 0, ujd, sc, queries);  // goes on active
    sched.queCmd(b41);

    ujd = makeUberJobData(qIdInc++, scanInfo, scanInteractive);
    Task::Ptr a33 = makeUTask(33, 0, ujd, sc, queries);  // goes on pending.
    sched.queCmd(a33);

    BOOST_CHECK(sched.ready() == true);
    auto aa38 = sched.getCmd(false);
    BOOST_CHECK(aa38.get() == a38.get());
    BOOST_CHECK(sched.getInFlight() == 1);
    sched.commandStart(aa38);
    BOOST_CHECK(sched.getInFlight() == 1);
    BOOST_CHECK(sched.ready() == true);
    sched.commandFinish(aa38);
    BOOST_CHECK(sched.getInFlight() == 0);

    BOOST_CHECK(sched.ready() == true);
    auto tsk1 = sched.getCmd(false);
    BOOST_CHECK(sched.getInFlight() == 1);
    sched.commandStart(tsk1);
    BOOST_CHECK(sched.ready() == true);
    auto tsk2 = sched.getCmd(false);
    BOOST_CHECK(sched.getInFlight() == 2);
    sched.commandStart(tsk2);
    // Test max of 2 tasks running at a time
    BOOST_CHECK(sched.ready() == false);
    sched.commandFinish(tsk2);
    BOOST_CHECK(sched.getInFlight() == 1);
    BOOST_CHECK(sched.ready() == true);
    auto tsk3 = sched.getCmd(false);
    BOOST_CHECK(sched.getInFlight() == 2);
    BOOST_CHECK(sched.ready() == false);
    sched.commandStart(tsk3);
    sched.commandFinish(tsk3);
    BOOST_CHECK(sched.getInFlight() == 1);
    BOOST_CHECK(sched.ready() == false);
    sched.commandFinish(tsk1);
    BOOST_CHECK(sched.getInFlight() == 0);
    BOOST_CHECK(sched.ready() == false);
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(BlendScheduleTest) {
#if 0   // &&& fix and re-enable
    LOGS(_log, LOG_LVL_DEBUG, "Test_case BlendScheduleTest");
    // Test that space is appropriately reserved for each scheduler as Tasks are started and finished.
    // TODO: This needs to be evaluated and removed.
    // In this case, memMan->lock(..) always returns true (really HandleType::ISEMPTY).
    // ChunkIds matter as they control the order Tasks come off individual schedulers.
    int const deadAfter = 1;
    int const examineAfter = 1;
    auto qac = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, deadAfter, examineAfter);
    SchedFixture fixt(60.0, qac);  // Values to keep QueriesAndChunk from triggering.

    auto scanInfoFastest = makeScanInfoFastest();
    auto scanInfoFast = makeScanInfoFast();
    auto scanInfoMedium = makeScanInfoMedium();
    auto scanInfoSlow = makeScanInfoSlow();
    bool const scanInteractiveT = true;
    bool const scanInteractiveF = false;
    shared_ptr<FileChannelShared> sc = nullptr;

    BOOST_CHECK(fixt.blend->ready() == false);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 5);

    // Put one message on each scheduler except ScanFast, which gets 2.
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduleTest-1 add Tasks");
    auto ujd = makeUberJobData(fixt.qIdInc++, scanInfoFastest, scanInteractiveT);
    Task::Ptr g1 = makeUTask(40, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(g1);
    BOOST_CHECK(fixt.group->getSize() == 1);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoFast, scanInteractiveF);
    Task::Ptr sF1 = makeUTask(27, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sF1);
    BOOST_CHECK(fixt.scanFast->getSize() == 1);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoFast, scanInteractiveF);
    Task::Ptr sF2 = makeUTask(40, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sF2);
    BOOST_CHECK(fixt.scanFast->getSize() == 2);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoSlow, scanInteractiveF);
    Task::Ptr sS1 = makeUTask(34, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sS1);
    BOOST_CHECK(fixt.scanSlow->getSize() == 1);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoMedium, scanInteractiveF);
    Task::Ptr sM1 = makeUTask(31, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sM1);
    BOOST_CHECK(fixt.scanMed->getSize() == 1);
    BOOST_CHECK(fixt.blend->ready() == true);

    BOOST_CHECK(fixt.blend->getSize() == 5);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 5);

    // Start all the Tasks.
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduleTest-1 start all tasks");
    // Tasks should come out in order of scheduler priority.
    auto og1 = fixt.blend->getCmd(false);
    BOOST_CHECK(og1.get() == g1.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 4);
    auto osF1 = fixt.blend->getCmd(false);
    BOOST_CHECK(osF1.get() == sF1.get());  // sF1 has lower chunkId than sF2
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 3);
    auto osF2 = fixt.blend->getCmd(false);
    BOOST_CHECK(osF2.get() == sF2.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 2);
    auto osM1 = fixt.blend->getCmd(false);
    BOOST_CHECK(osM1.get() == sM1.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 1);
    auto osS1 = fixt.blend->getCmd(false);
    BOOST_CHECK(osS1.get() == sS1.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 0);
    BOOST_CHECK(fixt.blend->getSize() == 0);
    BOOST_CHECK(fixt.blend->ready() == false);

    // All threads should now be in use or reserved, should be able to start one
    // Task for each scheduler but second Task should remain on queue.
    ujd = makeUberJobData(fixt.qIdInc++, scanInfoFastest, scanInteractiveT);
    Task::Ptr g2 = makeUTask(41, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(g2);
    BOOST_CHECK(fixt.group->getSize() == 1);
    BOOST_CHECK(fixt.blend->getSize() == 1);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoFastest, scanInteractiveT);
    Task::Ptr g3 = makeUTask(12, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(g3);
    BOOST_CHECK(fixt.group->getSize() == 2);
    BOOST_CHECK(fixt.blend->getSize() == 2);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoFast, scanInteractiveF);
    Task::Ptr sF3 = makeUTask(70, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sF3);
    BOOST_CHECK(fixt.scanFast->getSize() == 1);
    BOOST_CHECK(fixt.blend->getSize() == 3);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoFast, scanInteractiveF);
    Task::Ptr sF4 = makeUTask(72, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sF4);
    BOOST_CHECK(fixt.scanFast->getSize() == 2);
    BOOST_CHECK(fixt.blend->getSize() == 4);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoMedium, scanInteractiveF);
    Task::Ptr sM2 = makeUTask(13, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sM2);
    BOOST_CHECK(fixt.scanMed->getSize() == 1);
    BOOST_CHECK(fixt.blend->getSize() == 5);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoMedium, scanInteractiveF);
    Task::Ptr sM3 = makeUTask(15, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sM3);
    BOOST_CHECK(fixt.scanMed->getSize() == 2);
    BOOST_CHECK(fixt.blend->getSize() == 6);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoSlow, scanInteractiveF);
    Task::Ptr sS2 = makeUTask(5, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sS2);
    BOOST_CHECK(fixt.scanSlow->getSize() == 1);
    BOOST_CHECK(fixt.blend->getSize() == 7);
    BOOST_CHECK(fixt.blend->ready() == true);

    ujd = makeUberJobData(fixt.qIdInc++, scanInfoSlow, scanInteractiveF);
    Task::Ptr sS3 = makeUTask(6, 0, ujd, sc, fixt.queries);
    fixt.blend->queCmd(sS3);
    BOOST_CHECK(fixt.scanSlow->getSize() == 2);
    BOOST_CHECK(fixt.blend->getSize() == 8);
    BOOST_CHECK(fixt.blend->ready() == true);

    // Expect 1 group, 1 fast, 1 medium, and 1 slow in that order
    auto og2 = fixt.blend->getCmd(false);
    BOOST_CHECK(og2.get() == g2.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 0);
    BOOST_CHECK(fixt.blend->ready() == true);
    auto osF3 = fixt.blend->getCmd(false);
    BOOST_CHECK(osF3.get() == sF3.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 0);
    BOOST_CHECK(fixt.blend->ready() == true);
    auto osM2 = fixt.blend->getCmd(false);
    BOOST_CHECK(osM2.get() == sM2.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 0);
    BOOST_CHECK(fixt.blend->ready() == true);
    auto osS2 = fixt.blend->getCmd(false);
    BOOST_CHECK(osS2.get() == sS2.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 0);
    BOOST_CHECK(fixt.blend->getSize() == 4);
    BOOST_CHECK(fixt.blend->ready() == false);  // all threads in use

    // Finishing a fast Task should allow the last fast Task to go.
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduleTest-1 call commandFinish");
    fixt.blend->commandFinish(osF3);
    auto osF4 = fixt.blend->getCmd(false);
    BOOST_CHECK(osF4.get() == sF4.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 0);
    BOOST_CHECK(fixt.blend->ready() == false);

    // Finishing 2 fast Tasks should allow a group Task to go.
    fixt.blend->commandFinish(osF1);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 0);
    fixt.blend->commandFinish(osF2);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 1);
    auto og3 = fixt.blend->getCmd(false);
    BOOST_CHECK(og3.get() == g3.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 1);
    BOOST_CHECK(fixt.blend->ready() == false);

    // Finishing the last fast Task should let a medium Task go.
    fixt.blend->commandFinish(osF4);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 2);
    auto osM3 = fixt.blend->getCmd(false);
    BOOST_CHECK(osM3.get() == sM3.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 2);
    BOOST_CHECK(fixt.blend->ready() == false);
    BOOST_CHECK(fixt.blend->getCmd(false) == nullptr);

    // Finishing a group Task should allow a slow Task to go (only remaining Task)
    BOOST_CHECK(fixt.blend->getSize() == 1);
    fixt.blend->commandFinish(og1);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 2);
    auto osS3 = fixt.blend->getCmd(false);
    BOOST_CHECK(osS3.get() == sS3.get());
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 2);
    BOOST_CHECK(fixt.blend->getSize() == 0);
    BOOST_CHECK(fixt.blend->ready() == false);

    // Close out all tasks and check counts.
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduleTest-1 close out all Tasks");
    fixt.blend->commandFinish(og2);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 2);
    BOOST_CHECK(fixt.blend->getInFlight() == 7);
    fixt.blend->commandFinish(og3);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 3);
    BOOST_CHECK(fixt.blend->getInFlight() == 6);
    fixt.blend->commandFinish(osM1);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 3);
    BOOST_CHECK(fixt.blend->getInFlight() == 5);
    fixt.blend->commandFinish(osM2);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 3);
    fixt.blend->commandFinish(osM3);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 4);
    fixt.blend->commandFinish(osS1);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 4);
    fixt.blend->commandFinish(osS2);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 4);
    fixt.blend->commandFinish(osS3);
    BOOST_CHECK(fixt.blend->calcAvailableTheads() == 5);
    BOOST_CHECK(fixt.blend->getInFlight() == 0);
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduleTest-1 done");
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(BlendScheduleThreadLimitingTest) {
#if 0   // &&& fix and re-enable
    LOGS(_log, LOG_LVL_DEBUG, "Test_case BlendScheduleThreadLimitingTest");
    int const deadAfter = 1;
    int const examineAfter = 1;
    auto qac = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, deadAfter, examineAfter);
    SchedFixture fixt(60.0, qac);  // Values to keep QueriesAndChunk from triggering.

    auto scanInfoFastest = makeScanInfoFastest();
    auto scanInfoFast = makeScanInfoFast();
    auto scanInfoMedium = makeScanInfoMedium();
    auto scanInfoSlow = makeScanInfoSlow();
    bool const scanInteractiveT = true;
    bool const scanInteractiveF = false;
    shared_ptr<FileChannelShared> sc = nullptr;

    // Test that only 6 threads can be started on a single ScanScheduler
    // This leaves 3 threads available, 1 for each other scheduler.
    BOOST_CHECK(fixt.blend->ready() == false);
    std::vector<Task::Ptr> scanTasks;
    for (int j = 0; j < 7; ++j) {
        auto ujd = makeUberJobData(fixt.qIdInc++, scanInfoMedium, scanInteractiveF);
        auto tsk = makeUTask(j, 0, ujd, sc, fixt.queries);
        fixt.blend->queCmd(tsk);
        if (j < 6) {
            BOOST_CHECK(fixt.blend->ready() == true);
            auto cmd = fixt.blend->getCmd(false);
            BOOST_CHECK(cmd != nullptr);
            auto task = std::dynamic_pointer_cast<lsst::qserv::wbase::Task>(cmd);
            scanTasks.push_back(task);
        }
        if (j == 6) {
            BOOST_CHECK(fixt.blend->ready() == false);
            BOOST_CHECK(fixt.blend->getCmd(false) == nullptr);
        }
    }
    {
        // Finishing one task should allow the 7th one to run.
        fixt.blend->commandFinish(scanTasks[0]);
        BOOST_CHECK(fixt.blend->ready() == true);
        auto cmd = fixt.blend->getCmd(false);
        BOOST_CHECK(cmd != nullptr);
        auto task = std::dynamic_pointer_cast<lsst::qserv::wbase::Task>(cmd);
        scanTasks.push_back(task);
    }
    // Finish all the scanTasks, scanTasks[0] is already finished.
    for (int j = 1; j < 7; ++j) fixt.blend->commandFinish(scanTasks[j]);
    BOOST_CHECK(fixt.blend->getInFlight() == 0);
    BOOST_CHECK(fixt.blend->ready() == false);

    // Test that only 6 threads can be started on a single GroupScheduler
    // This leaves 3 threads available, 1 for each other scheduler.
    std::vector<Task::Ptr> groupTasks;
    for (int j = 0; j < 7; ++j) {
        auto ujd = makeUberJobData(fixt.qIdInc++, scanInfoFastest, scanInteractiveT);
        auto tsk = makeUTask(j, 0, ujd, sc, fixt.queries);
        fixt.blend->queCmd(tsk);
        if (j < 6) {
            BOOST_CHECK(fixt.blend->ready() == true);
            auto cmd = fixt.blend->getCmd(false);
            BOOST_CHECK(cmd != nullptr);
            auto task = std::dynamic_pointer_cast<lsst::qserv::wbase::Task>(cmd);
            groupTasks.push_back(task);
        }
        if (j == 6) {
            BOOST_CHECK(fixt.blend->ready() == false);
            BOOST_CHECK(fixt.blend->getCmd(false) == nullptr);
        }
    }
    {
        // Finishing one task should allow the 7th one to run.
        fixt.blend->commandFinish(groupTasks[0]);
        BOOST_CHECK(fixt.blend->ready() == true);
        auto cmd = fixt.blend->getCmd(false);
        BOOST_CHECK(cmd != nullptr);
        auto task = std::dynamic_pointer_cast<lsst::qserv::wbase::Task>(cmd);
        groupTasks.push_back(task);
    }
    // Finish all the groupTasks, groupTasks[0] is already finished.
    for (int j = 1; j < 7; ++j) fixt.blend->commandFinish(groupTasks[j]);
    BOOST_CHECK(fixt.blend->getInFlight() == 0);
    BOOST_CHECK(fixt.blend->ready() == false);
    LOGS(_log, LOG_LVL_DEBUG, "BlendScheduleTest-2 done");
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(BlendScheduleQueryRemovalTest) {
#if 0   // &&& fix and re-enable
    // Test that space is appropriately reserved for each scheduler as Tasks are started and finished.
    // In this case, memMan->lock(..) always returns true (really HandleType::ISEMPTY).
    // ChunkIds matter as they control the order Tasks come off individual schedulers.
    int const deadAfter = 1;
    int const examineAfter = 1;
    auto qac = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, deadAfter, examineAfter);
    SchedFixture fixt(60.0, qac);  // Values to keep QueriesAndChunk from triggering.

    auto scanInfoFastest = makeScanInfoFastest();
    auto scanInfoFast = makeScanInfoFast();
    auto scanInfoMedium = makeScanInfoMedium();
    auto scanInfoSlow = makeScanInfoSlow();
    bool const scanInteractiveF = false;
    shared_ptr<FileChannelShared> sc = nullptr;

    LOGS(_log, LOG_LVL_DEBUG, "Test_case BlendScheduleQueryRemovalTest");
    // Add two queries to scanFast scheduler and then move one query to scanSlow.
    int startChunk = 70;
    unsigned int jobs = 11;
    unsigned int jobsA = jobs;
    unsigned int jobsB = jobs;
    std::vector<Task::Ptr> queryATasks;
    std::vector<Task::Ptr> queryBTasks;
    lsst::qserv::QueryId qIdA = fixt.qIdInc++;
    lsst::qserv::QueryId qIdB = fixt.qIdInc++;
    {
        int jobId = 0;
        int chunkId = startChunk;
        auto ujdA = makeUberJobData(qIdA, scanInfoFast, scanInteractiveF);
        auto ujdB = makeUberJobData(qIdB, scanInfoFast, scanInteractiveF);
        for (unsigned int j = 0; j < jobs; ++j) {
            Task::Ptr mv = makeUTask(chunkId, jobId, ujdA, sc, fixt.queries);
            queryATasks.push_back(mv);
            fixt.queries->addTask(mv);
            fixt.blend->queCmd(mv);
            mv = makeUTask(chunkId, jobId, ujdB, sc, fixt.queries);
            queryBTasks.push_back(mv);
            fixt.queries->addTask(mv);
            fixt.blend->queCmd(mv);
            chunkId++;
            jobId++;
        }
    }
    BOOST_CHECK(fixt.scanFast->getSize() == jobs * 2);
    BOOST_CHECK(fixt.scanSlow->getSize() == 0);

    // This should run 1 job from one of the queries, but there are no guarantees about which one.
    // This is to test that moveUserQuery behaves appropriately for running Tasks.
    auto poppedTask = fixt.blend->getCmd(false);
    bool poppedFromA = false;
    for (auto const& tk : queryATasks) {
        if (tk == poppedTask) {
            poppedFromA = true;
            break;
        }
    }
    if (poppedFromA)
        --jobsA;
    else
        --jobsB;

    fixt.blend->moveUserQuery(qIdA, fixt.scanFast, fixt.scanSlow);  // move query qIdA to scanSlow.
    LOGS(_log, LOG_LVL_DEBUG,
         "fastSize=" << fixt.scanFast->getSize() << " slowSize=" << fixt.scanSlow->getSize());
    BOOST_CHECK(fixt.scanFast->getSize() == jobsB);
    BOOST_CHECK(fixt.scanSlow->getSize() == jobsA);
    // Can't use queryATasks[0] for this as it was popped from the queue before the move.
    auto taskFromA = queryATasks[1];
    auto schedForA = std::dynamic_pointer_cast<wsched::ScanScheduler>(taskFromA->getTaskScheduler());
    LOGS(_log, LOG_LVL_DEBUG, "taskFromA=" << taskFromA->getIdStr() << " sched=" << schedForA->getName());
    BOOST_CHECK(schedForA == fixt.scanSlow);
}

BOOST_AUTO_TEST_CASE(BlendScheduleQueryBootTaskTest) {
#if 0   // &&& fix and re-enable
    // Test if a task is removed if it takes takes too long.
    // Give the user query 0.1 seconds to run and run it for a second, it should get removed.
    double tenthOfSecInMinutes = 1.0 / 600.0;  // task
    int const deadAfter = 1;
    int const examineAfter = 1;
    auto qac = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, deadAfter, examineAfter);
    SchedFixture fixt(tenthOfSecInMinutes, qac);  // Values to keep QueriesAndChunk from triggering.

    auto scanInfoFastest = makeScanInfoFastest();
    auto scanInfoFast = makeScanInfoFast();
    auto scanInfoMedium = makeScanInfoMedium();
    auto scanInfoSlow = makeScanInfoSlow();
    bool const scanInteractiveF = false;
    shared_ptr<FileChannelShared> sc = nullptr;
    LOGS(_log, LOG_LVL_DEBUG, "Test_case BlendScheduleQueryBootTaskTest");

    // Create a thread pool to run task
    auto pool = lsst::qserv::util::ThreadPool::newThreadPool(20, 1000, fixt.blend);

    // Create fake data - one query to get a baseline time, another to take too long.
    // IMPORTANT: the "fast" taskl is needed to establish the baseline in QueriesAndChunks.
    // Otherwise the next task (the one which is going to be booted from its scheduler)
    // won't be booted.
    int const qidA = 5;
    int const qidB = 6;
    auto ujd = makeUberJobData(qidA, scanInfoFast, scanInteractiveF);
    Task::Ptr task = makeUTask(27, 0, ujd, sc, fixt.queries);
    std::atomic<bool> running{false};
    auto fastFunc = [&running, &task, queriesAndChunks = fixt.queries](lsst::qserv::util::CmdData*) {
        queriesAndChunks->startedTask(task);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        queriesAndChunks->finishedTask(task);
        running = true;
    };
    task->setUnitTest(fastFunc);
    fixt.queries->addTask(task);
    fixt.blend->queCmd(task);
    while (!running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    running = false;
    // fixt.queries should now have a baseline for chunk 27.
    LOGS(_log, LOG_LVL_DEBUG, "Chunks after fastFunc " << *fixt.queries);

    ujd = makeUberJobData(qidB, scanInfoFast, scanInteractiveF);
    task = makeUTask(27, 0, ujd, sc, fixt.queries);
    std::atomic<bool> slowSleepDone{false};
    auto slowFunc = [&running, &slowSleepDone, &task,
                     queriesAndChunks = fixt.queries](lsst::qserv::util::CmdData*) {
        queriesAndChunks->startedTask(task);
        running = true;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        slowSleepDone = true;
        // Keep this thread running until told to stop.
        while (running) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        queriesAndChunks->finishedTask(task);
        LOGS(_log, LOG_LVL_DEBUG, "slowFunc end");
    };
    task->setUnitTest(slowFunc);
    fixt.queries->addTask(task);
    auto queryStats = fixt.queries->getStats(qidA);
    BOOST_CHECK(queryStats != nullptr);
    if (queryStats != nullptr) {
        BOOST_CHECK(queryStats->getTasksBooted() == 0);
    }
    fixt.blend->queCmd(task);
    // Wait for slowFunc to start running then wait for slowFunc to finish sleeping.
    while (!running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    while (!slowSleepDone) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // By now the slowFunc query has taken a second, far longer than the 0.1 seconds it was allowed.
    // examineAll() should boot the query.
    LOGS(_log, LOG_LVL_INFO, "Chunks after slowFunc " << *fixt.queries);
    fixt.queries->examineAll();
    running = false;  // allow slowFunc to exit its loop and finish.
    LOGS(_log, LOG_LVL_INFO, "Chunks after examineAll " << *fixt.queries);

    // Check if the tasks booted value for qid has gone up.
    queryStats = fixt.queries->getStats(qidB);
    BOOST_CHECK(queryStats != nullptr);
    if (queryStats != nullptr) {
        LOGS(_log, LOG_LVL_INFO, "taskBooted=" << queryStats->getTasksBooted());
        BOOST_CHECK(queryStats->getTasksBooted() == 1);
    }

    LOGS(_log, LOG_LVL_INFO, "BlendScheduleQueryBootTaskTest waiting for pool to finish.");
    pool->shutdownPool();
    LOGS(_log, LOG_LVL_INFO, "BlendScheduleQueryBootTaskTest done");
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(SlowTableHeapTest) {
    LOGS(_log, LOG_LVL_DEBUG, "Test_case SlowTableHeapTest start, see ScanInfo::compareTables");

    int const deadAfter = 1;
    int const examineAfter = 1;
    auto qac = setupQueries(maxBootedC, maxDarkTasksC, resetForTestingC, deadAfter, examineAfter);
    SchedFixture fixt(60.0, qac);  // Values to keep QueriesAndChunk from triggering.

    bool const scanInteractiveF = false;
    shared_ptr<FileChannelShared> sc = nullptr;
#if 0   // &&& fix and re-enable
    LOGS(_log, LOG_LVL_DEBUG, "Test_case SlowTableHeapTest start");
    auto queries = QueriesAndChunks::setupGlobal(chrono::seconds(1), chrono::seconds(300), maxBootedC,
                                                 maxDarkTasksC, resetForTestingC);
    wsched::ChunkTasks::SlowTableHeap heap{};
    lsst::qserv::QueryId qIdInc = 1;

    BOOST_CHECK(heap.empty() == true);

    auto scanI = makeScanInfoMedium("charlie");
    auto ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a1 = makeUTask(7, 0, ujd, sc, fixt.queries);
    heap.push(a1);
    BOOST_CHECK(heap.top().get() == a1.get());
    BOOST_CHECK(heap.empty() == false);

    scanI = makeScanInfoMedium("delta");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a2 = makeUTask(7, 0, ujd, sc, fixt.queries);
    heap.push(a2);
    auto hTop = heap.top();
    logCmd(hTop, "hTop a2");
    BOOST_CHECK(hTop.get() == a2.get());

    scanI = makeScanInfoSlow("bravo");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a3 = makeUTask(7, 0, ujd, sc, fixt.queries);
    heap.push(a3);
    hTop = heap.top();
    logCmd(hTop, "hTop a3 first");
    BOOST_CHECK(heap.top().get() == a3.get());

    scanI = makeScanInfoFast("alpha");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a4 = makeUTask(7, 0, ujd, sc, fixt.queries);
    heap.push(a4);
    hTop = heap.top();
    logCmd(hTop, "hTop a3 second");
    BOOST_CHECK(heap.top().get() == a3.get());
    BOOST_CHECK(heap.size() == 4);

    auto hPop = heap.pop();
    logCmd(hPop, "hPop expect a3 bravo");
    BOOST_CHECK(hPop.get() == a3.get());

    hPop = heap.pop();
    logCmd(hPop, "hPop expect a2 delta");
    BOOST_CHECK(hPop.get() == a2.get());

    hPop = heap.pop();
    logCmd(hPop, "hPop expect a1 charlie");
    BOOST_CHECK(hPop.get() == a1.get());

    hPop = heap.pop();
    logCmd(hPop, "hPop expect a4 alpha");
    BOOST_CHECK(hPop.get() == a4.get());
    BOOST_CHECK(heap.empty() == true);
    LOGS(_log, LOG_LVL_DEBUG, "SlowTableHeapTest done");
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(ChunkTasksTest) {
#if 0   // &&& fix and re-enable
    LOGS(_log, LOG_LVL_DEBUG, "Test_case ChunkTasksTest start");
    auto queries = QueriesAndChunks::setupGlobal(chrono::seconds(1), chrono::seconds(300), maxBootedC,
                                                 maxDarkTasksC, resetForTestingC);
    int chunkId = 7;
    wsched::ChunkTasks chunkTasks{chunkId};
    lsst::qserv::QueryId qIdInc = 1;

    BOOST_CHECK(chunkTasks.empty() == true);
    BOOST_CHECK(chunkTasks.readyToAdvance() == true);

    auto scanI = makeScanInfoMedium("charlie");
    auto ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a1 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    chunkTasks.queTask(a1);
    BOOST_CHECK(chunkTasks.empty() == false);
    BOOST_CHECK(chunkTasks.readyToAdvance() == false);
    BOOST_CHECK(chunkTasks.size() == 1);

    scanI = makeScanInfoMedium("delta");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a2 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    chunkTasks.queTask(a2);
    BOOST_CHECK(chunkTasks.size() == 2);

    scanI = makeScanInfoSlow("bravo");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a3 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    chunkTasks.queTask(a3);
    BOOST_CHECK(chunkTasks.size() == 3);

    scanI = makeScanInfoFast("alpha");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a4 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    chunkTasks.queTask(a4);
    BOOST_CHECK(chunkTasks.size() == 4);

    BOOST_CHECK(chunkTasks.getTask(true).get() == a3.get());
    BOOST_CHECK(chunkTasks.getTask(true).get() == a2.get());
    BOOST_CHECK(chunkTasks.getTask(true).get() == a1.get());
    BOOST_CHECK(chunkTasks.getTask(true).get() == a4.get());
    chunkTasks.taskComplete(a1);
    chunkTasks.taskComplete(a1);  // duplicate should not cause problems.
    chunkTasks.taskComplete(a2);
    chunkTasks.taskComplete(a4);
    BOOST_CHECK(chunkTasks.empty() == true);
    BOOST_CHECK(chunkTasks.readyToAdvance() == false);
    chunkTasks.taskComplete(a3);
    BOOST_CHECK(chunkTasks.readyToAdvance() == true);

    chunkTasks.setActive();
    chunkTasks.queTask(a3);
    chunkTasks.queTask(a4);
    chunkTasks.queTask(a2);
    chunkTasks.queTask(a1);
    BOOST_CHECK(chunkTasks.readyToAdvance() == true);
    BOOST_CHECK(chunkTasks.empty() == false);
    BOOST_CHECK(chunkTasks.size() == 4);

    // move tasks from pending to active
    chunkTasks.setActive(false);
    BOOST_CHECK(chunkTasks.readyToAdvance() == false);
    BOOST_CHECK(chunkTasks.empty() == false);
    BOOST_CHECK(chunkTasks.size() == 4);

    BOOST_CHECK(chunkTasks.getTask(true).get() == a3.get());
    BOOST_CHECK(chunkTasks.getTask(true).get() == a2.get());
    BOOST_CHECK(chunkTasks.getTask(true).get() == a1.get());
    BOOST_CHECK(chunkTasks.getTask(true).get() == a4.get());
    BOOST_CHECK(chunkTasks.empty() == true);
    BOOST_CHECK(chunkTasks.readyToAdvance() == false);
    chunkTasks.taskComplete(a1);
    chunkTasks.taskComplete(a2);
    chunkTasks.taskComplete(a3);
    chunkTasks.taskComplete(a4);
    BOOST_CHECK(chunkTasks.readyToAdvance() == true);
    LOGS(_log, LOG_LVL_DEBUG, "ChunkTasksTest done");
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_CASE(ChunkTasksQueueTest) {
#if 0   // &&& fix and re-enable
    LOGS(_log, LOG_LVL_DEBUG, "Test_case ChunkTasksQueueTest start");
    auto queries = QueriesAndChunks::setupGlobal(chrono::seconds(1), chrono::seconds(300), maxBootedC,
                                                 maxDarkTasksC, resetForTestingC);
    int firstChunkId = 100;
    int secondChunkId = 150;
    int chunkId = firstChunkId;
    wsched::ChunkTasksQueue ctl{nullptr};
    lsst::qserv::QueryId qIdInc = 1;

    BOOST_CHECK(ctl.empty() == true);
    BOOST_CHECK(ctl.ready(true) == false);

    auto scanI = makeScanInfoMedium("charlie");
    auto ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a1 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(a1);
    BOOST_CHECK(ctl.empty() == false);

    scanI = makeScanInfoMedium("delta");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a2 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(a2);

    scanI = makeScanInfoSlow("bravo");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a3 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(a3);

    scanI = makeScanInfoFast("alpha");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr a4 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(a4);

    BOOST_CHECK(ctl.ready(true) == true);
    BOOST_CHECK(ctl.getTask(true).get() == a3.get());
    BOOST_CHECK(ctl.getTask(true).get() == a2.get());
    BOOST_CHECK(ctl.getTask(true).get() == a1.get());
    BOOST_CHECK(ctl.getTask(true).get() == a4.get());
    BOOST_CHECK(ctl.ready(true) == false);
    BOOST_CHECK(ctl.empty() == false);
    ctl.taskComplete(a1);
    ctl.taskComplete(a2);
    ctl.taskComplete(a3);
    ctl.taskComplete(a4);
    BOOST_CHECK(ctl.ready(true) == false);
    BOOST_CHECK(ctl.empty() == true);

    chunkId = secondChunkId;
    scanI = makeScanInfoMedium("c");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr b1 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(b1);
    BOOST_CHECK(ctl.empty() == false);

    scanI = makeScanInfoMedium("d");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr b2 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(b2);

    scanI = makeScanInfoSlow("b");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr b3 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(b3);

    scanI = makeScanInfoFast("a");
    ujd = makeUberJobData(qIdInc++, scanI, scanInteractiveF);
    Task::Ptr b4 = makeUTask(chunkId, 0, ujd, sc, fixt.queries);
    ctl.queueTask(b4);
    ctl.queueTask(a3);
    ctl.queueTask(a4);
    ctl.queueTask(a2);
    ctl.queueTask(a1);

    BOOST_CHECK(ctl.ready(true) == true);
    BOOST_CHECK(ctl.getTask(true).get() == a3.get());
    BOOST_CHECK(ctl.getTask(true).get() == a2.get());
    BOOST_CHECK(ctl.getTask(true).get() == a1.get());
    BOOST_CHECK(ctl.getTask(true).get() == a4.get());
    BOOST_CHECK(ctl.getTask(true).get() == b3.get());
    BOOST_CHECK(ctl.getTask(true).get() == b2.get());
    ctl.taskComplete(a1);
    ctl.taskComplete(a2);
    ctl.taskComplete(a3);
    ctl.taskComplete(a4);
    BOOST_CHECK(ctl.getTask(true).get() == b1.get());
    BOOST_CHECK(ctl.ready(true) == true);
    BOOST_CHECK(ctl.getTask(true).get() == b4.get());
    BOOST_CHECK(ctl.empty() == false);
    BOOST_CHECK(ctl.ready(true) == false);
    ctl.taskComplete(b1);
    ctl.taskComplete(b2);
    ctl.taskComplete(b3);
    ctl.taskComplete(b4);
    BOOST_CHECK(ctl.empty() == false);
    BOOST_CHECK(ctl.ready(true) == false);
    BOOST_CHECK(ctl.empty() == true);

    // test wrap around and pending
    ctl.queueTask(b1);
    ctl.queueTask(b2);
    BOOST_CHECK(ctl.getActiveChunkId() == -1);
    BOOST_CHECK(ctl.getTask(true).get() == b2.get());
    BOOST_CHECK(ctl.getActiveChunkId() == secondChunkId);
    ctl.queueTask(a1);
    ctl.queueTask(a2);
    ctl.queueTask(a3);
    ctl.queueTask(b3);  // test pendingTasks
    ctl.queueTask(b4);
    ctl.queueTask(a4);
    BOOST_CHECK(ctl.getTask(true).get() == b1.get());
    BOOST_CHECK(ctl.getActiveChunkId() == secondChunkId);
    BOOST_CHECK(ctl.getTask(true).get() == a3.get());
    BOOST_CHECK(ctl.getTask(true).get() == a2.get());
    BOOST_CHECK(ctl.getTask(true).get() == a1.get());
    BOOST_CHECK(ctl.getTask(true).get() == a4.get());
    BOOST_CHECK(ctl.ready(true) == false);
    BOOST_CHECK(ctl.getActiveChunkId() == secondChunkId);
    ctl.taskComplete(b1);
    ctl.taskComplete(b2);
    BOOST_CHECK(ctl.getActiveChunkId() == secondChunkId);
    BOOST_CHECK(ctl.getTask(true).get() == b3.get());
    BOOST_CHECK(ctl.getActiveChunkId() == firstChunkId);
    ctl.taskComplete(a1);
    ctl.taskComplete(a2);
    ctl.taskComplete(a3);
    ctl.taskComplete(a4);
    BOOST_CHECK(ctl.getTask(true).get() == b4.get());
    BOOST_CHECK(ctl.getActiveChunkId() == secondChunkId);
    ctl.taskComplete(b3);
    ctl.taskComplete(b4);
    BOOST_CHECK(ctl.ready(true) == false);
    BOOST_CHECK(ctl.getActiveChunkId() == -1);
    LOGS(_log, LOG_LVL_DEBUG, "ChunkTasksQueueTest done");
#endif  // &&& fix and re-enable
}

BOOST_AUTO_TEST_SUITE_END()
