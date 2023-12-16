// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
 * @brief Simple testing for class QueryRunner
 * Requires some setup, and assumes some access to a mysqld
 *
 * @author Daniel L. Wang, SLAC
 */

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "proto/worker.pb.h"
#include "proto/ProtoImporter.h"
#include "util/StringHash.h"
#include "wbase/FileChannelShared.h"
#include "wbase/Task.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/SqlConnMgr.h"
#include "wcontrol/TransmitMgr.h"
#include "wdb/ChunkResource.h"
#include "wdb/QueryRunner.h"
#include "wpublish/QueriesAndChunks.h"

// Boost unit test header
#define BOOST_TEST_MODULE QueryRunner
#include <boost/test/unit_test.hpp>

using namespace std;

namespace test = boost::test_tools;
namespace gio = google::protobuf::io;
namespace util = lsst::qserv::util;

using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::mysql::MySqlConnection;

using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::proto::Result;
using lsst::qserv::proto::TaskMsg;
using lsst::qserv::proto::TaskMsg_Fragment;
using lsst::qserv::proto::TaskMsg_Subchunk;

using lsst::qserv::wbase::FileChannelShared;
using lsst::qserv::wbase::SendChannel;
using lsst::qserv::wbase::Task;
using lsst::qserv::wconfig::WorkerConfig;
using lsst::qserv::wcontrol::SqlConnMgr;
using lsst::qserv::wcontrol::TransmitMgr;
using lsst::qserv::wdb::ChunkResource;
using lsst::qserv::wdb::ChunkResourceMgr;
using lsst::qserv::wdb::FakeBackend;
using lsst::qserv::wdb::QueryRunner;
using lsst::qserv::wpublish::QueriesAndChunks;

TransmitMgr::Ptr locTransmitMgr = make_shared<TransmitMgr>(50, 4);

struct Fixture {
    shared_ptr<TaskMsg> newTaskMsg() {
        shared_ptr<TaskMsg> t = make_shared<TaskMsg>();
        t->set_protocol(2);
        t->set_session(123456);
        t->set_chunkid(3240);  // hardcoded
        t->set_db("LSST");     // hardcoded
        auto scanTbl = t->add_scantable();
        scanTbl->set_db("LSST");
        scanTbl->set_table("Object");
        scanTbl->set_lockinmemory(false);
        scanTbl->set_scanrating(1);
        lsst::qserv::proto::TaskMsg::Fragment* f = t->add_fragment();
        f->add_query("SELECT AVG(yFlux_PS) from LSST.Object_3240");
        return t;
    }

    MySqlConfig newMySqlConfig() {
        string user = "qsmaster";
        string password = "";
        string socket = "SET ME HERE";
        MySqlConfig mySqlConfig(user, password, socket);
        if (not MySqlConnection::checkConnection(mySqlConfig)) {
            throw runtime_error("Unable to connect to MySQL database with params: " + mySqlConfig.toString());
        }
        return mySqlConfig;
    }
    shared_ptr<QueriesAndChunks> queriesAndChunks() {
        bool resetForTesting = true;
        return QueriesAndChunks::setupGlobal(chrono::seconds(1), chrono::seconds(300), 5, resetForTesting);
    }
};

BOOST_FIXTURE_TEST_SUITE(Basic, Fixture)

BOOST_AUTO_TEST_CASE(Simple) {
    WorkerConfig::create();
    shared_ptr<TaskMsg> msg(newTaskMsg());
    shared_ptr<SendChannel> sendC(SendChannel::newNopChannel());
    auto sc = FileChannelShared::create(sendC, locTransmitMgr, msg);
    FakeBackend::Ptr backend = make_shared<FakeBackend>();
    shared_ptr<ChunkResourceMgr> crm = ChunkResourceMgr::newMgr(backend);
    SqlConnMgr::Ptr sqlConnMgr = make_shared<SqlConnMgr>(20, 15);
    auto const queries = queriesAndChunks();
    auto taskVect = Task::createTasks(msg, sc, crm, newMySqlConfig(), sqlConnMgr, queries);
    Task::Ptr task = taskVect[0];
    QueryRunner::Ptr a(QueryRunner::newQueryRunner(task, crm, newMySqlConfig(), sqlConnMgr, queries));
    BOOST_CHECK(a->runQuery());
}

BOOST_AUTO_TEST_CASE(Output) {
    WorkerConfig::create();
    string out;
    shared_ptr<TaskMsg> msg(newTaskMsg());
    shared_ptr<SendChannel> sendC(SendChannel::newStringChannel(out));
    auto sc = FileChannelShared::create(sendC, locTransmitMgr, msg);
    FakeBackend::Ptr backend = make_shared<FakeBackend>();
    shared_ptr<ChunkResourceMgr> crm = ChunkResourceMgr::newMgr(backend);
    SqlConnMgr::Ptr sqlConnMgr = make_shared<SqlConnMgr>(20, 15);
    auto const queries = queriesAndChunks();
    auto taskVect = Task::createTasks(msg, sc, crm, newMySqlConfig(), sqlConnMgr, queries);
    Task::Ptr task = taskVect[0];
    QueryRunner::Ptr a(QueryRunner::newQueryRunner(task, crm, newMySqlConfig(), sqlConnMgr, queries));
    BOOST_CHECK(a->runQuery());

    unsigned char phSize = *reinterpret_cast<unsigned char const*>(out.data());
    char const* cursor = out.data() + 1;
    int remain = out.size() - 1;
    lsst::qserv::proto::ProtoHeader ph;
    BOOST_REQUIRE(ProtoImporter<ProtoHeader>::setMsgFrom(ph, cursor, phSize));
    cursor += phSize;  // Advance to Result msg
    remain -= phSize;
    BOOST_CHECK_EQUAL(remain, ph.size());
    ph.PrintDebugString();
    lsst::qserv::proto::Result result;
    BOOST_REQUIRE(ProtoImporter<Result>::setMsgFrom(result, cursor, remain));
    result.PrintDebugString();
    string computedMd5 = util::StringHash::getMd5(cursor, remain);
    BOOST_CHECK_EQUAL(ph.md5(), computedMd5);
    BOOST_CHECK_EQUAL(task->getSession(), result.session());
}

BOOST_AUTO_TEST_SUITE_END()
