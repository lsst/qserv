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

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/worker.pb.h"
#include "proto/ProtoImporter.h"
#include "util/StringHash.h"
#include "wbase/SendChannel.h"
#include "wbase/Task.h"
#include "wdb/ChunkResource.h"
#include "wdb/QueryRunner.h"

// Boost unit test header
#define BOOST_TEST_MODULE QueryRunner
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace gio = google::protobuf::io;
namespace util = lsst::qserv::util;

using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::mysql::MySqlConnection;

using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::proto::Result;
using lsst::qserv::proto::TaskMsg;
using lsst::qserv::proto::TaskMsg_Subchunk;
using lsst::qserv::proto::TaskMsg_Fragment;

using lsst::qserv::wbase::SendChannel;
using lsst::qserv::wbase::Task;
using lsst::qserv::wdb::ChunkResource;
using lsst::qserv::wdb::ChunkResourceMgr;
using lsst::qserv::wdb::QueryRunner;

struct Fixture {
    std::shared_ptr<TaskMsg> newTaskMsg() {
        std::shared_ptr<TaskMsg> t = std::make_shared<TaskMsg>();
        t->set_protocol(2);
        t->set_session(123456);
        t->set_chunkid(3240); // hardcoded
        t->set_db("LSST"); // hardcoded
        auto scanTbl = t->add_scantable();
        scanTbl->set_db("LSST");
        scanTbl->set_table("Object");
        scanTbl->set_lockinmemory(false);
        scanTbl->set_scanrating(1);
        lsst::qserv::proto::TaskMsg::Fragment* f = t->add_fragment();
        f->add_query("SELECT AVG(yFlux_PS) from LSST.Object_3240");
        return t;
    }
    std::shared_ptr<Task> newTask() {
        std::shared_ptr<TaskMsg> msg(newTaskMsg());
        std::shared_ptr<SendChannel> sc(SendChannel::newNopChannel());
        return std::make_shared<lsst::qserv::wbase::Task>(msg, sc);
    }

    MySqlConfig newMySqlConfig() {
        std::string user = "qsmaster";
        std::string password = "";
        std::string socket = "SET ME HERE";
        MySqlConfig mySqlConfig(user, password, socket);
        if (not MySqlConnection::checkConnection(mySqlConfig)) {
            throw std::runtime_error("Unable to connect to MySQL database with params: "+mySqlConfig.toString());
        }
        return mySqlConfig;
    }
};

BOOST_FIXTURE_TEST_SUITE(Basic, Fixture)

BOOST_AUTO_TEST_CASE(Simple) {
    std::shared_ptr<TaskMsg> msg(newTaskMsg());
    std::shared_ptr<SendChannel> sc(SendChannel::newNopChannel());
    std::shared_ptr<Task> task = std::make_shared<Task>(msg, sc);
    QueryRunner::Ptr a{QueryRunner::newQueryRunner(task, ChunkResourceMgr::newFakeMgr(), newMySqlConfig())};
    BOOST_CHECK(a->runQuery());
}

BOOST_AUTO_TEST_CASE(Output) {
    std::string out;
    std::shared_ptr<TaskMsg> msg(newTaskMsg());
    std::shared_ptr<SendChannel> sc(SendChannel::newStringChannel(out));
    std::shared_ptr<Task> task = std::make_shared<Task>(msg, sc);
    QueryRunner::Ptr a{QueryRunner::newQueryRunner(task, ChunkResourceMgr::newFakeMgr(), newMySqlConfig())};
    BOOST_CHECK(a->runQuery());

    unsigned char phSize = *reinterpret_cast<unsigned char const*>(out.data());
    char const* cursor = out.data() + 1;
    int remain = out.size() - 1;
    lsst::qserv::proto::ProtoHeader ph;
    BOOST_REQUIRE(ProtoImporter<ProtoHeader>::setMsgFrom(ph, cursor, phSize));
    cursor += phSize; // Advance to Result msg
    remain -= phSize;
    BOOST_CHECK_EQUAL(remain, ph.size());
    ph.PrintDebugString();
    lsst::qserv::proto::Result result;
    BOOST_REQUIRE(ProtoImporter<Result>::setMsgFrom(result, cursor, remain));
    result.PrintDebugString();
    std::string computedMd5 = util::StringHash::getMd5(cursor, remain);
    BOOST_CHECK_EQUAL(ph.md5(), computedMd5);
    BOOST_CHECK_EQUAL(task->msg->session(), result.session());
}

BOOST_AUTO_TEST_SUITE_END()
