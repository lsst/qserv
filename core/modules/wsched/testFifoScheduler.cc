// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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

// Third-party headers

// Qserv headers
#include "proto/worker.pb.h"
#include "wbase/Task.h"
#include "wsched/FifoScheduler.h"

// Boost unit test header
#define BOOST_TEST_MODULE FifoScheduler_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using lsst::qserv::proto::TaskMsg;
using lsst::qserv::wbase::Task;
using lsst::qserv::wbase::SendChannel;

Task::Ptr makeTask(std::shared_ptr<TaskMsg> tm) {
    return std::make_shared<Task>(tm, std::shared_ptr<SendChannel>());
}
struct SchedulerFixture {
    typedef std::shared_ptr<TaskMsg> TaskMsgPtr;

    SchedulerFixture(void) {
        counter = 1;
    }
    ~SchedulerFixture(void) { }

    TaskMsgPtr newTaskMsg(int seq) {
        TaskMsg* t;
        t = new TaskMsg();
        t->set_session(123456);
        t->set_chunkid(20 + seq);
        t->set_db("elephant");
        for(int i=0; i < 3; ++i) {
            TaskMsg::Fragment* f = t->add_fragment();
            f->add_query("Hello, this is a query.");
            f->mutable_subchunks()->add_id(100+i);
            f->set_resulttable("r_341");
        }
        ++counter;
        return TaskMsgPtr(t);
    }
    TaskMsgPtr nextTaskMsg() {
        return newTaskMsg(counter++);
    }

    int counter;

    lsst::qserv::wsched::FifoScheduler fs;
};


BOOST_FIXTURE_TEST_SUITE(FifoSchedulerSuite, SchedulerFixture)

BOOST_AUTO_TEST_CASE(Basic) {
    Task::Ptr first = makeTask(nextTaskMsg());
    fs.queCmd(first);

    Task::Ptr second = makeTask(nextTaskMsg());
    fs.queCmd(second);

    Task::Ptr third = makeTask(nextTaskMsg());
    fs.queCmd(third);

    auto t1 = fs.getCmd();
    auto t2 = fs.getCmd();
    auto t3 = fs.getCmd();

    BOOST_CHECK_EQUAL(first.get(), t1.get());
    BOOST_CHECK_EQUAL(second.get(), t2.get());
    BOOST_CHECK_EQUAL(third.get(), t3.get());

    auto t4 = fs.getCmd(false);
    BOOST_CHECK(t4 == nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
