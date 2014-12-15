// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
#include "boost/make_shared.hpp"

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
using lsst::qserv::wbase::TaskQueue;
using lsst::qserv::wbase::TaskQueuePtr;


Task::Ptr makeTask(boost::shared_ptr<TaskMsg> tm) {
    return boost::make_shared<Task>(tm);
}
struct SchedulerFixture {
    typedef boost::shared_ptr<TaskMsg> TaskMsgPtr;

    SchedulerFixture(void)
        : fs(1) {
        counter = 1;
        emptyTqp = boost::make_shared<TaskQueue>();
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
    TaskQueuePtr tqp;
    TaskQueuePtr nullTqp;
    TaskQueuePtr emptyTqp;
    lsst::qserv::wsched::FifoScheduler fs;
};


BOOST_FIXTURE_TEST_SUITE(FifoSchedulerSuite, SchedulerFixture)

BOOST_AUTO_TEST_CASE(Basic) {
    BOOST_CHECK_EQUAL(nullTqp, fs.nopAct(nullTqp));
    Task::Ptr first = makeTask(nextTaskMsg());
    fs.queueTaskAct(first);

    Task::Ptr second = makeTask(nextTaskMsg());
    TaskQueuePtr next = fs.newTaskAct(second, emptyTqp);
    BOOST_REQUIRE(next.get());
    BOOST_CHECK_EQUAL(next->front(), first);
    BOOST_CHECK_EQUAL(next->size(), 1U);

    next = fs.taskFinishAct(first, emptyTqp);
    BOOST_REQUIRE(next.get());
    BOOST_CHECK_EQUAL(next->front(), second);
}

BOOST_AUTO_TEST_SUITE_END()
