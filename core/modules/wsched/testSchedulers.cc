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
#include "wsched/ChunkDisk.h"
#include "wsched/BlendScheduler.h"
#include "wsched/FifoScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"

// Boost unit test header
#define BOOST_TEST_MODULE FifoScheduler_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace wsched = lsst::qserv::wsched;

using lsst::qserv::proto::TaskMsg;
using lsst::qserv::wbase::Task;
using lsst::qserv::wbase::SendChannel;


Task::Ptr makeTask(std::shared_ptr<TaskMsg> tm) {
    return std::make_shared<Task>(tm, std::shared_ptr<SendChannel>());
}
struct SchedulerFixture {
    typedef std::shared_ptr<TaskMsg> TaskMsgPtr;

    SchedulerFixture(void) {
        counter = 20;
    }
    ~SchedulerFixture(void) { }

    TaskMsgPtr newTaskMsg(int seq) {
        TaskMsgPtr t = std::make_shared<TaskMsg>();
        t->set_session(123456);
        t->set_chunkid(seq);
        t->set_db("elephant");
        for(int i=0; i < 3; ++i) {
            TaskMsg::Fragment* f = t->add_fragment();
            f->add_query("Hello, this is a query.");
            f->mutable_subchunks()->add_id(100+i);
            f->set_resulttable("r_341");
        }
        ++counter;
        return t;
    }
    TaskMsgPtr nextTaskMsg() {
        return newTaskMsg(counter++);
    }

    TaskMsgPtr newTaskMsgSimple(int seq) {
            TaskMsgPtr t = std::make_shared<TaskMsg>();
            t->set_session(123456);
            t->set_chunkid(seq);
            t->set_db("moose");
            ++counter;
            return t;
        }

    Task::Ptr queMsgWithChunkId(wsched::GroupScheduler &gs, int chunkId) {
        Task::Ptr t = makeTask(newTaskMsg(chunkId));
        gs.queCmd(t);
        return t;
    }

    int counter;
};


BOOST_FIXTURE_TEST_SUITE(FifoSchedulerSuite, SchedulerFixture)

BOOST_AUTO_TEST_CASE(Fifo) {
    wsched::FifoScheduler fs;
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


BOOST_AUTO_TEST_CASE(Grouping) {
    // Test grouping by chunkId. Max entries added to a single group set to 3.
    wsched::GroupScheduler gs{100, 3};
    // chunk Ids
    int a = 50;
    int b = 11;
    int c = 75;
    int d = 4;

    BOOST_CHECK(gs.empty() == true);
    BOOST_CHECK(gs.ready() == false);

    Task::Ptr a1 = queMsgWithChunkId(gs, a);
    BOOST_CHECK(gs.empty() == false);
    BOOST_CHECK(gs.ready() == true);

    Task::Ptr b1 = queMsgWithChunkId(gs, b);
    Task::Ptr c1 = queMsgWithChunkId(gs, c);
    Task::Ptr b2 = queMsgWithChunkId(gs, b);
    Task::Ptr b3 = queMsgWithChunkId(gs, b);
    Task::Ptr b4 = queMsgWithChunkId(gs, b);
    Task::Ptr a2 = queMsgWithChunkId(gs, a);
    Task::Ptr a3 = queMsgWithChunkId(gs, a);
    Task::Ptr b5 = queMsgWithChunkId(gs, b);
    Task::Ptr d1 = queMsgWithChunkId(gs, d);
    BOOST_CHECK(gs.getSize() == 5);
    BOOST_CHECK(gs.ready() == true);

    // Should get all the first 3 'a' commands in order
    auto aa1 = gs.getCmd(false);
    auto aa2 = gs.getCmd(false);
    Task::Ptr a4 = queMsgWithChunkId(gs, a); // this should get its own group
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
}


BOOST_AUTO_TEST_CASE(GroupMaxThread) {
    // Test that maxThreads is meaningful.
    wsched::GroupScheduler gs{3, 100};
    int a = 42;
    Task::Ptr a1 = queMsgWithChunkId(gs, a);
    Task::Ptr a2 = queMsgWithChunkId(gs, a);
    Task::Ptr a3 = queMsgWithChunkId(gs, a);
    Task::Ptr a4 = queMsgWithChunkId(gs, a);
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
}

BOOST_AUTO_TEST_CASE(DiskMinHeap) {
    wsched::ChunkDisk::MinHeap minHeap{};

    BOOST_CHECK(minHeap.empty() == true);

    Task::Ptr a47 = makeTask(newTaskMsg(47));
    minHeap.push(a47);
    BOOST_CHECK(minHeap.top().get() == a47.get());
    BOOST_CHECK(minHeap.empty() == false);

    Task::Ptr a42 = makeTask(newTaskMsg(42));
    minHeap.push(a42);
    BOOST_CHECK(minHeap.top().get() == a42.get());

    Task::Ptr a60 = makeTask(newTaskMsg(60));
    minHeap.push(a60);
    BOOST_CHECK(minHeap.top().get() == a42.get());

    Task::Ptr a18 = makeTask(newTaskMsg(18));
    minHeap.push(a18);
    BOOST_CHECK(minHeap.top().get() == a18.get());

    BOOST_CHECK(minHeap.pop().get() == a18.get());
    BOOST_CHECK(minHeap.pop().get() == a42.get());
    BOOST_CHECK(minHeap.pop().get() == a47.get());
    BOOST_CHECK(minHeap.pop().get() == a60.get());
    BOOST_CHECK(minHeap.empty() == true);
}

BOOST_AUTO_TEST_CASE(ChunkDiskTest) {
    wsched::ChunkDisk cDisk{LOG_GET("")};

    BOOST_CHECK(cDisk.empty() == true);
    BOOST_CHECK(cDisk.getSize() == 0);
    BOOST_CHECK(cDisk.busy() == false);
    BOOST_CHECK(cDisk.ready() == false);

    Task::Ptr a47 = makeTask(newTaskMsg(47));
    cDisk.enqueue(a47); // should go on active
    BOOST_CHECK(cDisk.empty() == false);
    BOOST_CHECK(cDisk.getSize() == 1);
    BOOST_CHECK(cDisk.busy() == false);
    BOOST_CHECK(cDisk.ready() == true);

    Task::Ptr a42 = makeTask(newTaskMsg(42));
    cDisk.enqueue(a42); // should go on active
    BOOST_CHECK(cDisk.empty() == false);
    BOOST_CHECK(cDisk.getSize() == 2);
    BOOST_CHECK(cDisk.busy() == false);
    BOOST_CHECK(cDisk.ready() == true);
    BOOST_CHECK(cDisk.getInflight().size() == 0);

    Task::Ptr b42 = makeTask(newTaskMsg(42));
    cDisk.enqueue(b42); // should go on active
    BOOST_CHECK(cDisk.empty() == false);
    BOOST_CHECK(cDisk.getSize() == 3);
    BOOST_CHECK(cDisk.busy() == false);
    BOOST_CHECK(cDisk.ready() == true);

    auto aa42 = cDisk.getTask();
    BOOST_CHECK(aa42.get() == a42.get());
    BOOST_CHECK(cDisk.empty() == false);
    BOOST_CHECK(cDisk.getSize() == 2);
    BOOST_CHECK(cDisk.busy() == true);
    BOOST_CHECK(cDisk.ready() == true);
    BOOST_CHECK(cDisk.getInflight().size() == 1);

    Task::Ptr a18 = makeTask(newTaskMsg(18));
    cDisk.enqueue(a18); // should go on pending
    BOOST_CHECK(cDisk.empty() == false);
    BOOST_CHECK(cDisk.getSize() == 3);
    BOOST_CHECK(cDisk.busy() == true);
    BOOST_CHECK(cDisk.ready() == true);

    // Test that active tasks from the same chunk can be started.
    auto bb42 = cDisk.getTask();
    BOOST_CHECK(bb42.get() == b42.get());
    BOOST_CHECK(cDisk.empty() == false);
    BOOST_CHECK(cDisk.busy() == true);
    BOOST_CHECK(cDisk.ready() == false);
    BOOST_CHECK(cDisk.getInflight().size() == 2);

    // Check that completing all tasks on chunk 42 lets us move to chunk 47
    cDisk.removeInflight(bb42);
    BOOST_CHECK(cDisk.getInflight().size() == 1);
    BOOST_CHECK(cDisk.ready() == true);

    auto aa47 = cDisk.getTask();
    BOOST_CHECK(aa47.get() == a47.get());
    BOOST_CHECK(cDisk.empty() == false);
    BOOST_CHECK(cDisk.getSize() == 1);
    BOOST_CHECK(cDisk.busy() == true);
    BOOST_CHECK(cDisk.ready() == false);
    BOOST_CHECK(cDisk.getInflight().size() == 2);

    // finishing a47 should let us get a18
    cDisk.removeInflight(a47);
    BOOST_CHECK(cDisk.getInflight().size() == 1);
    BOOST_CHECK(cDisk.ready() == true);
    BOOST_CHECK(cDisk.busy() == false);

    auto aa18 = cDisk.getTask();
    BOOST_CHECK(aa18.get() == a18.get());
    BOOST_CHECK(cDisk.empty() == true);
    BOOST_CHECK(cDisk.getSize() == 0);
    BOOST_CHECK(cDisk.busy() == true);
    BOOST_CHECK(cDisk.ready() == false);
    BOOST_CHECK(cDisk.getInflight().size() == 2);

    cDisk.removeInflight(a18);
    BOOST_CHECK(cDisk.getInflight().size() == 1);
    BOOST_CHECK(cDisk.empty() == true);
    BOOST_CHECK(cDisk.getSize() == 0);
    BOOST_CHECK(cDisk.ready() == false);
    BOOST_CHECK(cDisk.busy() == false);

    cDisk.removeInflight(a42);
    BOOST_CHECK(cDisk.getInflight().size() == 0);
    BOOST_CHECK(cDisk.empty() == true);
    BOOST_CHECK(cDisk.getSize() == 0);
    BOOST_CHECK(cDisk.ready() == false);
    BOOST_CHECK(cDisk.busy() == false);
}


BOOST_AUTO_TEST_CASE(ScanScheduleTest) {
    wsched::ScanScheduler sched{2};

    // Test ready state as Tasks added and removed.
    BOOST_CHECK(sched.ready() == false);

    Task::Ptr a40 = makeTask(newTaskMsg(40));
    sched.queCmd(a40);
    BOOST_CHECK(sched.ready() == true);

    Task::Ptr b40 = makeTask(newTaskMsg(40));
    sched.queCmd(b40);

    Task::Ptr c40 = makeTask(newTaskMsg(40));
    sched.queCmd(c40);

    Task::Ptr a33 = makeTask(newTaskMsg(33));
    sched.queCmd(a33);

    BOOST_CHECK(sched.ready() == true);
    auto aa33 = sched.getCmd(false);
    BOOST_CHECK(aa33.get() == a33.get());
    BOOST_CHECK(sched.getInFlight() == 1);
    sched.commandStart(aa33);
    BOOST_CHECK(sched.getInFlight() == 1);
    BOOST_CHECK(sched.ready() == false);
    sched.commandFinish(aa33);
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

}


BOOST_AUTO_TEST_CASE(BlendScheduleTest) {
    // max 2 inflight and group size 3. Not testing that functionality here, so arbitrary.
    auto group = std::make_shared<wsched::GroupScheduler>(2, 3);
    auto scan = std::make_shared<wsched::ScanScheduler>(2);
    wsched::BlendScheduler blend(group, scan);

    BOOST_CHECK(blend.ready() == false);

    // This should go on group scheduler
    Task::Ptr a40 = makeTask(newTaskMsgSimple(40));
    blend.queCmd(a40);
    BOOST_CHECK(group->getSize() == 1);
    BOOST_CHECK(group->ready() == true);
    BOOST_CHECK(blend.ready() == true);

    auto aa40 = blend.getCmd(false);
    blend.commandStart(aa40);
    blend.commandFinish(aa40);
    BOOST_CHECK(aa40.get() == a40.get());
    BOOST_CHECK(group->ready() == false);
    BOOST_CHECK(blend.ready() == false);

    // Make a message with a scantable so it goes to scan.
    Task::Ptr s1 = makeTask(newTaskMsg(27));
    blend.queCmd(s1); // should go to scan
    BOOST_CHECK(group->getSize() == 1);
    BOOST_CHECK(group->ready() == true);
    BOOST_CHECK(blend.ready() == true);

    auto ss1 = blend.getCmd(false);
    blend.commandStart(ss1);
    blend.commandFinish(ss1);
    BOOST_CHECK(ss1.get() == ss1.get());
    BOOST_CHECK(group->ready() == false);
    BOOST_CHECK(blend.ready() == false);

}

BOOST_AUTO_TEST_SUITE_END()
