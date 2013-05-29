/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
  * @file FifoScheduler_1.cc
  *
  * @brief Simple testing for class FifoScheduler
  *
  * @author Daniel L. Wang, SLAC
  */ 
#define BOOST_TEST_MODULE FifoScheduler_1
#include "boost/test/included/unit_test.hpp"
#include "lsst/qserv/worker/FifoScheduler.h"

namespace test = boost::test_tools;
namespace qWorker = lsst::qserv::worker;

struct TodoWatcher : public qWorker::TodoList::Watcher {
    typedef boost::shared_ptr<TodoWatcher> Ptr;
    void handleAccept(qWorker::Task::Ptr t) {
        task = t;
    }
    qWorker::Task::Ptr task;
};

struct SchedulerFixture {
    typedef boost::shared_ptr<qWorker::TodoList::TaskQueue> TaskQueuePtr;
    typedef boost::shared_ptr<lsst::qserv::TaskMsg> TaskMsgPtr;

    SchedulerFixture(void) {
        counter = 1;
        todo.reset(new qWorker::TodoList());
        watcher.reset(new TodoWatcher());
        todo->addWatcher(watcher);
        emptyTqp.reset(new qWorker::TodoList::TaskQueue());

    }
    ~SchedulerFixture(void) { }
    
    TaskMsgPtr newTaskMsg(int seq) {
        lsst::qserv::TaskMsg* t;
        t = new lsst::qserv::TaskMsg();
        t->set_session(123456);
        t->set_chunkid(20 + seq);
        t->set_db("elephant");
        for(int i=0; i < 3; ++i) {
            lsst::qserv::TaskMsg::Fragment* f = t->add_fragment();
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
    qWorker::FifoScheduler fs;
    qWorker::TodoList::Ptr todo;
    TodoWatcher::Ptr watcher;
};


BOOST_FIXTURE_TEST_SUITE(FifoSchedulerSuite, SchedulerFixture)

BOOST_AUTO_TEST_CASE(Basic) {
    BOOST_CHECK_EQUAL(nullTqp, fs.nopAct(todo, nullTqp));

    todo->accept(nextTaskMsg());
    qWorker::Task::Ptr t = watcher->task;
    TaskQueuePtr next = fs.newTaskAct(t, todo, emptyTqp);
    BOOST_CHECK(next.get());
    BOOST_CHECK_EQUAL(next->front(), t);
    BOOST_CHECK_EQUAL(next->size(), 1);

    todo->accept(nextTaskMsg());
    qWorker::Task::Ptr t2 = watcher->task;
    next = fs.taskFinishAct(t, todo, emptyTqp);
    BOOST_CHECK(next.get());
    BOOST_CHECK_EQUAL(next->front(), t2);
    BOOST_CHECK_EQUAL(next->size(), 1);
    
}

BOOST_AUTO_TEST_SUITE_END()
