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
/**
 *
 * @brief test EventThread
 *
 * @author John Gates, SLAC
 */

// System headers
#include <iostream>
#include <sstream>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/ThreadPool.h"
#include "util/InstanceCount.h"

// Boost unit test header
#define BOOST_TEST_MODULE common
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using namespace lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(EventThreadTest) {
    LOGS_DEBUG("EventThread test");

    struct SumUnprotected {
        int total{0};
        void add(int val) { total += val; }
    };

    /// Queue a sum depending on mutex protection within the EventThread.
    {
        EventThread et{};
        SumUnprotected sum;
        int total{0};
        int cycles = 99; // Arbitrary number of times add message to queue.
        for (int j=1; j<cycles; j++) {
            auto cmdSum = std::make_shared<Command>([&sum, j](CmdData*){sum.add(j);});
            total += j;
            et.queCmd(cmdSum);
        }
        et.run();
        for (int j=1; j<cycles; j++) {
            auto cmdSum = std::make_shared<Command>([&sum, j](CmdData*){sum.add(j);});
            total += j;
            et.queCmd(cmdSum);
        }
        et.queEnd();
        et.join();
        BOOST_CHECK(total == sum.total);
    }

    /// Create a thread pool
    std::weak_ptr<CommandQueue> weak_que;
    std::weak_ptr<ThreadPool> weak_pool;


    /// Create a thread pool
    {
        auto cmdQueue = std::make_shared<CommandQueue>();
        weak_que = cmdQueue;
        uint sz = 2; // size of thread pool to create
        auto pool = ThreadPool::newThreadPool(sz, cmdQueue);
        weak_pool = pool;
        LOGS_DEBUG("pool size=" << sz);
        BOOST_CHECK(pool->size() == sz);

        // Shrink the pool to zero and verify that the pool is shutdown.
        pool->shutdownPool();
        LOGS_DEBUG("pool size=" << 0 << " weak_pool.use_count=" << weak_pool.use_count());
        pool->resize(20); // Size should remain zero, since shutdownPool() called.
        BOOST_CHECK(pool->size() == 0);
    }
    BOOST_CHECK(weak_pool.use_count() == 0);
    BOOST_CHECK(weak_que.use_count() == 0);


    {
        auto cmdQueue = std::make_shared<CommandQueue>();
        weak_que = cmdQueue;
        uint sz = 10; // size of thread pool to create
        auto pool = ThreadPool::newThreadPool(sz, cmdQueue);
        weak_pool = pool;
        LOGS_DEBUG("pool size=" << sz);
        BOOST_CHECK(pool->size() == sz);
        sz += 10; // test increase in size of thread pool.
        pool->resize(sz);
        LOGS_DEBUG("pool size=" << sz << " weak_pool.use_count=" << weak_pool.use_count());
        BOOST_CHECK(pool->size() == sz);
        sz = 5; // test decrease in size of thread pool.
        pool->resize(sz);
        pool->waitForResize(10000);
        LOGS_DEBUG("pool size=" << sz << " weak_pool.use_count=" << weak_pool.use_count());
        BOOST_CHECK(pool->size() == sz);

        /// Queue up a sum using the pool.
        struct Sum {
            std::atomic<int> total{0};
            void add(int val) { total += val; }
        };

        Sum poolSum;
        int total = 0;
        auto poolQueue = pool->getQueue();
        LOGS_DEBUG("Summing with pool");
        sz = 20; // Want enough threads so that there are reasonable chance of collisions.
        pool->resize(sz);
        LOGS_DEBUG("pool size=" << sz << " weak_pool.use_count=" << weak_pool.use_count());
        BOOST_CHECK(pool->size() == sz);

        for (int j=1;j<2000;j++) {
            auto cmdSum = std::make_shared<Command>([&poolSum, j](CmdData*){poolSum.add(j);});
            total += j;
            poolQueue->queCmd(cmdSum);
        }
        LOGS_DEBUG("stopping all threads in pool");
        pool->endAll(); // These are added to end of queue, everything on queue should complete.
        pool->waitForResize(0);
        LOGS_DEBUG("pool size=" << 0 << " weak_pool.use_count=" << weak_pool.use_count());
        BOOST_CHECK(total == poolSum.total);

        // Test that a threads can leave the pool and complete and the pool size recovers.
        sz = 5;
        pool->resize(sz);
        pool->waitForResize(0);
        LOGS_DEBUG("pool size=" << sz << " weak_pool.use_count=" << weak_pool.use_count());
        Sum sum;
        std::vector<Tracker::Ptr> trackedCmds;
        bool go = false;
        std::condition_variable goCV;
        std::mutex goCVMtx;
        // Create more threads than can fit in the pool and don't let any complete.
        int threadsRunning = 2*sz;
        for (int j=0; j<threadsRunning; j++) {
            // The command to run.
            auto cmdDelaySum = std::make_shared<CommandThreadPool>(
                [&sum, &go, &goCV, &goCVMtx](CmdData* eventThread){
                    PoolEventThread* peThread = dynamic_cast<PoolEventThread*>(eventThread);
                    peThread->leavePool();
                    sum.add(1);
                    LOGS_DEBUG("Wait for goCVTest.");
                    auto goCVTest = [&go](){ return go; };
                    std::unique_lock<std::mutex> goLock(goCVMtx);
                    goCV.wait(goLock, goCVTest); // wait on go == true;
                    sum.add(1);
            });
            trackedCmds.push_back(cmdDelaySum); // Remember the command so we can check status later.
            poolQueue->queCmd(cmdDelaySum); // Have the pool run the command when it can.
        }
        // Wait briefly (5sec) for all threads to be running.
        LOGS_DEBUG("Wait for all threads to be running.");
        for (int j = 0; sum.total<threadsRunning && j<50; ++j) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        // Verify pool size
        BOOST_CHECK(pool->size() == sz);
        BOOST_CHECK(sum.total == threadsRunning);
        // Shrink the pool to zero and verify the separated threads complete.
        pool->resize(0);
        pool->waitForResize(0);
        LOGS_DEBUG("pool size=" << 0 << " weak_pool.use_count=" << weak_pool.use_count());
        BOOST_CHECK(pool->size() == 0);
        // Have the separated threads finish.
        {
            std::lock_guard<std::mutex> lock(goCVMtx);
            go = true;
        }
        goCV.notify_all();
        // Wait for all separated threads to finish
        for (auto const& ptc : trackedCmds) {
            LOGS_DEBUG("Wait for thread to finish.");
            ptc->waitComplete();
        }
        // sum.total should now be double what it was.
        BOOST_CHECK(sum.total == 2*threadsRunning);
        LOGS_DEBUG("Shutting down pool.");
        pool->shutdownPool();
        pool.reset();
        LOGS_DEBUG("pool !exists weak_pool.use_count=" << weak_pool.use_count());
    }
    BOOST_CHECK(weak_pool.use_count() == 0);
    BOOST_CHECK(weak_que.use_count() == 0);

    // Wait for a moderately long calculation to finish with CommandTracked.
    {
        SumUnprotected sum;
        auto cmdQueue = std::make_shared<CommandQueue>();
        weak_que = cmdQueue;
        uint sz = 10;
        auto pool = ThreadPool::newThreadPool(sz, cmdQueue);
        weak_pool = pool;
        auto func = [&sum](CmdData*){
            for (int j=0; j<900000;j++) {
                sum.add(1);
            }
        };

        auto cmdSumUnprotected = std::make_shared<CommandTracked>(func);

        class CommandData : public CommandTracked {
        public:
            void action(CmdData*) override {
                for (int j=0; j<900000;j++) {
                    total += 1;
                }
                setComplete();
            };
            int total{0};
        };
        auto commandData = std::make_shared<CommandData>();

        cmdQueue->queCmd(cmdSumUnprotected);
        cmdQueue->queCmd(commandData);

        cmdSumUnprotected->waitComplete();
        commandData->waitComplete();
        LOGS_DEBUG("cmdSumUnprotected=" << sum.total
                   << " commandData=" << commandData->total);
        BOOST_CHECK(sum.total == commandData->total);
        pool->shutdownPool();
    }

    // Give it some time to finish deleting everything (5 seconds)
    for (int j = 0; weak_pool.use_count() > 0 && j<50 ; ++j) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    BOOST_CHECK(weak_pool.use_count() == 0);
    BOOST_CHECK(weak_que.use_count() == 0);
}


BOOST_AUTO_TEST_CASE(InstanceCountTest) {

    struct CA {
        InstanceCount instanceCount{"CA"};
    };

    struct CB {
        InstanceCount instanceCount{"CB"};
    };

    CB cb;
    {
        CA ca1;
        BOOST_CHECK(ca1.instanceCount.getCount() == 1);
        CA ca2;
        BOOST_CHECK(ca1.instanceCount.getCount() == 2);
        CA ca3(ca1);
        BOOST_CHECK(ca1.instanceCount.getCount() == 3);
        CA ca4 = std::move(ca1);
        BOOST_CHECK(ca1.instanceCount.getCount() == 4);
        CA ca5;
        BOOST_CHECK(ca1.instanceCount.getCount() == 5);
        ca5 = ca2;
        BOOST_CHECK(ca1.instanceCount.getCount() == 5);
        BOOST_CHECK(cb.instanceCount.getCount() == 1);
    }
    BOOST_CHECK(cb.instanceCount.getCount() == 1);
    CA ca0;
    BOOST_CHECK(ca0.instanceCount.getCount() == 1);
}

BOOST_AUTO_TEST_SUITE_END()
