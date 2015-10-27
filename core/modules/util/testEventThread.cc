// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#include "util/EventThread.h"

// Boost unit test header
#define BOOST_TEST_MODULE common
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using namespace lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

/** @test
 * Print a MultiError object containing only one error
 */
BOOST_AUTO_TEST_CASE(EventThreadTest) {
    LOG_DEBUG("EventThread test");

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
            auto cmdSum = std::make_shared<Command>([&sum, j](){sum.add(j);});
            total += j;
            et.queCmd(cmdSum);
        }
        et.run();
        for (int j=1; j<cycles; j++) {
            auto cmdSum = std::make_shared<Command>([&sum, j](){sum.add(j);});
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
    {
        auto cmdQueue = std::make_shared<CommandQueue>();
        weak_que = cmdQueue;
        uint sz = 10; // size of thread poo to create
        auto pool = ThreadPool::newThreadPool(sz, cmdQueue);
        weak_pool = pool;
        LOGF_DEBUG("pool size=%1%" % sz);
        BOOST_CHECK(pool->size() == sz);
        sz += 10; // test increase in size of thread pool.
        pool->resize(sz);
        LOGF_DEBUG("pool size=%1%" % sz);
        BOOST_CHECK(pool->size() == sz);
        sz = 5; // test decrease in size of thread pool.
        pool->resize(sz);
        pool->waitForResize(10000);
        LOGF_DEBUG("pool size=%1%" % sz);
        BOOST_CHECK(pool->size() == sz);

        /// Queue up a sum using the pool.
        struct Sum {
            std::atomic<int> total{0};
            void add(int val) { total += val; }
        };

        Sum poolSum;
        int total = 0;
        auto poolQueue = pool->getQueue();
        LOGF_DEBUG("Summing with pool");
        sz = 20; // Want enough threads so that there are reasonable chance of collisions.
        pool->resize(sz);
        LOGF_DEBUG("pool size=%1%" % sz);
        BOOST_CHECK(pool->size() == sz);

        for (int j=1;j<2000;j++) {
            auto cmdSum = std::make_shared<Command>([&poolSum, j](){poolSum.add(j);});
            total += j;
            poolQueue->queCmd(cmdSum);
        }
        LOGF_DEBUG("stopping all threads in pool");
        pool->endAll(); // These are added to end of queue, everything on queue should complete.
        pool->waitForResize(0);
        BOOST_CHECK(total == poolSum.total);
    }

    // Give it some time to finish deleting everything (5 seconds total)
    for (int j = 0; weak_pool.use_count() > 0 && j<50 ; j++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
        auto func = [&sum](){
            for (int j=0; j<900000;j++) {
                sum.add(1);
            }
        };

        auto cmdSumUnprotected = std::make_shared<CommandTracked>(func);

        class CommandData : public CommandTracked {
        public:
            int action() override {
                for (int j=0; j<900000;j++) {
                    total += 1;
                }
                setComplete();
                return 0;
            };
            int total{0};
        };
        auto commandData = std::make_shared<CommandData>();

        cmdQueue->queCmd(cmdSumUnprotected);
        cmdQueue->queCmd(commandData);

        cmdSumUnprotected->waitComplete();
        commandData->waitComplete();
        LOGF_DEBUG("cmdSumUnprotected=%1% commandData=%2%" % sum.total % commandData->total);
        BOOST_CHECK(sum.total == commandData->total);
        pool->endAll();
    }

    // Give it some time to finish deleting everything (5 seconds)
    for (int j = 0; weak_pool.use_count() > 0 && j<50 ; ++j) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    BOOST_CHECK(weak_pool.use_count() == 0);
    BOOST_CHECK(weak_que.use_count() == 0);
}

BOOST_AUTO_TEST_SUITE_END()
