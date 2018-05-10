// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
  * @brief test Mutex
  */

// System headers
#include <atomic>
#include <mutex>
#include <thread>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/BlockPost.h"
#include "util/Mutex.h"

// Boost unit test header
#define BOOST_TEST_MODULE Mutex
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using namespace lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(MutexTest) {

    LOGS_DEBUG("Mutex test begins");

    // The mutex won't be locked by anyone
    Mutex mtx1;
    BOOST_CHECK(not mtx1.lockedByCaller());

    // The mutex will be locked by the current thread
    Mutex mtx2;
    std::lock_guard<Mutex> lockGuard2(mtx2);
    BOOST_CHECK(mtx2.lockedByCaller());

    // Lock this mutex within a separate thread. Let the thread to run,
    // then run the lock ownership test from the current thread.
    //
    // Note that the life expectancy of the launched thread once it locks
    // the mutex will be a random duration of time (milliseconds) within
    // an interval of passed into the constructor of class BlockPost.

    Mutex mtx3;
    std::atomic<bool> threadFinished(false);
    std::thread thr([&mtx3,&threadFinished]() {
        std::lock_guard<Mutex> lockGuard3(mtx3);
        BlockPost blockPost(1000,2000);
        blockPost.wait();
        threadFinished = true;
    });
    thr.detach();

    // Recheck the lock status more frequently to allow multiple attempts
    // while the previously launched thread is still alive.
    while (not threadFinished) {
        BOOST_CHECK(not mtx3.lockedByCaller());
        BlockPost blockPost(100,200);
        blockPost.wait();
    }
    BOOST_CHECK(not mtx3.lockedByCaller());
    std::lock_guard<Mutex> lockGuard4(mtx3);
    BOOST_CHECK(mtx3.lockedByCaller());

    LOGS_DEBUG("Mutex test ends");
}

 BOOST_AUTO_TEST_SUITE_END()
