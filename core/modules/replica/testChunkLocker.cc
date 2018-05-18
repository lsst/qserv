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
  * @brief test ChunkLocker
  */

// System headers
#include <thread>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/ChunkLocker.h"

// Boost unit test header
#define BOOST_TEST_MODULE ChunkLocker
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ChunkLockerTest) {

    LOGS_INFO("ChunkLocker test begins");

    // Basic tests of struct Chunk

    Chunk chunk{"test", 123UL};

    BOOST_CHECK_EQUAL(chunk.databaseFamily, "test");
    BOOST_CHECK_EQUAL(chunk.number, 123UL);

    Chunk chunk1{"test", 123UL};

    BOOST_CHECK(chunk == chunk1);
    BOOST_CHECK((not (chunk < chunk1)) and (not (chunk1 < chunk)));

    Chunk chunk2{"test", 124UL};
    Chunk chunk3{"test", 125UL};
    Chunk chunk4{"prod", 125UL};

    BOOST_CHECK(not (chunk1 == chunk2));
    BOOST_CHECK(chunk1 < chunk2);

    BOOST_CHECK(not (chunk3 == chunk4));
    BOOST_CHECK(chunk4 < chunk3);

    // Tests of the empty locker

    ChunkLocker locker;

    BOOST_CHECK(not locker.isLocked(chunk1));

    BOOST_CHECK_EQUAL(locker.locked().size(), 0UL);
    BOOST_CHECK_EQUAL(locker.locked("qserv").size(), 0UL);

    std::string owner;

    BOOST_CHECK(not locker.release(chunk1));
    BOOST_CHECK((not locker.release(chunk1, owner)) and (std::string() == owner));
    BOOST_CHECK_EQUAL(locker.release("qserv").size(), 0UL);

    // Test chunk insertion

    BOOST_CHECK(locker.lock(chunk1, "qserv"));
    BOOST_CHECK(locker.lock(chunk2, "root"));
    BOOST_CHECK(locker.lock(chunk3, "qserv"));
    BOOST_CHECK(locker.lock(chunk4, "root"));

    BOOST_CHECK(locker.isLocked(chunk1));
    BOOST_CHECK(locker.isLocked(chunk1, owner) and ("qserv" == owner));

    BOOST_CHECK_EQUAL(locker.locked().size(), 2UL);
    BOOST_CHECK_EQUAL(locker.locked().at("qserv").size(), 2UL);
    BOOST_CHECK_EQUAL(locker.locked().at("root").size(), 2UL);

    BOOST_CHECK_EQUAL(locker.locked("qserv").size(), 1UL);
    BOOST_CHECK_EQUAL(locker.locked("root").size(), 1UL);

    BOOST_CHECK(locker.isLocked(chunk2));
    BOOST_CHECK(locker.isLocked(chunk3));
    BOOST_CHECK(locker.isLocked(chunk4));

    // Test chunk removal

    BOOST_CHECK_EQUAL(locker.locked().at("root").size(), 2UL);
    BOOST_CHECK(locker.release(chunk2));
    BOOST_CHECK(not locker.release(chunk2));
    BOOST_CHECK_EQUAL(locker.locked().at("root").size(), 1UL);

    BOOST_CHECK_EQUAL(locker.locked().at("qserv").size(), 2UL);
    BOOST_CHECK(locker.release(chunk3, owner) and ("qserv" == owner));
    BOOST_CHECK(not locker.release(chunk3, owner));
    BOOST_CHECK_EQUAL(locker.locked().at("qserv").size(), 1UL);

    BOOST_CHECK_EQUAL(locker.locked().size(), 2UL);
    BOOST_CHECK_EQUAL(locker.release("root").size(), 1UL);
    BOOST_CHECK_EQUAL(locker.locked().count("root"), 0UL);
    BOOST_CHECK_EQUAL(locker.locked().size(), 1UL);
    BOOST_CHECK_EQUAL(locker.locked().count("qserv"), 1UL);
    BOOST_CHECK_EQUAL(locker.locked("qserv").size(), 1UL);

    BOOST_CHECK(locker.release(chunk1));
    BOOST_CHECK_EQUAL(locker.locked().size(), 0UL);

    // A this point the locker must be completelly empty

    // Run a limited thread safety test if the hardware concurrency permits so.
    // The test will be attempting to insert the same sequence of chunks from
    // multiple simultaneously running threads and then testing that all chunks
    // were registed in the locker.
    //
    // ATTENTION: this test is not reliable as it depends on the OS scheduler
    //            of a machine where the test is run
    //
    // IMPLEMENTATION NOTE: result reporting is made through the main thread
    //                      because BOOST unit test can't be made directly
    //                      inside threads.

    unsigned int const concurrency = std::thread::hardware_concurrency();
    if (concurrency > 1) {

        LOGS_INFO("ChunkLocker run thread-safety test: hardware concurrency " << concurrency);

        unsigned int const num = 200000UL;
        std::map<std::string,size_t> numTestsFailedByOwner;
    
        // This function will attempt to ingets 'num' locks on behalf
        // of 'thisOwner'. In case if that's not possible for a particular
        // chunk a further test will be made to ensurr that the chunk was already
        // locked by 'otherOwner'.
        //
        // Any devitations will be accounted for and returned into the main thread
        // via dictionary 'numTestsFailedByOwner'.

        auto ingest = [&locker,&numTestsFailedByOwner](std::string const& thisOwner,
                                                       std::string const& otherOwner,
                                                       unsigned int const num) {
            size_t numTestsFailed = 0UL;
            for (unsigned int i = 0UL; i < num; ++i) {

                Chunk const chunk{"test", i};
                std::string owner;

                bool const passed =
                    locker.lock(chunk, thisOwner) or
                    (locker.isLocked(chunk, owner) and (otherOwner == owner));

                if (not passed) numTestsFailed++;

            }
            numTestsFailedByOwner[thisOwner] = numTestsFailed;
        };
        std::thread t1(ingest, "qserv", "root",  num);
        std::thread t2(ingest, "root",  "qserv", num);
        t1.join();
        t2.join();

        // Test and report failures
    
        BOOST_CHECK_EQUAL(numTestsFailedByOwner["qserv"], 0UL);
        BOOST_CHECK_EQUAL(numTestsFailedByOwner["root"],  0UL);

        // Analyze the content of the locker

        ChunkLocker::OwnerToChunks const ownerToChunks = locker.locked();
        unsigned int const numOwners = ownerToChunks.size();
    
        BOOST_CHECK((1 <= numOwners) and (numOwners <= 2));
    
        unsigned int numChunks = 0UL;
        for (auto&& entry: ownerToChunks) {
            numChunks += entry.second.size();
        }
        BOOST_CHECK_EQUAL(numChunks, num);

    } else {
        LOGS_INFO("ChunkLocker skip thread-safety test: insufficient hardware concurrency " << concurrency);
    }
    LOGS_INFO("ChunkLocker test ends");
}

BOOST_AUTO_TEST_SUITE_END()
