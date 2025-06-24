// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
 * @file
 *
 * @brief Unit test for the implementations of the Common State System Interface.
 *
 * @Author Jacek Becla, SLAC
 */

// System headers
#include <algorithm>  // sort
#include <cstddef>    // nullptr
#include <cstdlib>    // rand, srand
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h>  // memset
#include <time.h>    // time

// Third-party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "css/KvInterfaceImplMem.h"
#include "css/KvInterfaceImplMySql.h"

// Boost unit test header
#define BOOST_TEST_MODULE MyTest
#include <boost/test/unit_test.hpp>
struct KvInterfaceFixture {
    KvInterfaceFixture(void) {
        srand(time(nullptr));
        prefix = "/unittest_" + boost::lexical_cast<std::string>(rand());
        k1 = prefix + "/xyzA";
        k2 = prefix + "/xyzB";
        k3 = prefix + "/xyzC";
        k4 = prefix + "/xyz_unique_";
        v1 = "firstOne";
        v2 = "secondOne";
    };

    ~KvInterfaceFixture(void) {};

    void doIt(lsst::qserv::css::KvInterface* kvI) {
        kvI->create(prefix, v1);
        kvI->create(k1, v1);
        kvI->create(k2, v2);

        BOOST_CHECK(kvI->get(k1) == v1);
        BOOST_CHECK(kvI->exists(k1));
        BOOST_CHECK(!kvI->exists(k3));

        std::vector<std::string> v = kvI->getChildren(prefix);
        BOOST_CHECK(2 == v.size());
        std::sort(v.begin(), v.end());
        BOOST_CHECK(v[0] == "xyzA");
        BOOST_CHECK(v[1] == "xyzB");

        kvI->deleteKey(k1);
        BOOST_CHECK(kvI->get(k1, "xyz4") == "xyz4");

        v = kvI->getChildren(prefix);
        BOOST_CHECK(1 == v.size());

        kvI->deleteKey(k2);
        kvI->deleteKey(prefix);

        // Test setting a key that already exists
        kvI->create(k1, v1);
        BOOST_CHECK(kvI->get(k1) == v1);
        BOOST_CHECK(kvI->exists(k1));
        kvI->set(k1, v2);
        BOOST_CHECK(kvI->get(k1) == v2);

        // test unique
        std::string key;
        BOOST_CHECK_NO_THROW(key = kvI->create(k4, "uniqueValue", true));
        BOOST_CHECK_EQUAL(key, k4 + "0000000001");
        BOOST_CHECK_EQUAL(kvI->get(key), "uniqueValue");
        BOOST_CHECK_NO_THROW(key = kvI->create(k4, "", true));
        BOOST_CHECK_EQUAL(key, k4 + "0000000002");

        delete kvI;
    }

    std::string prefix, k1, k2, k3, k4, v1, v2;
};

BOOST_FIXTURE_TEST_SUITE(KvInterfaceTest, KvInterfaceFixture)

BOOST_AUTO_TEST_CASE(testMem) {
    std::cout << "========== Testing MEM ==========\n";
    doIt(new lsst::qserv::css::KvInterfaceImplMem());
}

BOOST_AUTO_TEST_SUITE_END()
