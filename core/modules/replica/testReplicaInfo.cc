// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
  * @brief test ReplicaInfo
  */

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/ReplicaInfo.h"

// Boost unit test header
#define BOOST_TEST_MODULE ReplicaInfo
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ReplicaInfoTest) {

    LOGS_INFO("ReplicaInfo test begins");

    QservReplicaCollection one;
    one.emplace_back(1U, "db1", 0);
    one.emplace_back(1U, "db2", 0);   // only here
    one.emplace_back(2U, "db1", 0);
    one.emplace_back(3U, "db3", 0);   // only here

    QservReplicaCollection two;
    two.emplace_back(1U, "db1", 0);
    two.emplace_back(2U, "db1", 0);
    two.emplace_back(3U, "db2", 0);   // only here
    two.emplace_back(4U, "db3", 0);   // only here

    // Test one-way comparison

    QservReplicaCollection inFirstOnly;
    BOOST_CHECK(diff(one, two, inFirstOnly));
    BOOST_CHECK(inFirstOnly.size() == 2);
    BOOST_CHECK(inFirstOnly.size() == 2 &&
                (inFirstOnly[0].chunk == 1) && (inFirstOnly[0].database == "db2") && 
                (inFirstOnly[1].chunk == 3) && (inFirstOnly[1].database == "db3"));

    QservReplicaCollection inSecondOnly;
    BOOST_CHECK(diff(two, one, inSecondOnly));
    BOOST_CHECK(inSecondOnly.size() == 2);
    BOOST_CHECK(inSecondOnly.size() == 2 &&
                (inSecondOnly[0].chunk == 3) && (inSecondOnly[0].database == "db2") && 
                (inSecondOnly[1].chunk == 4) && (inSecondOnly[1].database == "db3"));

    // Two-way comparison

    BOOST_CHECK(diff2(one, two, inFirstOnly, inSecondOnly));
    BOOST_CHECK(inFirstOnly.size() == 2);
    BOOST_CHECK(inFirstOnly.size() == 2 &&
                (inFirstOnly[0].chunk == 1) && (inFirstOnly[0].database == "db2") && 
                (inFirstOnly[1].chunk == 3) && (inFirstOnly[1].database == "db3"));
    BOOST_CHECK(inSecondOnly.size() == 2);
    BOOST_CHECK(inSecondOnly.size() == 2 &&
                (inSecondOnly[0].chunk == 3) && (inSecondOnly[0].database == "db2") && 
                (inSecondOnly[1].chunk == 4) && (inSecondOnly[1].database == "db3"));

    LOGS_INFO("ReplicaInfo test ends");
}

BOOST_AUTO_TEST_SUITE_END()
