// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

// System headers
#include <map>
#include <set>
#include <string>

// Third-party headers

// Local headers
#include "css/EmptyChunks.h"

// Boost unit test header
#define BOOST_TEST_MODULE testEmptyChunks
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;

using lsst::qserv::css::EmptyChunks;

struct Fixture {
    Fixture() {
        for (int chunk = 1; chunk < 20; ++chunk) {
            database2chunks["TestOne"].insert(chunk);
        }
        for (int chunk = 100; chunk < 200; ++chunk) {
            database2chunks["TestTwo"].insert(chunk);
        }
    }
    ~Fixture() {}
    std::map<std::string, std::set<int>> database2chunks;
};

BOOST_FIXTURE_TEST_SUITE(Suite, Fixture)

BOOST_AUTO_TEST_CASE(Basic) {
    EmptyChunks ec(database2chunks);
    auto s = ec.getEmpty("TestOne");
    BOOST_CHECK(s->find(3) != s->end());
    BOOST_CHECK(s->find(103) == s->end());
    BOOST_CHECK(s->find(1001) == s->end());

    s = ec.getEmpty("TestTwo");
    BOOST_CHECK(s->find(3) == s->end());
    BOOST_CHECK(s->find(103) != s->end());
    BOOST_CHECK(s->find(1001) == s->end());

    BOOST_CHECK(ec.isEmpty("TestOne", 3));
    BOOST_CHECK(ec.isEmpty("TestTwo", 103));
}

BOOST_AUTO_TEST_SUITE_END()
