// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
/// testResourceUnit

// Third-party headers

// Qserv headers
#include "global/ResourceUnit.h"

// Boost unit test header
#define BOOST_TEST_MODULE ResourceUnit_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using lsst::qserv::ResourceUnit;

struct Fixture {
    Fixture() {}

    int dummy;
    ~Fixture(void) {};
};
int const MAGIC_SIZE = 80;

BOOST_FIXTURE_TEST_SUITE(Suite, Fixture)

BOOST_AUTO_TEST_CASE(Garbage) {
    char p[][MAGIC_SIZE] = { // Convert to std vector list init when available
        // Missing chunk number
        "/chk/qservTest_case01_qserv",
        "/chk/abc/",
        // Bad resource type
        "/chk2/abc",
        "/abc/",
        "/abc/chk/g",
        // Missing/bad params
        "/q",
        "/q/",
        "/q/Hello",
        "/result",
        "/result/"
        };
    int const pSize = 10;
    for(auto i=p, e=p+pSize; i != e; ++i) {
        ResourceUnit r(*i);
        BOOST_CHECK_MESSAGE(
            r.unitType() == ResourceUnit::GARBAGE,
            std::string("Expected garbage: ") + *i);
    }    
}

BOOST_AUTO_TEST_CASE(DbChunk) {
    char p[][MAGIC_SIZE] = {
        "/chk/qservTest_case01_qserv/123",
        "/chk/abc/456",
    };
    int const pSize = 2;
    std::vector<ResourceUnit> r;
    for(auto i=p, e=p+pSize; i != e; ++i) {
        r.push_back(ResourceUnit(*i));
        BOOST_CHECK_EQUAL(r.back().unitType(), ResourceUnit::DBCHUNK);
    }    
    BOOST_CHECK_EQUAL(r[0].db(), "qservTest_case01_qserv");
    BOOST_CHECK_EQUAL(r[1].db(), "abc");
    BOOST_CHECK_EQUAL(r[0].chunk(), 123);
    BOOST_CHECK_EQUAL(r[1].chunk(), 456);

    r[0].setAsDbChunk("foo", 1111);
    r[1].setAsDbChunk("bar", 968);
    BOOST_CHECK_EQUAL(r[0].path(), "/chk/foo/1111");
    BOOST_CHECK_EQUAL(r[1].path(), "/chk/bar/968");
}

BOOST_AUTO_TEST_CASE(Old) {
    ResourceUnit cq("/q/Foo/123");
    ResourceUnit res("/result/1234567890abcde");
    BOOST_CHECK_EQUAL(cq.unitType(), ResourceUnit::CQUERY);
    BOOST_CHECK_EQUAL(res.unitType(), ResourceUnit::RESULT);
}

BOOST_AUTO_TEST_SUITE_END()
