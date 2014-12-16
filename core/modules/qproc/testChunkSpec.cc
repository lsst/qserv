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

 /**
  * @file
  *
  * @brief Test ChunkSpec operations.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <sstream>
#include <string>

// Third-party headers
#include "boost/algorithm/string.hpp"

// Local headers
#include "global/intTypes.h"
#include "qproc/ChunkSpec.h"

// Boost unit test header
#define BOOST_TEST_MODULE ChunkSpec
#include "boost/test/included/unit_test.hpp"


namespace test = boost::test_tools;

using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::ChunkSpecVector;
using lsst::qserv::IntVector;

namespace {
    ChunkSpec c1 = ChunkSpec::makeFake(101);
    ChunkSpec c2 = ChunkSpec::makeFake(102);
    ChunkSpec c3 = ChunkSpec::makeFake(103);

} // anonymous namespace

struct Fixture {
    Fixture(void) :
        c1(ChunkSpec::makeFake(101)),
        c2(ChunkSpec::makeFake(102)),
        c3(ChunkSpec::makeFake(103)),
        c4(ChunkSpec::makeFake(104)),
        c5(ChunkSpec::makeFake(105)) {
    }
   
    ~Fixture(void) { };

    ChunkSpec c1;
    ChunkSpec c2;
    ChunkSpec c3;
    ChunkSpec c4;
    ChunkSpec c5;
};

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(Suite, Fixture)

BOOST_AUTO_TEST_CASE(Basic) {
    BOOST_CHECK_EQUAL(c1.chunkId, 101);
    BOOST_CHECK_EQUAL(c2.chunkId, 102);
    BOOST_CHECK_EQUAL(c3.chunkId, 103);
    BOOST_CHECK_EQUAL(c4.chunkId, 104);
    BOOST_CHECK_EQUAL(c5.chunkId, 105);
}

BOOST_AUTO_TEST_CASE(Intersect) {
    ChunkSpec c1c2;

    int forC1 = 5;
    int forC2 = 10;
    c1.subChunks.push_back(1);
    c2.subChunks.push_back(2);
    c3.subChunks.push_back(3);

    for(int i=10; i < 20; ++i) {
        if(forC1 > 0) {
            c1.subChunks.push_back(i);
            --forC1;
        }
        if(forC2 > 0) {
            c2.subChunks.push_back(i);
            --forC2;
        }
        c3.subChunks.push_back(i);
    }
    // Throws because chunkIds are different.
    BOOST_CHECK_THROW(c1c2 = c1.intersect(c2), std::logic_error);
    c1.chunkId = c2.chunkId = 100; // Make them the same
    c1c2 = c1.intersect(c2);
    BOOST_CHECK_EQUAL(c1c2.subChunks.size(), 5);
    BOOST_CHECK(c1c2.subChunks.size() != c2.subChunks.size());
    
    // Same results after shuffling
    std::random_shuffle(c1c2.subChunks.begin(), c1c2.subChunks.end());
    ChunkSpec nc1c2 = c1c2.intersect(c2);
    // Sort c1c2 so that the equals comparison works.
    std::sort(c1c2.subChunks.begin(), c1c2.subChunks.end());
    BOOST_CHECK_EQUAL(c1c2, nc1c2);
}
BOOST_AUTO_TEST_CASE(Vector) {
    // Test the intersection where:
    // ChunkSpec is the same
    // ChunkSpec has the same chunkId, but share no subChunks
    // ChunkSpec has the same chunkId, and share some subchunks
    // ChunkSpec has the same chunkId, and one has no subchunks.
    // Non-matching chunkId
    c1 = ChunkSpec::makeFake(11, true);
    c2 = ChunkSpec::makeFake(12, true);
    c3 = ChunkSpec::makeFake(13, true);
    c4 = ChunkSpec::makeFake(14, true);
    c5 = ChunkSpec::makeFake(15, true);

    ChunkSpecVector v1;
    v1.push_back(c1);
    v1.push_back(c2);
    v1.push_back(c3);
    v1.push_back(c4);
    v1.push_back(c5);

    ChunkSpecVector v2;
    c1.chunkId = 42; // Different chunkId
    // Same chunkId, different subchunks
    for(IntVector::iterator i=c2.subChunks.begin(), e=c2.subChunks.end();
        i != e; ++i) {
        *i += 100;
    }
    // Same chunkId, some subChunks shared.
    c3.subChunks[0] -= 4;
    // Same chunkId, one has no subChunks
    c4.subChunks.clear();
    // c5: exactly the same

    v2.push_back(c1);
    v2.push_back(c2);
    v2.push_back(c3);
    v2.push_back(c4);
    v2.push_back(c5);
    
    ChunkSpecVector v1v2 = intersect(v1, v2);
}

BOOST_AUTO_TEST_SUITE_END()
