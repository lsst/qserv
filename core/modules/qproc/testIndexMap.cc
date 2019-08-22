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

 /**
  * @file
  *
  * @brief Test SecondaryIndex and IndexMap operations.
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

// Qserv headers
#include "global/intTypes.h"
#include "qproc/ChunkSpec.h"
#include "qproc/SecondaryIndex.h"
#include "query/InPredicate.h"
#include "query/QsRestrictor.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "util/IterableFormatter.h"

// Boost unit test header
#define BOOST_TEST_MODULE IndexMap
#include "boost/test/included/unit_test.hpp"


namespace test = boost::test_tools;

using namespace lsst::qserv;

using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::ChunkSpecVector;
using lsst::qserv::qproc::SecondaryIndex;
using lsst::qserv::IntVector;

struct Fixture {
    Fixture(void) :
        si() {
    }

    ~Fixture(void) { };

    SecondaryIndex si;
    // TODO: IndexMap with fake secondary index: see DM-4047
    // TODO: 2+ StripingParams sets.
};

////////////////////////////////////////////////////////////////////////
// SecondaryIndex and IndexMap test suite
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(Suite, Fixture)

BOOST_AUTO_TEST_CASE(SecLookup) {
    std::vector<std::string> vals = {"111", "112","113"};
    std::vector<std::shared_ptr<query::ValueExpr>> inCandidates = {
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor("386950783579546")),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor("386942193651348"))};
    auto inPredicate = std::make_shared<query::InPredicate>(
        query::ValueExpr::newColumnExpr("LSST", "Object", "", "objectId"),
        inCandidates,
        false);
    ChunkSpecVector csv = si.lookup({std::make_shared<query::SIInRestrictor>(inPredicate)});
    // Verify the values produced by the SecondaryIndex FakeBackend...
    // (The only thing this really verifies is that a secondary index restrictor instance was passed in to
    // the lookup function.)
    BOOST_CHECK_EQUAL(csv.size(), 3u);
    BOOST_CHECK_EQUAL(csv[0], ChunkSpec(100, {1, 2, 3}));
    BOOST_CHECK_EQUAL(csv[1], ChunkSpec(101, {1, 2, 3}));
    BOOST_CHECK_EQUAL(csv[2], ChunkSpec(102, {1, 2, 3}));
}

BOOST_AUTO_TEST_CASE(IndLookupArea) {
    // Lookup area using IndexMap interface
}

BOOST_AUTO_TEST_CASE(IndLookupPoint) {
    // Lookup specific fake points in the secondary index using IndexMap
    // interface.
}

BOOST_AUTO_TEST_SUITE_END()
