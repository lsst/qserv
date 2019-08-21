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
#include "query/QsRestrictor.h"

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
#if 0 // TODO

BOOST_AUTO_TEST_CASE(SecLookup) {
    query::QsRestrictor::PtrVector restrictors;
    std::vector<std::string> vals = {"111", "112","113"};
    restrictors.push_back(std::make_shared<query::QsRestrictorFunction>("sIndex", vals));
    ChunkSpecVector csv = si.lookup(restrictors);
    std::cout << "SecLookup\n";
    std::copy(csv.begin(), csv.end(),
              std::ostream_iterator<ChunkSpec>(std::cout, ",\n"));
}

BOOST_AUTO_TEST_CASE(SecLookupMultipleObjectIdIN) {
    query::QsRestrictor::PtrVector restrictors;
    std::vector<std::string> vals = {"LSST", "Object", "objectId", "386950783579546", "386942193651348"};
    restrictors.push_back(std::make_shared<query::QsRestrictorFunction>("sIndex", vals));
    ChunkSpecVector csv = si.lookup(restrictors);
    std::cout << "SecLookupMultipleObjectIdIN\n";
    std::copy(csv.begin(), csv.end(),
              std::ostream_iterator<ChunkSpec>(std::cout, ",\n"));
}

BOOST_AUTO_TEST_CASE(SecLookupMultipleObjectIdBETWEEN) {
    query::QsRestrictor::PtrVector restrictors;
    std::vector<std::string> vals = {"LSST", "Object", "objectId", "386942193651348", "386950783579546"};
    restrictors.push_back(std::make_shared<query::QsRestrictorFunction>("sIndexBetWeen", vals));
    ChunkSpecVector csv = si.lookup(restrictors);
    std::cout << "SecLookupMultipleObjectIdBETWEEN\n";
    std::copy(csv.begin(), csv.end(),
              std::ostream_iterator<ChunkSpec>(std::cout, ",\n"));
}

BOOST_AUTO_TEST_CASE(IndLookupArea) {
    // Lookup area using IndexMap interface
}

BOOST_AUTO_TEST_CASE(IndLookupPoint) {
    // Lookup specific fake points in the secondary index using IndexMap
    // interface.
}

#endif

BOOST_AUTO_TEST_SUITE_END()
