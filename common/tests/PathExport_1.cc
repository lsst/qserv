/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
#define BOOST_TEST_MODULE PathExport_1
#include "boost/test/included/unit_test.hpp"

#include <iostream>

#include "QservPathExport.hh"

namespace test = boost::test_tools;
namespace qsrv = lsst::qserv;

struct PerTestFixture {
    PerTestFixture(void) {}
    ~PerTestFixture(void) { };
};

BOOST_FIXTURE_TEST_SUITE(PathExportTestSuite, PerTestFixture)

BOOST_AUTO_TEST_CASE(PathCreate) {
    std::vector<std::string> v;
    v.push_back("/u1/lsst/export/dir1/fileA");
    v.push_back("/u1/lsst/export/dir1/fileB");
    v.push_back("/u1/lsst/export/dir2/fileC");
    v.push_back("/u1/lsst/export/dir3/fileD");
    
    qsrv::QservPathExport p;
    BOOST_CHECK_EQUAL(p.createPaths(v), 0);
}

BOOST_AUTO_TEST_SUITE_END()
