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
    qsrv::QservPathExport p;

    std::vector<std::string> pV;
    pV.push_back("/u1/qserv/export/dir1/fileA");
    pV.push_back("/u1/qserv/export/dir1/fileB");
    pV.push_back("/u1/qserv/export/dir2/fileC");
    pV.push_back("/u1/qserv/export/dir3/fileD");

    std::vector<std::string> dV;
    BOOST_CHECK_EQUAL(p.extractUniqueDirs(pV, dV), true);
    BOOST_CHECK_EQUAL(dV.size(), 6);
    std::vector<std::string>::iterator dItr;
    for ( dItr=dV.begin(); dItr!=dV.end(); ++dItr) {
        std::cout << "found unique path: " << *dItr << std::endl;
    }
    BOOST_CHECK_EQUAL(p.mkDirs(dV), true);
    
    dV.clear();    
    pV.push_back("fileD");
    BOOST_CHECK_EQUAL(p.extractUniqueDirs(pV, dV), false);
}

BOOST_AUTO_TEST_SUITE_END()
