// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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
  *
  * @brief Simple testing for the TableRef class
  *
  */

// System headers
#include <fstream>

// list must be included before boost/test/data/test_case.hpp, because it is used there but not included.
// (or that file could be included after boost/test/included/unit_test.hpp, which does cause list to be
// included. But, we like to include our headers alphabetically so I'm including list here.
#include <list>
#include <stdexcept>
#include <string>

// Qserv headers
#include "query/TableRef.h"

// Boost unit test header
#define BOOST_TEST_MODULE TableRef
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"


using namespace lsst::qserv::query;


BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(verifyPopulated_noDb) {
    TableRef tableRef("", "table", "");
    BOOST_REQUIRE_THROW(tableRef.verifyPopulated(), std::logic_error);
}

BOOST_AUTO_TEST_CASE(verifyPopulated_setDb) {
    TableRef tableRef("", "table", "");
    tableRef.verifyPopulated("database");
    TableRef expectedTableRef("database", "table", "");
    BOOST_REQUIRE_EQUAL(tableRef, expectedTableRef);
}

BOOST_AUTO_TEST_CASE(verifyPopulated_dbIsSet) {
    TableRef tableRef("database", "table", "");
    tableRef.verifyPopulated();
    TableRef expectedTableRef("database", "table", "");
    BOOST_REQUIRE_EQUAL(tableRef, expectedTableRef);
}

BOOST_AUTO_TEST_CASE(verifyPopulated_dbIsSetIgnoreDefault) {
    TableRef tableRef("database", "table", "");
    tableRef.verifyPopulated("otherDatabase");
    TableRef expectedTableRef("database", "table", "");
    BOOST_REQUIRE_EQUAL(tableRef, expectedTableRef);
}

BOOST_AUTO_TEST_SUITE_END()
