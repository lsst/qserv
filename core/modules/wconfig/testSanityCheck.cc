/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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

#define BOOST_TEST_MODULE SanityCheck
#include "boost/test/included/unit_test.hpp"

// System headers
#include <iostream>

// Third-party headers
#include "boost/scoped_ptr.hpp"

// Local headers
#include "wconfig/Config.h"


namespace test = boost::test_tools;
using lsst::qserv::worker::Config;

struct SanityFixture {
    SanityFixture(void) {
    };
    ~SanityFixture(void) { };
};

BOOST_FIXTURE_TEST_SUITE(SanityCheckTest, SanityFixture)

BOOST_AUTO_TEST_CASE(CheckGood) {
    Config c;
    BOOST_CHECK_EQUAL(c.getError(), "");
}

BOOST_AUTO_TEST_SUITE_END()
