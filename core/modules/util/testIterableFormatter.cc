// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
 * @ingroup util
 *
 * @brief test IterableFormatter class
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

// System headers
#include <iostream>
#include <sstream>
#include <string>

// Third-party headers

// Qserv headers
#include "util/IterableFormatter.h"

// Boost unit test header
#define BOOST_TEST_MODULE IterableFormatter
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace util = lsst::qserv::util;


BOOST_AUTO_TEST_SUITE(Suite)

/** @test
 * Print an empty vector
 */
BOOST_AUTO_TEST_CASE(EmptyVector) {

    test::output_test_stream output;
    std::vector<int> iterable {};
    auto formatable = util::printable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[]"));
}

/** @test
 * Print a vector of int with default formatting
 */
BOOST_AUTO_TEST_CASE(Vector) {

    test::output_test_stream output;
    std::vector<int> iterable { 1, 2, 3, 4, 5, 6};
    auto formatable = util::printable(iterable);

    output << formatable;
    BOOST_REQUIRE(output.is_equal("[1, 2, 3, 4, 5, 6]"));
}

/** @test
 * Print a list of string with custom formatting
 */
BOOST_AUTO_TEST_CASE(Array) {

    test::output_test_stream output;
    std::array<std::string, 6> iterable { {"1", "2", "3", "4", "5", "6"} };
    auto start = std::next(iterable.begin(), 2);
    auto formatable = util::printable( start, iterable.end(), "", "", "; ");

    output << formatable;
    BOOST_REQUIRE(output.is_equal("3; 4; 5; 6"));
}

BOOST_AUTO_TEST_SUITE_END()
