// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
 * @brief test MultiError class
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

// System headers
#include <iostream>
#include <sstream>
#include <string>

// Third-party headers

// Qserv headers
#include "MultiError.h"

// Boost unit test header
#define BOOST_TEST_MODULE multiError
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

namespace util = lsst::qserv::util;

struct Fixture {
    void _throw_it(std::exception e) {
        throw e;
    }
};

BOOST_FIXTURE_TEST_SUITE(Basic, Fixture)

/** @test
 * Print a MultiError object containing only one error
 */
BOOST_AUTO_TEST_CASE(MonoError) {

    test::output_test_stream output;
    util::MultiError multiError;

    int errCode = 1;
    std::string errMsg = "Stupid error message";
    util::Error error(errCode, errMsg);
    multiError.push_back(error);

    output << multiError;
    std::cout << multiError;
    BOOST_REQUIRE(output.is_equal("[1] Stupid error message\n"));
}

/** @test
 * Print a MultiError object containing several errors
 */
BOOST_AUTO_TEST_CASE(MultiError) {

    test::output_test_stream output;
    util::MultiError multiError;

    const char* str = "Multi-error:\n"
            "[10] Error code is: 10\n"
            "[11] Error code is: 11\n"
            "[12] Error code is: 12\n";

    for (int errCode = 10; errCode < 13; errCode = errCode + 1) {
        std::stringstream ss;
        ss << "Error code is: " << errCode;
        std::string errMsg = ss.str();
        util::Error error(errCode, errMsg);
        multiError.push_back(error);
    }

    output << multiError;
    std::cout << multiError;
    BOOST_CHECK(output.is_equal(str));
}

/** @test
 * Throw a MultiError object containing one error
 */
BOOST_AUTO_TEST_CASE(ThrowMultiError) {
    util::MultiError multiError;
    int errCode = 5;
    std::string errMsg = "Error stack thrown";
    util::Error error(errCode, errMsg);
    multiError.push_back(error);

    BOOST_REQUIRE_THROW(_throw_it(multiError), std::exception);
}

BOOST_AUTO_TEST_CASE(Exception) {
    std::string out;
}

BOOST_AUTO_TEST_SUITE_END()
