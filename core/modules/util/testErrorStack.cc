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
 * @brief Simple testing for class ErrorStack
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

// System headers
#include <iostream>
#include <sstream>
#include <string>

// Third-party headers

// Qserv headers
#include "util/ErrorStack.h"

// Boost unit test header
#define BOOST_TEST_MODULE ErrorStack
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

namespace util = lsst::qserv::util;

struct Fixture {
    void throw_it(std::exception e)
    {
      throw e;
    }
};

BOOST_FIXTURE_TEST_SUITE(Basic, Fixture)

BOOST_AUTO_TEST_CASE(MonoError) {

    test::output_test_stream output;
    util::ErrorStack errorStack;

    int errCode = 1;
    std::string errMsg = "Stupid error message";
    util::Error error(errCode, errMsg);
    errorStack.push(error);

    output << errorStack;
    std::cout << errorStack;
    BOOST_REQUIRE(output.is_equal("[1] Stupid error message\n"));
}

BOOST_AUTO_TEST_CASE(MultiError) {

    test::output_test_stream output;
    util::ErrorStack errorStack;

    const char* str = "Multi-error:\n"
            "[10] Error code is: 10\n"
            "[11] Error code is: 11\n"
            "[12] Error code is: 12\n";

    for (int errCode = 10; errCode < 13; errCode = errCode + 1) {
        std::stringstream ss;
        ss << "Error code is: " << errCode;
        std::string errMsg = ss.str();
        util::Error error(errCode, errMsg);
        errorStack.push(error);
    }

    output << errorStack;
    std::cout << errorStack;
    BOOST_CHECK(output.is_equal(str));
}

BOOST_AUTO_TEST_CASE(ThrowErrorStack) {
    util::ErrorStack errorStack;
    int errCode = 5;
    std::string errMsg = "Error stack thrown";
    util::Error error(errCode, errMsg);
    errorStack.push(error);

    BOOST_REQUIRE_THROW(throw_it(errorStack), std::exception);
}

BOOST_AUTO_TEST_CASE(Exception) {
    std::string out;
}

BOOST_AUTO_TEST_SUITE_END()
