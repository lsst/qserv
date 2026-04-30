// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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
#include "util/MultiError.h"

// Boost unit test header
#define BOOST_TEST_MODULE MultiError
#include <boost/test/unit_test.hpp>
#include <boost/test/tools/output_test_stream.hpp>

namespace test = boost::test_tools;

namespace util = lsst::qserv::util;

struct Fixture {
    void _throw_it(std::exception e) { throw e; }
};

BOOST_FIXTURE_TEST_SUITE(Basic, Fixture)

/** @test
 * Print a MultiError object containing only one error
 */
BOOST_AUTO_TEST_CASE(MonoError) {
    test::output_test_stream output;
    util::MultiError multiError;

    std::string expected_err_msg = "[count=1][code=1] Stupid error message";

    int errCode = 1;
    std::string errMsg = "Stupid error message";
    util::Error error(errCode, util::Error::NONE, errMsg);
    multiError.insert(error);

    output << multiError;
    std::cout << multiError;
    BOOST_REQUIRE(output.is_equal(expected_err_msg));
}

/** @test
 * Print a MultiError object containing several errors
 */
BOOST_AUTO_TEST_CASE(MultiError) {
    util::MultiError multiError;

    std::string expected_err_msg =
            "[count=1][code=10] Error code is: 10\n"
            "[count=1][code=11] Error code is: 11\n"
            "[count=1][code=12] Error code is: 12";

    for (int errCode = 10; errCode < 13; errCode = errCode + 1) {
        std::stringstream ss;
        ss << "Error code is: " << errCode;
        std::string errMsg = ss.str();
        util::Error error(errCode, util::Error::NONE, errMsg);
        multiError.insert(error);
    }

    test::output_test_stream output;
    output << multiError;
    std::cout << "a=" << multiError << std::endl;
    BOOST_CHECK(output.is_equal(expected_err_msg));

    util::MultiError multiErrB(multiError);
    test::output_test_stream outputB;
    outputB << multiErrB;
    std::cout << "b=" << multiErrB << std::endl;
    BOOST_CHECK(outputB.is_equal(expected_err_msg));
}

BOOST_AUTO_TEST_CASE(MultiErrorEqual) {
    test::output_test_stream output;
    util::MultiError multiErrorA;
    util::MultiError multiErrorB;
    util::MultiError multiErrorC;

    BOOST_REQUIRE(multiErrorA == multiErrorB);
    BOOST_REQUIRE(multiErrorA == multiErrorC);

    util::Error err1(34, util::Error::NONE, "test rando");
    util::Error err2(-1, 25, "cancel blah");
    multiErrorA.insert(err1);
    BOOST_REQUIRE(multiErrorA != multiErrorB);
    multiErrorB.insert(err2);
    BOOST_REQUIRE(multiErrorA != multiErrorB);
    multiErrorB.insert(err1);
    BOOST_REQUIRE(multiErrorA != multiErrorB);
    multiErrorC.insert(err1);
    BOOST_REQUIRE(multiErrorA == multiErrorC);
    multiErrorA.insert(err2);
    BOOST_REQUIRE(multiErrorA == multiErrorB);
    BOOST_REQUIRE(multiErrorA != multiErrorC);
}

BOOST_AUTO_TEST_SUITE_END()
