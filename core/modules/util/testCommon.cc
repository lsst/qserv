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
 *
 * @brief test common.h
 *
 * @author John Gates, SLAC
 */

// System headers
#include <iostream>
#include <sstream>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/common.h"

// Boost unit test header
#define BOOST_TEST_MODULE common
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

namespace util = lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

/** @test
 * Print a MultiError object containing only one error
 */
BOOST_AUTO_TEST_CASE(prettyPrint) {
    std::string s;
    for(auto c='a'; c <= 'z'; ++c) {
        s += c;
    }

    std::string expectedList{"[97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, "
        "110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]"};
    auto strList = util::prettyCharList(s);
    LOGF_INFO("strList=%1%" % strList);
    BOOST_CHECK(strList.compare(expectedList) == 0);

    std::string expectedList13{"[[0]=97, [1]=98, [2]=99, [3]=100, [4]=101, [5]=102, [6]=103, [7]=104, "
        "[8]=105, [9]=106, [10]=107, [11]=108, [12]=109, [13]=110, [14]=111, [15]=112, [16]=113, "
        "[17]=114, [18]=115, [19]=116, [20]=117, [21]=118, [22]=119, [23]=120, [24]=121, [25]=122]"};
    auto strList13 = util::prettyCharList(s, 13);
    LOGF_INFO("strList13=%1%" % strList13);
    BOOST_CHECK(strList13.compare(expectedList13) == 0);
    auto strList30 = util::prettyCharList(s, 30);
    LOGF_INFO("strList30=%1%" % strList30);
    BOOST_CHECK(strList30.compare(expectedList13) == 0);

    std::string expectedList3{"[[0]=97, [1]=98, [2]=99, ..., [23]=120, [24]=121, [25]=122]"};
    auto strList3 = util::prettyCharList(s, 3);
    LOGF_INFO("strList3=%1%" % strList3);
    BOOST_CHECK(strList3.compare(expectedList3) == 0);

    const char* buf = s.c_str();
    auto bufLen = strlen(buf);
    auto strBuf13 = util::prettyCharBuf(buf, bufLen, 13);
    LOGF_INFO("strBuf13=%1%" % strBuf13);
    BOOST_CHECK(strBuf13.compare(expectedList13) == 0);
    auto strBuf30 = util::prettyCharBuf(buf, bufLen, 30);
    LOGF_INFO("strBuf30=%1%" % strBuf30);
    BOOST_CHECK(strBuf30.compare(expectedList13) == 0);

    auto strBuf3 = util::prettyCharBuf(buf, bufLen, 3);
    LOGF_INFO("strBuf3=%1%" % strBuf3);
    BOOST_CHECK(strBuf3.compare(expectedList3) == 0);
}
/*
    test::output_test_stream output;
    util::MultiError multiError;

    std::string expected_err_msg = util::MultiError::HEADER_MSG +
            "\t[1] Stupid error message";

    int errCode = 1;
    std::string errMsg = "Stupid error message";
    util::Error error(errCode, errMsg);
    multiError.push_back(error);

    output << multiError;
    std::cout << multiError;
    BOOST_REQUIRE(output.is_equal(expected_err_msg));
}
*/


BOOST_AUTO_TEST_SUITE_END()
