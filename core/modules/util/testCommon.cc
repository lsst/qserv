// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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

#include "StringHelper.h"
// Qserv headers
#include "util/common.h"
#include "util/IterableFormatter.h"

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
    LOGS_DEBUG("strList=" << strList);
    BOOST_CHECK(strList.compare(expectedList) == 0);

    std::string expectedList13{"[[0]=97, [1]=98, [2]=99, [3]=100, [4]=101, [5]=102, [6]=103, [7]=104, "
        "[8]=105, [9]=106, [10]=107, [11]=108, [12]=109, [13]=110, [14]=111, [15]=112, [16]=113, "
        "[17]=114, [18]=115, [19]=116, [20]=117, [21]=118, [22]=119, [23]=120, [24]=121, [25]=122]"};
    auto strList13 = util::prettyCharList(s, 13);
    LOGS_DEBUG("strList13=" << strList13);
    BOOST_CHECK(strList13.compare(expectedList13) == 0);
    auto strList30 = util::prettyCharList(s, 30);
    LOGS_DEBUG("strList30=" << strList30);
    BOOST_CHECK(strList30.compare(expectedList13) == 0);

    std::string expectedList3{"[[0]=97, [1]=98, [2]=99, ..., [23]=120, [24]=121, [25]=122]"};
    auto strList3 = util::prettyCharList(s, 3);
    LOGS_DEBUG("strList3=" << strList3);
    BOOST_CHECK(strList3.compare(expectedList3) == 0);

    const char* buf = s.c_str();
    auto bufLen = strlen(buf);
    auto strBuf13 = util::prettyCharBuf(buf, bufLen, 13);
    LOGS_DEBUG("strBuf13=" << strBuf13);
    BOOST_CHECK(strBuf13.compare(expectedList13) == 0);
    auto strBuf30 = util::prettyCharBuf(buf, bufLen, 30);
    LOGS_DEBUG("strBuf30=" << strBuf30);
    BOOST_CHECK(strBuf30.compare(expectedList13) == 0);

    auto strBuf3 = util::prettyCharBuf(buf, bufLen, 3);
    LOGS_DEBUG("strBuf3=" << strBuf3);
    BOOST_CHECK(strBuf3.compare(expectedList3) == 0);
}

BOOST_AUTO_TEST_CASE(stringToVector) {
    {
        auto vect = util::StringHelper::splitString("testing123,qsa4$3,hjdw q,,7321,ml;oujh", ",");
        LOGS_ERROR("vect=" << util::printable(vect));
        BOOST_CHECK(vect.size() == 6);
        BOOST_CHECK(vect[0] == "testing123");
        BOOST_CHECK(vect[1] == "qsa4$3");
        BOOST_CHECK(vect[2] == "hjdw q");
        BOOST_CHECK(vect[3] == "");
        BOOST_CHECK(vect[4] == "7321");
        BOOST_CHECK(vect[5] == "ml;oujh");
    }
    {
        auto vect = util::StringHelper::splitString("testing123::q:sa4$3:::hjdw q::::7321::ml;oujh", "::");
        BOOST_CHECK(vect.size() == 6);
        BOOST_CHECK(vect[0] == "testing123");
        BOOST_CHECK(vect[1] == "q:sa4$3");
        BOOST_CHECK(vect[2] == ":hjdw q");
        BOOST_CHECK(vect[3] == "");
        BOOST_CHECK(vect[4] == "7321");
        BOOST_CHECK(vect[5] == "ml;oujh");
    }
    {
        auto vect = util::StringHelper::splitString(":testing123:qsa4$3:hjdw q::7321:ml;oujh:", ":");
        BOOST_CHECK(vect.size() == 8);
        BOOST_CHECK(vect[0] == "");
        BOOST_CHECK(vect[1] == "testing123");
        BOOST_CHECK(vect[2] == "qsa4$3");
        BOOST_CHECK(vect[3] == "hjdw q");
        BOOST_CHECK(vect[4] == "");
        BOOST_CHECK(vect[5] == "7321");
        BOOST_CHECK(vect[6] == "ml;oujh");
        BOOST_CHECK(vect[7] == "");
    }
    {
        auto vect = util::StringHelper::splitString("qsa4$3", ":");
        BOOST_CHECK(vect.size() == 1);
        BOOST_CHECK(vect[0] == "qsa4$3");
    }
    {
        auto vect = util::StringHelper::splitString("", ":");
        BOOST_CHECK(vect.size() == 1);
        BOOST_CHECK(vect[0] == "");
    }

    {
        auto vect = util::StringHelper::getIntVectFromStr("987:23:0:1:-123", ":");
        unsigned int j = 0;
        BOOST_CHECK(vect[j++] == 987);
        BOOST_CHECK(vect[j++] == 23);
        BOOST_CHECK(vect[j++] == 0);
        BOOST_CHECK(vect[j++] == 1);
        BOOST_CHECK(vect[j++] == -123);
        BOOST_CHECK(vect.size() == j);
    }
    {
        bool caught=false;
        try {
            auto vect = util::StringHelper::getIntVectFromStr("987:23:x:1:-123", ":");
        } catch (std::invalid_argument const& e) {
            caught=true;
        }
        BOOST_CHECK(caught);
    }
    {
        auto vect = util::StringHelper::getIntVectFromStr("987:23:x8owlq:1:-123:", ":", false, 99);
        unsigned int j = 0;
        BOOST_CHECK(vect[j++] == 987);
        BOOST_CHECK(vect[j++] == 23);
        BOOST_CHECK(vect[j++] == 99);
        BOOST_CHECK(vect[j++] == 1);
        BOOST_CHECK(vect[j++] == -123);
        BOOST_CHECK(vect[j++] == 99);
        BOOST_CHECK(vect.size() == j);
    }
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
