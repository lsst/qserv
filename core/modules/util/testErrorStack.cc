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

};


BOOST_FIXTURE_TEST_SUITE(Basic, Fixture)

BOOST_AUTO_TEST_CASE(Simple) {

    util::ErrorStack<util::IntStringError> errorStack;
    //BOOST_CHECK(errorStack);
}

BOOST_AUTO_TEST_CASE(Output) {
    std::string out;
    util::ErrorStack<util::IntStringError> errorStack;

    for( int errCode = 10; errCode < 20; errCode = errCode + 1 ) {
        std::stringstream ss;
        ss << "Error code is: " << errCode;
        std::string errMsg = ss.str();
        util::IntStringError error(errCode, errMsg);
        errorStack.push(error);
    }

    std::cout << errorStack;
    //BOOST_CHECK(errorStack);
/*
    unsigned char phSize = *reinterpret_cast<unsigned char const*>(out.data());
    char const* cursor = out.data() + 1;
    int remain = out.size() - 1;
    lsst::qserv::proto::ProtoHeader ph;
    BOOST_REQUIRE(ProtoImporter<ProtoHeader>::setMsgFrom(ph, cursor, phSize));
    cursor += phSize; // Advance to Result msg
    remain -= phSize;
    BOOST_CHECK_EQUAL(remain, ph.size());
    ph.PrintDebugString();
    lsst::qserv::proto::Result result;
    BOOST_REQUIRE(ProtoImporter<Result>::setMsgFrom(result, cursor, remain));
    result.PrintDebugString();
    std::string computedMd5 = util::StringHash::getMd5(cursor, remain);
    BOOST_CHECK_EQUAL(ph.md5(), computedMd5);
    BOOST_CHECK_EQUAL(aa.task->msg->session(), result.session());
    */

}


BOOST_AUTO_TEST_SUITE_END()
