// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
  * @brief test ChunkLocker
  */

// System headers
#include <sstream>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/ChunkNumber.h"

// Boost unit test header
#define BOOST_TEST_MODULE ChunkNumber
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

namespace {
    
/// One more policy for policy equality tests

}


BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ChunkNumberTest) {

    LOGS_INFO("ChunkNumber test begins");

    // -------------------------
    // Test equality of policies

    ChunkNumberValidator::Ptr const validator1 = std::make_shared<ChunkNumberSingleRangeValidator>(0, 1000);
    ChunkNumberValidator::Ptr const validator2 = std::make_shared<ChunkNumberSingleRangeValidator>(0, 1000);
    
    BOOST_CHECK(*validator1 == *validator1);
    BOOST_CHECK(*validator1 != *validator2);

    // ----------------------
    // Test policy interfaces
    // ----------------------

    unsigned int const beginRange = 10,
                         endRange = 20;

    ChunkNumberValidator::Ptr const validator10to20 =
        std::make_shared<ChunkNumberSingleRangeValidator>(beginRange, endRange);

    // Overflow values are always valid
    
    BOOST_CHECK(validator10to20->valid(   validator10to20->overflowValue()));
    BOOST_CHECK(validator10to20->overflow(validator10to20->overflowValue()));

    // All values within a range (inclusive at both ends) are also valid
    // Others aren't.

    for (unsigned int value = 0; value < beginRange; ++value) {
        BOOST_CHECK(not validator10to20->valid(value));
    }
    for (unsigned int value = beginRange; value <= endRange; ++value) {
        BOOST_CHECK(validator10to20->valid(value));
    }
    for (unsigned int value = endRange + 1; value < endRange + 2; ++value) {
        BOOST_CHECK(not validator10to20->valid(value));
    }

    // ------------------
    // Test chunk numbers
    // ------------------

    // Default constructor

    ChunkNumber const defaultConstrutedChunk;

    BOOST_CHECK(not defaultConstrutedChunk.valid());
    BOOST_CHECK(not defaultConstrutedChunk.overflow());

    // Factory method for the overflow chunk

    ChunkNumber const overflowChunk = ChunkNumber::makeOverflow(validator10to20);

    BOOST_CHECK(overflowChunk.valid());
    BOOST_CHECK(overflowChunk.overflow());

    BOOST_CHECK(*(defaultConstrutedChunk.validator()) != *(overflowChunk.validator()));

    // Copy constructor (inherit a state of the input object)

    ChunkNumber const copyConstructedChunk(overflowChunk);

    BOOST_CHECK(*(copyConstructedChunk.validator()) == *(overflowChunk.validator()));
    BOOST_CHECK(copyConstructedChunk.valid());
    BOOST_CHECK(copyConstructedChunk.overflow());
    BOOST_CHECK(copyConstructedChunk == overflowChunk);

    // Assignment operator (inherit a state of the input object)

    ChunkNumber assignConstructedChunk;
    assignConstructedChunk = overflowChunk;

    BOOST_CHECK(*(assignConstructedChunk.validator()) == *(overflowChunk.validator()));
    BOOST_CHECK(assignConstructedChunk.valid());
    BOOST_CHECK(assignConstructedChunk.overflow());
    BOOST_CHECK(assignConstructedChunk == overflowChunk);

    // Range scan.
    //
    // All values within a range (inclusive at both ends) are also valid
    // Others aren't (unless it's the overflow)

    for (unsigned int value = 0; value < beginRange; ++value) {
        if (validator10to20->overflow(value)) {
            BOOST_REQUIRE_NO_THROW(ChunkNumber(value, validator10to20));
            ChunkNumber const chunk(value, validator10to20);
            BOOST_CHECK(chunk.valid());
            BOOST_CHECK(chunk.overflow());
        } else {
            BOOST_CHECK_THROW(ChunkNumber(value, validator10to20), ChunkNumberNotValid);
        }
    }
    for (unsigned int value = beginRange; value <= endRange; ++value) {
        BOOST_REQUIRE_NO_THROW(ChunkNumber(value, validator10to20));
        ChunkNumber const chunk(value, validator10to20);
        if (validator10to20->overflow(value)) {
            BOOST_CHECK(chunk.valid());
            BOOST_CHECK(chunk.overflow());
        } else {
            BOOST_CHECK(chunk.valid());
        }
    }
    for (unsigned int value = endRange + 1; value < endRange + 2; ++value) {
        if (validator10to20->overflow(value)) {
            BOOST_REQUIRE_NO_THROW(ChunkNumber(value, validator10to20));
            ChunkNumber const chunk(value, validator10to20);
            BOOST_CHECK(chunk.valid());
            BOOST_CHECK(chunk.overflow());
        } else {
            BOOST_CHECK_THROW(ChunkNumber(value, validator10to20), ChunkNumberNotValid);
        }
    }

    // Compare chunks constructed with the same validator

    ChunkNumber const chunk10(10, validator10to20);
    ChunkNumber const chunk11(11, validator10to20);

    BOOST_CHECK(chunk10 != chunk11);
    BOOST_CHECK(chunk10  < chunk11);

    BOOST_CHECK(chunk10 == 10);
    BOOST_CHECK(chunk10 != 11);
    BOOST_CHECK(chunk10  < 11);

    BOOST_CHECK(chunk10.value() == 10);
    BOOST_CHECK(chunk10.value() != 11);
    BOOST_CHECK(chunk10.value()  < 11);

    // Compare chunks constructed with different validators

    ChunkNumberSingleRangeValidator::Ptr const otherValidator10to20 =
        std::make_shared<ChunkNumberSingleRangeValidator>(beginRange, endRange);

    BOOST_CHECK(*otherValidator10to20 != *validator10to20);

    ChunkNumber const otherChunk10(10, otherValidator10to20);
    ChunkNumber const otherChunk11(10, otherValidator10to20);

    BOOST_CHECK_THROW(chunk10 == otherChunk10, ChunkNumberNotValid);
    BOOST_CHECK_THROW(chunk10 != otherChunk11, ChunkNumberNotValid);
    BOOST_CHECK_THROW(chunk10  < otherChunk11, ChunkNumberNotValid);

    // Though, these tests should be fine

    BOOST_CHECK(otherChunk10.value() == chunk10.value());
    BOOST_CHECK(otherChunk10.value() != chunk11.value());
    BOOST_CHECK(otherChunk10.value()  < chunk11.value());

    // Testing the streaming operator

    std::ostringstream ss;
    ss << defaultConstrutedChunk;

    BOOST_CHECK_EQUAL(ss.str(), "invalid");

    ss.str(std::string());
    ss << chunk10;
    BOOST_CHECK_EQUAL(ss.str(), "10");

    LOGS_INFO("ChunkNumber test ends");
}

BOOST_AUTO_TEST_SUITE_END()
