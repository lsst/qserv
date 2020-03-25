/*
 * LSST Data Management System
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

#define BOOST_TEST_MODULE Global Spatial Index

// Boost unit test header (must come after the BOOST_TEST_... defines)
#include "boost/test/included/unit_test.hpp"
#include "boost/test/unit_test_suite.hpp"
#include "boost/test/test_tools.hpp"

// System headers
#include <stdexcept>

// Qserv headers
#include "DirIdxRedisClient.h"


using namespace lsst::qserv::diridx;

namespace lsst {
namespace qserv {
namespace diridx {

class TestAccessor {
public:
    static std::pair<unsigned long long, short> getSubIdAndField(unsigned long long objectId) {
        return DirIdxRedisClient::_getSubIdAndField(objectId);
    }

    static std::string combineChunkData(unsigned long long chunkId, unsigned short subChunkId) {
        return DirIdxRedisClient::_combineChunkData(chunkId, subChunkId);
    }

    static ChunkData splitChunkData(std::string chunkData) {
        return DirIdxRedisClient::_splitChunkData(chunkData);
    }
};

}}}

BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(makeSubIdAndField) {
    auto subIdAndField = TestAccessor::getSubIdAndField(123456789);
    BOOST_CHECK_EQUAL(subIdAndField.first, 1234567u);
    BOOST_CHECK_EQUAL(subIdAndField.second, 89);
}


BOOST_AUTO_TEST_CASE(combineChunkData) {
    BOOST_CHECK_EQUAL(TestAccessor::combineChunkData(18446744073709551615u, 65535u),
                      "18446744073709551615.65535");
    BOOST_CHECK_EQUAL(TestAccessor::combineChunkData(0, 0),
                      "0.0");
}


BOOST_AUTO_TEST_CASE(splitChunkData) {
    auto chunkData = TestAccessor::splitChunkData("18446744073709551615.65535");
    BOOST_CHECK_EQUAL(chunkData.chunkId(), 18446744073709551615u);
    BOOST_CHECK_EQUAL(chunkData.subChunkId(), 65535u);
}


BOOST_AUTO_TEST_CASE(splitChunkData_invalidData) {
    // the string must be have a dot with a number on each side:
    BOOST_CHECK_THROW(TestAccessor::splitChunkData(""), std::runtime_error);
    BOOST_CHECK_THROW(TestAccessor::splitChunkData("."), std::runtime_error);
    BOOST_CHECK_THROW(TestAccessor::splitChunkData(".123"), std::runtime_error);
    BOOST_CHECK_THROW(TestAccessor::splitChunkData("123."), std::runtime_error);
    // the string must not contain letters (numbers only)
    BOOST_CHECK_THROW(TestAccessor::splitChunkData("abc.def"), std::runtime_error);
}



BOOST_AUTO_TEST_SUITE_END()

