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
/**
 * @brief Test data types shared by all classes of the module.
 */

// System headers
#include <string>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/Common.h"

// Boost unit test header
#define BOOST_TEST_MODULE CommonTypes
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(CommonTypesTest) {
    LOGS_INFO("CommonTypesTest test begins");

    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(overlapSelector2str(ChunkOverlapSelector::CHUNK), "CHUNK");
        BOOST_CHECK_EQUAL(overlapSelector2str(ChunkOverlapSelector::OVERLAP), "OVERLAP");
        BOOST_CHECK_EQUAL(overlapSelector2str(ChunkOverlapSelector::CHUNK_AND_OVERLAP), "CHUNK_AND_OVERLAP");
    });
    BOOST_CHECK_THROW({ overlapSelector2str(static_cast<ChunkOverlapSelector>(0)); }, invalid_argument);

    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(str2overlapSelector("CHUNK"), ChunkOverlapSelector::CHUNK);
        BOOST_CHECK_EQUAL(str2overlapSelector("OVERLAP"), ChunkOverlapSelector::OVERLAP);
        BOOST_CHECK_EQUAL(str2overlapSelector("CHUNK_AND_OVERLAP"), ChunkOverlapSelector::CHUNK_AND_OVERLAP);
    });
    BOOST_CHECK_THROW(
            {
                str2overlapSelector("");
                str2overlapSelector("ABC");
            },
            invalid_argument);

    LOGS_INFO("CommonTypesTest test ends");
}

BOOST_AUTO_TEST_SUITE_END()
