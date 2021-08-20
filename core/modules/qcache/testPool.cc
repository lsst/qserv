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
  * @brief test ReplicaInfo
  */

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qcache/Exceptions.h"
#include "qcache/Page.h"
#include "qcache/Pool.h"

// System headers
#include <cstring>

// Boost unit test header
#define BOOST_TEST_MODULE Pool
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv::qcache;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(PoolTest) {

    LOGS_INFO("Pool test begins");

    std::size_t const pageCapacityBytes = 124;
    std::size_t const numPages = 2;
    std::shared_ptr<Pool> const pool = Pool::create(pageCapacityBytes, numPages);

    const char* s1 = "A";
    const char* s2 = "BC";
    const char* s3 = "DEF";
    const char* s4 = nullptr;

    unsigned int const numFields = 4;
    char const* row[] = {s1, s2, s3, s4};
    long const lengths[] = {
        static_cast<long>(std::strlen(s1)),
        static_cast<long>(std::strlen(s2)),
        static_cast<long>(std::strlen(s3)),
        0
    };
    size_t expectedSizeBytes = numFields * sizeof(long);
    for (unsigned int i = 0; i < numFields; ++i) {
        expectedSizeBytes += lengths[i];
    }

    // Create the page that has the capacity equal to the amount of data in
    // the above-defined row.
    std::shared_ptr<Page> const page = Page::create(expectedSizeBytes);
    BOOST_CHECK_EQUAL(0ULL, page->sizeBytes());
    BOOST_CHECK_EQUAL(0ULL, page->sizeRows());

    // Test the compatibility of the interface with MySQL row type.
    // The operation must throw because of the null pointer. Page counters
    // should not be affected.
    BOOST_REQUIRE_THROW({
        MYSQL_ROW row = nullptr;
        page->add(numFields, row, lengths);
    }, std::invalid_argument);
    BOOST_CHECK_EQUAL(0ULL, page->sizeBytes());
    BOOST_CHECK_EQUAL(0ULL, page->sizeRows());

    // The first row should be accommodated in the page.
    BOOST_REQUIRE_NO_THROW(page->add(numFields, row, lengths));
    BOOST_CHECK_EQUAL(expectedSizeBytes, page->sizeBytes());
    BOOST_CHECK_EQUAL(1ULL, page->sizeRows());

    // Any further inserts are required to fail due to page overflow.
    // Page counters should not be affected.
    BOOST_REQUIRE_THROW(page->add(numFields, row, lengths), PageOverflow);
    BOOST_CHECK_EQUAL(expectedSizeBytes, page->sizeBytes());
    BOOST_CHECK_EQUAL(1ULL, page->sizeRows());

    LOGS_INFO("Pooltest ends");
}

BOOST_AUTO_TEST_SUITE_END()
