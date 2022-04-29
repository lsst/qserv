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

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/ChunkedTable.h"

// Boost unit test header
#define BOOST_TEST_MODULE ChunkedTableTest
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ChunkedTableTest) {
    LOGS_INFO("ChunkedTableTest begins");

    // Test default construction

    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const invalidTable;
        BOOST_CHECK(not invalidTable.valid());
    });
    ChunkedTable const invalidTable;
    BOOST_CHECK_THROW({ invalidTable.baseName(); }, invalid_argument);
    BOOST_CHECK_THROW({ invalidTable.overlap(); }, invalid_argument);
    BOOST_CHECK_THROW({ invalidTable.chunk(); }, invalid_argument);
    BOOST_CHECK_THROW({ invalidTable.name(); }, invalid_argument);

    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const invalidTable;
        ChunkedTable const invalidTable2;
        BOOST_CHECK(invalidTable2 == invalidTable);
        BOOST_CHECK(invalidTable == invalidTable2);

        ChunkedTable const invalidTable3(invalidTable2);
        BOOST_CHECK(not invalidTable3.valid());
        BOOST_CHECK(invalidTable3 == invalidTable2);

        ChunkedTable invalidTable4;
        invalidTable4 = invalidTable3;
        BOOST_CHECK(not invalidTable4.valid());
        BOOST_CHECK(invalidTable4 == invalidTable3);
    });

    // Test construction from components

    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const table("Object", 123);
        BOOST_CHECK(table.valid());
        BOOST_CHECK(table.baseName() == "Object");
        BOOST_CHECK(not table.overlap());
        BOOST_CHECK(table.chunk() == 123);
        BOOST_CHECK(table.name() == "Object_123");
    });

    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const table("Object", 124, true);
        BOOST_CHECK(table.valid());
        BOOST_CHECK(table.baseName() == "Object");
        BOOST_CHECK(table.overlap());
        BOOST_CHECK(table.chunk() == 124);
        BOOST_CHECK(table.name() == "ObjectFullOverlap_124");
    });
    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const table1("Object", 123);
        ChunkedTable const table2("Object", 123);
        BOOST_CHECK(table1 == table2);
    });
    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const table("Object", 123);
        ChunkedTable const tableOverlap("Object", 123, true);
        BOOST_CHECK(table != tableOverlap);
    });

    // Check parsing from the full name

    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const table("Object_123");
        BOOST_CHECK(table.valid());
        BOOST_CHECK(table.baseName() == "Object");
        BOOST_CHECK(not table.overlap());
        BOOST_CHECK(table.chunk() == 123);
        BOOST_CHECK(table.name() == "Object_123");
    });
    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const table("ObjectFullOverlap_123");
        BOOST_CHECK(table.valid());
        BOOST_CHECK(table.baseName() == "Object");
        BOOST_CHECK(table.overlap());
        BOOST_CHECK(table.chunk() == 123);
        BOOST_CHECK(table.name() == "ObjectFullOverlap_123");
    });
    BOOST_CHECK_THROW({ ChunkedTable const table1(""); }, invalid_argument);
    BOOST_CHECK_THROW({ ChunkedTable const table1("123"); }, invalid_argument);
    BOOST_CHECK_THROW({ ChunkedTable const table1("_123"); }, invalid_argument);
    BOOST_CHECK_THROW({ ChunkedTable const table("Object_"); }, invalid_argument);
    BOOST_CHECK_THROW({ ChunkedTable const table("Object"); }, invalid_argument);
    BOOST_CHECK_THROW({ ChunkedTable const table("ObjectFullOverlap_"); }, invalid_argument);
    BOOST_CHECK_THROW({ ChunkedTable const table("FullOverlap_"); }, invalid_argument);

    // This is a perfectly valid table name
    BOOST_REQUIRE_NO_THROW({
        ChunkedTable const table("FullOverlap_123");
        BOOST_CHECK(table.valid());
        BOOST_CHECK(table.baseName() == "FullOverlap");
        BOOST_CHECK(not table.overlap());
        BOOST_CHECK(table.chunk() == 123);
        BOOST_CHECK(table.name() == "FullOverlap_123");
    });

    LOGS_INFO("ChunkedTableTest ends");
}

BOOST_AUTO_TEST_SUITE_END()
