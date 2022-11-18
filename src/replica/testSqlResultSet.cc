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
 * @brief test the iteration over rows in the result sets of the class SqlResultSet.
 */

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/DatabaseMySQLRow.h"
#include "replica/SqlResultSet.h"

// System headers
#include <stdexcept>

// Boost unit test header
#define BOOST_TEST_MODULE SqlResultSet
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(SqlResultSetTest) {
    LOGS_INFO("SqlResultSet test begins");

    // Minimal initialization of the result set object as required
    // for testing the iteration.
    SqlResultSet::ResultSet resultSet;
    resultSet.fields.push_back(SqlResultSet::ResultSet::Field("a"));
    resultSet.fields.push_back(SqlResultSet::ResultSet::Field("b"));
    resultSet.fields.push_back(SqlResultSet::ResultSet::Field("c"));
    SqlResultSet::ResultSet::Row row;
    row.cells = {"abc", "12", ""};
    row.nulls = {0, 0, 1};
    resultSet.rows.push_back(row);
    row.cells = {"1.2", "0", ""};
    row.nulls = {0, 0, 0};
    resultSet.rows.push_back(row);

    // Test the iterator
    size_t rowNum = 0;
    SqlResultSet::iterate(resultSet, [&rowNum](database::mysql::Row const& row) {
        if (rowNum == 0) {
            BOOST_CHECK(!row.isNull(0));
            BOOST_CHECK(!row.isNull("a"));
            string s;
            BOOST_CHECK(row.get(0, s));
            BOOST_CHECK(s == "abc");
            s = string();
            BOOST_CHECK(row.get("a", s));
            BOOST_CHECK(s == "abc");

            BOOST_CHECK(!row.isNull(1));
            BOOST_CHECK(!row.isNull("b"));
            int i = 0;
            BOOST_CHECK(row.get(1, i));
            BOOST_CHECK(i == 12);
            i = 0;
            BOOST_CHECK(row.get("b", i));
            BOOST_CHECK(i == 12);

            BOOST_CHECK(row.isNull(2));
            BOOST_CHECK(row.isNull("c"));
            BOOST_CHECK(!row.get(2, s));
            BOOST_CHECK(!row.get("c", s));
        } else {
            BOOST_CHECK(!row.isNull(0));
            BOOST_CHECK(!row.isNull("a"));
            float f = 0.;
            BOOST_CHECK(row.get(0, f));
            BOOST_CHECK_EQUAL(f, 1.2f);
            f = 0.;
            BOOST_CHECK(row.get("a", f));
            BOOST_CHECK_EQUAL(f, 1.2f);

            BOOST_CHECK(!row.isNull(1));
            BOOST_CHECK(!row.isNull("b"));
            bool b = true;
            BOOST_CHECK(row.get(1, b));
            BOOST_CHECK(b == false);
            b = true;
            BOOST_CHECK(row.get("b", b));
            BOOST_CHECK(b == false);

            BOOST_CHECK(!row.isNull(2));
            BOOST_CHECK(!row.isNull("c"));
            string s = "123";
            BOOST_CHECK(row.get(2, s));
            BOOST_CHECK(s.empty());
            s = "123";
            BOOST_CHECK(row.get("c", s));
            BOOST_CHECK(s.empty());
        }
        rowNum++;
    });

    LOGS_INFO("SqlResultSet test ends");
}

BOOST_AUTO_TEST_SUITE_END()