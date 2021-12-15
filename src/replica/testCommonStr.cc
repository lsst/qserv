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
#include <vector>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/Common.h"

// Boost unit test header
#define BOOST_TEST_MODULE CommonStr
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(CommonStrTest) {
    LOGS_INFO("CommonStrTest test begins");

    vector<string> words;
    BOOST_REQUIRE_NO_THROW({
        words = strsplit("");
        BOOST_CHECK(words.empty());
    });
    BOOST_REQUIRE_NO_THROW({
        words = strsplit("", '_');
        BOOST_CHECK(words.empty());
    });
    BOOST_REQUIRE_NO_THROW({
        words = strsplit("  a bc  def ");
        BOOST_CHECK_EQUAL(words.size(), 3U);
        BOOST_CHECK_EQUAL(words[0], "a");
        BOOST_CHECK_EQUAL(words[1], "bc");
        BOOST_CHECK_EQUAL(words[2], "def");
    });
    BOOST_REQUIRE_NO_THROW({
        words = strsplit("a bc def");
        BOOST_CHECK_EQUAL(words.size(), 3U);
        BOOST_CHECK_EQUAL(words[0], "a");
        BOOST_CHECK_EQUAL(words[1], "bc");
        BOOST_CHECK_EQUAL(words[2], "def");
    });
    BOOST_REQUIRE_NO_THROW({
        words = strsplit("a_bc_def", '_');
        BOOST_CHECK_EQUAL(words.size(), 3U);
        BOOST_CHECK_EQUAL(words[0], "a");
        BOOST_CHECK_EQUAL(words[1], "bc");
        BOOST_CHECK_EQUAL(words[2], "def");
    });
    BOOST_REQUIRE_NO_THROW({
        words = strsplit("_a_bc_def_", '_');
        BOOST_CHECK_EQUAL(words.size(), 3U);
        BOOST_CHECK_EQUAL(words[0], "a");
        BOOST_CHECK_EQUAL(words[1], "bc");
        BOOST_CHECK_EQUAL(words[2], "def");
    });
    LOGS_INFO("CommonStrTest test ends");
}

BOOST_AUTO_TEST_SUITE_END()
