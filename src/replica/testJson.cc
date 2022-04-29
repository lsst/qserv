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

// Third party headers
#include "nlohmann/json.hpp"

// System headers
#include <stdexcept>
#include <sstream>

// Boost unit test header
#define BOOST_TEST_MODULE JsonLibrary
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using json = nlohmann::json;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(TestJsonLibrary) {
    LOGS_INFO("JsonLibrary test begins");

    // Test the input w/o any spaces between keys
    BOOST_REQUIRE_NO_THROW({
        string const in =
                R"({"database":"test10","num_stripes":340,"num_sub_stripes":3,"overlap":0.01667,"auth_key":""})";
        json const out = json::parse(in);
        BOOST_CHECK(not out.is_null());
        BOOST_CHECK(out.is_object());
    });

    // Test the input which contain spaces.
    BOOST_REQUIRE_NO_THROW({
        char in[] =
                R"({"database":"test10", "num_stripes":340,"num_sub_stripes":3,"overlap":0.01667,"auth_key":""})";
        json const out = json::parse(in);
        BOOST_CHECK(not out.is_null());
        BOOST_CHECK(out.is_object());
    });

    // Test the input which contain spaces.
    BOOST_REQUIRE_NO_THROW({
        string const in =
                R"({"database":"test10", "num_stripes":340,"num_sub_stripes":3,"overlap":0.01667,"auth_key":""})";
        json const out = json::parse(in);
        BOOST_CHECK(not out.is_null());
        BOOST_CHECK(out.is_object());
    });

    // Test the input w/o any spaces between keys (via the streamer)
    BOOST_REQUIRE_NO_THROW({
        string const in =
                R"({"database":"test10","num_stripes":340,"num_sub_stripes":3,"overlap":0.01667,"auth_key":""})";
        istringstream is(in);
        string inFromStream;
        is >> inFromStream;
        json const out = json::parse(inFromStream);
        BOOST_CHECK(not out.is_null());
        BOOST_CHECK(out.is_object());
    });

    // Test the input which contain spaces (read all via the streamer)
    BOOST_REQUIRE_NO_THROW({
        string const in =
                R"({"database":"test10", "num_stripes":340,"num_sub_stripes":3,"overlap":0.01667,"auth_key":""})";
        istringstream is(in);
        string const inFromStream(istreambuf_iterator<char>(is), {});
        json const out = json::parse(inFromStream);
        BOOST_CHECK(not out.is_null());
        BOOST_CHECK(out.is_object());
    });

    // Test the input which contain spaces (via the streamer)
    BOOST_CHECK_THROW(
            {
                string const in =
                        R"({"database":"test10", "num_stripes":340,"num_sub_stripes":3,"overlap":0.01667,"auth_key":""})";
                istringstream is(in);
                string inFromStream;
                is >> inFromStream;
                json const out = json::parse(inFromStream);
                BOOST_CHECK(not out.is_null());
                BOOST_CHECK(out.is_object());
            },
            exception);
    LOGS_INFO("JsonLibrary test ends");
}

BOOST_AUTO_TEST_SUITE_END()
