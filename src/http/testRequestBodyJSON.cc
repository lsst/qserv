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

// System headers
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_map>

// Qserv headers
#include "http/RequestBodyJSON.h"

// Boost unit test header
#define BOOST_TEST_MODULE RequestBodyJSON
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::http;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(RequestBodyJSONTest) {
    RequestBodyJSON body;
    BOOST_CHECK(body.objJson.empty());

    body.objJson["key1"] = "value1";
    body.objJson["key2"] = 2;
    body.objJson["key3"] = -3;
    body.objJson["key4"] = 4.0f;
    body.objJson["key5"] = "5";
    body.objJson["key6"] = "-6";

    for (auto const& [key, value] : body.objJson.items()) {
        BOOST_REQUIRE_NO_THROW(BOOST_CHECK(body.has(key)));
    }

    // Test if required parameters are handled correctly.
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.required<string>("key1"), "value1"));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.required<unsigned int>("key2"), 2U));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.required<int>("key2"), 2));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.required<int>("key3"), -3));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.required<float>("key4"), 4.0f));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.requiredUInt("key2"), 2U));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.requiredInt("key2"), 2));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.requiredInt("key3"), -3));

    // Test if missing parameters are handled correctly.
    string const missingKey = "missing_key";
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK(!body.has("missingKey")));
    BOOST_CHECK_THROW(body.required<string>("missingKey"), invalid_argument);
    BOOST_CHECK_THROW(body.requiredUInt("missingKey"), invalid_argument);
    BOOST_CHECK_THROW(body.requiredInt("missingKey"), invalid_argument);

    // Test if optional parameters are handled correctly.
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.optional<string>("missingKey", string()), string()));
    BOOST_REQUIRE_NO_THROW(
            BOOST_CHECK_EQUAL(body.optional<string>("missingKey", "default"), string("default")));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.optionalUInt("missingKey"), 0U));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.optionalUInt("missingKey", 1), 1U));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.optionalInt("missingKey", 0), 0));
    BOOST_REQUIRE_NO_THROW(BOOST_CHECK_EQUAL(body.optionalInt("missingKey", 2), 2));
}

BOOST_AUTO_TEST_SUITE_END()
