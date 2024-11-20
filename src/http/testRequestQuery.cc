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
#include "http/RequestQuery.h"

// Boost unit test header
#define BOOST_TEST_MODULE RequestQuery
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::http;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(RequestQueryTest) {
    uint16_t const validUInt16Value = numeric_limits<uint16_t>::max();
    unsigned int const validUIntValue = numeric_limits<unsigned int>::max();
    unsigned int const validIntValue = numeric_limits<int>::max();
    uint64_t const validUInt64Value = numeric_limits<uint64_t>::max();
    double const validDoubleValue = 1.1d;
    std::vector<uint64_t> const validVectorOfUInt64Value = {1, 2, 3};
    std::vector<string> const validVectorOfStrValue = {"a", "b", "c"};

    unordered_map<string, string> const input = {
            {"str", "abc"},
            {"empty_str", ""},
            {"uint16", to_string(validUInt16Value)},
            {"out_of_range_uint16", to_string(numeric_limits<unsigned int>::max())},
            {"uint", to_string(validUIntValue)},
            {"out_of_range_uint", to_string(numeric_limits<uint64_t>::max())},
            {"int", to_string(validIntValue)},
            {"int_is_too_small", to_string(numeric_limits<int64_t>::min())},
            {"int_is_too_large", to_string(numeric_limits<int64_t>::max())},
            {"bool_true", "1"},
            {"bool_false", "0"},
            {"bool_too_true", "tRue"},
            {"bool_too_false", "falsE"},
            {"uint64", to_string(validUInt64Value)},
            {"double", to_string(validDoubleValue)},
            {"vec_of_uint64_1", "1,2,3"},
            {"vec_of_uint64_2", "1,2,3,"},
            {"vec_of_uint64_3", " 1,2,3"},
            {"vec_of_uint64_4", "1,2,3 "},
            {"vec_of_uint64_5", " 1, 2, 3 "},
            {"empty_vec_1", ""},
            {"empty_vec_2", ","},
            {"empty_vec_3", " ,"},
            {"empty_vec_4", ", "},
            {"vec_of_str_1", "a,b,c"},
            {"vec_of_str_2", ",a,b,c"},
            {"vec_of_str_3", ",a,b,c,"},
            {"vec_of_str_4", ",a,b,c,,"},
            {"empty_vec_of_str_1", ""},
            {"empty_vec_of_str_2", ","},
            {"empty_vec_of_str_3", ",,"}};
    RequestQuery const query(input);

    // Test names of parameters

    for (auto itr : input) {
        BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.has(itr.first), true); });
    }
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.has("some_unknown_param"), false); });

    // Test parsing 'std::string'

    BOOST_CHECK_THROW({ query.requiredString("some_unknown_param"); }, std::invalid_argument);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredString("str"), input.at("str")); });
    BOOST_CHECK_THROW({ query.requiredString("empty_str"); }, std::invalid_argument);

    string const defaultStringValue = "some_unknown_value";
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalString("str"), input.at("str")); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK_EQUAL(query.optionalString("str", defaultStringValue), input.at("str")); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalString("empty_str"), input.at("empty_str")); });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(query.optionalString("empty_str", defaultStringValue), input.at("empty_str"));
    });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalString("some_unknown_param"), string()); });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(query.optionalString("some_unknown_param", defaultStringValue), defaultStringValue);
    });

    // Test parsing 'uint16_t'

    BOOST_CHECK_THROW({ query.requiredUInt16("some_unknown_param"); }, std::invalid_argument);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredUInt16("uint16"), validUInt16Value); });
    BOOST_CHECK_THROW({ query.requiredUInt16("out_of_range_uint16"); }, std::out_of_range);

    uint16_t const defaultUInt16Value = 1U;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalUInt16("uint16"), validUInt16Value); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK_EQUAL(query.optionalUInt16("uint16", defaultUInt16Value), validUInt16Value); });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(query.optionalUInt16("some_unknown_param", defaultUInt16Value), defaultUInt16Value);
    });
    BOOST_CHECK_THROW({ query.optionalUInt16("out_of_range_uint16"); }, std::out_of_range);
    BOOST_CHECK_THROW(
            { query.optionalUInt16("out_of_range_uint16", defaultUInt16Value); }, std::out_of_range);

    // Test parsing 'unsigned int'

    BOOST_CHECK_THROW({ query.requiredUInt("some_unknown_param"); }, std::invalid_argument);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredUInt("uint"), validUIntValue); });
    BOOST_CHECK_THROW({ query.requiredUInt("out_of_range_uint"); }, std::out_of_range);

    unsigned int const defaultUIntValue = 1U;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalUInt("uint"), validUIntValue); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK_EQUAL(query.optionalUInt("uint", defaultUIntValue), validUIntValue); });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(query.optionalUInt("some_unknown_param", defaultUIntValue), defaultUIntValue);
    });
    BOOST_CHECK_THROW({ query.optionalUInt("out_of_range_uint"); }, std::out_of_range);
    BOOST_CHECK_THROW({ query.optionalUInt("out_of_range_uint", defaultUIntValue); }, std::out_of_range);

    // Test parsing 'int'

    BOOST_CHECK_THROW({ query.requiredInt("some_unknown_param"); }, std::invalid_argument);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredInt("int"), validIntValue); });
    BOOST_CHECK_THROW({ query.requiredInt("int_is_too_small"); }, std::out_of_range);
    BOOST_CHECK_THROW({ query.requiredInt("int_is_too_large"); }, std::out_of_range);

    int const defaultIntValue = 1U;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalInt("int"), validIntValue); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalInt("int", defaultIntValue), validIntValue); });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(query.optionalUInt("some_unknown_param", defaultIntValue), defaultIntValue);
    });
    BOOST_CHECK_THROW({ query.optionalInt("int_is_too_small"); }, std::out_of_range);
    BOOST_CHECK_THROW({ query.optionalInt("int_is_too_small", defaultIntValue); }, std::out_of_range);
    BOOST_CHECK_THROW({ query.optionalInt("int_is_too_large"); }, std::out_of_range);
    BOOST_CHECK_THROW({ query.optionalInt("int_is_too_large", defaultIntValue); }, std::out_of_range);

    // Test parsing 'bool'

    BOOST_CHECK_THROW({ query.requiredBool("some_unknown_param"); }, std::invalid_argument);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredBool("bool_true"), true); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredBool("bool_false"), false); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredBool("bool_too_true"), true); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredBool("bool_too_false"), false); });

    bool const defaultBoolValue = true;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalBool("bool_true", defaultBoolValue), true); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalBool("bool_false", defaultBoolValue), false); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK_EQUAL(query.optionalBool("bool_too_false", defaultBoolValue), false); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK_EQUAL(query.optionalBool("bool_too_true", defaultBoolValue), true); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalBool("some_unknown_param"), false); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK_EQUAL(query.optionalBool("some_unknown_param", defaultBoolValue), true); });

    // Test parsing 'uint64_t'

    BOOST_CHECK_THROW({ query.requiredUInt64("some_unknown_param"); }, std::invalid_argument);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredUInt64("uint64"), validUInt64Value); });

    uint64_t const defaultUInt64Value = 1U;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalUInt64("uint64"), validUInt64Value); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK_EQUAL(query.optionalUInt64("uint64", defaultUInt64Value), validUInt64Value); });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(query.optionalUInt64("some_unknown_param", defaultUInt64Value), defaultUInt64Value);
    });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.optionalUInt64("some_unknown_param"), 0U); });

    // Test parsing 'double'

    BOOST_CHECK_THROW({ query.requiredDouble("some_unknown_param"); }, std::invalid_argument);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(query.requiredDouble("double"), validDoubleValue); });

    // Test parsing 'std::vector<uint64_t>'

    std::vector<uint64_t> const emptyVec;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorUInt64("some_unknown_param") == emptyVec); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorUInt64("vec_of_uint64_1") == validVectorOfUInt64Value); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorUInt64("vec_of_uint64_2") == validVectorOfUInt64Value); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorUInt64("vec_of_uint64_3") == validVectorOfUInt64Value); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorUInt64("vec_of_uint64_4") == validVectorOfUInt64Value); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorUInt64("vec_of_uint64_5") == validVectorOfUInt64Value); });

    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorUInt64("empty_vec_1") == emptyVec); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorUInt64("empty_vec_2") == emptyVec); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorUInt64("empty_vec_3") == emptyVec); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorUInt64("empty_vec_4") == emptyVec); });

    // Test parsing 'std::vector<std::string>'

    std::vector<std::string> const emptyVecOfStr;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorStr("some_unknown_param") == emptyVecOfStr); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorStr("vec_of_str_1") == validVectorOfStrValue); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorStr("vec_of_str_2") == validVectorOfStrValue); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorStr("vec_of_str_3") == validVectorOfStrValue); });
    BOOST_REQUIRE_NO_THROW(
            { BOOST_CHECK(query.optionalVectorStr("vec_of_str_4") == validVectorOfStrValue); });

    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorStr("empty_vec_of_str_1") == emptyVecOfStr); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorStr("empty_vec_of_str_2") == emptyVecOfStr); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(query.optionalVectorStr("empty_vec_of_str_3") == emptyVecOfStr); });
}

BOOST_AUTO_TEST_SUITE_END()
