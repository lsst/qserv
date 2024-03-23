// -*- LSST-C++ -*-
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

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/String.h"

// Boost unit test header
#define BOOST_TEST_MODULE String
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;
namespace util = lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(SplitStringTest) {
    LOGS_INFO("SplitStringTest begins");
    {
        std::string const emptyStr;
        std::string const delimiter = " ";
        auto const vect = util::String::split(emptyStr, delimiter);
        LOGS_ERROR("vect=" << util::String::toString(vect, delimiter, "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], emptyStr);
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    {
        std::string const emptyStr;
        std::string const delimiter = " ";
        bool const greedy = true;
        auto const vect = util::String::split(emptyStr, delimiter, greedy);
        LOGS_ERROR("vect=" << util::String::toString(vect, delimiter, "'", "'"));
        BOOST_CHECK_EQUAL(vect.size(), 0UL);
    }
    {
        std::string const str = " a b  cd   e f  ";
        std::string const emptyDelimiter;
        auto const vect = util::String::split(str, emptyDelimiter);
        LOGS_ERROR("vect=" << util::String::toString(vect, emptyDelimiter, "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], str);
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    {
        auto const vect = util::String::split(" a b  cd   e f  ", " ");
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], "");
        BOOST_CHECK_EQUAL(vect[j++], "a");
        BOOST_CHECK_EQUAL(vect[j++], "b");
        BOOST_CHECK_EQUAL(vect[j++], "");
        BOOST_CHECK_EQUAL(vect[j++], "cd");
        BOOST_CHECK_EQUAL(vect[j++], "");
        BOOST_CHECK_EQUAL(vect[j++], "");
        BOOST_CHECK_EQUAL(vect[j++], "e");
        BOOST_CHECK_EQUAL(vect[j++], "f");
        BOOST_CHECK_EQUAL(vect[j++], "");
        BOOST_CHECK_EQUAL(vect[j++], "");
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    {
        bool const greedy = true;
        auto const vect = util::String::split(" a b  cd   e f  ", " ", greedy);
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], "a");
        BOOST_CHECK_EQUAL(vect[j++], "b");
        BOOST_CHECK_EQUAL(vect[j++], "cd");
        BOOST_CHECK_EQUAL(vect[j++], "e");
        BOOST_CHECK_EQUAL(vect[j++], "f");
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    {
        auto const vect = util::String::split("testing123,qsa4$3,hjdw q,,7321,ml;oujh", ",");
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        BOOST_CHECK_EQUAL(vect.size(), 6U);
        BOOST_CHECK_EQUAL(vect[0], "testing123");
        BOOST_CHECK_EQUAL(vect[1], "qsa4$3");
        BOOST_CHECK_EQUAL(vect[2], "hjdw q");
        BOOST_CHECK_EQUAL(vect[3], "");
        BOOST_CHECK_EQUAL(vect[4], "7321");
        BOOST_CHECK_EQUAL(vect[5], "ml;oujh");
    }
    {
        auto const vect = util::String::split("testing123::q:sa4$3:::hjdw q::::7321::ml;oujh", "::");
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        BOOST_CHECK_EQUAL(vect.size(), 6U);
        BOOST_CHECK_EQUAL(vect[0], "testing123");
        BOOST_CHECK_EQUAL(vect[1], "q:sa4$3");
        BOOST_CHECK_EQUAL(vect[2], ":hjdw q");
        BOOST_CHECK_EQUAL(vect[3], "");
        BOOST_CHECK_EQUAL(vect[4], "7321");
        BOOST_CHECK_EQUAL(vect[5], "ml;oujh");
    }
    {
        auto const vect = util::String::split(":testing123:qsa4$3:hjdw q::7321:ml;oujh:", ":");
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        BOOST_CHECK_EQUAL(vect.size(), 8U);
        BOOST_CHECK_EQUAL(vect[0], "");
        BOOST_CHECK_EQUAL(vect[1], "testing123");
        BOOST_CHECK_EQUAL(vect[2], "qsa4$3");
        BOOST_CHECK_EQUAL(vect[3], "hjdw q");
        BOOST_CHECK_EQUAL(vect[4], "");
        BOOST_CHECK_EQUAL(vect[5], "7321");
        BOOST_CHECK_EQUAL(vect[6], "ml;oujh");
        BOOST_CHECK_EQUAL(vect[7], "");
    }
    {
        auto const vect = util::String::split("qsa4$3", ":");
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        BOOST_CHECK_EQUAL(vect.size(), 1U);
        BOOST_CHECK_EQUAL(vect[0], "qsa4$3");
    }
    {
        auto const vect = util::String::split("", ":");
        BOOST_CHECK_EQUAL(vect.size(), 1U);
        BOOST_CHECK_EQUAL(vect[0], "");
    }
}

BOOST_AUTO_TEST_CASE(GetVecFromStrTest) {
    LOGS_INFO("GetVecFromStrTest begins");
    std::string const str11 = "987:23:0:1:-123";
    std::string const str12 = "987:23:x:1:-123";
    {
        auto const vect = util::String::parseToVectInt(str11, ":");
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], 987);
        BOOST_CHECK_EQUAL(vect[j++], 23);
        BOOST_CHECK_EQUAL(vect[j++], 0);
        BOOST_CHECK_EQUAL(vect[j++], 1);
        BOOST_CHECK_EQUAL(vect[j++], -123);
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    {
        bool caught = false;
        try {
            auto vect = util::String::parseToVectInt(str12, ":");
        } catch (std::invalid_argument const& e) {
            caught = true;
        }
        BOOST_CHECK(caught);
    }
    std::string const str2 = ":987:23:x8owlq:1:-123:";
    {
        int const defaultVal = 99;
        auto const vect = util::String::parseToVectInt(str2, ":", false, defaultVal);
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // The empty string in the non-greedy mode
        BOOST_CHECK_EQUAL(vect[j++], 987);
        BOOST_CHECK_EQUAL(vect[j++], 23);
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // Couldn't parse "x8owlq" as a number
        BOOST_CHECK_EQUAL(vect[j++], 1);
        BOOST_CHECK_EQUAL(vect[j++], -123);
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // The empty string in the non-greedy mode
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    {
        int const defaultVal = 99;
        bool const greedy = true;
        auto const vect = util::String::parseToVectInt(str2, ":", false, defaultVal, greedy);
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], 987);
        BOOST_CHECK_EQUAL(vect[j++], 23);
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // Couldn't parse "x8owlq" as a number
        BOOST_CHECK_EQUAL(vect[j++], 1);
        BOOST_CHECK_EQUAL(vect[j++], -123);
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    std::string const str3 = ":123456789123123:23:x8owlq::1:-123:";
    {
        auto const defaultVal = std::numeric_limits<std::uint64_t>::max();
        bool const greedy = true;
        auto const vect = util::String::parseToVectUInt64(str3, ":", false, defaultVal, greedy);
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], 123456789123123ULL);
        BOOST_CHECK_EQUAL(vect[j++], 23ULL);
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // Couldn't parse "x8owlq" as a number
        BOOST_CHECK_EQUAL(vect[j++], 1ULL);
        BOOST_CHECK_EQUAL(vect[j++], -123LL);
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
    {
        auto const defaultVal = std::numeric_limits<std::uint64_t>::max();
        auto const vect = util::String::parseToVectUInt64(str3, ":", false, defaultVal);
        LOGS_ERROR("vect=" << util::String::toString(vect, " ", "'", "'"));
        size_t j = 0;
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // The empty string in the non-greedy mode
        BOOST_CHECK_EQUAL(vect[j++], 123456789123123ULL);
        BOOST_CHECK_EQUAL(vect[j++], 23ULL);
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // Couldn't parse "x8owlq" as a number
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // The empty string in the non-greedy mode
        BOOST_CHECK_EQUAL(vect[j++], 1ULL);
        BOOST_CHECK_EQUAL(vect[j++], -123LL);
        BOOST_CHECK_EQUAL(vect[j++], defaultVal);  // The empty string in the non-greedy mode
        BOOST_CHECK_EQUAL(vect.size(), j);
    }
}

BOOST_AUTO_TEST_CASE(ToStringTest) {
    LOGS_INFO("ToStringTest test begins");

    // These values match thevdefault values of the corresponding parameters of
    // the utility function.
    std::string sep = ",";
    std::string openBrkt = "";
    std::string closeBrkt = "";

    std::vector<int> const empty;
    BOOST_CHECK_EQUAL(util::String::toString(empty), "");
    BOOST_CHECK_EQUAL(util::String::toString(empty, " "), "");
    BOOST_CHECK_EQUAL(util::String::toString(empty, sep, openBrkt, closeBrkt), "");

    std::vector<int> const one = {1};
    BOOST_CHECK_EQUAL(util::String::toString(one), "1");
    BOOST_CHECK_EQUAL(util::String::toString(one, " "), "1");
    BOOST_CHECK_EQUAL(util::String::toString(one, "", openBrkt, closeBrkt), "1");

    std::vector<int> const integers = {1, 2, 3, 4, 5};
    BOOST_CHECK_EQUAL(util::String::toString(integers), "1,2,3,4,5");
    BOOST_CHECK_EQUAL(util::String::toString(integers, sep), "1,2,3,4,5");
    BOOST_CHECK_EQUAL(util::String::toString(integers, " ", openBrkt, closeBrkt), "1 2 3 4 5");
    BOOST_CHECK_EQUAL(util::String::toString(integers, "", openBrkt, closeBrkt), "12345");

    std::vector<std::string> const strings = {"a", "b", "c", "d", "e"};
    BOOST_CHECK_EQUAL(util::String::toString(strings), "a,b,c,d,e");
    BOOST_CHECK_EQUAL(util::String::toString(strings, sep), "a,b,c,d,e");
    BOOST_CHECK_EQUAL(util::String::toString(strings, " ", openBrkt, closeBrkt), "a b c d e");
    BOOST_CHECK_EQUAL(util::String::toString(strings, "", openBrkt, closeBrkt), "abcde");
    BOOST_CHECK_EQUAL(util::String::toString(strings, sep, "[", "]"), "[a],[b],[c],[d],[e]");
    BOOST_CHECK_EQUAL(util::String::toString(strings, " ", "[", "]"), "[a] [b] [c] [d] [e]");
}

BOOST_AUTO_TEST_CASE(ToHexTest) {
    LOGS_INFO("ToHexTest test begins");

    // This test ensures that the empty string is always returned for the empty
    // input regardleass.
    char const empty[] = "";
    BOOST_CHECK_EQUAL(util::String::toHex(empty, 0), std::string());

    // Null pointer is treated as an illegal input.
    std::string hex;
    BOOST_CHECK_THROW(util::String::toHex(nullptr, 0), std::invalid_argument);

    // The translation map from unsigned character into the two character strings
    // corresponding to the hexadecimal representation of the characters used as
    // vector indexes.
    std::vector<std::string> const char2hex = {
            "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
            "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
            "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
            "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
            "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
            "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
            "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
            "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
            "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
            "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"};

    for (int i = 0; i < 256; ++i) {
        char buf[1];
        buf[0] = static_cast<char>(i);
        BOOST_CHECK_EQUAL(util::String::toHex(buf, 1), char2hex[i]);
    }

    // Translate the long string that made of a monotonic sequence of all 256 characters.
    std::string in;
    in.resize(256);
    for (size_t i = 0; i < 256; ++i) {
        in[i] = static_cast<char>(i);
    }
    std::string out;
    for (size_t i = 0; i < 256; ++i) {
        out = out + char2hex[i];
    }
    BOOST_CHECK_EQUAL(util::String::toHex(in.data(), in.size()), out);
}

BOOST_AUTO_TEST_SUITE_END()
