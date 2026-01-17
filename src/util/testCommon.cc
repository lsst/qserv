// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
 *
 * @brief test common.h
 *
 * @author John Gates, SLAC
 */

// System headers
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/common.h"

// Boost unit test header
#define BOOST_TEST_MODULE common
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;

namespace util = lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

/** @test
 * Print a MultiError object containing only one error
 */
BOOST_AUTO_TEST_CASE(prettyPrint) {
    std::string s;
    for (auto c = 'a'; c <= 'z'; ++c) {
        s += c;
    }

    std::string expectedList{
            "[97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, "
            "110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]"};
    auto strList = util::prettyCharList(s);
    LOGS_DEBUG("strList=" << strList);
    BOOST_CHECK(strList.compare(expectedList) == 0);

    std::string expectedList13{
            "[[0]=97, [1]=98, [2]=99, [3]=100, [4]=101, [5]=102, [6]=103, [7]=104, "
            "[8]=105, [9]=106, [10]=107, [11]=108, [12]=109, [13]=110, [14]=111, [15]=112, [16]=113, "
            "[17]=114, [18]=115, [19]=116, [20]=117, [21]=118, [22]=119, [23]=120, [24]=121, [25]=122]"};
    auto strList13 = util::prettyCharList(s, 13);
    LOGS_DEBUG("strList13=" << strList13);
    BOOST_CHECK(strList13.compare(expectedList13) == 0);
    auto strList30 = util::prettyCharList(s, 30);
    LOGS_DEBUG("strList30=" << strList30);
    BOOST_CHECK(strList30.compare(expectedList13) == 0);

    std::string expectedList3{"[[0]=97, [1]=98, [2]=99, ..., [23]=120, [24]=121, [25]=122]"};
    auto strList3 = util::prettyCharList(s, 3);
    LOGS_DEBUG("strList3=" << strList3);
    BOOST_CHECK(strList3.compare(expectedList3) == 0);

    const char* buf = s.c_str();
    auto bufLen = strlen(buf);
    auto strBuf13 = util::prettyCharBuf(buf, bufLen, 13);
    LOGS_DEBUG("strBuf13=" << strBuf13);
    BOOST_CHECK(strBuf13.compare(expectedList13) == 0);
    auto strBuf30 = util::prettyCharBuf(buf, bufLen, 30);
    LOGS_DEBUG("strBuf30=" << strBuf30);
    BOOST_CHECK(strBuf30.compare(expectedList13) == 0);

    auto strBuf3 = util::prettyCharBuf(buf, bufLen, 3);
    LOGS_DEBUG("strBuf3=" << strBuf3);
    BOOST_CHECK(strBuf3.compare(expectedList3) == 0);
}

/** @test
 * Test get_current_host_fqdn function for getting the current host's FQDN
 */
BOOST_AUTO_TEST_CASE(getCurrentHostFqdn) {
    // Test basic functionality - should return a non-empty string
    std::string fqdn;
    BOOST_CHECK_NO_THROW(fqdn = util::get_current_host_fqdn());
    LOGS_DEBUG("Current host FQDN: " << fqdn);
    BOOST_CHECK(!fqdn.empty());

    // FQDN should contain at least some alphanumeric characters
    bool hasAlphaNum = false;
    for (char c : fqdn) {
        if (std::isalnum(c)) {
            hasAlphaNum = true;
            break;
        }
    }
    BOOST_CHECK(hasAlphaNum);

    // Test with all=false (default) - should return single FQDN
    std::string fqdnSingle;
    BOOST_CHECK_NO_THROW(fqdnSingle = util::get_current_host_fqdn(false));
    LOGS_DEBUG("Current host FQDN (single): " << fqdnSingle);
    BOOST_CHECK(!fqdnSingle.empty());
    // Single mode should not contain commas (no multiple FQDNs)
    BOOST_CHECK(fqdnSingle.find(',') == std::string::npos);

    // Test with all=true - may return multiple comma-separated FQDNs
    std::string fqdnAll;
    BOOST_CHECK_NO_THROW(fqdnAll = util::get_current_host_fqdn(true));
    LOGS_DEBUG("Current host FQDN (all): " << fqdnAll);
    BOOST_CHECK(!fqdnAll.empty());

    // The first FQDN in "all" mode should match the single mode result
    // (or be a prefix if single mode result is truncated)
    std::string firstFqdnFromAll = fqdnAll;
    size_t commaPos = fqdnAll.find(',');
    if (commaPos != std::string::npos) {
        firstFqdnFromAll = fqdnAll.substr(0, commaPos);
        LOGS_DEBUG("Multiple FQDNs found, first one: " << firstFqdnFromAll);
    }
    // Both should return the same first FQDN
    BOOST_CHECK(fqdnSingle == firstFqdnFromAll);

    // Verify the result can be used with hostNameToAddr (should resolve)
    std::string addr;
    try {
        BOOST_CHECK_NO_THROW(addr = util::hostNameToAddr(fqdnSingle));
        LOGS_DEBUG("FQDN '" << fqdnSingle << "' resolved to: " << addr);
        BOOST_CHECK(!addr.empty());
    } catch (std::runtime_error const& e) {
        // Some systems may have DNS issues, log but don't fail the test
        LOGS_INFO("Could not resolve FQDN '" << fqdnSingle << "': " << e.what());
    }
}

/** @test
 * Test hostNameToAddr function for resolving hostnames to IP addresses
 */
BOOST_AUTO_TEST_CASE(hostNameToAddr) {
    // Test with localhost - should always resolve
    std::string localhostAddr;
    BOOST_CHECK_NO_THROW(localhostAddr = util::hostNameToAddr("localhost"));
    LOGS_DEBUG("localhost resolved to: " << localhostAddr);
    // localhost should resolve to either IPv4 loopback or IPv6 loopback
    BOOST_CHECK(localhostAddr == "127.0.0.1" || localhostAddr == "::1");

    // Test with IPv4 loopback address (should work as hostname input)
    std::string loopbackAddr;
    BOOST_CHECK_NO_THROW(loopbackAddr = util::hostNameToAddr("127.0.0.1"));
    LOGS_DEBUG("127.0.0.1 resolved to: " << loopbackAddr);
    BOOST_CHECK(loopbackAddr == "127.0.0.1");

    // Test with well-known public DNS name (if network is available)
    // Using google.com as it's highly available
    try {
        std::string googleAddr = util::hostNameToAddr("google.com");
        LOGS_DEBUG("google.com resolved to: " << googleAddr);
        // Should get a valid IP address (either IPv4 or IPv6)
        BOOST_CHECK(!googleAddr.empty());
        // Check that it contains either dots (IPv4) or colons (IPv6)
        bool isValidFormat =
                (googleAddr.find('.') != std::string::npos) || (googleAddr.find(':') != std::string::npos);
        BOOST_CHECK(isValidFormat);
    } catch (std::runtime_error const& e) {
        // Network might not be available, log and skip this test
        LOGS_INFO("Skipping google.com test - network may not be available: " << e.what());
    }

    // Test with invalid hostname - should throw
    BOOST_CHECK_THROW(util::hostNameToAddr("this-hostname-should-not-exist-12345.invalid"),
                      std::runtime_error);

    // Test with empty string - should throw
    BOOST_CHECK_THROW(util::hostNameToAddr(""), std::runtime_error);
}

BOOST_AUTO_TEST_SUITE_END()
