// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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
#include <iostream>
#include <sstream>
#include <string>

// Third-party headers

// Qserv headers
#include "Histogram.h"

// Boost unit test header
#define BOOST_TEST_MODULE MultiError
#include <boost/test/unit_test.hpp>

using namespace std;

namespace test = boost::test_tools;

namespace util = lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(HistogramSize) {
    std::vector<double> bucketMaxVals{0.01, 0.1, 1};
    size_t maxSize = 10;
    string hId = "Test1";
    util::HistogramRolling hist(hId, bucketMaxVals, 1h, maxSize);

    size_t sz = 0;
    for (int j = 0; j < 4; ++j) {
        BOOST_REQUIRE(hist.getBucketCount(j) == 0);
    }
    hist.addEntry(1);
    ++sz;
    BOOST_REQUIRE(hist.getBucketCount(2) == 1);
    hist.addEntry(0.2);
    ++sz;
    BOOST_REQUIRE(hist.getBucketCount(2) == 2);
    hist.addEntry(0);
    ++sz;
    BOOST_REQUIRE(hist.getBucketCount(0) == 1);
    BOOST_REQUIRE(hist.getSize() == sz);
    hist.addEntry(1.1);
    ++sz;
    BOOST_REQUIRE(hist.getBucketCount(3) == 1);
    for (int j = 0; j < 6; ++j) {
        hist.addEntry(0.05);
        ++sz;
    }
    BOOST_REQUIRE(hist.getBucketCount(1) == 6);
    BOOST_REQUIRE(hist.getBucketCount(2) == 2);
    BOOST_REQUIRE(hist.getSize() == sz);

    auto jsn = hist.getJson();
    cout << "jsn:" << jsn << endl;

    BOOST_REQUIRE(jsn["HistogramId"] == hId);
    BOOST_REQUIRE(jsn["totalCount"] == hist.getTotalCount());

    for (int j = 0; j < 4; ++j) {
        BOOST_REQUIRE(jsn["buckets"][j]["count"] == hist.getBucketCount(j));
        if (j < 3) {
            BOOST_REQUIRE(jsn["buckets"][j]["maxVal"] == hist.getBucketMaxVal(j));
        } else {
            BOOST_REQUIRE(jsn["buckets"][j]["maxVal"] == "infinity");
        }
    }

    // check that the size is limited to 10.
    hist.addEntry(0.05);
    BOOST_REQUIRE(hist.getSize() == maxSize);
    BOOST_REQUIRE(hist.getBucketCount(0) == 1);
    BOOST_REQUIRE(hist.getBucketCount(1) == 7);
    BOOST_REQUIRE(hist.getBucketCount(2) == 1);
    BOOST_REQUIRE(hist.getBucketCount(3) == 1);

    hist.addEntry(0.05);
    BOOST_REQUIRE(hist.getSize() == maxSize);
    BOOST_REQUIRE(hist.getBucketCount(0) == 1);
    BOOST_REQUIRE(hist.getBucketCount(1) == 8);
    BOOST_REQUIRE(hist.getBucketCount(2) == 0);
    BOOST_REQUIRE(hist.getBucketCount(3) == 1);

    hist.addEntry(0.05);
    BOOST_REQUIRE(hist.getSize() == maxSize);
    BOOST_REQUIRE(hist.getBucketCount(0) == 0);
    BOOST_REQUIRE(hist.getBucketCount(1) == 9);
    BOOST_REQUIRE(hist.getBucketCount(2) == 0);
    BOOST_REQUIRE(hist.getBucketCount(3) == 1);

    hist.addEntry(0.05);
    BOOST_REQUIRE(hist.getSize() == maxSize);
    BOOST_REQUIRE(hist.getBucketCount(0) == 0);
    BOOST_REQUIRE(hist.getBucketCount(1) == 10);
    BOOST_REQUIRE(hist.getBucketCount(2) == 0);
    BOOST_REQUIRE(hist.getBucketCount(3) == 0);

    hist.setMaxAge(std::chrono::milliseconds(1));
    usleep(2000);
    hist.checkEntries();
    BOOST_REQUIRE(hist.getSize() == 0);
    BOOST_REQUIRE(hist.getAvg() == 0.0);
}

BOOST_AUTO_TEST_SUITE_END()
