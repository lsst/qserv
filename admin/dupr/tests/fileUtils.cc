/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE FileUtils
#include "boost/test/unit_test.hpp"

#include "FileUtils.h"
#include "TempFile.h"

namespace dupr = lsst::qserv::admin::dupr;

BOOST_AUTO_TEST_CASE(FileTest) {
    uint8_t buf1[16], buf2[16];
    for (uint8_t i = 0; i < sizeof(buf1); ++i) {
        buf1[i] = i;
    }
    TempFile t;
    dupr::OutputFile of1(t.path(), true);
    dupr::InputFile if1(t.path());
    BOOST_CHECK_EQUAL(if1.size(), 0);
    of1.append(buf1, 8);
    if1.read(buf2, 0, 8);
    dupr::OutputFile of2(t.path(), false);
    of2.append(buf1 + 8, 8);
    if1.read(buf2 + 8, 8, 8);
    BOOST_CHECK(memcmp(buf1, buf2, sizeof(buf1)) == 0);
    dupr::InputFile if2(t.path());
    BOOST_CHECK_EQUAL(if2.size(), static_cast<int>(sizeof(buf1)));
}

BOOST_AUTO_TEST_CASE(BufferedAppenderTest) {
    uint8_t buf1[256], buf2[128];
    for (size_t i = 0; i < sizeof(buf1); ++i) {
        buf1[i] = static_cast<uint8_t>(i);
    }
    TempFile t1, t2;
    {
        dupr::BufferedAppender b(48);
        b.open(t1.path(), false);
        b.append(buf1, 48);
        b.append(buf1 + 48, 49);
        b.append(buf1 + 97, 31);
        b.open(t2.path(), false);
        b.append(buf1 + 128, 16);
        b.append(buf1 + 144, 32);
        b.append(buf1 + 176, 80);
    }
    dupr::InputFile if1(t1.path());
    dupr::InputFile if2(t2.path());
    BOOST_CHECK_EQUAL(if1.size(), 128);
    BOOST_CHECK_EQUAL(if2.size(), 128);
    if1.read(buf2, 0, 128);
    BOOST_CHECK(memcmp(buf1, buf2, 128) == 0);
    if2.read(buf2, 0, 128);
    BOOST_CHECK(memcmp(buf1 + 128, buf2, 128) == 0);
}
