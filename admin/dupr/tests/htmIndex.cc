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

#include <stdexcept>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE HtmIndex
#include "boost/test/unit_test.hpp"

#include "Constants.h"
#include "FileUtils.h"
#include "HtmIndex.h"
#include "TempFile.h"

namespace fs = boost::filesystem;
namespace dupr = lsst::qserv::admin::dupr;

using std::exception;
using std::vector;
using dupr::HtmIndex;
using dupr::HTM_MAX_LEVEL;

BOOST_AUTO_TEST_CASE(HtmIndexTest) {
    BOOST_CHECK_THROW(HtmIndex(-1), exception);
    BOOST_CHECK_THROW(HtmIndex(HTM_MAX_LEVEL + 1), exception);
    HtmIndex idx(HTM_MAX_LEVEL);
    uint32_t id = static_cast<uint32_t>(0x8) << (2*HTM_MAX_LEVEL);
    BOOST_CHECK_EQUAL(idx.size(), 0u);
    BOOST_CHECK(idx.empty());
    BOOST_CHECK_EQUAL(idx.getLevel(), HTM_MAX_LEVEL);
    BOOST_CHECK_EQUAL(idx.getNumRecords(), 0u);
    BOOST_CHECK_THROW(idx.mapToNonEmpty(id), exception);
    BOOST_CHECK_EQUAL(idx(id), 0u);
    idx.add(id, 1u);
    BOOST_CHECK_EQUAL(idx.size(), 1u);
    BOOST_CHECK(!idx.empty());
    BOOST_CHECK_EQUAL(idx.getNumRecords(), 1u);
    BOOST_CHECK_EQUAL(idx.mapToNonEmpty(id), id);
    BOOST_CHECK_EQUAL(idx.mapToNonEmpty(123), id);
    BOOST_CHECK_EQUAL(idx(id), 1u);
    idx.add(id, 1u);
    id += 1;
    idx.add(id, 1u);
    BOOST_CHECK_EQUAL(idx.size(), 2u);
    BOOST_CHECK_EQUAL(idx.getNumRecords(), 3u);
    BOOST_CHECK_EQUAL(idx(id - 1), 2u);
    BOOST_CHECK_EQUAL(idx(id), 1u);
    idx.clear();
    BOOST_CHECK_EQUAL(idx.size(), 0u);
    BOOST_CHECK(idx.empty());
    BOOST_CHECK_THROW(idx.mapToNonEmpty(id), exception);
    BOOST_CHECK_EQUAL(idx(id), 0u);
}

BOOST_AUTO_TEST_CASE(HtmIndexMergeTest) {
    HtmIndex i1(2);
    HtmIndex i2(2);
    HtmIndex i3(HTM_MAX_LEVEL);
    BOOST_CHECK_THROW(i1.merge(i3), exception);
    i1.add(0x80, 3u);
    i1.add(0xf2, 3u);
    i2.add(0xf2, 3u);
    i2.add(0x93, 3u);
    i1.merge(i2);
    BOOST_CHECK_EQUAL(i1.size(), 3u);
    BOOST_CHECK_EQUAL(i1.getNumRecords(), 12u);
    BOOST_CHECK_EQUAL(i1(0x93), 3u);
    BOOST_CHECK_EQUAL(i1(0x80), 3u);
    BOOST_CHECK_EQUAL(i1(0xf2), 6u);
}

BOOST_AUTO_TEST_CASE(HtmIndexIoTest) {
    HtmIndex i1(2), i2(2), i3(2), i4(4);
    TempFile t1, t2, t3;
    i1.add(0x80, 1u);
    i1.add(0x8f, 1u);
    i2.add(0x8f, 1u);
    i2.add(0xc3, 1u);
    i4.add(0x800, 1u);
    i1.write(t1.path(), false);
    i2.write(t2.path(), false);
    i4.write(t3.path(), false);
    i3 = HtmIndex(t1.path());
    BOOST_CHECK_EQUAL(i1.size(), i3.size());
    BOOST_CHECK_EQUAL(i1.getNumRecords(), i3.getNumRecords());
    BOOST_CHECK_EQUAL(i1(0x80), i3(0x80));
    BOOST_CHECK_EQUAL(i1(0x8f), i3(0x8f));
    i3 = HtmIndex(2);
    i3.merge(i1);
    i3.merge(i2);
    vector<fs::path> v;
    v.push_back(t1.path());
    v.push_back(t2.path());
    i4 = HtmIndex(v);
    BOOST_CHECK_EQUAL(i3.size(), i4.size());
    BOOST_CHECK_EQUAL(i3.getNumRecords(), i4.getNumRecords());
    BOOST_CHECK_EQUAL(i3(0x80), i4(0x80));
    BOOST_CHECK_EQUAL(i3(0x8f), i4(0x8f));
    BOOST_CHECK_EQUAL(i3(0xc3), i4(0xc3));
    // t3 contains level 4 indexes, while t1 and t2 contain level 2 indexes.
    v.push_back(t3.path());
    BOOST_CHECK_THROW((HtmIndex(v)), exception);
    // Check that the concatenation of temporary files 1 and 2 is equivalent
    // to the merge of both indexes.
    t3.concatenate(t1, t2);
    i4 = HtmIndex(t3.path());
    BOOST_CHECK_EQUAL(i3.size(), i4.size());
    BOOST_CHECK_EQUAL(i3.getNumRecords(), i4.getNumRecords());
    BOOST_CHECK_EQUAL(i3(0x80), i4(0x80));
    BOOST_CHECK_EQUAL(i3(0x8f), i4(0x8f));
    BOOST_CHECK_EQUAL(i3(0xc3), i4(0xc3));
}
