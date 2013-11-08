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

#include <cmath>
#include <stdexcept>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE ChunkIndex
#include "boost/test/unit_test.hpp"

#include "ChunkIndex.h"
#include "FileUtils.h"
#include "TempFile.h"

namespace fs = boost::filesystem;
namespace dupr = lsst::qserv::admin::dupr;

using std::exception;
using std::sqrt;
using std::vector;

using dupr::ChunkIndex;
using dupr::ChunkLocation;

namespace {
    bool operator==(ChunkIndex::Entry const & e1,
                    ChunkIndex::Entry const & e2) {
        return e1.numRecords == e2.numRecords;
    }
}

BOOST_AUTO_TEST_CASE(ChunkIndexTest) {
    ChunkIndex idx;
    BOOST_CHECK_EQUAL(idx.size(), 0u);
    BOOST_CHECK(idx.empty());
    ChunkLocation loc;
    loc.chunkId = 1;
    for (int k = 0; k < 2; ++k) {
        loc.overlap = (k != 0);
        for (int sc = 1; sc < 5; ++sc) {
            loc.subChunkId = sc;
            idx.add(loc, static_cast<size_t>(sc));
        }
    }
    BOOST_CHECK_EQUAL(idx(1).numRecords, 10u);
    BOOST_CHECK_EQUAL(idx(1).numOverlapRecords, 10u);
    for (unsigned i = 1; i <= 4; ++i) {
        loc.subChunkId = i;
        BOOST_CHECK_EQUAL(idx(1, i).numRecords, i);
        BOOST_CHECK_EQUAL(idx(1, i).numOverlapRecords, i);
        loc.overlap = false;
        BOOST_CHECK_EQUAL(idx(loc), i);
        loc.overlap = true;
        BOOST_CHECK_EQUAL(idx(loc), i);
    }
    ChunkIndex::Stats stats;
    for (int k = 0; k < 2; ++k) {
        bool overlap = (k != 0);
        stats = idx.getChunkStats(overlap);
        BOOST_CHECK_EQUAL(stats.nrec, 10u);
        BOOST_CHECK_EQUAL(stats.n, 1u);
        BOOST_CHECK_EQUAL(stats.min, 10u);
        BOOST_CHECK_EQUAL(stats.max, 10u);
        BOOST_CHECK_EQUAL(stats.quartile[0], stats.quartile[1]);
        BOOST_CHECK_EQUAL(stats.quartile[1], stats.quartile[2]);
        BOOST_CHECK_EQUAL(stats.quartile[2], 10u);
        BOOST_CHECK_EQUAL(stats.mean, 10.0);
        BOOST_CHECK_EQUAL(stats.sigma, 0.0);
        stats = idx.getSubChunkStats(overlap);
        BOOST_CHECK_EQUAL(stats.nrec, 10u);
        BOOST_CHECK_EQUAL(stats.n, 4u);
        BOOST_CHECK_EQUAL(stats.min, 1u);
        BOOST_CHECK_EQUAL(stats.max, 4u);
        BOOST_CHECK_EQUAL(stats.quartile[0], 2u);
        BOOST_CHECK_EQUAL(stats.quartile[1], 3u);
        BOOST_CHECK_EQUAL(stats.quartile[2], 4u);
        BOOST_CHECK_EQUAL(stats.mean, 2.5);
        BOOST_CHECK_CLOSE_FRACTION(stats.sigma, sqrt(1.25), 1e-15);
    }
    idx.clear();
    BOOST_CHECK_EQUAL(idx(loc), 0u);
    BOOST_CHECK_EQUAL(idx.size(), 0u);
    BOOST_CHECK(idx.empty());
}

struct ChunkIndexFixture {
    ChunkIndex i1;
    ChunkIndex i2;
    ChunkIndex i3;

    ChunkIndexFixture() {
        ChunkLocation loc;
        loc.chunkId = 1;
        loc.subChunkId = 2;
        loc.overlap = false;
        i1.add(loc, 1u);
        i2.add(loc, 2u);
        loc.overlap = true;
        i1.add(loc, 2u);
        loc.chunkId = 2;
        loc.subChunkId = 3;
        loc.overlap = false;
        i1.add(loc, 1u);
        loc.overlap = true;
        i2.add(loc, 1u);
        loc.subChunkId = 4;
        loc.overlap = false;
        i2.add(loc, 1u);
    }

    ~ChunkIndexFixture() { }

    void checkMerge(ChunkIndex const & idx) {
        BOOST_CHECK_EQUAL(idx(1,2).numRecords, 3u);
        BOOST_CHECK_EQUAL(idx(1,2).numOverlapRecords, 2u);
        BOOST_CHECK_EQUAL(idx(2,3).numRecords, 1u);
        BOOST_CHECK_EQUAL(idx(2,3).numOverlapRecords, 1u);
        BOOST_CHECK_EQUAL(idx(2,4).numRecords, 1u);
        BOOST_CHECK_EQUAL(idx(2,4).numOverlapRecords, 0u);
    }
};

BOOST_FIXTURE_TEST_SUITE(ChunkIndexMergeSuite, ChunkIndexFixture);

BOOST_AUTO_TEST_CASE(ChunkIndexMergeTest) {
    ChunkIndex i3;
    i3.merge(i1);
    i3.merge(i2);
    checkMerge(i3);
}

BOOST_AUTO_TEST_CASE(ChunkIndexIoTest) {
    TempFile t1, t2, t3;
    i1.write(t1.path(), false);
    i2.write(t2.path(), false);
    vector<fs::path> v;
    v.push_back(t1.path());
    v.push_back(t2.path());
    ChunkIndex i3(v);
    checkMerge(i3);
    // Check that the concatenation of temporary files 1 and 2 is equivalent
    // to the merge of both indexes.
    t3.concatenate(t1, t2);
    i3 = ChunkIndex(t3.path());
    checkMerge(i3);
}

BOOST_AUTO_TEST_SUITE_END()
