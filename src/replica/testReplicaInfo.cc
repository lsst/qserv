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

// Qserv headers
#include "replica/ReplicaInfo.h"

// Boost unit test header
#define BOOST_TEST_MODULE ReplicaInfo
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ReplicaInfoTest) {

    LOGS_INFO("ReplicaInfo test begins");

    QservReplicaCollection one;
    one.emplace_back(1U, "db1", 0);
    one.emplace_back(1U, "db2", 0);   // is only present in 'one'
    one.emplace_back(2U, "db1", 0);
    one.emplace_back(3U, "db3", 0);   // is only present in 'one'

    QservReplicaCollection two;
    two.emplace_back(1U, "db1", 0);
    two.emplace_back(2U, "db1", 0);
    two.emplace_back(3U, "db2", 0);   // only present in 'two'
    two.emplace_back(4U, "db3", 0);   // only present in 'two'

    // Test one-way comparison

    QservReplicaCollection inFirstOnly;
    BOOST_CHECK(diff(one, two, inFirstOnly));
    BOOST_CHECK(inFirstOnly.size() == 2);
    BOOST_CHECK(inFirstOnly.size() == 2 &&
                (inFirstOnly[0].chunk == 1) && (inFirstOnly[0].database == "db2") &&
                (inFirstOnly[1].chunk == 3) && (inFirstOnly[1].database == "db3"));

    QservReplicaCollection inSecondOnly;
    BOOST_CHECK(diff(two, one, inSecondOnly));
    BOOST_CHECK(inSecondOnly.size() == 2);
    BOOST_CHECK(inSecondOnly.size() == 2 &&
                (inSecondOnly[0].chunk == 3) && (inSecondOnly[0].database == "db2") &&
                (inSecondOnly[1].chunk == 4) && (inSecondOnly[1].database == "db3"));

    // Two-way comparison

    BOOST_CHECK(diff2(one, two, inFirstOnly, inSecondOnly));
    BOOST_CHECK(inFirstOnly.size() == 2);
    BOOST_CHECK(inFirstOnly.size() == 2 &&
                (inFirstOnly[0].chunk == 1) && (inFirstOnly[0].database == "db2") &&
                (inFirstOnly[1].chunk == 3) && (inFirstOnly[1].database == "db3"));
    BOOST_CHECK(inSecondOnly.size() == 2);
    BOOST_CHECK(inSecondOnly.size() == 2 &&
                (inSecondOnly[0].chunk == 3) && (inSecondOnly[0].database == "db2") &&
                (inSecondOnly[1].chunk == 4) && (inSecondOnly[1].database == "db3"));

    LOGS_INFO("ReplicaInfo test ends");
}

BOOST_AUTO_TEST_CASE(ReplicaInfoFileInfoTest) {

    LOGS_INFO("ReplicaInfo::FileInfo test begins");

    // Fields which aren't tested. They're only needed for constructing objects
    // of the class which is being tested here.

    uint64_t const size = 0;
    time_t   const mtime = 0;
    string   const cs;
    uint64_t const beginTransferTime = 0;
    uint64_t const endTransferTime = 0;
    uint64_t const inSize = 0;

    auto const makeFileInfo = [&](string const& name) -> ReplicaInfo::FileInfo {
        return ReplicaInfo::FileInfo{
            name, size, mtime, cs, beginTransferTime, endTransferTime, inSize
        };
    };
    auto const a = makeFileInfo("A.MYD");
    BOOST_CHECK_EQUAL(a.baseTable(), "A");
    BOOST_CHECK(not a.isOverlap());
    BOOST_CHECK(    a.isData());
    BOOST_CHECK(not a.isIndex());

    auto const b = makeFileInfo("B.MYI");
    BOOST_CHECK_EQUAL(b.baseTable(), "B");
    BOOST_CHECK(not b.isOverlap());
    BOOST_CHECK(not b.isData());
    BOOST_CHECK(    b.isIndex());

    auto const c = makeFileInfo("C.frm");
    BOOST_CHECK_EQUAL(c.baseTable(), "C");
    BOOST_CHECK(not c.isOverlap());
    BOOST_CHECK(not c.isData());
    BOOST_CHECK(not c.isIndex());

    auto const d = makeFileInfo("D_123.MYD");
    BOOST_CHECK_EQUAL(d.baseTable(), "D");
    BOOST_CHECK(not d.isOverlap());
    BOOST_CHECK(    d.isData());
    BOOST_CHECK(not d.isIndex());

    auto const e = makeFileInfo("EFullOverlap_123.MYD");
    BOOST_CHECK_EQUAL(e.baseTable(), "E");
    BOOST_CHECK(    e.isOverlap());
    BOOST_CHECK(    e.isData());
    BOOST_CHECK(not e.isIndex());

    auto const f = makeFileInfo("FullOverlap_123.MYD");
    BOOST_CHECK_EQUAL(f.baseTable(), "FullOverlap");
    BOOST_CHECK(not f.isOverlap());
    BOOST_CHECK(    f.isData());
    BOOST_CHECK(not f.isIndex());

    auto const g = makeFileInfo("FullOverlap.MYD");
    BOOST_CHECK_EQUAL(g.baseTable(), "FullOverlap");
    BOOST_CHECK(not g.isOverlap());
    BOOST_CHECK(    g.isData());
    BOOST_CHECK(not g.isIndex());

    LOGS_INFO("ReplicaInfo::FileInfo test ends");
}


BOOST_AUTO_TEST_SUITE_END()
