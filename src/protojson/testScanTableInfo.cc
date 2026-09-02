// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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

// Qserv headers
#include "protojson/ScanTableInfo.h"
#include "query/ScanTableInfo.h"

// Boost unit test header
#define BOOST_TEST_MODULE RequestQuery
#include <boost/test/unit_test.hpp>

using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ScanInfoFromQueryScanInfo) {
    query::ScanInfo qScanInfo;
    qScanInfo.scanRating = 42;
    qScanInfo.infoTables.emplace_back("db1", "table1", true, 10);
    qScanInfo.infoTables.emplace_back("db2", "table2", false, 20);
    qScanInfo.infoTables.emplace_back("db3", "table3", false, 30);

    auto pjScanInfo = protojson::ScanInfo::create(qScanInfo);

    BOOST_REQUIRE_EQUAL(pjScanInfo->scanRating, qScanInfo.scanRating);
    BOOST_REQUIRE_EQUAL(pjScanInfo->infoTables.size(), qScanInfo.infoTables.size());
    for (size_t j = 0; j < qScanInfo.infoTables.size(); ++j) {
        auto const& qTbl = qScanInfo.infoTables[j];
        auto const& pjTbl = pjScanInfo->infoTables[j];
        BOOST_REQUIRE_EQUAL(pjTbl.db, qTbl.db);
        BOOST_REQUIRE_EQUAL(pjTbl.table, qTbl.table);
        BOOST_REQUIRE_EQUAL(pjTbl.lockInMemory, qTbl.lockInMemory);
        BOOST_REQUIRE_EQUAL(pjTbl.scanRating, qTbl.scanRating);
    }
}

BOOST_AUTO_TEST_CASE(ScanInfoJsonRoundTrip) {
    auto scanInfo = protojson::ScanInfo::create();
    scanInfo->scanRating = 7;
    scanInfo->infoTables.emplace_back("db1", "table1", true, 30);
    scanInfo->infoTables.emplace_back("db1", "table2", true, 10);
    scanInfo->infoTables.emplace_back("db2", "table3", false, 99);
    scanInfo->sortTablesSlowestFirst();

    // to JSON
    auto js = scanInfo->toJson();

    // Check wire format keys explicitly
    BOOST_REQUIRE_EQUAL(js.at("infoscanrating").get<int>(), 7);
    auto const& jsTables = js.at("infotables");
    BOOST_REQUIRE_EQUAL(jsTables.size(), 3u);
    BOOST_REQUIRE_EQUAL(jsTables[0].at("sidb").get<std::string>(), "db1");
    BOOST_REQUIRE_EQUAL(jsTables[0].at("sitable").get<std::string>(), "table1");
    BOOST_REQUIRE_EQUAL(jsTables[0].at("sirating").get<int>(), 30);
    BOOST_REQUIRE_EQUAL(jsTables[0].at("silockinmem").get<bool>(), true);

    // and back
    auto roundTripped = protojson::ScanInfo::createFromJson(js);

    BOOST_REQUIRE_EQUAL(roundTripped->scanRating, scanInfo->scanRating);
    BOOST_REQUIRE_EQUAL(roundTripped->infoTables.size(), scanInfo->infoTables.size());
    for (size_t k = 0; k < scanInfo->infoTables.size(); ++k) {
        auto const& orig = scanInfo->infoTables[k];
        auto const& rt = roundTripped->infoTables[k];
        BOOST_REQUIRE_EQUAL(rt.db, orig.db);
        BOOST_REQUIRE_EQUAL(rt.table, orig.table);
        BOOST_REQUIRE_EQUAL(rt.lockInMemory, orig.lockInMemory);
        BOOST_REQUIRE_EQUAL(rt.scanRating, orig.scanRating);
    }
}

BOOST_AUTO_TEST_SUITE_END()
