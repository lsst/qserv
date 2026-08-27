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
#include "ccontrol/ParseRunner.h"
#include "css/CssAccess.h"
#include "css/ScanTableParams.h"
#include "qana/ScanTablePlugin.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/TestFactory.h"
#include "sql/SqlConfig.h"

// Boost unit test header
#define BOOST_TEST_MODULE ScanTablePlugin
#include <boost/test/unit_test.hpp>

using namespace lsst::qserv;

namespace {

char const* const KVMAP = R"kvmap(
{
    "\/css_meta": "",
    "\/css_meta\/version": "1",
    "\/DBS": "",
    "\/DBS\/Somedb": "READY",
    "\/DBS\/Somedb\/TABLES": "",
    "\/DBS\/Somedb\/TABLES\/Object": "READY"
}
)kvmap";

}  // namespace

struct TestFixture {
    TestFixture()
            : schemaCfg(sql::SqlConfig::MockDbTableColumns({{"Somedb", {{"Object", {"objectId"}}}}})),
              css(css::CssAccess::createFromData(KVMAP, false)) {
        css->setScanTableParams("Somedb", "Object", css::ScanTableParams(true, 1));
    }

    query::ScanInfo::Ptr runScanTablePlugin(std::string const& query, int interactiveChunkLimit = 0,
                                            int chunkCount = 0) {
        query::SelectStmt::Ptr selectStmt;
        BOOST_REQUIRE_NO_THROW(selectStmt = ccontrol::ParseRunner::makeSelectStmt(query));
        BOOST_REQUIRE(selectStmt != nullptr);

        query::TestFactory factory;
        std::shared_ptr<query::QueryContext> context = factory.newContext(css, schemaCfg);
        context->chunkCount = chunkCount;

        qana::ScanTablePlugin plugin(interactiveChunkLimit);
        plugin.applyLogical(*selectStmt, *context);
        plugin.applyFinal(*context);
        return context->scanInfo;
    }

    sql::SqlConfig schemaCfg;
    std::shared_ptr<css::CssAccess> css;
};

BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

BOOST_AUTO_TEST_CASE(BareStarSelectIsScan) {
    auto const scanInfo = runScanTablePlugin("SELECT * FROM Somedb.Object");
    BOOST_REQUIRE(scanInfo);
    BOOST_CHECK_EQUAL(scanInfo->infoTables.size(), 1u);
    BOOST_CHECK_EQUAL(scanInfo->scanRating, 1);
}

BOOST_AUTO_TEST_CASE(ColumnListNoWhereIsScan) {
    auto const scanInfo = runScanTablePlugin("SELECT objectId FROM Somedb.Object");
    BOOST_REQUIRE(scanInfo);
    BOOST_CHECK_EQUAL(scanInfo->infoTables.size(), 1u);
    BOOST_CHECK_EQUAL(scanInfo->scanRating, 1);
}

// COUNT(*) reads no specific columns and has no WHERE, so it's NOT a scan.
BOOST_AUTO_TEST_CASE(CountStarNotScan) {
    auto const scanInfo = runScanTablePlugin("SELECT COUNT(*) FROM Somedb.Object");
    BOOST_REQUIRE(scanInfo);
    BOOST_CHECK_EQUAL(scanInfo->infoTables.size(), 0u);
}

// COUNT(*) reads no specific columns, but a WHERE column ref still makes it a scan.
BOOST_AUTO_TEST_CASE(CountStarWithWhereIsScan) {
    auto const scanInfo = runScanTablePlugin("SELECT COUNT(*) FROM Somedb.Object WHERE objectId > 5");
    BOOST_REQUIRE(scanInfo);
    BOOST_CHECK_EQUAL(scanInfo->infoTables.size(), 1u);
    BOOST_CHECK_EQUAL(scanInfo->scanRating, 1);
}

// Test applyFinal() squash when the query touches less chunks than interactive chunk limit.
BOOST_AUTO_TEST_CASE(ScanSquashedBelowInteractiveChunkLimit) {
    auto const scanInfo = runScanTablePlugin("SELECT * FROM Somedb.Object", 10, 3);
    BOOST_REQUIRE(scanInfo);
    BOOST_CHECK_EQUAL(scanInfo->infoTables.size(), 0u);
    BOOST_CHECK_EQUAL(scanInfo->scanRating, 0);
}

// Opposite the case above, not squashed because we're at the interactive chunk limit.
BOOST_AUTO_TEST_CASE(ScanNotSquashedAtInteractiveChunkLimit) {
    auto const scanInfo = runScanTablePlugin("SELECT * FROM Somedb.Object", 10, 10);
    BOOST_REQUIRE(scanInfo);
    BOOST_CHECK_EQUAL(scanInfo->infoTables.size(), 1u);
    BOOST_CHECK_EQUAL(scanInfo->scanRating, 1);
}

BOOST_AUTO_TEST_SUITE_END()
