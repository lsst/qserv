// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @file
  *
  * @brief Test C++ parsing and query analysis logic for select expressions
  * with an "ORDER BY" clause.
  *
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <string>

// Third-party headers
#include "boost/algorithm/string.hpp"
#include "boost/format.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaOrderBy
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "parser/SelectParser.h"
#include "query/SelectStmt.h"
#include "sql/MockSql.h"
#include "tests/QueryAnaFixture.h"

using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::parser::SelectParser;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::SelectStmt;
using lsst::qserv::sql::MockSql;
using lsst::qserv::tests::QueryAnaFixture;
using lsst::qserv::tests::QueryAnaHelper;


inline void check(QuerySession::Test qsTest, QueryAnaHelper queryAnaHelper,
                  std::string stmt, std::string expectedParallel,
                  std::string expectedMerge, std::string expectedProxyOrderBy) {
    std::vector<std::string> expectedQueries = { expectedParallel, expectedMerge, expectedProxyOrderBy };
    std::vector<std::string> queries;
    BOOST_REQUIRE_NO_THROW(queries = queryAnaHelper.getInternalQueries(qsTest, stmt));
    BOOST_CHECK_EQUAL_COLLECTIONS(queries.begin(), queries.end(),
                                  expectedQueries.begin(), expectedQueries.end());
}

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(OrderBy, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(OrderBy) {
    std::string stmt = "SELECT objectId, taiMidPoint "
        "FROM Source "
        "ORDER BY objectId ASC";
    std::string expectedParallel = "SELECT `LSST.Source`.objectId AS `objectId`,`LSST.Source`.taiMidPoint AS `taiMidPoint` FROM LSST.Source_100 AS `LSST.Source`";
    std::string expectedMerge = "";
    std::string expectedProxyOrderBy = "ORDER BY `objectId` ASC";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Source", {"objectId", "taiMidPoint"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByNotChunked) {
    std::string stmt = "SELECT filterId FROM Filter ORDER BY filterId";
    std::string expectedParallel = "SELECT `LSST.Filter`.filterId AS `filterId` FROM LSST.Filter AS `LSST.Filter`";
    std::string expectedMerge = "";
    std::string expectedProxyOrderBy = "ORDER BY `filterId`";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Filter", {"filterId"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByTwoField) {
    std::string stmt = "SELECT objectId, taiMidPoint "
        "FROM Source "
        "ORDER BY objectId, taiMidPoint ASC";
    std::string expectedParallel = "SELECT `LSST.Source`.objectId AS `objectId`,`LSST.Source`.taiMidPoint AS `taiMidPoint` FROM LSST.Source_100 AS `LSST.Source`";
    std::string expectedMerge = "";
    std::string expectedProxyOrderBy = "ORDER BY `objectId`, `taiMidPoint` ASC";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Source", {"objectId", "taiMidPoint"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByThreeField) {
    std::string stmt = "SELECT objectId, taiMidPoint, xFlux "
        "FROM Source "
        "ORDER BY objectId, taiMidPoint, xFlux DESC";
    std::string expectedParallel = "SELECT `LSST.Source`.objectId AS `objectId`,"
                                   "`LSST.Source`.taiMidPoint AS `taiMidPoint`,`LSST.Source`.xFlux AS `xFlux` "
                                   "FROM LSST.Source_100 AS `LSST.Source`";
    std::string expectedMerge = "";
    std::string expectedProxyOrderBy = "ORDER BY `objectId`, `taiMidPoint`, `xFlux` DESC";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Source", {"objectId", "taiMidPoint", "xFlux"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByAggregate) {
    std::string stmt = "SELECT objectId, AVG(taiMidPoint) "
        "FROM Source "
        "GROUP BY objectId "
        "ORDER BY objectId ASC";
    std::string expectedParallel = "SELECT `LSST.Source`.objectId AS `objectId`,COUNT(`LSST.Source`.taiMidPoint) AS `QS1_COUNT`,SUM(`LSST.Source`.taiMidPoint) AS `QS2_SUM` "
                                   "FROM LSST.Source_100 AS `LSST.Source` "
                                   "GROUP BY `objectId`";
    std::string expectedMerge = "SELECT objectId AS `objectId`,(SUM(QS2_SUM)/SUM(QS1_COUNT)) AS `AVG(taiMidPoint)` GROUP BY `objectId`";
    std::string expectedProxyOrderBy = "ORDER BY `objectId` ASC";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Source", {"objectId", "taiMidPoint"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByAggregateNotChunked) {
    std::string stmt =
            "SELECT filterId, SUM(photClam) FROM Filter GROUP BY filterId ORDER BY filterId";
    std::string expectedParallel =
            "SELECT `LSST.Filter`.filterId AS `filterId`,SUM(`LSST.Filter`.photClam) AS `QS1_SUM` FROM LSST.Filter AS `LSST.Filter` GROUP BY `filterId`";
    // FIXME merge query is not useful here, see DM-3166
    std::string expectedMerge = "SELECT filterId AS `filterId`,SUM(QS1_SUM) AS `SUM(photClam)` GROUP BY `filterId`";
    std::string expectedProxyOrderBy = "ORDER BY `filterId`";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Filter", {"filterId", "photClam"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByLimit) {
    std::string stmt = "SELECT objectId, taiMidPoint "
            "FROM Source "
            "ORDER BY objectId ASC LIMIT 5";
    std::string expectedParallel =
            "SELECT `LSST.Source`.objectId AS `objectId`,`LSST.Source`.taiMidPoint AS `taiMidPoint` FROM LSST.Source_100 AS `LSST.Source` ORDER BY `objectId` ASC LIMIT 5";
    std::string expectedMerge =
            "SELECT objectId AS `objectId`,taiMidPoint AS `taiMidPoint` ORDER BY `objectId` ASC LIMIT 5";
    std::string expectedProxyOrderBy = "ORDER BY `objectId` ASC";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Source", {"objectId", "taiMidPoint"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByLimitNotChunked) { // Test flipped syntax in DM-661
    std::string bad =  "SELECT run, field FROM LSST.Science_Ccd_Exposure limit 2 order by field";
    std::string good = "SELECT run, field FROM LSST.Science_Ccd_Exposure order by field limit 2";
    std::string expectedParallel = "SELECT `LSST.Science_Ccd_Exposure`.run AS `run`,`LSST.Science_Ccd_Exposure`.field AS `field` "
                                   "FROM LSST.Science_Ccd_Exposure AS `LSST.Science_Ccd_Exposure` "
                                   "ORDER BY `field` "
                                   "LIMIT 2";
    std::string expectedMerge = "";
    std::string expectedProxyOrderBy = "ORDER BY `field`";
    // TODO: commented out test that is supposed to fail but it does not currently
    // check(qsTest, queryAnaHelper, bad, expectedParallel, expectedMerge, expectedProxyOrderBy);
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Science_Ccd_Exposure", {"run", "field"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, good, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByAggregateLimit) {
    std::string stmt = "SELECT objectId, AVG(taiMidPoint) "
        "FROM Source "
        "GROUP BY objectId "
        "ORDER BY objectId ASC LIMIT 2";
    std::string expectedParallel = "SELECT `LSST.Source`.objectId AS `objectId`,COUNT(`LSST.Source`.taiMidPoint) AS `QS1_COUNT`,SUM(`LSST.Source`.taiMidPoint) AS `QS2_SUM` "
                                   "FROM LSST.Source_100 AS `LSST.Source` "
                                   "GROUP BY `objectId` "
                                   "ORDER BY `objectId` ASC LIMIT 2";
    std::string expectedMerge = "SELECT objectId AS `objectId`,(SUM(QS2_SUM)/SUM(QS1_COUNT)) AS `AVG(taiMidPoint)` "
                                "GROUP BY `objectId` "
                                "ORDER BY `objectId` ASC LIMIT 2";
    std::string expectedProxyOrderBy = "ORDER BY `objectId` ASC";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Source", {"objectId", "taiMidPoint"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_CASE(OrderByAggregateNotChunkedLimit) {
    std::string stmt = "SELECT filterId, SUM(photClam) FROM Filter GROUP BY filterId ORDER BY filterId LIMIT 3";
    std::string expectedParallel = "SELECT `LSST.Filter`.filterId AS `filterId`,SUM(`LSST.Filter`.photClam) AS `QS1_SUM` "
                                   "FROM LSST.Filter AS `LSST.Filter` "
                                   "GROUP BY `filterId` "
                                   "ORDER BY `filterId` "
                                   "LIMIT 3";
    // FIXME merge query is not useful here, see DM-3166
    std::string expectedMerge = "SELECT filterId AS `filterId`,SUM(QS1_SUM) AS `SUM(photClam)` GROUP BY `filterId` ORDER BY `filterId` LIMIT 3";
    std::string expectedProxyOrderBy = "ORDER BY `filterId`";
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Filter", {"filterId", "photClam"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    check(qsTest, queryAnaHelper, stmt, expectedParallel, expectedMerge, expectedProxyOrderBy);
}

BOOST_AUTO_TEST_SUITE_END()
