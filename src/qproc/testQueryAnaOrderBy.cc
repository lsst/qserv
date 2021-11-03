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
#include <string>
#include <ostream>
#include <vector>

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaOrderBy
#include <boost/test/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

// Qserv headers
#include "css/CssAccess.h"
#include "qproc/QuerySession.h"
#include "query/SelectStmt.h"
#include "sql/SqlConfig.h"
#include "tests/QueryAnaHelper.h"
#include "tests/testKvMap.h"


namespace {
    const std::string defaultDb = "LSST";
}


using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(OrderBy)

struct Data {
    Data(std::string const& stmt_,
         std::string const& expectedParallel_,
         std::string const& expectedMerge_,
         std::string const& expectedProxyOrderBy_,
         sql::SqlConfig const& sqlConfig_)
        : stmt(stmt_), expectedParallel(expectedParallel_), expectedMerge(expectedMerge_),
          expectedProxyOrderBy(expectedProxyOrderBy_), sqlConfig(sqlConfig_)
    {}

    std::string stmt;
    std::string expectedParallel;
    std::string expectedMerge;
    std::string expectedProxyOrderBy;
    sql::SqlConfig sqlConfig;
};


std::ostream& operator<<(std::ostream& os, Data const& data) {
    os << "\n\t stmt: " << data.stmt << "\n";
    os << "\t expected paralellel: " << data.expectedParallel << "\n";
    os << "\t expected merge: " << data.expectedMerge << "\n";
    os << "\t expected proxy order by: " << data.expectedProxyOrderBy << "\n";
    return os;
}


static const std::vector<Data> DATA = {
    // Order By:
    Data("SELECT objectId, taiMidPoint "
            "FROM Source "
            "ORDER BY objectId ASC",
        "SELECT `LSST.Source`.`objectId` AS `objectId`,`LSST.Source`.`taiMidPoint` AS `taiMidPoint` FROM `LSST`.`Source_100` AS `LSST.Source`",
        "",
        "ORDER BY `objectId` ASC",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Source", {"objectId", "taiMidPoint"}}}}}))),
    // Order by not chunked:

    Data("SELECT filterId FROM Filter ORDER BY filterId",
        "SELECT `LSST.Filter`.`filterId` AS `filterId` FROM `LSST`.`Filter` AS `LSST.Filter`",
        "",
        "ORDER BY `filterId`",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Filter", {"filterId"}}}}}))),

    // OrderByTwoField
    Data("SELECT objectId, taiMidPoint "
            "FROM Source "
            "ORDER BY objectId, taiMidPoint ASC",
        "SELECT `LSST.Source`.`objectId` AS `objectId`,`LSST.Source`.`taiMidPoint` AS `taiMidPoint` FROM `LSST`.`Source_100` AS `LSST.Source`",
        "",
        "ORDER BY `objectId`, `taiMidPoint` ASC",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Source", {"objectId", "taiMidPoint"}}}}}))),

    // OrderByThreeField
    Data("SELECT objectId, taiMidPoint, xFlux "
            "FROM Source "
            "ORDER BY objectId, taiMidPoint, xFlux DESC",
        "SELECT `LSST.Source`.`objectId` AS `objectId`,"
            "`LSST.Source`.`taiMidPoint` AS `taiMidPoint`,`LSST.Source`.`xFlux` AS `xFlux` "
            "FROM `LSST`.`Source_100` AS `LSST.Source`",
        "",
        "ORDER BY `objectId`, `taiMidPoint`, `xFlux` DESC",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Source", {"objectId", "taiMidPoint", "xFlux"}}}}}))),

    // OrderByAggregate
    Data("SELECT objectId, AVG(taiMidPoint) "
            "FROM Source "
            "GROUP BY objectId "
            "ORDER BY objectId ASC",
        "SELECT `LSST.Source`.`objectId` AS `objectId`,COUNT(`LSST.Source`.`taiMidPoint`) AS `QS1_COUNT`,SUM(`LSST.Source`.`taiMidPoint`) AS `QS2_SUM` "
            "FROM `LSST`.`Source_100` AS `LSST.Source` "
            "GROUP BY `objectId`",
        "SELECT `objectId` AS `objectId`,(SUM(`QS2_SUM`)/SUM(`QS1_COUNT`)) AS `AVG(taiMidPoint)` "
            "FROM `LSST`.`Source` AS `LSST.Source` "
            "GROUP BY `objectId`",
        "ORDER BY `objectId` ASC",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Source", {"objectId", "taiMidPoint"}}}}}))),

    // OrderByAggregateNotChunked)
    Data("SELECT filterId, SUM(photClam) FROM Filter GROUP BY filterId ORDER BY filterId",
        "SELECT `LSST.Filter`.`filterId` AS `filterId`,SUM(`LSST.Filter`.`photClam`) AS `QS1_SUM` FROM `LSST`.`Filter` AS `LSST.Filter` GROUP BY `filterId`",
        // FIXME merge query is not useful here, see DM-3166
        "SELECT `filterId` AS `filterId`,SUM(`QS1_SUM`) AS `SUM(photClam)` "
            "FROM `LSST`.`Filter` AS `LSST.Filter` "
            "GROUP BY `filterId`",
        "ORDER BY `filterId`",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Filter", {"filterId", "photClam"}}}}}))),

    // OrderByLimit
    Data("SELECT objectId, taiMidPoint "
            "FROM Source "
            "ORDER BY objectId ASC LIMIT 5",
        "SELECT `LSST.Source`.`objectId` AS `objectId`,`LSST.Source`.`taiMidPoint` AS `taiMidPoint` FROM `LSST`.`Source_100` AS `LSST.Source` ORDER BY `objectId` ASC LIMIT 5",
        "SELECT `objectId` AS `objectId`,`taiMidPoint` AS `taiMidPoint` "
            "FROM `LSST`.`Source` AS `LSST.Source` "
            "ORDER BY `objectId` ASC LIMIT 5",
        "ORDER BY `objectId` ASC",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Source", {"objectId", "taiMidPoint"}}}}}))),

    // OrderByLimitNotChunked
        Data("SELECT run, field FROM LSST.Science_Ccd_Exposure order by field limit 2",
            "SELECT `LSST.Science_Ccd_Exposure`.`run` AS `run`,`LSST.Science_Ccd_Exposure`.`field` AS `field` "
                "FROM `LSST`.`Science_Ccd_Exposure` AS `LSST.Science_Ccd_Exposure` "
                "ORDER BY `field` "
                "LIMIT 2",
            "",
            "ORDER BY `field`",
            sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Science_Ccd_Exposure", {"run", "field"}}}}}))),

    // OrderByAggregateLimit
        Data( "SELECT objectId, AVG(taiMidPoint) "
            "FROM Source "
            "GROUP BY objectId "
            "ORDER BY objectId ASC LIMIT 2",
        "SELECT `LSST.Source`.`objectId` AS `objectId`,COUNT(`LSST.Source`.`taiMidPoint`) AS `QS1_COUNT`,SUM(`LSST.Source`.`taiMidPoint`) AS `QS2_SUM` "
            "FROM `LSST`.`Source_100` AS `LSST.Source` "
            "GROUP BY `objectId` "
            "ORDER BY `objectId` ASC",
        "SELECT `objectId` AS `objectId`,(SUM(`QS2_SUM`)/SUM(`QS1_COUNT`)) AS `AVG(taiMidPoint)` "
            "FROM `LSST`.`Source` AS `LSST.Source` "
            "GROUP BY `objectId` "
            "ORDER BY `objectId` ASC LIMIT 2",
        "ORDER BY `objectId` ASC",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Source", {"objectId", "taiMidPoint"}}}}}))),

    // OrderByAggregateNotChunkedLimit
    Data("SELECT filterId, SUM(photClam) FROM Filter GROUP BY filterId ORDER BY filterId LIMIT 3",
        "SELECT `LSST.Filter`.`filterId` AS `filterId`,SUM(`LSST.Filter`.`photClam`) AS `QS1_SUM` "
            "FROM `LSST`.`Filter` AS `LSST.Filter` "
            "GROUP BY `filterId` "
            "ORDER BY `filterId`",
       // FIXME merge query is not useful here, see DM-3166
        "SELECT `filterId` AS `filterId`,SUM(`QS1_SUM`) AS `SUM(photClam)` "
            "FROM `LSST`.`Filter` AS `LSST.Filter` "
            "GROUP BY `filterId` ORDER BY `filterId` LIMIT 3",
        "ORDER BY `filterId`",
        sql::SqlConfig(sql::SqlConfig::MockDbTableColumns({{defaultDb, {{"Filter", {"filterId", "photClam"}}}}}))),
};


BOOST_DATA_TEST_CASE(OrderByTest, DATA, data) {
    qproc::QuerySession::Test qsTest(0, // "Config number". I don't know its purpose; it has always been 0.
                                     css::CssAccess::createFromData(testKvMap, "."),
                                     defaultDb,
                                     data.sqlConfig);
    std::vector<std::string> queries;
    tests::QueryAnaHelper queryAnaHelper;
    auto querySession = queryAnaHelper.buildQuerySession(qsTest, data.stmt);

    BOOST_REQUIRE_NO_THROW(
        BOOST_CHECK_EQUAL(queryAnaHelper.buildFirstParallelQuery(), data.expectedParallel));

    if (querySession->needsMerge()) {
        BOOST_CHECK_EQUAL(querySession->getMergeStmt()->getQueryTemplate().sqlFragment(), data.expectedMerge);
    }
    else {
        BOOST_CHECK_EQUAL(data.expectedMerge.empty(), true);
        BOOST_CHECK_EQUAL(querySession->getMergeStmt(), nullptr);
    }

    BOOST_CHECK_EQUAL(querySession->getResultOrderBy(), data.expectedProxyOrderBy);
}

BOOST_AUTO_TEST_SUITE_END()
