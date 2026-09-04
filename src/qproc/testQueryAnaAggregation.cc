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
#include <utility>
#include <vector>

// Third-party headers

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaAggregation
#include <boost/test/unit_test.hpp>

// LSST headers

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "tests/ParserExpected.h"
#include "qproc/QuerySession.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "sql/SqlConfig.h"
#include "tests/QueryAnaFixture.h"

using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::QueryContext;
using lsst::qserv::query::SelectStmt;
using lsst::qserv::sql::SqlConfig;
using lsst::qserv::tests::QueryAnaFixture;

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(Aggregate, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(Aggregate) {
    std::string stmt =
            "select sum(pm_declErr),chunkId, avg(bMagF2) bmf2 "
            "from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";
    std::string expPar =
            "SELECT sum(`LSST.Object`.`pm_declErr`) AS `QS1_SUM`,"
            "`LSST.Object`.`chunkId` AS `chunkId`,"
            "COUNT(`LSST.Object`.`bMagF2`) AS `QS2_COUNT`,"
            "SUM(`LSST.Object`.`bMagF2`) AS `QS3_SUM` "
            "FROM `LSST`.`Object_100` AS `LSST.Object` "
            "WHERE `LSST.Object`.`bMagF`>20.0 "
            "GROUP BY `chunkId`";
    qsTest.sqlConfig = SqlConfig(SqlConfig::MockDbTableColumns(
            {{"LSST", {{"Object", {"pm_declErr", "chunkId", "bMagF2", "bMagF"}}}}}));
    queryAnaHelper.buildQuerySession(qsTest, stmt);
    auto& qs = queryAnaHelper.querySession;
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_TEST_MESSAGE("produced stmt:" << qs->getStmt());

    BOOST_CHECK(context);
    BOOST_CHECK(!context->secIdxRestrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_REQUIRE(ss.hasGroupBy());

    std::string parallel = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(expPar, parallel);
}

BOOST_AUTO_TEST_CASE(Avg) {
    std::string stmt = "select chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0;";
    std::string expPar =
            "SELECT `LSST.Object`.`chunkId` AS `chunkId`,"
            "COUNT(`LSST.Object`.`bMagF2`) AS `QS1_COUNT`,"
            "SUM(`LSST.Object`.`bMagF2`) AS `QS2_SUM` "
            "FROM `LSST`.`Object_100` AS `LSST.Object` "
            "WHERE `LSST.Object`.`bMagF`>20.0";
    qsTest.sqlConfig = SqlConfig(
            SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "bMagF2", "bMagF"}}}}}));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();

    BOOST_CHECK(context);
    BOOST_CHECK(!context->secIdxRestrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());

    std::string parallel = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(expPar, parallel);
}

/// Check the parallel and merge queries produced for `stmt`.
void checkInternalQueries(QueryAnaFixture& fixture, std::string const& stmt, std::string const& expPar,
                          std::string const& expMerge) {
    lsst::qserv::tests::QueryAnaHelper helper;
    auto queries = helper.getInternalQueries(fixture.qsTest, stmt);
    BOOST_CHECK_EQUAL(expPar, queries[0]);
    BOOST_CHECK_EQUAL(expMerge, queries[1]);
}

BOOST_AUTO_TEST_CASE(ConstantWithAggregate) {
    qsTest.sqlConfig = SqlConfig(
            SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "bMagF", "objectId"}}}}}));

    checkInternalQueries(*this, "select sum(bMagF)+1 from LSST.Object",
                         "SELECT sum(`LSST.Object`.`bMagF`) AS `QS1_SUM` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT (SUM(`QS1_SUM`)+1) AS `(sum(bMagF)+1)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select 1+sum(bMagF) from LSST.Object",
                         "SELECT sum(`LSST.Object`.`bMagF`) AS `QS1_SUM` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT (1+SUM(`QS1_SUM`)) AS `(1+sum(bMagF))` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select count(*)*100 from LSST.Object",
                         "SELECT count(*) AS `QS1_COUNT` FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT (SUM(`QS1_COUNT`)* 100) AS `(count(*)* 100)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select avg(bMagF)-0.5 as m from LSST.Object",
                         "SELECT COUNT(`LSST.Object`.`bMagF`) AS `QS1_COUNT`,"
                         "SUM(`LSST.Object`.`bMagF`) AS `QS2_SUM` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT ((SUM(`QS2_SUM`)/SUM(`QS1_COUNT`))-0.5) AS `m` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");
}

BOOST_AUTO_TEST_CASE(AggregateOnlyExpressions) {
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "bMagF"}}}}}));

    checkInternalQueries(*this, "select sum(bMagF)/count(*) from LSST.Object",
                         "SELECT sum(`LSST.Object`.`bMagF`) AS `QS1_SUM`,"
                         "count(*) AS `QS2_COUNT` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT (SUM(`QS1_SUM`)/SUM(`QS2_COUNT`)) AS `(sum(bMagF)/count(*))` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select max(bMagF)-min(bMagF) from LSST.Object",
                         "SELECT max(`LSST.Object`.`bMagF`) AS `QS1_MAX`,"
                         "min(`LSST.Object`.`bMagF`) AS `QS2_MIN` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT (MAX(`QS1_MAX`)-MIN(`QS2_MIN`)) AS `(max(bMagF)-min(bMagF))` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");
}

BOOST_AUTO_TEST_CASE(ColumnWithAggregateIsRejected) {
    // statement -> the select item quoted back to the user in the error message
    std::vector<std::pair<std::string, std::string>> const stmts = {
            {"select sum(bMagF)+chunkId from LSST.Object group by chunkId",
             "(sum(`LSST`.`Object`.`bMagF`)+`LSST`.`Object`.`chunkId`)"},
            {"select sum(bMagF)+length(designation) from LSST.Object group by designation",
             "(sum(`LSST`.`Object`.`bMagF`)+length(`LSST`.`Object`.`designation`))"},
            {"select sum(bMagF)+(bMagF*2) from LSST.Object group by bMagF",
             "(sum(`LSST`.`Object`.`bMagF`)+(`LSST`.`Object`.`bMagF`* 2))"},
    };
    for (auto const& [stmt, fragment] : stmts) {
        BOOST_TEST_MESSAGE("checking that this is rejected: " << stmt);
        qsTest.sqlConfig = SqlConfig(
                SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "bMagF", "designation"}}}}}));
        lsst::qserv::tests::QueryAnaHelper helper;
        auto qs = helper.buildQuerySession(qsTest, stmt, true);
        BOOST_CHECK_EQUAL(qs->getError(),
                          "AnalysisError:Qserv does not support an aggregate combined with a "
                          "column-dependent expression: \"" +
                                  fragment + "\". Select the aggregate on its own.");
    }
}

BOOST_AUTO_TEST_CASE(NestedAggregateExpressions) {
    qsTest.sqlConfig = SqlConfig(
            SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "bMagF", "objectId"}}}}}));

    checkInternalQueries(*this, "select sum(bMagF)/count(*)*100 from LSST.Object",
                         "SELECT sum(`LSST.Object`.`bMagF`) AS `QS1_SUM`,"
                         "count(*) AS `QS2_COUNT` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT ((SUM(`QS1_SUM`)/SUM(`QS2_COUNT`))* 100) "
                         "AS `((sum(bMagF)/count(*))* 100)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select 100*sum(bMagF)/count(*) from LSST.Object",
                         "SELECT sum(`LSST.Object`.`bMagF`) AS `QS1_SUM`,"
                         "count(*) AS `QS2_COUNT` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT ((100 * SUM(`QS1_SUM`))/SUM(`QS2_COUNT`)) "
                         "AS `((100 * sum(bMagF))/count(*))` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select sum(bMagF)+sum(objectId)+sum(chunkId) from LSST.Object",
                         "SELECT sum(`LSST.Object`.`bMagF`) AS `QS1_SUM`,"
                         "sum(`LSST.Object`.`objectId`) AS `QS2_SUM`,"
                         "sum(`LSST.Object`.`chunkId`) AS `QS3_SUM` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT ((SUM(`QS1_SUM`)+SUM(`QS2_SUM`))+SUM(`QS3_SUM`)) "
                         "AS `((sum(bMagF)+sum(objectId))+sum(chunkId))` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select (max(bMagF)-min(bMagF))/2 from LSST.Object",
                         "SELECT max(`LSST.Object`.`bMagF`) AS `QS1_MAX`,"
                         "min(`LSST.Object`.`bMagF`) AS `QS2_MIN` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT ((MAX(`QS1_MAX`)-MIN(`QS2_MIN`))/2) "
                         "AS `((max(bMagF)-min(bMagF))/2)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select (sum(bMagF)+1)*2 from LSST.Object",
                         "SELECT sum(`LSST.Object`.`bMagF`) AS `QS1_SUM` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT ((SUM(`QS1_SUM`)+1)* 2) AS `((sum(bMagF)+1)* 2)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select (avg(bMagF)+1)*2 from LSST.Object",
                         "SELECT COUNT(`LSST.Object`.`bMagF`) AS `QS1_COUNT`,"
                         "SUM(`LSST.Object`.`bMagF`) AS `QS2_SUM` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT (((SUM(`QS2_SUM`)/SUM(`QS1_COUNT`))+1)* 2) "
                         "AS `((avg(bMagF)+1)* 2)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");
}

BOOST_AUTO_TEST_CASE(NestedScalarAndAggregate) {
    qsTest.sqlConfig = SqlConfig(SqlConfig::MockDbTableColumns(
            {{"LSST", {{"Object", {"chunkId", "bMagF", "objectId", "designation"}}}}}));

    checkInternalQueries(*this, "select max(length(designation)) from LSST.Object",
                         "SELECT max(length(`LSST.Object`.`designation`)) AS `QS1_MAX` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT MAX(`QS1_MAX`) AS `max(length(designation))` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select round(max(bMagF), 2) from LSST.Object",
                         "SELECT max(`LSST.Object`.`bMagF`) AS `QS1_MAX` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT round(MAX(`QS1_MAX`),2) AS `round(max(bMagF),2)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select round(avg(bMagF), 2) from LSST.Object",
                         "SELECT COUNT(`LSST.Object`.`bMagF`) AS `QS1_COUNT`,"
                         "SUM(`LSST.Object`.`bMagF`) AS `QS2_SUM` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT round((SUM(`QS2_SUM`)/SUM(`QS1_COUNT`)),2) "
                         "AS `round(avg(bMagF),2)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select coalesce(NULL, max(bMagF)) from LSST.Object",
                         "SELECT max(`LSST.Object`.`bMagF`) AS `QS1_MAX` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT coalesce(NULL,MAX(`QS1_MAX`)) AS `coalesce(NULL,max(bMagF))` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this,
                         "select chunkId, max(length(designation)) m from LSST.Object group by chunkId",
                         "SELECT `LSST.Object`.`chunkId` AS `chunkId`,"
                         "max(length(`LSST.Object`.`designation`)) AS `QS1_MAX` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object` GROUP BY `chunkId`",
                         "SELECT `chunkId` AS `chunkId`,MAX(`QS1_MAX`) AS `m` "
                         "FROM `LSST`.`Object` AS `LSST.Object` GROUP BY `chunkId`");
}

BOOST_AUTO_TEST_CASE(PercentageStressCase) {
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "designation"}}}}}));

    checkInternalQueries(*this, "select round(100.0*count(designation)/count(*), 1) from LSST.Object",
                         "SELECT count(`LSST.Object`.`designation`) AS `QS1_COUNT`,"
                         "count(*) AS `QS2_COUNT` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT round((100.0 * SUM(`QS1_COUNT`))/SUM(`QS2_COUNT`),1) "
                         "AS `round((100.0 * count(designation))/count(*),1)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this,
                         "select round(100.0*count(designation)/count(*), 1) as pct "
                         "from LSST.Object group by chunkId",
                         "SELECT count(`LSST.Object`.`designation`) AS `QS1_COUNT`,"
                         "count(*) AS `QS2_COUNT` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object` "
                         "GROUP BY `LSST.Object`.`chunkId`",
                         "SELECT round((100.0 * SUM(`QS1_COUNT`))/SUM(`QS2_COUNT`),1) AS `pct` "
                         "FROM `LSST`.`Object` AS `LSST.Object` "
                         "GROUP BY `LSST.Object`.`chunkId`");
}

// An aggregate inside another aggregate is invalid SQL; reject it (previously this was done in the
// parser adapter layer, but provides better diagnostics in qana).
BOOST_AUTO_TEST_CASE(IllegalAggregatePlacement) {
    std::vector<std::pair<std::string, std::string>> const stmts = {
            {"select max(sum(bMagF)) from LSST.Object",
             "AnalysisError:Qserv does not support an aggregate directly inside another aggregate"},
            {"select sum(count(objectId)) from LSST.Object",
             "AnalysisError:Qserv does not support an aggregate directly inside another aggregate"},
            {"select objectId from LSST.Object where sum(bMagF) > 5",
             "ParseException:HyriseAdapter unsupported SQL construct: aggregate function in WHERE"},
            {"select objectId from LSST.Object where round(sum(bMagF), 2) > 5",
             "ParseException:HyriseAdapter unsupported SQL construct: aggregate function in WHERE"},
    };
    for (auto const& [stmt, expectedPrefix] : stmts) {
        BOOST_TEST_MESSAGE("checking that this is rejected: " << stmt);
        qsTest.sqlConfig = SqlConfig(
                SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "bMagF", "objectId"}}}}}));
        lsst::qserv::tests::QueryAnaHelper helper;
        auto qs = helper.buildQuerySession(qsTest, stmt, true);
        BOOST_CHECK_MESSAGE(qs->getError().rfind(expectedPrefix, 0) == 0,
                            "expected an error starting \"" << expectedPrefix << "\" but got \""
                                                            << qs->getError() << "\"");
    }
}

BOOST_AUTO_TEST_CASE(BitwiseAggregates) {
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST", {{"Object", {"chunkId", "flags"}}}}}));

    checkInternalQueries(*this, "select bit_or(flags) from LSST.Object",
                         "SELECT bit_or(`LSST.Object`.`flags`) AS `QS1_BIT_OR` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT BIT_OR(`QS1_BIT_OR`) AS `bit_or(flags)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select bit_and(flags) from LSST.Object",
                         "SELECT bit_and(`LSST.Object`.`flags`) AS `QS1_BIT_AND` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT BIT_AND(`QS1_BIT_AND`) AS `bit_and(flags)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select bit_xor(flags) from LSST.Object",
                         "SELECT bit_xor(`LSST.Object`.`flags`) AS `QS1_BIT_XOR` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT BIT_XOR(`QS1_BIT_XOR`) AS `bit_xor(flags)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");

    checkInternalQueries(*this, "select chunkId, bit_or(flags), max(flags) from LSST.Object group by chunkId",
                         "SELECT `LSST.Object`.`chunkId` AS `chunkId`,"
                         "bit_or(`LSST.Object`.`flags`) AS `QS1_BIT_OR`,"
                         "max(`LSST.Object`.`flags`) AS `QS2_MAX` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object` GROUP BY `chunkId`",
                         "SELECT `chunkId` AS `chunkId`,"
                         "BIT_OR(`QS1_BIT_OR`) AS `bit_or(flags)`,"
                         "MAX(`QS2_MAX`) AS `max(flags)` "
                         "FROM `LSST`.`Object` AS `LSST.Object` GROUP BY `chunkId`");

    checkInternalQueries(*this, "select bit_or(flags & 255) from LSST.Object",
                         "SELECT bit_or(`LSST.Object`.`flags`&255) AS `QS1_BIT_OR` "
                         "FROM `LSST`.`Object_100` AS `LSST.Object`",
                         "SELECT BIT_OR(`QS1_BIT_OR`) AS `bit_or(flags&255)` "
                         "FROM `LSST`.`Object` AS `LSST.Object`");
}

BOOST_AUTO_TEST_SUITE_END()
