// -*- LSST-C++ -*-
/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

/**
 * @file
 *
 * @brief Test the chunk-level rewrite of qana/MatchEquiJoinRewrite.h.
 */

// System headers
#include <memory>
#include <optional>
#include <string>

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaMatchEquiJoin
#include <boost/test/unit_test.hpp>

// Qserv headers
#include "qana/MatchEquiJoinRewrite.h"
#include "qproc/QuerySession.h"
#include "sql/SqlConfig.h"
#include "tests/QueryAnaFixture.h"

using lsst::qserv::qana::forceMatchEquiJoinRewrite;
using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::sql::SqlConfig;
using lsst::qserv::tests::QueryAnaFixture;

namespace {

struct MatchEquiJoinFixture : QueryAnaFixture {
    MatchEquiJoinFixture() { forceMatchEquiJoinRewrite(true); }
    ~MatchEquiJoinFixture() { forceMatchEquiJoinRewrite(std::nullopt); }

    /// Return the chunk query spec built for (fake) chunk 100 of `stmt`.
    std::shared_ptr<ChunkQuerySpec> buildChunk100QuerySpec(std::string const& stmt) {
        std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
        qs->addChunk(ChunkSpec::makeFake(100, true));
        BOOST_REQUIRE(qs->cQueryBegin() != qs->cQueryEnd());
        auto queryTemplates = qs->makeQueryTemplates();
        return qs->buildChunkQuerySpec(queryTemplates, *qs->cQueryBegin());
    }
};

}  // namespace

BOOST_FIXTURE_TEST_SUITE(MatchEquiJoin, MatchEquiJoinFixture)

BOOST_AUTO_TEST_CASE(FirstDirectorOverlap) {
    std::string stmt =
            "SELECT count(*) FROM   Object o "
            "INNER JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId) "
            "INNER JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) "
            "WHERE  closestToObj = 1 OR closestToObj is NULL;";
    std::string expected_100_chunk =
            "SELECT count(*) AS `QS1_COUNT` "
            "FROM `LSST`.`Object_100` AS `o` "
            "JOIN `LSST`.`RefObjMatch_100` AS `o2t`"
            " ON `o`.`objectIdObjTest`=`o2t`.`objectId` "
            "JOIN `LSST`.`SimRefObject_100` AS `t` ON "
            "`o2t`.`refObjectId`=`t`.`refObjectId` "
            "WHERE `o`.`closestToObj`=1 OR `o`.`closestToObj` IS NULL";
    std::string expected_100_chunk_overlap =
            "SELECT count(*) AS `QS1_COUNT` "
            "FROM `LSST`.`Object_100` AS `o` "
            "JOIN `LSST`.`RefObjMatch_100` AS `o2t`"
            " ON `o`.`objectIdObjTest`=`o2t`.`objectId` "
            "JOIN `LSST`.`SimRefObjectFullOverlap_100` AS `t` ON "
            "`o2t`.`refObjectId`=`t`.`refObjectId` "
            "LEFT OUTER JOIN `LSST`.`SimRefObjectFullOverlap_100` AS `_qserv_overlap_dup` ON "
            "`_qserv_overlap_dup`.`refObjectId`=`t`.`refObjectId` AND "
            "`_qserv_overlap_dup`.`subChunkId`<`t`.`subChunkId` "
            "WHERE (`o2t`.`flags`&1)=0 AND `_qserv_overlap_dup`.`refObjectId` IS NULL AND "
            "(`o`.`closestToObj`=1 OR `o`.`closestToObj` IS NULL)";
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST",
                                                      {{"Object", {"objectIdObjTest", "closestToObj"}},
                                                       {"RefObjMatch", {"objectId", "refObjectId"}},
                                                       {"SimRefObject", {"refObjectId"}}}}}));
    auto spec = buildChunk100QuerySpec(stmt);
    auto const& templates = spec->queries->getTemplates();
    BOOST_REQUIRE_EQUAL(templates.size(), 2u);
    BOOST_CHECK_EQUAL(templates[0], expected_100_chunk);
    BOOST_CHECK_EQUAL(templates[1], expected_100_chunk_overlap);
    BOOST_CHECK(spec->subChunkTables.empty());
    BOOST_CHECK(spec->subChunkIds.empty());
}

BOOST_AUTO_TEST_CASE(SecondDirectorOverlap) {
    std::string stmt =
            "SELECT t.refRa, o.ra_Test FROM SimRefObject t "
            "JOIN RefObjMatch o2t ON (o2t.refObjectId = t.refObjectId) "
            "JOIN Object o ON (o.objectIdObjTest = o2t.objectId)";
    std::string expected_100_chunk_overlap =
            "SELECT `t`.`refRa` AS `t.refRa`,`o`.`ra_Test` AS `o.ra_Test` "
            "FROM `LSST`.`SimRefObject_100` AS `t` "
            "JOIN `LSST`.`RefObjMatch_100` AS `o2t` ON `o2t`.`refObjectId`=`t`.`refObjectId` "
            "JOIN `LSST`.`ObjectFullOverlap_100` AS `o` ON `o`.`objectIdObjTest`=`o2t`.`objectId` "
            "LEFT OUTER JOIN `LSST`.`ObjectFullOverlap_100` AS `_qserv_overlap_dup` ON "
            "`_qserv_overlap_dup`.`objectIdObjTest`=`o`.`objectIdObjTest` AND "
            "`_qserv_overlap_dup`.`subChunkId`<`o`.`subChunkId` "
            "WHERE (`o2t`.`flags`&2)=0 AND `_qserv_overlap_dup`.`objectIdObjTest` IS NULL";
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST",
                                                      {{"Object", {"objectIdObjTest", "ra_Test"}},
                                                       {"RefObjMatch", {"objectId", "refObjectId"}},
                                                       {"SimRefObject", {"refObjectId", "refRa"}}}}}));
    auto spec = buildChunk100QuerySpec(stmt);
    auto const& templates = spec->queries->getTemplates();
    BOOST_REQUIRE_EQUAL(templates.size(), 2u);
    BOOST_CHECK_EQUAL(templates[1], expected_100_chunk_overlap);
    BOOST_CHECK(spec->subChunkIds.empty());
}

BOOST_AUTO_TEST_CASE(WithNearNeighbor) {
    // near-neighbor predicate between directors still requires sub-chunks. Ensure we do not optimize.
    std::string stmt =
            "SELECT o.ra_Test FROM Object o "
            "JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId) "
            "JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) "
            "WHERE scisql_angSep(o.ra_Test, o.decl_Test, t.refRa, t.refDecl) < 0.001";
    qsTest.sqlConfig = SqlConfig(
            SqlConfig::MockDbTableColumns({{"LSST",
                                            {{"Object", {"objectIdObjTest", "ra_Test", "decl_Test"}},
                                             {"RefObjMatch", {"objectId", "refObjectId"}},
                                             {"SimRefObject", {"refObjectId", "refRa", "refDecl"}}}}}));
    auto spec = buildChunk100QuerySpec(stmt);
    auto const& templates = spec->queries->getTemplates();
    BOOST_REQUIRE_EQUAL(templates.size(), 2u);
    BOOST_CHECK_NE(templates[1].find("`SimRefObjectFullOverlap_100_%S\007S%`"), std::string::npos);
    BOOST_CHECK_EQUAL(templates[1].find("_qserv_overlap_dup"), std::string::npos);
    BOOST_CHECK(!spec->subChunkIds.empty());
}

BOOST_AUTO_TEST_CASE(SelectStar) {
    // The anti-join would add columns to "SELECT *", so sub-chunks are still used. Should not impact users
    // running behind the TAP service upstream.
    std::string stmt =
            "SELECT * FROM Object o "
            "JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId) "
            "JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId)";
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST",
                                                      {{"Object", {"objectIdObjTest"}},
                                                       {"RefObjMatch", {"objectId", "refObjectId"}},
                                                       {"SimRefObject", {"refObjectId"}}}}}));
    auto spec = buildChunk100QuerySpec(stmt);
    BOOST_CHECK(!spec->subChunkIds.empty());
    for (auto const& qt : spec->queries->getTemplates()) {
        BOOST_CHECK_EQUAL(qt.find("_qserv_overlap_dup"), std::string::npos);
    }
}

BOOST_AUTO_TEST_CASE(OneDirectorNoOverlap) {
    std::string stmt =
            "SELECT o.ra_Test FROM Object o JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId)";
    qsTest.sqlConfig = SqlConfig(SqlConfig::MockDbTableColumns(
            {{"LSST", {{"Object", {"objectIdObjTest", "ra_Test"}}, {"RefObjMatch", {"objectId"}}}}}));
    auto spec = buildChunk100QuerySpec(stmt);
    auto const& templates = spec->queries->getTemplates();
    BOOST_REQUIRE_EQUAL(templates.size(), 1u);
    BOOST_CHECK_EQUAL(templates[0].find("Overlap"), std::string::npos);
    BOOST_CHECK(spec->subChunkIds.empty());
}

BOOST_AUTO_TEST_CASE(OverlapSubChunkReferences) {
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST",
                                                      {{"Object", {"objectIdObjTest", "subChunkId"}},
                                                       {"RefObjMatch", {"objectId", "refObjectId"}},
                                                       {"SimRefObject", {"refObjectId", "subChunkId"}}}}}));
    std::string const from =
            " FROM Object o JOIN RefObjMatch m ON o.objectIdObjTest=m.objectId "
            "JOIN SimRefObject t ON t.refObjectId=m.refObjectId";
    std::string const stmts[] = {
            "SELECT t.subChunkId" + from,
            "SELECT o.objectIdObjTest" + from + " WHERE t.subChunkId=o.subChunkId",
            "SELECT o.objectIdObjTest" + from + " AND t.subChunkId=o.subChunkId",
            "SELECT t.subChunkId, COUNT(*)" + from + " GROUP BY t.subChunkId",
            "SELECT MAX(t.subChunkId) AS sc" + from + " HAVING sc>0",
            "SELECT t.subChunkId AS sc" + from + " ORDER BY sc LIMIT 5",
            "SELECT t.subChunkId+1" + from,
            "SELECT o.objectIdObjTest FROM Object o "
            "JOIN RefObjMatch m ON o.objectIdObjTest=m.objectId NATURAL JOIN SimRefObject t",
            "SELECT o.objectIdObjTest FROM Object o "
            "JOIN RefObjMatch m ON o.objectIdObjTest=m.objectId "
            "JOIN SimRefObject t USING(subChunkId) WHERE t.refObjectId=m.refObjectId",
    };
    for (auto const& stmt : stmts) {
        BOOST_TEST_CONTEXT(stmt) {
            forceMatchEquiJoinRewrite(false);
            auto disabled = buildChunk100QuerySpec(stmt);
            forceMatchEquiJoinRewrite(true);
            auto enabled = buildChunk100QuerySpec(stmt);
            BOOST_CHECK(!enabled->subChunkIds.empty());
            BOOST_CHECK(enabled->queries->getTemplates() == disabled->queries->getTemplates());
        }
    }
}

BOOST_AUTO_TEST_CASE(AnchorSubChunkStillOptimized) {
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST",
                                                      {{"Object", {"objectIdObjTest", "subChunkId"}},
                                                       {"RefObjMatch", {"objectId", "refObjectId"}},
                                                       {"SimRefObject", {"refObjectId", "subChunkId"}}}}}));
    auto spec = buildChunk100QuerySpec(
            "SELECT o.subChunkId FROM Object o "
            "JOIN RefObjMatch m ON o.objectIdObjTest=m.objectId "
            "JOIN SimRefObject t ON t.refObjectId=m.refObjectId WHERE o.subChunkId=20");
    BOOST_CHECK(spec->subChunkIds.empty());
    BOOST_REQUIRE_EQUAL(spec->queries->getTemplates().size(), 2u);
    BOOST_CHECK_NE(spec->queries->getTemplates()[1].find("_qserv_overlap_dup"), std::string::npos);
}

BOOST_AUTO_TEST_CASE(OuterJoinsRejected) {
    // Outer joins between a match table and its directors are not admissible (see RelationGraph.h).
    std::string const stmts[] = {
            "SELECT o.ra_Test, t.refRa FROM Object o "
            "LEFT JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId) "
            "LEFT JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId)",
            "SELECT o.ra_Test, t.refRa FROM Object o "
            "JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId) "
            "LEFT JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId)",
            "SELECT o.ra_Test, t.refRa FROM SimRefObject t "
            "RIGHT JOIN RefObjMatch o2t ON (o2t.refObjectId = t.refObjectId) "
            "RIGHT JOIN Object o ON (o.objectIdObjTest = o2t.objectId)",
    };
    qsTest.sqlConfig =
            SqlConfig(SqlConfig::MockDbTableColumns({{"LSST",
                                                      {{"Object", {"objectIdObjTest", "ra_Test"}},
                                                       {"RefObjMatch", {"objectId", "refObjectId"}},
                                                       {"SimRefObject", {"refObjectId", "refRa"}}}}}));
    for (auto const& stmt : stmts) {
        BOOST_TEST_CONTEXT(stmt) {
            std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt, true);
            BOOST_CHECK_NE(
                    qs->getError().find("Unable to evaluate query by joining only partition-local data"),
                    std::string::npos);
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
