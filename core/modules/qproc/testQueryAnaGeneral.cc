// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2017 AURA/LSST.
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
  * @brief Test C++ parsing and query analysis logic.
  *
  * Note: All tests should have been migrated to the current parsing model.
  * 5 test cases should fail with the current state of functionality.
  *
  * @author Daniel L. Wang, SLAC
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

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnalysis
#include "boost/test/included/unit_test.hpp"

// Qserv headers
#include "parser/ParseException.h"
#include "parser/SelectParser.h"
#include "qdisp/ChunkMeta.h"
#include "qproc/QuerySession.h"
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "tests/QueryAnaFixture.h"


using lsst::qserv::parser::SelectParser;
using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::QsRestrictor;
using lsst::qserv::query::QueryContext;
using lsst::qserv::query::SelectStmt;
using lsst::qserv::StringPair;
using lsst::qserv::tests::QueryAnaFixture;

namespace {
char const* const NOT_EVALUABLE_MSG = "AnalysisError:Query involves "
    "partitioned table joins that Qserv does not know how to evaluate "
    "using only partition-local data";
}
////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(CppParser, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(TrivialSub) {
    std::string stmt = "SELECT * FROM Object WHERE someField > 5.0;";
    std::string expected = "SELECT * FROM LSST.Object_100 AS QST_1_ WHERE someField>5.0";
    BOOST_CHECK(qsTest.css);
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(!ss.hasGroupBy());
    BOOST_CHECK(!context->needsMerge);

    std::string parallel = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(expected, parallel);
}
BOOST_AUTO_TEST_CASE(NoContext) {
    std::string stmt = "SELECT * FROM LSST.Object WHERE someField > 5.0;";
    std::string expected = "SELECT * FROM LSST.Object_100 AS QST_1_ WHERE someField>5.0";
    qsTest.defaultDb = "";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    //SelectStmt const& ss = qs->getStmt();
}
BOOST_AUTO_TEST_CASE(NoSub) {
    std::string stmt = "SELECT * FROM Filter WHERE filterId=4;";
    std::string goodRes = "SELECT * FROM LSST.Filter AS QST_1_ WHERE filterId=4";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(!context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(!ss.hasGroupBy());
    BOOST_CHECK(!context->needsMerge);
    std::string parallel = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(goodRes, parallel);
}

BOOST_AUTO_TEST_CASE(Limit) {
    std::string stmt = "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 limit 2;";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    if (context->restrictors) {
        QsRestrictor& r = *context->restrictors->front();
        std::cout << "front restr is " << r << "\n";
    }

    BOOST_CHECK_EQUAL(ss.getLimit(), 2);
}

BOOST_AUTO_TEST_CASE(OrderBy) {
    std::string stmt = "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 ORDER BY objectId;";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_REQUIRE(ss.hasOrderBy());
    // TODO add testing of order-by clause
    //OrderByClause const& oc = ss->getOrderBy();
}

BOOST_AUTO_TEST_CASE(RestrictorBox) {
    std::string stmt = "select * from Object where qserv_areaspec_box(0,0,1,1);";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "qserv_areaspec_box");
    char const* params[] = {"0","0","1","1"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+4);
    BOOST_CHECK(!context->needsMerge);
    BOOST_CHECK_EQUAL(context->anonymousTable, "Object");
    BOOST_CHECK(!context->hasSubChunks());
}

BOOST_AUTO_TEST_CASE(RestrictorNeighborCount) {
    std::string stmt = "select count(*) from Object as o1, Object as o2 "
        "where qserv_areaspec_box(6,6,7,7) AND rFlux_PS<0.005 AND scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) < 0.001;";
    std::string expected_100_100000_core =
        "SELECT count(*) AS QS1_COUNT FROM Subchunks_LSST_100.Object_100_100000 AS o1,Subchunks_LSST_100.Object_100_100000 AS o2 "
        "WHERE scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,6,6,7,7)=1 AND scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,6,6,7,7)=1 AND rFlux_PS<0.005 AND scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test)<0.001";
    std::string expected_100_100010_overlap =
        "SELECT count(*) AS QS1_COUNT FROM Subchunks_LSST_100.Object_100_100010 AS o1,Subchunks_LSST_100.ObjectFullOverlap_100_100010 AS o2 "
        "WHERE scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,6,6,7,7)=1 AND scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,6,6,7,7)=1 AND rFlux_PS<0.005 AND scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test)<0.001";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);

    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "qserv_areaspec_box");
    char const* params[] = {"6","6","7","7"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+4);

    qs->addChunk(ChunkSpec::makeFake(100,true));
    auto i = qs->cQueryBegin();
    auto e = qs->cQueryEnd();
    BOOST_REQUIRE(i != e);
    auto queryTemplates = qs->makeQueryTemplates();
    ChunkQuerySpec first = qs->buildChunkQuerySpec(queryTemplates, *i);
    int numQueries = first.queries.size();
    BOOST_CHECK_EQUAL(numQueries, 6);
    BOOST_REQUIRE(numQueries > 0);
    // DEBUG
    //std::copy(first.queries.begin(), first.queries.end(), std::ostream_iterator<std::string>(std::cout, "\n\n"));
    BOOST_CHECK_EQUAL(first.queries[0], expected_100_100000_core);
    BOOST_REQUIRE(numQueries > 3);
    BOOST_CHECK_EQUAL(first.queries[3], expected_100_100010_overlap);
}

BOOST_AUTO_TEST_CASE(Triple) {
    std::string stmt =
        "select * from LSST.Object as o1, LSST.Object as o2, LSST.Source "
        "where o1.id != o2.id and "
        "0.024 > scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) and "
        "Source.objectIdSourceTest=o2.objectIdObjTest;";
    std::string expected =
        "SELECT * FROM Subchunks_LSST_100.Object_100_100000 AS o1,Subchunks_LSST_100.Object_100_100000 AS o2,LSST.Source_100 AS QST_1_ "
        "WHERE o1.id!=o2.id AND "
        "0.024>scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) AND "
        "QST_1_.objectIdSourceTest=o2.objectIdObjTest";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    //SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    std::string parallel = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(parallel, expected);
}

BOOST_AUTO_TEST_CASE(BadDbAccess) {
    std::string stmt = "select count(*) from Bad.Object as o1, Object o2 where qserv_areaspec_box(6,6,7,7) AND o1.ra_PS between 6 and 7 and o1.decl_PS between 6 and 7 ;";
    char expectedErr[] = "AnalysisError:Invalid db/table:Bad.Object";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("Bad"));
}

BOOST_AUTO_TEST_CASE(ObjectSourceJoin) {
    std::string stmt = "select * from LSST.Object o, Source s WHERE "
        "qserv_areaspec_box(2,2,3,3) AND o.objectIdObjTest = s.objectIdSourceTest;";
    std::string expected = "SELECT * "
        "FROM LSST.Object_100 AS o,LSST.Source_100 AS s "
        "WHERE scisql_s2PtInBox(o.ra_Test,o.decl_Test,2,2,3,3)=1 "
        "AND scisql_s2PtInBox(s.raObjectTest,s.declObjectTest,2,2,3,3)=1 "
        "AND o.objectIdObjTest=s.objectIdSourceTest";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);

    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "qserv_areaspec_box");
    char const* params[] = {"2","2","3","3"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+4);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoin) {
    std::string stmt = "select count(*) from Object as o1, Object as o2;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), NOT_EVALUABLE_MSG);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoinQualified) {
    std::string stmt = "select count(*) from LSST.Object as o1, LSST.Object as o2 "
        "WHERE o1.objectIdObjTest = o2.objectIdObjTest and o1.iFlux > 0.4 and o2.gFlux > 0.4;";
    std::string expected = "SELECT count(*) AS QS1_COUNT "
        "FROM LSST.Object_100 AS o1,LSST.Object_100 AS o2 "
        "WHERE o1.objectIdObjTest=o2.objectIdObjTest AND o1.iFlux>0.4 AND o2.gFlux>0.4";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);

    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoinWithAs) {
    // AS alias in column select, <> operator
    std::string stmt = "select o1.objectId, o2.objectI2, scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS distance "
        "from LSST.Object as o1, LSST.Object as o2 "
        "where o1.foo <> o2.foo and o1.objectIdObjTest = o2.objectIdObjTest;";
    std::string expected = "SELECT o1.objectId,o2.objectI2,"
        "scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS distance "
        "FROM LSST.Object_100 AS o1,LSST.Object_100 AS o2 "
        "WHERE o1.foo<>o2.foo AND o1.objectIdObjTest=o2.objectIdObjTest";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(!context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}
#if 0
BOOST_AUTO_TEST_CASE(ObjectSelfJoinOutBand) {
    std::string stmt = "select count(*) from LSST.Object as o1, LSST.Object as o2;";
    std::string expected ="select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sc2%$# as o2 WHERE (scisql_s2PtInCircle(o1.ra_Test,o1.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInCircle(o2.ra_Test,o2.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,5,2,6,3) = 1) AND (scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,5,2,6,3) = 1) UNION select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sfo%$# as o2 WHERE (scisql_s2PtInCircle(o1.ra_Test,o1.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInCircle(o2.ra_Test,o2.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,5,2,6,3) = 1) AND (scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,5,2,6,3) = 1);";

    std::map<std::string, std::string> hintedCfg(config);
    hintedCfg["query.hints"] = "circle,1,1,1.3;box,5,2,6,3";
    SqlParseRunner::Ptr spr = getRunner(stmt, hintedCfg);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->queryAnaHelper.getParseresult() << "\n";
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->queryAnaHelper.getParseresult(), expected);
}
#endif

BOOST_AUTO_TEST_CASE(ObjectSelfJoinDistance) {
    std::string stmt = "select count(*) from LSST.Object o1,LSST.Object o2 "
        "WHERE qserv_areaspec_box(5.5, 5.5, 6.1, 6.1) AND "
        "scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) < 0.02";
    std::string expected = "SELECT count(*) AS QS1_COUNT "
        "FROM Subchunks_LSST_100.Object_100_100000 AS o1,"
        "Subchunks_LSST_100.Object_100_100000 AS o2 "
        "WHERE scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,5.5,5.5,6.1,6.1)=1 "
        "AND scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,5.5,5.5,6.1,6.1)=1 "
        "AND scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test)<0.02";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(context->hasSubChunks());
    BOOST_CHECK(context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(SelfJoinAliased) {
    // o2.ra_PS and o2.ra_PS_Sigma have to be aliased in order to produce
    // a result that can't be stored in a table as-is.
    // It's also a non-distance-bound spatially-unlimited query. Qserv should
    // reject this during query analysis. But the parser should still handle it.
    std::string stmt =
       "select o1.ra_PS, o1.ra_PS_Sigma, o2.ra_PS ra_PS2, o2.ra_PS_Sigma ra_PS_Sigma2 "
       "from Object o1, Object o2 "
       "where o1.ra_PS_Sigma < 4e-7 and o2.ra_PS_Sigma < 4e-7;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), NOT_EVALUABLE_MSG);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(!context->needsMerge);
}

BOOST_AUTO_TEST_CASE(AliasHandling) {
    std::string stmt = "select o1.ra_PS, o1.ra_PS_Sigma, s.dummy, Exposure.exposureTime "
        "from LSST.Object o1,  Source s, Exposure "
        "WHERE o1.objectIdObjTest = s.objectIdSourceTest AND Exposure.id = o1.exposureId;";
    std::string expected = "SELECT o1.ra_PS,o1.ra_PS_Sigma,s.dummy,QST_1_.exposureTime "
        "FROM LSST.Object_100 AS o1,LSST.Source_100 AS s,LSST.Exposure AS QST_1_ "
        "WHERE o1.objectIdObjTest=s.objectIdSourceTest AND QST_1_.id=o1.exposureId";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks()); // Design question: do subchunks?
    BOOST_CHECK(!context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(SpatialRestr) {
    std::string stmt = "select count(*) from Object where qserv_areaspec_box(359.1, 3.16, 359.2,3.17);";
    std::string expected = "SELECT count(*) AS QS1_COUNT "
        "FROM LSST.Object_100 AS QST_1_ "
        "WHERE scisql_s2PtInBox(QST_1_.ra_Test,QST_1_.decl_Test,359.1,3.16,359.2,3.17)=1";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(SpatialRestr2) { // Redundant?
    std::string stmt = "select count(*) from LSST.Object where qserv_areaspec_box(359.1, 3.16, 359.2,3.17);";
    std::string expected = "SELECT count(*) AS QS1_COUNT "
        "FROM LSST.Object_100 AS QST_1_ "
        "WHERE scisql_s2PtInBox(QST_1_.ra_Test,QST_1_.decl_Test,359.1,3.16,359.2,3.17)=1";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(ChunkDensityFail) {
    // Should fail since leading _ is disallowed.
    std::string stmt = " SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), _chunkId FROM Object GROUP BY _chunkId;";
    char const expectedErr[] = "ParseException:Parse token mismatch error:expecting a character string, found 'FROM':";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);
    // Remaining session state is undefined after unknown antlr error.
}

BOOST_AUTO_TEST_CASE(ChunkDensity) {
    std::string stmt = " SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), x_chunkId FROM Object GROUP BY x_chunkId;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(context->needsMerge);
}

BOOST_AUTO_TEST_CASE(AltDbName) {
    std::string stmt = "select count(*) from Object where qserv_areaspec_box(359.1, 3.16, 359.2, 3.17);";
    std::string expected = "SELECT count(*) AS QS1_COUNT "
        "FROM rplante_PT1_2_u_pt12prod_im3000_qserv.Object_100 AS QST_1_ "
        "WHERE scisql_s2PtInBox(QST_1_.ra,QST_1_.decl,359.1,3.16,359.2,3.17)=1";

    qsTest.defaultDb ="rplante_PT1_2_u_pt12prod_im3000_qserv";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, "rplante_PT1_2_u_pt12prod_im3000_qserv");
    BOOST_CHECK(context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(actual, expected);
}

// Ticket 2048
BOOST_AUTO_TEST_CASE(NonpartitionedTable) {
    std::string stmt = "SELECT offset, mjdRef, drift FROM LeapSeconds where offset = 10";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(!context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(!context->needsMerge);
}

BOOST_AUTO_TEST_CASE(CountQuery) {
    std::string stmt = "SELECT count(*) from Object;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(context->needsMerge);
}

BOOST_AUTO_TEST_CASE(CountQuery2) {
    std::string stmt = "SELECT count(*) from LSST.Source;";
    std::string expected_100 = "SELECT count(*) AS QS1_COUNT FROM LSST.Source_100 AS QST_1_";

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);

    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);

    qs->addChunk(ChunkSpec::makeFake(100,true));
    auto i = qs->cQueryBegin();
    auto e = qs->cQueryEnd();
    BOOST_REQUIRE(i != e);
    auto queryTemplates = qs->makeQueryTemplates();
    ChunkQuerySpec first = qs->buildChunkQuerySpec(queryTemplates, *i);
    BOOST_CHECK_EQUAL(first.queries.size(), 1U);
    BOOST_CHECK_EQUAL(first.queries[0], expected_100);
}

BOOST_AUTO_TEST_CASE(SimpleScan) {
    std::string stmt[] = {
        "SELECT count(*) FROM Object WHERE iFlux < 0.4;",
        "SELECT rFlux FROM Object WHERE iFlux < 0.4 ;",
        "SELECT * FROM Object WHERE iRadius_SG between 0.02 AND 0.021 LIMIT 3;"
    };
    int const num=3;
    for(int i=0; i < num; ++i) {
        std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt[i]);

        std::shared_ptr<QueryContext> context = qs->dbgGetContext();
        BOOST_CHECK(context);
        BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
        BOOST_CHECK(!context->restrictors);
        BOOST_CHECK_EQUAL(context->scanInfo.infoTables.size(), 1U);
        if (context->scanInfo.infoTables.size() >= 1) {
            auto p = context->scanInfo.infoTables.front();
            BOOST_CHECK_EQUAL(p.db, "LSST");
            BOOST_CHECK_EQUAL(p.table, "Object");
        }
    }
}

BOOST_AUTO_TEST_CASE(UnpartLimit) {
    std::string stmt = "SELECT * from Science_Ccd_Exposure limit 3;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);

    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    // BOOST_CHECK(!spr->getHasChunks());
    // BOOST_CHECK(!spr->getHasSubChunks());
    // BOOST_CHECK(!spr->getHasAggregate());
}

BOOST_AUTO_TEST_CASE(Subquery) { // ticket #2053
    std::string stmt = "SELECT subQueryColumn FROM (SELECT * FROM Object WHERE filterId=4) WHERE rFlux_PS > 0.3;";
    SelectParser::Ptr p;
    BOOST_CHECK_THROW(p = queryAnaHelper.getParser(stmt), lsst::qserv::parser::ParseException);
   // Expected failure: Subqueries are unsupported.
}

BOOST_AUTO_TEST_CASE(FromParen) { // Extra paren. Not supported by our grammar.
    std::string stmt = "SELECT * FROM (Object) WHERE rFlux_PS > 0.3;";
    SelectParser::Ptr p;
    BOOST_CHECK_THROW(p = queryAnaHelper.getParser(stmt), lsst::qserv::parser::ParseException);
}

BOOST_AUTO_TEST_CASE(NewParser) {
    char stmts[][128] = {
        "SELECT table1.* from Science_Ccd_Exposure limit 3;",
        "SELECT * from Science_Ccd_Exposure limit 1;",
        "select ra_PS ra1,decl_PS as dec1 from Object order by dec1;",
        "select o1.iflux_PS o1ps, o2.iFlux_PS o2ps, computeX(o1.one, o2.one) from Object o1, Object o2 order by o1.objectId;",

        "select ra_PS from LSST.Object where ra_PS between 3 and 4;",
        // Test column ref stuff.
        "select count(*) from LSST.Object_3840, usnob.Object_3840 where LSST.Object_3840.objectId > usnob.Object_3840.objectId;",
        "select count(*), max(iFlux_PS) from LSST.Object where iFlux_PS > 100 and col1=col2;",
        "select count(*), max(iFlux_PS) from LSST.Object where qserv_areaspec_box(0,0,1,1) and iFlux_PS > 100 and col1=col2 and col3=4;"
    };
    for(int i=0; i < 8; ++i) {
        std::string stmt = stmts[i];
        //std::cout << "----" << stmt << "----" << "\n";
        SelectParser::Ptr p = queryAnaHelper.getParser(stmt);
        p->setup();
    }
 }
BOOST_AUTO_TEST_CASE(Mods) {
    char stmts[][128] = {
        "SELECT * from Object order by ra_PS limit 3;",
        "SELECT run FROM LSST.Science_Ccd_Exposure order by field limit 2;",
        "SELECT count(*) from Science_Ccd_Exposure group by visit;",
        "select count(*) from Object group by flags having count(*) > 3;"
    };
    for(int i=0; i < 4; ++i) {
        std::string stmt = stmts[i];
        queryAnaHelper.buildQuerySession(qsTest, stmt);
    }
 }

BOOST_AUTO_TEST_CASE(CountNew) {
    std::string stmt = "SELECT count(*), sum(Source.flux), flux2, Source.flux3 from Source where qserv_areaspec_box(0,0,1,1) and flux4=2 and Source.flux5=3;";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
}
BOOST_AUTO_TEST_CASE(FluxMag) {
    std::string stmt = "SELECT count(*) FROM Object"
        " WHERE  qserv_areaspec_box(1,3,2,4) AND"
        "  scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5;";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
}

BOOST_AUTO_TEST_CASE(ArithTwoOp) {
    std::string stmt = "SELECT f(one)/f2(two) FROM  Object where qserv_areaspec_box(0,0,1,1);";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
}

BOOST_AUTO_TEST_CASE(FancyArith) {
    std::string stmt = "SELECT (1+f(one))/f2(two) FROM  Object where qserv_areaspec_box(0,0,1,1);";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
}

BOOST_AUTO_TEST_CASE(Petasky1) {
    // An example slow query from French Petasky colleagues
    std::string stmt = "SELECT objectId as id, COUNT(sourceId) AS c"
        " FROM Source GROUP BY objectId HAVING  c > 1000 LIMIT 10;";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
}

BOOST_AUTO_TEST_CASE(Expression) {
    // A query with some expressions
    std::string stmt = "SELECT "
        "ROUND(scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS), 0) AS UG, "
        "ROUND(scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS), 0) AS GR "
        "FROM Object "
        "WHERE scisql_fluxToAbMag(gFlux_PS) < 0.2 "
        "AND scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) >=-0.27 "
        "AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) >=-0.24 "
        "AND scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) >=-0.27 "
        "AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) >=-0.35 "
        "AND scisql_fluxToAbMag(zFlux_PS)-scisql_fluxToAbMag(yFlux_PS) >=-0.40;";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
}

BOOST_AUTO_TEST_CASE(dm646) {
    // non-chunked query
    std::string stmt = "SELECT DISTINCT foo FROM Filter f;";
    std::string expected = "SELECT DISTINCT foo FROM LSST.Filter AS f";
    // FIXME: non-chunked query shouldn't require merge operation, see DM-3165
    std::string expectedMerge = "SELECT DISTINCT foo";
    auto queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
    BOOST_CHECK_EQUAL(queries[1], expectedMerge);

    // chunked query
    stmt = "SELECT DISTINCT zNumObs FROM Object;";
    expected = "SELECT DISTINCT zNumObs FROM LSST.Object_100 AS QST_1_";
    expectedMerge = "SELECT DISTINCT zNumObs";
    queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
    BOOST_CHECK_EQUAL(queries[1], expectedMerge);
}

BOOST_AUTO_TEST_CASE(dm681) {
    // Stricter sql_stmt grammer rules: reject trailing garbage
    std::string stmt = "SELECT foo FROM Filter f limit 5";
    std::string stmt2 = "SELECT foo FROM Filter f limit 5;";
    std::string stmt3 = "SELECT foo FROM Filter f limit 5;; ";
    std::string expected = "SELECT foo FROM LSST.Filter AS f LIMIT 5";
    auto queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
    queries = queryAnaHelper.getInternalQueries(qsTest, stmt2);
    BOOST_CHECK_EQUAL(queries[0], expected);
    queries = queryAnaHelper.getInternalQueries(qsTest, stmt3);
    BOOST_CHECK_EQUAL(queries[0], expected);

    stmt = "SELECT foo from Filter f limit 5 garbage query !#$%!#$";
    stmt2 = "SELECT foo from Filter f limit 5; garbage query !#$%!#$";
    char const expectedErr[] = "ParseException:Parse token mismatch error:expecting EOF, found 'garbage':";
    std::shared_ptr<QuerySession> qs;
    qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);
    qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);
}

BOOST_AUTO_TEST_CASE(FuncExprPred) {
    // DM-1784: Nested ValueExpr in function calls.
    std::string stmt = "SELECT  o1.objectId "
        "FROM Object o1 "
        "WHERE ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) ) < 1;";
    std::string expected = "SELECT o1.objectId FROM LSST.Object_100 AS o1 WHERE ABS((scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS))-(scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)))<1";
    auto queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
    stmt = "SELECT  o1.objectId, o2.objectId objectId2 "
        "FROM Object o1, Object o2 "
        "WHERE scisql_angSep(o1.ra_Test, o1.decl_Test, o2.ra_Test, o2.decl_Test) < 0.00001 "
        "AND o1.objectId <> o2.objectId AND "
        "ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)) ) < 1;";
    expected = "SELECT o1.objectId,o2.objectId AS objectId2 "
        "FROM Subchunks_LSST_100.Object_100_100000 AS o1,Subchunks_LSST_100.Object_100_100000 AS o2 "
        "WHERE scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test)<0.00001 "
        "AND o1.objectId<>o2.objectId AND "
        "ABS((scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS))-(scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)))<1";

    queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
}

BOOST_AUTO_TEST_SUITE_END()
////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(Match, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(MatchTableWithoutWhere) {
    std::string stmt = "SELECT * FROM RefObjMatch;";
    std::string expected = "SELECT * FROM LSST.RefObjMatch_100 AS QST_1_ WHERE "
                           "(refObjectId IS NULL OR flags<>2)";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(!ss.hasGroupBy());
    BOOST_CHECK(!context->needsMerge);
    std::string actual = queryAnaHelper.buildFirstParallelQuery(false);
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(MatchTableWithWhere) {
    std::string stmt = "SELECT * FROM RefObjMatch WHERE "
                       "foo!=bar AND baz<3.14159;";
    std::string expected = "SELECT * FROM LSST.RefObjMatch_100 AS QST_1_ WHERE "
                           "(refObjectId IS NULL OR flags<>2) "
                           "AND foo!=bar AND baz<3.14159";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::string actual = queryAnaHelper.buildFirstParallelQuery(false);
    BOOST_CHECK_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_SUITE_END()
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(Garbage, QueryAnaFixture)
BOOST_AUTO_TEST_CASE(Garbled) {
    std::string stmt = "LECT sce.filterName,sce.field "
        "FROM LSST.Science_Ccd_Exposure AS sce "
        "WHERE sce.field=535 AND sce.camcol LIKE '%' ";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), "ParseException:Parse error(ANTLR):unexpected token: LECT:");

}
BOOST_AUTO_TEST_SUITE_END()
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(EquiJoin, QueryAnaFixture)
BOOST_AUTO_TEST_CASE(FreeIndex) {
    // Equi-join using index and free-form syntax
    std::string stmt = "SELECT s.ra, s.decl, o.foo FROM Source s, Object o "
        "WHERE s.objectIdSourceTest=o.objectIdObjTest and o.objectIdObjTest = 430209694171136;";
    std::string expected = "SELECT s.ra,s.decl,o.foo "
        "FROM LSST.Source_100 AS s,LSST.Object_100 AS o "
        "WHERE s.objectIdSourceTest=o.objectIdObjTest AND o.objectIdObjTest=430209694171136";

    auto queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
}

BOOST_AUTO_TEST_CASE(SpecIndexUsing) {
    // Equi-join syntax, not supported yet
    std::string stmt = "SELECT s.ra, s.decl, o.foo "
        "FROM Object o JOIN Source2 s USING (objectIdObjTest) JOIN Source2 s2 USING (objectIdObjTest) "
        "WHERE o.objectId = 430209694171136;";
    std::string expected = "SELECT s.ra,s.decl,o.foo "
        "FROM LSST.Object_100 AS o "
        "JOIN LSST.Source2_100 AS s USING(objectIdObjTest) "
        "JOIN LSST.Source2_100 AS s2 USING(objectIdObjTest) "
        "WHERE o.objectId=430209694171136";
    auto queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
}

BOOST_AUTO_TEST_CASE(SpecIndexOn) {
    std::string stmt = "SELECT s.ra, s.decl, o.foo "
        "FROM Object o "
        "JOIN Source s ON s.objectIdSourceTest = Object.objectIdObjTest "
        "JOIN Source s2 ON s.objectIdSourceTest = s2.objectIdSourceTest "
        "WHERE LSST.Object.objectId = 430209694171136;";
    std::string expected = "SELECT s.ra,s.decl,o.foo "
        "FROM LSST.Object_100 AS o "
        "JOIN LSST.Source_100 AS s ON s.objectIdSourceTest=o.objectIdObjTest "
        "JOIN LSST.Source_100 AS s2 ON s.objectIdSourceTest=s2.objectIdSourceTest "
        "WHERE o.objectId=430209694171136";
    auto queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expected);
}

BOOST_AUTO_TEST_SUITE_END()

/// table JOIN table syntax
BOOST_FIXTURE_TEST_SUITE(JoinSyntax, QueryAnaFixture)
BOOST_AUTO_TEST_CASE(NoSpec) {
    std::string stmt = "SELECT s1.foo, s2.foo AS s2_foo "
        "FROM Source s1 NATURAL LEFT JOIN Source s2 "
        "WHERE s1.bar = s2.bar;";
    std::string expected = "SELECT s1.foo,s2.foo AS s2_foo "
        "FROM LSST.Source_100 AS s1 "
        "NATURAL LEFT OUTER JOIN LSST.Source_100 AS s2 "
        "WHERE s1.bar=s2.bar";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    qs->addChunk(ChunkSpec::makeFake(100,true));
    auto i = qs->cQueryBegin();
    auto e = qs->cQueryEnd();
    BOOST_REQUIRE(i != e);
    auto queryTemplates = qs->makeQueryTemplates();
    ChunkQuerySpec first = qs->buildChunkQuerySpec(queryTemplates, *i);
    BOOST_CHECK_EQUAL(first.queries.size(), 1U);
    BOOST_CHECK_EQUAL(first.queries[0], expected);
    BOOST_CHECK_EQUAL(first.subChunkTables.size(), 0U);
    BOOST_CHECK_EQUAL(first.db, "LSST");
    BOOST_CHECK_EQUAL(first.chunkId, 100);
    ++i;
    BOOST_CHECK(i == e);
}

BOOST_AUTO_TEST_CASE(Union) {
    std::string stmt = "SELECT s1.foo, s2.foo AS s2_foo "
        "FROM Source s1 UNION JOIN Source s2 "
        "WHERE s1.bar = s2.bar;";
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), "AnalysisError:UNION JOIN queries are not currently supported.");
}
BOOST_AUTO_TEST_CASE(Cross) {
    std::string stmt = "SELECT * "
        "FROM Source s1 CROSS JOIN Source s2 "
        "WHERE s1.bar = s2.bar;";
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), NOT_EVALUABLE_MSG);
}
BOOST_AUTO_TEST_CASE(Using) {
    // Equi-join syntax, non-partitioned
    std::string stmt = "SELECT * "
        "FROM Filter f JOIN Science_Ccd_Exposure USING(exposureId);";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
}

BOOST_AUTO_TEST_SUITE_END()

////////////////////////////////////////////////////////////////////////
// Case01
////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(Case01Parse, QueryAnaFixture)
BOOST_AUTO_TEST_CASE(Case01_0002) {
    std::string stmt = "SELECT * FROM Object WHERE objectIdObjTest = 430213989000;";
    //std::string expected = "SELECT * FROM LSST.%$#Object%$# WHERE objectId=430213989000;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "430213989000"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+4);
}

BOOST_AUTO_TEST_CASE(Case01_0003) {
    std::string stmt = "SELECT s.ra, s.decl, o.raRange, o.declRange "
        "FROM   Object o "
        "JOIN   Source2 s USING (objectIdObjTest) "
        "WHERE  o.objectIdObjTest = 390034570102582 "
        "AND    o.latestObsTime = s.taiMidPoint;";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
}

BOOST_AUTO_TEST_CASE(Case01_0012) {
    // This is ticket #2048, actually a proxy problem.
    // Missing paren "(" after WHERE was what the parser received.
    std::string stmt = "SELECT sce.filterId, sce.filterName "
        "FROM Science_Ccd_Exposure AS sce "
        "WHERE (sce.visit = 887404831) "
        "AND (sce.raftName = '3,3') "
        "AND (sce.ccdName LIKE '%')";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    // BOOST_CHECK(!spr->getHasChunks());
    // BOOST_CHECK(!spr->getHasSubChunks());
    // BOOST_CHECK(!spr->getHasAggregate());
    // BOOST_CHECK(spr->getError() == "");
    // should parse okay as a full-scan of sce, non-partitioned.
    // Optional parens may be confusing the parser.
}

BOOST_AUTO_TEST_CASE(Case01_1012) {
    // This is unsupported by the SQL92 grammar, which rejects
    // expressions in ORDER BY because it follows SQL92. Consider
    // patching the grammar to support this.
    std::string stmt = "SELECT objectId, iE1_SG, ABS(iE1_SG) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ABS(iE1_SG);";
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), "ParseException:Parse error(ANTLR):unexpected token: (:");
}

BOOST_AUTO_TEST_CASE(Case01_1013) {
    // This is unsupported in SQL92, so the parser rejects
    // expressions in ORDER BY because it uses a SQL92 grammar. Consider
    // patching the grammar to support this.
    std::string stmt = "SELECT objectId, ROUND(iE1_SG, 3), ROUND(ABS(iE1_SG), 3) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ROUND(ABS(iE1_SG), 3);";
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), "ParseException:Parse error(ANTLR):unexpected token: (:");
}


// ASC and maybe USING(...) syntax not supported currently.
// Bug applying spatial restrictor to Filter (non-partitioned) is #2052
BOOST_AUTO_TEST_CASE(Case01_1030) {
    std::string stmt = "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) "
        "FROM   Source "
        "JOIN   Object USING(objectId) JOIN   Filter USING(filterId) "
        "WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' "
        "ORDER BY objectId, taiMidPoint ASC;";
    // Besides the bugs mentioned above, this query is also not evaluable
    // because the Source and Object director column name is not objectId...
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), NOT_EVALUABLE_MSG);
#if 0    // FIXME
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    // Aggregation for qserv means a different chunk query
    // and some form of post-fixup query.
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->queryAnaHelper.getParseresult(), expected);
#endif
    // std::cout << "Parse output:" << spr->queryAnaHelper.getParseresult() << "\n";
    // But should have a queryAnaHelper.check for ordering-type fixups.
    // "JOIN" syntax, "ORDER BY" with "ASC"
}

BOOST_AUTO_TEST_CASE(Case01_1052) {
    std::string stmt = "SELECT DISTINCT rFlux_PS FROM Object;";
    std::string expected = "SELECT DISTINCT rFlux_PS FROM LSST.%$#Object%$#;";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
#if 0 // FIXME
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->queryAnaHelper.getParseresult(), expected);
#endif
    // FIXME: this is a different kind of aggregation syntax than
    // sum() or count(). Maybe another queryAnaHelper.check separate from
    // HasAggregate().

    // DISTINCT syntax (simplified from 1052)
    // not currently supported? (parser or aggregator)
}

BOOST_AUTO_TEST_CASE(Case01_1081) {
    // The original statement uses "LEFT JOIN SimRefObject"
    // rather than "INNER JOIN SimRefObject", but we currently cannot
    // evaluate left joins involving overlap.
    std::string stmt = "SELECT count(*) FROM   Object o "
        "INNER JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId) "
        "INNER JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) "
        "WHERE  closestToObj = 1 OR closestToObj is NULL;";
    std::string expected_100_100000_core = "SELECT count(*) AS QS1_COUNT "
        "FROM Subchunks_LSST_100.Object_100_100000 AS o "
        "INNER JOIN LSST.RefObjMatch_100 AS o2t ON o.objectIdObjTest=o2t.objectId "
        "INNER JOIN Subchunks_LSST_100.SimRefObject_100_100000 AS t ON o2t.refObjectId=t.refObjectId "
        "WHERE closestToObj=1 OR closestToObj IS NULL";
    std::string expected_100_100020_overlap = "SELECT count(*) AS QS1_COUNT "
        "FROM Subchunks_LSST_100.Object_100_100020 AS o "
        "INNER JOIN LSST.RefObjMatch_100 AS o2t ON o.objectIdObjTest=o2t.objectId "
        "INNER JOIN Subchunks_LSST_100.SimRefObjectFullOverlap_100_100020 AS t ON o2t.refObjectId=t.refObjectId "
        "WHERE closestToObj=1 OR closestToObj IS NULL";
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    qs->addChunk(ChunkSpec::makeFake(100,true));
    auto i = qs->cQueryBegin();
    auto e = qs->cQueryEnd();
    BOOST_REQUIRE(i != e);
    auto queryTemplates = qs->makeQueryTemplates();
    ChunkQuerySpec first = qs->buildChunkQuerySpec(queryTemplates, *i);
    int numQueries = first.queries.size();
    BOOST_CHECK_EQUAL(numQueries, 6);
    BOOST_REQUIRE(numQueries > 0);
    BOOST_CHECK_EQUAL(first.queries[0], expected_100_100000_core);
    BOOST_REQUIRE(numQueries > 5);
    BOOST_CHECK_EQUAL(first.queries[5], expected_100_100020_overlap);
    // JOIN syntax, "is NULL" syntax
}

BOOST_AUTO_TEST_CASE(Case01_1083) {
    std::string stmt = "select objectId, sro.*, (sro.refObjectId-1)/2%pow(2,10), typeId "
        "from Source s join RefObjMatch rom using (objectId) "
        "join SimRefObject sro using (refObjectId) where isStar =1 limit 10;";
    // % is not valid for arithmetic in SQL92
    char const expectedErr[] = "ParseException:Parse error(ANTLR):unexpected token: 2:";
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);
#if 0 // FIXME
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks()); // Are RefObjMatch and
                                          // SimRefObject chunked/subchunked?
    BOOST_CHECK(!spr->getHasAggregate());
    // arith expr in column spec, JOIN ..USING() syntax
#endif
}

BOOST_AUTO_TEST_CASE(Case01_2001) {
    std::string stmt = "SELECT objectId, "
        "scisql_fluxToAbMag(uFlux_PS), scisql_fluxToAbMag(gFlux_PS), "
        "scisql_fluxToAbMag(rFlux_PS), scisql_fluxToAbMag(iFlux_PS), "
        "scisql_fluxToAbMag(zFlux_PS), scisql_fluxToAbMag(yFlux_PS), "
        "ra_PS, decl_PS FROM   Object "
        "WHERE  ( scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.7 OR scisql_fluxToAbMag(gFlux_PS) > 22.3 ) "
"AND    scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.1 "
"AND    ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) "
"< (0.08 + 0.42 * (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) - 0.96)) "
        " OR scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 1.26 ) "
        "AND    scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) < 0.8;";
    queryAnaHelper.buildQuerySession(qsTest, stmt);
#if 0 // FIXME
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    // complex.
#endif
}

BOOST_AUTO_TEST_CASE(Case01_2004) {
    // simplified to test only:
    // 1) aggregation with aliasing in column spec,
    // 2) case statement in column spec
    std::string stmt = "SELECT  COUNT(*) AS totalCount, "
        "SUM(CASE WHEN (typeId=3) THEN 1 ELSE 0 END) AS galaxyCount "
        "FROM Object WHERE rFlux_PS > 10;";
    std::string expected = "SELECT COUNT(*) AS totalCount,SUM(CASE WHEN(typeId=3) THEN 1 ELSE 0 END) AS galaxyCount FROM LSST.%$#Object%$# WHERE rFlux_PS>10;";

    // CASE in column spec is illegal.
    char const expectedErr[] = "ParseException:ValueFactorFactory::newColumnFactor with :CASE WHEN OR_OP THEN VALUE_EXP ELSE VALUE_EXP END";
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);
}

BOOST_AUTO_TEST_CASE(Case01_2006) {
    std::string stmt = "SELECT scisql_fluxToAbMag(uFlux_PS) "
        "FROM   Object WHERE  (objectId % 100 ) = 40;";
    // % is not a valid arithmetic operator in SQL92.
    char const expectedErr[] = "ParseException:Parse error(ANTLR):unexpected token: objectId:";
    auto qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    BOOST_CHECK_EQUAL(qs->getError(), expectedErr);
#if 0 // FIXME
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
#endif
    //std::cout << "--SAMPLING--" << spr->queryAnaHelper.getParseresult() << "\n";
    // % op in WHERE clause
}

BOOST_AUTO_TEST_SUITE_END()

// SELECT o1.id as o1id,o2.id as o2id,
//        LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl)
//  AS dist FROM Object AS o1, Object AS o2
//  WHERE ABS(o1.decl-o2.decl) < 0.001
//      AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.001
//      AND o1.id != o2.id;
