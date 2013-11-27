/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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
  * @file testCppParser.cc
  *
  * @brief Test C++ parsing logic.
  *
  * Note: Most tests have not yet been migrated to new parsing model.
  *
  * @author Daniel L. Wang, SLAC
  */

#define BOOST_TEST_MODULE testCppParser
#include "boost/test/included/unit_test.hpp"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <list>
#include <map>
#include <string>
#include <antlr/NoViableAltException.hpp>

#include "css/Facade.h"
#include "qdisp/ChunkMeta.h"
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"
#include "qproc/QuerySession.h"
#include "parser/ParseException.h"
#include "parser/SelectParser.h"
#include "query/SelectStmt.h"
#include "query/Constraint.h"

using lsst::qserv::parser::SelectParser;
using lsst::qserv::qdisp::ChunkMeta;
using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::Constraint;
using lsst::qserv::query::ConstraintVec;
using lsst::qserv::query::ConstraintVector;
using lsst::qserv::query::QsRestrictor;
using lsst::qserv::query::QueryContext;
using lsst::qserv::query::SelectStmt;
using lsst::qserv::util::StringPair;

namespace test = boost::test_tools;

namespace {
ChunkMeta newTestCmeta(bool withSubchunks=true) {
    ChunkMeta m;
    m.add("LSST","Object",2);
    m.add("LSST","Source",1);
    return m;
}

ChunkSpec makeChunkSpec(int chunkNum, bool withSubChunks=false) {
    ChunkSpec cs;
    cs.chunkId = chunkNum;
    if(withSubChunks) {
        int base = 1000 * chunkNum;
        cs.subChunks.push_back(base);
        cs.subChunks.push_back(base+10);
        cs.subChunks.push_back(base+20);
    }
    return cs;
}
void testParse(SelectParser::Ptr p) {
}

    boost::shared_ptr<QuerySession> testStmt3(
                        boost::shared_ptr<lsst::qserv::css::Facade> cssFacade, 
                        std::string const& stmt) {
    boost::shared_ptr<QuerySession> qs(new QuerySession(cssFacade));
    qs->setQuery(stmt);
    BOOST_CHECK_EQUAL(qs->getError(), "");
    ConstraintVec cv(qs->getConstraints());
    boost::shared_ptr<ConstraintVector> cvRaw = cv.getVector();
    if(cvRaw) {
        std::copy(cvRaw->begin(), cvRaw->end(), std::ostream_iterator<Constraint>(std::cout, ","));
        typedef ConstraintVector::iterator Iter;
        for(Iter i=cvRaw->begin(), e=cvRaw->end(); i != e; ++i) {
            std::cout << *i << ",";
        }
    }
    return qs;
}

void printChunkQuerySpecs(boost::shared_ptr<QuerySession> qs) {
    QuerySession::Iter i;
    QuerySession::Iter e = qs->cQueryEnd();
    for(i = qs->cQueryBegin(); i != e; ++i) {
        lsst::qserv::qproc::ChunkQuerySpec& cs = *i;
        std::cout << "Spec: " << cs << std::endl;
    }
}
} // anonymous namespace

struct ParserFixture {
    ParserFixture(void)
        : delimiter("%$#") {
        cMeta.add("LSST", "Source", 1);
        cMeta.add("LSST", "Object", 2);
        tableNames.push_back("Object");
        tableNames.push_back("Source");
        config["table.defaultdb"] ="LSST";
        config["table.partitioncols"] = "Object:ra_Test,decl_Test,objectIdObjTest;"
            "Source:raObjectTest,declObjectTest,objectIdSourceTest";

        // To learn how to dump the map, see qserv/core/css/KvInterfaceImplMem.cc
        // Use client/examples/testCppParser_generateMap
        std::string kvMapPath = "./modules/qproc/testCppParser.kvmap"; // FIXME
        cssFacade = lsst::qserv::css::FacadeFactory::createMemFacade(kvMapPath);
    };
    ~ParserFixture(void) { };

    SelectParser::Ptr getParser(std::string const& stmt) {
        return getParser(stmt, config);
    }
    SelectParser::Ptr getParser(std::string const& stmt,
                                std::map<std::string,std::string> const& cfg) {
        SelectParser::Ptr p;
        p = SelectParser::newInstance(stmt);
        p->setup();
        return p;
    }

    ChunkMeta cMeta;
    std::list<std::string> tableNames;
    std::string delimiter;
    std::map<std::string, std::string> config;
    std::map<std::string, int> whiteList;
    std::string defaultDb;
    boost::shared_ptr<lsst::qserv::css::Facade> cssFacade;
};


//BOOST_FIXTURE_TEST_SUITE(QservSuite, ParserFixture)


//BOOST_AUTO_TEST_CASE(SqlSubstitution) {

std::string computeFirst(QuerySession& qs) {
    qs.addChunk(makeChunkSpec(100,true));
    QuerySession::Iter i = qs.cQueryBegin();
    QuerySession::Iter e = qs.cQueryEnd();
    BOOST_REQUIRE(i != e);
    ChunkQuerySpec& first = *i;
    return first.queries[0];
}
////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(CppParser, ParserFixture)

BOOST_AUTO_TEST_CASE(TrivialSub) {
    std::string stmt = "SELECT * FROM Object WHERE someField > 5.0;";
    std::string expected = "SELECT * FROM LSST.Object_100 AS QST_1_ WHERE someField>5.0";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(!ss.hasGroupBy());
    BOOST_CHECK(!context->needsMerge);

    std::string parallel = computeFirst(*qs);
    BOOST_CHECK_EQUAL(expected, parallel);
}

BOOST_AUTO_TEST_CASE(NoSub) {
    std::string stmt = "SELECT * FROM Filter WHERE filterId=4;";
    std::string goodRes = "SELECT * FROM LSST.Filter AS QST_1_ WHERE filterId=4";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(!context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_CHECK(!ss.hasGroupBy());
    BOOST_CHECK(!context->needsMerge);
    std::string parallel = computeFirst(*qs);
    BOOST_CHECK_EQUAL(goodRes, parallel);
}

BOOST_AUTO_TEST_CASE(Aggregate) {
    std::string stmt = "select sum(pm_declErr),chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";
    std::string expPar = "SELECT sum(pm_declErr) AS QS1_SUM,chunkId AS QS2_PASS,COUNT(bMagF2) AS QS3_COUNT,SUM(bMagF2) AS QS4_SUM FROM LSST.Object_100 AS QST_1_ WHERE bMagF>20.0 GROUP BY chunkId";
    std::string mer = "";

    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_REQUIRE(ss.hasGroupBy());

    std::string parallel = computeFirst(*qs);
    BOOST_CHECK_EQUAL(expPar, parallel);
}

BOOST_AUTO_TEST_CASE(Limit) {
    std::string stmt = "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 limit 2;";

    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    if(context->restrictors) {
        QsRestrictor& r = *context->restrictors->front();
        std::cout << "front restr is " << r << std::endl;
    }

    BOOST_CHECK_EQUAL(ss.getLimit(), 2);
}

BOOST_AUTO_TEST_CASE(OrderBy) {
    std::string stmt = "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 ORDER BY objectId;";

    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_REQUIRE(ss.hasOrderBy());
    // TODO add testing of order-by clause
    //OrderByClause const& oc = ss->getOrderBy();
}

BOOST_AUTO_TEST_CASE(RestrictorBox) {
    std::string stmt = "select * from Object where qserv_areaspec_box(0,0,1,1);";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
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
BOOST_AUTO_TEST_CASE(RestrictorObjectId) {
    std::string stmt = "select * from Object where qserv_objectId(2,3145,9999);";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, "LSST");
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "2","3145","9999"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+6);

}
BOOST_AUTO_TEST_CASE(SecondaryIndex) {
    std::string stmt = "select * from Object where objectIdObjTest in (2,3145,9999);";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "2","3145","9999"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+6);
}

BOOST_AUTO_TEST_CASE(RestrictorObjectIdAlias) {
    std::string stmt = "select * from Object as o1 where qserv_objectId(2,3145,9999);";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "2","3145","9999"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+6);

}
BOOST_AUTO_TEST_CASE(RestrictorNeighborCount) {
    std::string stmt = "select count(*) from Object as o1, Object as o2 "
        "where qserv_areaspec_box(6,6,7,7) AND rFlux_PS<0.005;";
    std::string expected_100_100000_core =
        "SELECT count(*) AS QS1_COUNT FROM Subchunks_LSST_100.Object_100_100000 AS o1,Subchunks_LSST_100.ObjectFullOverlap_100_100000 AS o2 "
        "WHERE scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,6,6,7,7)=1 AND scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,6,6,7,7)=1 AND rFlux_PS<0.005";
    std::string expected_100_100010_overlap =
        "SELECT count(*) AS QS1_COUNT FROM Subchunks_LSST_100.Object_100_100010 AS o1,Subchunks_LSST_100.Object_100_100010 AS o2 "
        "WHERE scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,6,6,7,7)=1 AND scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,6,6,7,7)=1 AND rFlux_PS<0.005";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);

    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "qserv_areaspec_box");
    char const* params[] = {"6","6","7","7"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+4);

    qs->addChunk(makeChunkSpec(100,true));
    QuerySession::Iter i = qs->cQueryBegin();
    QuerySession::Iter e = qs->cQueryEnd();
    BOOST_REQUIRE(i != e);
    ChunkQuerySpec& first = *i;
    BOOST_CHECK(first.queries.size() == 6);
    BOOST_CHECK_EQUAL(first.queries[1], expected_100_100000_core);
    BOOST_CHECK_EQUAL(first.queries[2], expected_100_100010_overlap);
}

BOOST_AUTO_TEST_CASE(Triple) {
    std::string stmt = "select * from LSST.Object as o1, LSST.Object as o2, LSST.Source where o1.id != o2.id and dista(o1.ra,o1.decl,o2.ra,o2.decl) < 1 and Source.oid=o1.id;";
    std::string expected = "SELECT * FROM Subchunks_LSST_100.Object_100_100000 AS o1,Subchunks_LSST_100.Object_100_100000 AS o2,LSST.Source_100 AS QST_1_ WHERE o1.id!=o2.id AND dista(o1.ra,o1.decl,o2.ra,o2.decl)<1 AND QST_1_.oid=o1.id";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_CHECK(context);
    std::string parallel = computeFirst(*qs);
    BOOST_CHECK_EQUAL(parallel, expected);
}

BOOST_AUTO_TEST_CASE(BadDbAccess) {
    std::string stmt = "select count(*) from Bad.Object as o1, Object o2 where qserv_areaspec_box(6,6,7,7) AND o1.ra_PS between 6 and 7 and o1.decl_PS between 6 and 7 ;";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "2","3145","9999"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+6);
    std::string parallel = computeFirst(*qs);
    BOOST_CHECK_EQUAL(parallel, ""); // FIXME
}

BOOST_AUTO_TEST_CASE(ObjectSourceJoin) {
    std::string stmt = "select * from LSST.Object o, Source s WHERE "
        "qserv_areaspec_box(2,2,3,3) AND o.objectId = s.objectId;";
    std::string expected = "select * from LSST.%$#Object%$# o,LSST.%$#Source%$# s WHERE (scisql_s2PtInBox(o.ra_Test,o.decl_Test,2,2,3,3) = 1) AND (scisql_s2PtInBox(s.raObjectTest,s.declObjectTest,2,2,3,3) = 1) AND o.objectId=s.objectId;";

    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);

    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "qserv_areaspec_box");
    char const* params[] = {"2","2","3","3"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+4);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoin) {
    std::string stmt = "select count(*) from Object as o1, Object as o2;";
    std::string expected = "select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sc2%$# as o2 UNION select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sfo%$# as o2;";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);

    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
}
#if 0

BOOST_AUTO_TEST_CASE(ObjectSelfJoinQualified) {
    std::string stmt = "select count(*) from LSST.Object as o1, LSST.Object as o2;";
    std::string expected = "select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sc2%$# as o2 UNION select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sfo%$# as o2;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoinWithAs) {
    // AS alias in column select, <> operator
    std::string stmt = "select o1.objectId, o2.objectI2, scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS distance "
        "from LSST.Object as o1, LSST.Object as o2 "
        "where o1.objectId <> o2.objectId;";
    std::string expected = "select o1.objectId,o2.objectI2,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS distance from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sc2%$# as o2 where o1.objectId<>o2.objectId UNION select o1.objectId,o2.objectI2,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS distance from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sfo%$# as o2 where o1.objectId<>o2.objectId;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoinOutBand) {
    std::string stmt = "select count(*) from LSST.Object as o1, LSST.Object as o2;";
    std::string expected ="select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sc2%$# as o2 WHERE (scisql_s2PtInCircle(o1.ra_Test,o1.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInCircle(o2.ra_Test,o2.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,5,2,6,3) = 1) AND (scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,5,2,6,3) = 1) UNION select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sfo%$# as o2 WHERE (scisql_s2PtInCircle(o1.ra_Test,o1.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInCircle(o2.ra_Test,o2.decl_Test,1,1,1.3) = 1) AND (scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,5,2,6,3) = 1) AND (scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,5,2,6,3) = 1);";

    std::map<std::string, std::string> hintedCfg(config);
    hintedCfg["query.hints"] = "circle,1,1,1.3;box,5,2,6,3";
    SqlParseRunner::Ptr spr = getRunner(stmt, hintedCfg);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoinDistance) {
    std::string stmt = "select count(*) from LSST.Object o1,LSST.Object o2 WHERE scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) < 0.2";
    std::string expected = "select count(*) from LSST.%$#Object_sc1%$# o1,LSST.%$#Object_sc2%$# o2 WHERE (scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,5.5,5.5,6.1,6.1) = 1) AND (scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,5.5,5.5,6.1,6.1) = 1) AND scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS)<0.2 UNION select count(*) from LSST.%$#Object_sc1%$# o1,LSST.%$#Object_sfo%$# o2 WHERE (scisql_s2PtInBox(o1.ra_Test,o1.decl_Test,5.5,5.5,6.1,6.1) = 1) AND (scisql_s2PtInBox(o2.ra_Test,o2.decl_Test,5.5,5.5,6.1,6.1) = 1) AND scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS)<0.2;";

    std::map<std::string, std::string> hintedCfg(config);
    hintedCfg["query.hints"] = "box,5.5,5.5,6.1,6.1";
    SqlParseRunner::Ptr spr = getRunner(stmt, hintedCfg);
    testStmt2(spr);

    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(SelfJoinAliased) {
    // This is actually invalid for Qserv right now because it produces
    // a result that can't be stored in a table as-is.
    // It's also a non-distance-bound spatially-unlimited query. Qserv should
    // reject this. But the parser should still handle it.
    std::string stmt = "select o1.ra_PS, o1.ra_PS_Sigma, o2.ra_PS, o2.ra_PS_Sigma from Object o1, Object o2 where o1.ra_PS_Sigma < 4e-7 and o2.ra_PS_Sigma < 4e-7;";
    std::string expected = "select o1.ra_PS,o1.ra_PS_Sigma,o2.ra_PS,o2.ra_PS_Sigma from LSST.%$#Object_sc1%$# o1,LSST.%$#Object_sc2%$# o2 where o1.ra_PS_Sigma<4e-7 and o2.ra_PS_Sigma<4e-7 UNION select o1.ra_PS,o1.ra_PS_Sigma,o2.ra_PS,o2.ra_PS_Sigma from LSST.%$#Object_sc1%$# o1,LSST.%$#Object_sfo%$# o2 where o1.ra_PS_Sigma<4e-7 and o2.ra_PS_Sigma<4e-7;";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(AliasHandling) {
    std::string stmt = "select o1.ra_PS, o1.ra_PS_Sigma, s.dummy, Exposure.exposureTime from LSST.Object o1,  Source s, Exposure WHERE o1.id = s.objectId AND Exposure.id = o1.exposureId;";
    std::string expected = "select o1.ra_PS,o1.ra_PS_Sigma,s.dummy,Exposure.exposureTime from LSST.%$#Object%$# o1,LSST.%$#Source%$# s,LSST.Exposure WHERE o1.id=s.objectId AND Exposure.id=o1.exposureId;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(SpatialRestr) {
    std::string stmt = "select count(*) from Object where qserv_areaspec_box(359.1, 3.16, 359.2,3.17);";
    std::string expected = "select count(*) from LSST.%$#Object%$# where (scisql_s2PtInBox(LSST.%$#Object%$#.ra_Test,LSST.%$#Object%$#.decl_Test,359.1,3.16,359.2,3.17) = 1);";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(SpatialRestr2) {
    std::string stmt = "select count(*) from LSST.Object where qserv_areaspec_box(359.1, 3.16, 359.2,3.17);";
    std::string expected = "select count(*) from LSST.%$#Object%$# where (scisql_s2PtInBox(LSST.%$#Object%$#.ra_Test,LSST.%$#Object%$#.decl_Test,359.1,3.16,359.2,3.17) = 1);";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}


BOOST_AUTO_TEST_CASE(ChunkDensityFail) {
    std::string stmt = " SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), _chunkId FROM Object GROUP BY _chunkId;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr, true); // Should fail since leading _ is disallowed.
}

BOOST_AUTO_TEST_CASE(ChunkDensity) {
    std::string stmt = " SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), x_chunkId FROM Object GROUP BY x_chunkId;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
}

BOOST_AUTO_TEST_CASE(AltDbName) {
    std::string stmt = "select count(*) from Object where qserv_areaspec_box(359.1, 3.16, 359.2, 3.17);";
    std::string expected = "select count(*) from rplante_PT1_2_u_pt12prod_im3000_qserv.%$#Object%$# where (scisql_s2PtInBox(rplante_PT1_2_u_pt12prod_im3000_qserv.%$#Object%$#.ra_Test,rplante_PT1_2_u_pt12prod_im3000_qserv.%$#Object%$#.decl_Test,359.1,3.16,359.2,3.17) = 1);";

    config["table.defaultdb"] ="rplante_PT1_2_u_pt12prod_im3000_qserv";
    config["table.alloweddbs"] = "LSST,rplante_PT1_2_u_pt12prod_im3000_qserv";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

// Ticket 2048
BOOST_AUTO_TEST_CASE(NonpartitionedTable) {
    std::string stmt = "SELECT offset, mjdRef, drift FROM LeapSeconds where offset = 10";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse output:" << spr->getParseResult() << std::endl;
    BOOST_CHECK(!spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
}

BOOST_AUTO_TEST_CASE(CountQuery) {
    std::string stmt = "SELECT count(*) from Object;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
}
#endif // End: Tests to migrate to new parser framework (needs work in parser too)

BOOST_AUTO_TEST_CASE(CountQuery2) {
    std::string stmt = "SELECT count(*) from LSST.Source;";
    std::string expected_100 = "SELECT count(*) AS QS1_COUNT FROM LSST.Source_100 AS QST_1_";


    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);

    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);

    qs->addChunk(makeChunkSpec(100,true));
    QuerySession::Iter i = qs->cQueryBegin();
    QuerySession::Iter e = qs->cQueryEnd();
    BOOST_REQUIRE(i != e);
    ChunkQuerySpec& first = *i;
    BOOST_CHECK_EQUAL(first.queries.size(), 1);
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
        boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt[i]);

        boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
        BOOST_CHECK(context);
        BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
        BOOST_CHECK(!context->restrictors);
        BOOST_CHECK_EQUAL(context->scanTables.size(), 1);
        if(context->scanTables.size() >= 1) {
            StringPair p = context->scanTables.front();
            BOOST_CHECK_EQUAL(p.first, "LSST");
            BOOST_CHECK_EQUAL(p.second, "Object");
        }
    }
}

BOOST_AUTO_TEST_CASE(UnpartLimit) {
    std::string stmt = "SELECT * from Science_Ccd_Exposure limit 3;";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);

    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(!context->restrictors);
    // BOOST_CHECK(!spr->getHasChunks());
    // BOOST_CHECK(!spr->getHasSubChunks());
    // BOOST_CHECK(!spr->getHasAggregate());
}

BOOST_AUTO_TEST_CASE(Subquery) { // ticket #2053
    std::string stmt = "SELECT subQueryColumn FROM (SELECT * FROM Object WHERE filterId=4) WHERE rFlux_PS > 0.3;";
    SelectParser::Ptr p = getParser(stmt);
    testParse(p);
}

BOOST_AUTO_TEST_CASE(FromParen) { // Extra paren. Not supported by our grammar.
    std::string stmt = "SELECT * FROM (Object) WHERE rFlux_PS > 0.3;";
    SelectParser::Ptr p;
    BOOST_CHECK_THROW(p = getParser(stmt), lsst::qserv::parser::ParseException);
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
        std::cout << "----" << stmt << "----" << std::endl;
        SelectParser::Ptr p = getParser(stmt);
        testParse(p);
    }
 }
BOOST_AUTO_TEST_CASE(Mods) {
    char stmts[][128] = {
        "SELECT * from Object order by ra_PS limit 3;",
        "SELECT count(*) from Science_Ccd_Exposure group by visit;",
        "select count(*) from Object group by flags having count(*) > 3;"
    };
    for(int i=0; i < 3; ++i) {
        std::string stmt = stmts[i];
        testStmt3(cssFacade, stmt);
    }
 }

BOOST_AUTO_TEST_CASE(CountNew) {
    std::string stmt = "SELECT count(*), sum(Source.flux), flux2, Source.flux3 from Source where qserv_areaspec_box(0,0,1,1) and flux4=2 and Source.flux5=3;";
    testStmt3(cssFacade, stmt);
}
BOOST_AUTO_TEST_CASE(FluxMag) {
    std::string stmt = "SELECT count(*) FROM Object"
        " WHERE  qserv_areaspec_box(1,3,2,4) AND"
        "  scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5;";
    testStmt3(cssFacade, stmt);
}

BOOST_AUTO_TEST_CASE(ArithTwoOp) {
    std::string stmt = "SELECT f(one)/f2(two) FROM  Object where qserv_areaspec_box(0,0,1,1);";
    testStmt3(cssFacade, stmt);
}

BOOST_AUTO_TEST_CASE(FancyArith) {
    std::string stmt = "SELECT (1+f(one))/f2(two) FROM  Object where qserv_areaspec_box(0,0,1,1);";
    testStmt3(cssFacade, stmt);
}

BOOST_AUTO_TEST_CASE(Petasky1) {
    // An example slow query from French Petasky colleagues
    std::string stmt = "SELECT objectId as id, COUNT(sourceId) AS c"
        " FROM Source GROUP BY objectId HAVING  c > 1000 LIMIT 10;";
    testStmt3(cssFacade, stmt);
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
    testStmt3(cssFacade, stmt);
}


BOOST_AUTO_TEST_SUITE_END()
////////////////////////////////////////////////////////////////////////
// Case01
////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(Case01Parse, ParserFixture)
BOOST_AUTO_TEST_CASE(Case01_0002) {
    std::string stmt = "SELECT * FROM Object WHERE objectIdObjTest = 430213989000;";
    //std::string expected = "SELECT * FROM LSST.%$#Object%$# WHERE objectId=430213989000;";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "430213989000"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+4);
}

BOOST_AUTO_TEST_CASE(Case01_0012) {
    // This is ticket #2048, actually a proxy problem.
    // Missing paren "(" after WHERE was what the parser received.
    std::string stmt = "SELECT sce.filterId, sce.filterName "
        "FROM Science_Ccd_Exposure AS sce "
        "WHERE (sce.visit = 887404831) "
        "AND (sce.raftName = '3,3') "
        "AND (sce.ccdName LIKE '%')";
    boost::shared_ptr<QuerySession> qs = testStmt3(cssFacade, stmt);
    boost::shared_ptr<QueryContext> context = qs->dbgGetContext();
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
    testStmt3(cssFacade, stmt);
}

BOOST_AUTO_TEST_CASE(Case01_1013) {
    // This is unsupported by the SQL92 grammar, which rejects
    // expressions in ORDER BY because it follows SQL92. Consider
    // patching the grammar to support this.
    std::string stmt = "SELECT objectId, ROUND(iE1_SG, 3), ROUND(ABS(iE1_SG), 3) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ROUND(ABS(iE1_SG), 3);";
    testStmt3(cssFacade, stmt);
}

#if 0
// ASC and maybe USING(...) syntax not supported currently.
// Bug applying spatial restrictor to Filter (non-partitioned) is #2052
BOOST_AUTO_TEST_CASE(Case01_1030) {
    std::string stmt = "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) "
        "FROM   Source "
        "JOIN   Object USING(objectId) JOIN   Filter USING(filterId) "
        "WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' "
        "ORDER BY objectId, taiMidPoint ASC;";
    std::string expected = "SELECT objectId,taiMidPoint,scisql_fluxToAbMag(psfFlux) FROM LSST.%$#Source%$# JOIN LSST.%$#Object%$# USING(objectId) JOIN LSST.Filter USING(filterId) WHERE (scisql_s2PtInBox(LSST.%$#Source%$#.raObjectTest,LSST.%$#Source%$#.declObjectTest,355,0,360,20) = 1) AND (scisql_s2PtInBox(LSST.%$#Object%$#.ra_Test,LSST.%$#Object%$#.decl_Test,355,0,360,20) = 1) AND filterName='g' ORDER BY objectId,taiMidPoint ASC;";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    // Aggregation for qserv means a different chunk query
    // and some form of post-fixup query.
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
    // std::cout << "Parse output:" << spr->getParseResult() << std::endl;
    // But should have a check for ordering-type fixups.
    // "JOIN" syntax, "ORDER BY" with "ASC"
}

BOOST_AUTO_TEST_CASE(Case01_1052) {
    std::string stmt = "SELECT DISTINCT rFlux_PS FROM Object;";
    std::string expected = "SELECT DISTINCT rFlux_PS FROM LSST.%$#Object%$#;";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
    // FIXME: this is a different kind of aggregation syntax than
    // sum() or count(). Maybe another check separate from
    // HasAggregate().

    // DISTINCT syntax (simplified from 1052)
    // not currently supported? (parser or aggregator)
}

BOOST_AUTO_TEST_CASE(Case01_1081) {
    std::string stmt = "SELECT count(*) FROM   Object o "
        "INNER JOIN RefObjMatch o2t ON (o.objectId = o2t.objectId) "
        "LEFT  JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) "
        "WHERE  closestToObj = 1 OR closestToObj is NULL;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks()); // Are RefObjMatch and
                                          // SimRefObject chunked/subchunked?
    BOOST_CHECK(spr->getHasAggregate());
    // JOIN syntax, "is NULL" syntax
}

BOOST_AUTO_TEST_CASE(Case01_1083) {
    std::string stmt = "select objectId, sro.*, (sro.refObjectId-1)/2%pow(2,10) typeId "
        "from Source s join RefObjMatch rom using (objectId) "
        "join SimRefObject sro using (refObjectId) where isStar =1 limit 10;";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks()); // Are RefObjMatch and
                                          // SimRefObject chunked/subchunked?
    BOOST_CHECK(!spr->getHasAggregate());
    // arith expr in column spec, JOIN ..USING() syntax
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
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    // complex.
}

BOOST_AUTO_TEST_CASE(Case01_2004) {
    // simplified to test only:
    // 1) aggregation with aliasing in column spec,
    // 2) case statement in column spec
    std::string stmt = "SELECT  COUNT(*) AS totalCount, "
        "SUM(CASE WHEN (typeId=3) THEN 1 ELSE 0 END) AS galaxyCount "
        "FROM Object WHERE rFlux_PS > 10;";
    std::string expected = "SELECT COUNT(*) AS totalCount,SUM(CASE WHEN(typeId=3) THEN 1 ELSE 0 END) AS galaxyCount FROM LSST.%$#Object%$# WHERE rFlux_PS>10;";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(Case01_2006) {
    std::string stmt = "SELECT scisql_fluxToAbMag(uFlux_PS) "
        "FROM   Object WHERE  (objectId % 100 ) = 40;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    //std::cout << "--SAMPLING--" << spr->getParseResult() << std::endl;
    // % op in WHERE clause
}
#endif // End: To be migrated to new parser interface in later ticket.
BOOST_AUTO_TEST_SUITE_END()

// SELECT o1.id as o1id,o2.id as o2id,
//        LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl)
//  AS dist FROM Object AS o1, Object AS o2
//  WHERE ABS(o1.decl-o2.decl) < 0.001
//      AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.001
//      AND o1.id != o2.id;
