/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#define BOOST_TEST_MODULE testCppParser
#include "boost/test/included/unit_test.hpp"
#include <list>
#include <map>
#include <string>
#include "lsst/qserv/master/SqlSubstitution.h"
#include "lsst/qserv/master/ChunkMeta.h"
#include "lsst/qserv/master/SqlParseRunner.h"

using lsst::qserv::master::ChunkMapping;
using lsst::qserv::master::ChunkMeta;
using lsst::qserv::master::SqlSubstitution;
using lsst::qserv::master::SqlParseRunner;
namespace test = boost::test_tools;

namespace {


std::auto_ptr<ChunkMapping> newTestMapping() {
    std::auto_ptr<ChunkMapping> cm(new ChunkMapping());
    cm->addChunkKey("Source");
    cm->addChunkKey("Object");
    return cm;
}

ChunkMeta newTestCmeta(bool withSubchunks=true) {
    ChunkMeta m;
    m.add("LSST","Object",2);
    m.add("LSST","Source",1);
    return m;
}


void tryStmt(std::string const& s, bool withSubchunks=false) {
    std::map<std::string,std::string> cfg; // dummy config
    SqlSubstitution ss(s, newTestCmeta(withSubchunks), cfg);
    if(!ss.getError().empty()) {
	std::cout << "ERROR constructing substitution: " 
		  << ss.getError() << std::endl;
	return;
    }

    for(int i = 4; i < 6; ++i) {
	std::cout << "--" << ss.transform(i, 3) 
                  << std::endl;
    }
}

void testStmt2(SqlParseRunner::Ptr spr, bool shouldFail=false) {
    std::cout << "Testing: " << spr->getStatement() << std::endl;
    std::string parseResult = spr->getParseResult();
    // std::cout << stmt << " is parsed into " << parseResult
    //           << std::endl;
    
    if(shouldFail) {
        BOOST_CHECK(!spr->getError().empty());
    } else {
        if(!spr->getError().empty()) { 
            std::cout << spr->getError() << std::endl;
        }
        BOOST_CHECK(spr->getError().empty());
        BOOST_CHECK(!parseResult.empty());
    }
}

} // anonymous namespace

struct ParserFixture {
    ParserFixture(void) 
        : delimiter("%$#") {
        cMeta.add("LSST", "Source", 1);
        cMeta.add("LSST", "Object", 2);
	cMapping.addChunkKey("Source");
	cMapping.addSubChunkKey("Object");
        tableNames.push_back("Object");
        tableNames.push_back("Source");
        config["table.defaultdb"] ="LSST";
        config["table.alloweddbs"] = "LSST";
        config["table.partitioncols"] = "Object:ra_Test,decl_Test,objectIdObjTest;"
            "Source:raObjectTest,declObjectTest,objectIdSourceTest";

    };
    ~ParserFixture(void) { };

    SqlParseRunner::Ptr getRunner(std::string const& stmt) {
        return getRunner(stmt, config);
    }

    SqlParseRunner::Ptr getRunner(std::string const& stmt, 
                                  std::map<std::string,std::string> const& cfg) {
        SqlParseRunner::Ptr p;
        
        p = SqlParseRunner::newInstance(stmt, delimiter, cfg);
        p->setup(tableNames);
        return p;
    }
    ChunkMapping cMapping;
    ChunkMeta cMeta;
    std::list<std::string> tableNames;
    std::string delimiter;
    std::map<std::string, std::string> config;
    std::map<std::string, int> whiteList;
    std::string defaultDb;
};



//BOOST_FIXTURE_TEST_SUITE(QservSuite, ParserFixture)


//BOOST_AUTO_TEST_CASE(SqlSubstitution) {

void tryAutoSubstitute() {
    std::string stmt = "select * from LSST.Object as o1, LSST.Source where o1.id = 4 and LSST.Source.flux > 4 and ra < 5 and dista(ra,decl,ra,decl) < 1; select * from Temp;";
    tryStmt(stmt);
}

void tryNnSubstitute() {
    std::string stmt = "select * from LSST.Object as o1, LSST.Object as o2 where o1.id != o2.id and spdist(o1.ra,o1.decl,o2.ra,o2.decl) < 1;";
    stmt = "select * from LSST.Object as o1, LSST.Object as o2 where o1.id != o2.id and LSST.spdist(o1.ra,o1.decl,o2.ra,o2.decl) < 1 AND o1.id != o2.id;";
    tryStmt(stmt, true);
}

void tryTriple() {
    std::string stmt = "select * from LSST.Object as o1, LSST.Object as o2, LSST.Source where o1.id != o2.id and dista(o1.ra,o1.decl,o2.ra,o2.decl) < 1 and Source.oid=o1.id;";
    std::map<std::string,std::string> cfg; // dummy config
    ChunkMeta c = newTestCmeta();
    c.add("LSST", "ObjectSub", 2);
    SqlSubstitution ss(stmt, c, cfg);
    for(int i = 4; i < 6; ++i) {
	std::cout << "--" << ss.transform(i,3) << std::endl;
    }
}

void tryAggregate() {
    std::string stmt = "select sum(pm_declErr),sum(bMagF), count(bMagF2) bmf2 from LSST.Object where bMagF > 20.0;";
    std::string stmt2 = "select sum(pm_declErr),chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";
    std::map<std::string,std::string> cfg; // dummy config

    ChunkMeta c = newTestCmeta();
    SqlSubstitution ss(stmt, c, cfg);
    for(int i = 4; i < 6; ++i) {
	std::cout << "--" << ss.transform(i,3) << std::endl;
    }
    SqlSubstitution ss2(stmt2, c, cfg);
    std::cout << "--" << ss2.transform(24,3) << std::endl;
    
}

#if 1
////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(CppParser, ParserFixture)

BOOST_AUTO_TEST_CASE(TrivialSub) {
    std::string stmt = "SELECT * FROM Object WHERE someField > 5.0;";
    SqlParseRunner::Ptr spr = SqlParseRunner::newInstance(stmt, 
                                                          delimiter,
                                                          config);
    spr->setup(tableNames);
    std::string parseResult = spr->getParseResult();
    // std::cout << stmt << " is parsed into " << parseResult
    //           << std::endl;
    BOOST_CHECK(!parseResult.empty());
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
}

BOOST_AUTO_TEST_CASE(NoSub) {
    std::string stmt = "SELECT * FROM Filter WHERE filterId=4;";
    std::string goodRes = "SELECT * FROM LSST.Filter WHERE filterId=4;";
    SqlParseRunner::Ptr spr(SqlParseRunner::newInstance(stmt, 
                                                        delimiter,
                                                        config));
    spr->setup(tableNames);
    std::string parseResult = spr->getParseResult();
    // std::cout << stmt << " is parsed into " << parseResult 
    //           << std::endl;
    BOOST_CHECK(!parseResult.empty());
    BOOST_CHECK_EQUAL(parseResult, goodRes);
    BOOST_CHECK(!spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    
}

BOOST_AUTO_TEST_CASE(Aggregate) {
    std::string stmt = "select sum(pm_declErr),chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";

    SqlSubstitution ss(stmt, cMeta, config);
    for(int i = 4; i < 6; ++i) {
	// std::cout << "--" << ss.transform(cMeta.getMapping(i,3),i,3) 
        //           << std::endl;
        BOOST_CHECK_EQUAL(ss.getChunkLevel(), 1);
        BOOST_CHECK(ss.getHasAggregate());
        // std::cout << "fixupsel " << ss.getFixupSelect() << std::endl
        //           << "fixuppost " << ss.getFixupPost() << std::endl;
        BOOST_CHECK_EQUAL(ss.getFixupSelect(), 
                          "sum(`sum(pm_declErr)`) AS `sum(pm_declErr)`, `chunkId`, SUM(avgs_bMagF2)/SUM(avgc_bMagF2) AS `bmf2`");
        BOOST_CHECK_EQUAL(ss.getFixupPost(), "GROUP BY `chunkId`");

    }
}

BOOST_AUTO_TEST_CASE(Limit) {
    std::string stmt = "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 limit 2;";
    
    SqlSubstitution ss(stmt, cMeta, config);
    for(int i = 4; i < 6; ++i) {
	//std::cout << "--" << ss.transform(cMeta.getMapping(i,3),i,3) 
        //           << std::endl;
        BOOST_CHECK_EQUAL(ss.getChunkLevel(), 1);
        BOOST_CHECK(!ss.getHasAggregate());
        if(!ss.getError().empty()) { std::cout << ss.getError() << std::endl;}
        BOOST_CHECK(ss.getError().empty());
        // std::cout << "fixupsel " << ss.getFixupSelect() << std::endl
        //           << "fixuppost " << ss.getFixupPost() << std::endl;

    }
}

BOOST_AUTO_TEST_CASE(OrderBy) {
    std::string stmt = "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 ORDER BY objectId;";
    
    SqlSubstitution ss(stmt, cMeta, config);
    for(int i = 4; i < 6; ++i) {
	//std::cout << "--" << ss.transform(cMeta.getMapping(i,3),i,3) 
        //           << std::endl;
        BOOST_CHECK_EQUAL(ss.getChunkLevel(), 1);
        BOOST_CHECK(!ss.getHasAggregate());
        if(!ss.getError().empty()) { std::cout << ss.getError() << std::endl;}
        BOOST_CHECK(ss.getError().empty());
        // std::cout << "fixupsel " << ss.getFixupSelect() << std::endl
        //           << "fixuppost " << ss.getFixupPost() << std::endl;

    }
}

BOOST_AUTO_TEST_CASE(RestrictorBox) {
    std::string stmt = "select * from Object where qserv_areaspec_box(0,0,1,1);";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());

}
BOOST_AUTO_TEST_CASE(RestrictorObjectId) {
    std::string stmt = "select * from Object where qserv_objectId(2,3145,9999);";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());

}
BOOST_AUTO_TEST_CASE(RestrictorObjectIdAlias) {
    std::string stmt = "select * from Object as o1 where qserv_objectId(2,3145,9999);";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
}
BOOST_AUTO_TEST_CASE(RestrictorNeighborCount) {
    std::string stmt = "select count(*) from Object as o1, Object as o2 where qserv_areaspec_box(6,6,7,7) AND o1.ra_PS between 6 and 7 and o1.decl_PS between 6 and 7 ;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
}

BOOST_AUTO_TEST_CASE(BadDbAccess) {
    std::string stmt = "select count(*) from Bad.Object as o1, Object o2 where qserv_areaspec_box(6,6,7,7) AND o1.ra_PS between 6 and 7 and o1.decl_PS between 6 and 7 ;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr, true);
}

BOOST_AUTO_TEST_CASE(ObjectSourceJoin) {
    std::string stmt = "select * from LSST.Object o, Source s WHERE "
        "qserv_areaspec_box(2,2,3,3) AND o.objectId = s.objectId;";
    std::string expected = "select * from LSST.%$#Object%$# o,LSST.%$#Source%$# s WHERE (scisql_s2PtInBox(o.ra_Test,o.decl_Test,2,2,3,3) = 1) AND (scisql_s2PtInBox(s.raObjectTest,s.declObjectTest,2,2,3,3) = 1) AND o.objectId=s.objectId;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
}

BOOST_AUTO_TEST_CASE(ObjectSelfJoin) {
    std::string stmt = "select count(*) from Object as o1, Object as o2;";
    std::string expected = "select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sc2%$# as o2 UNION select count(*) from LSST.%$#Object_sc1%$# as o1,LSST.%$#Object_sfo%$# as o2;";

    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    //std::cout << "Parse result: " << spr->getParseResult() << std::endl;
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
}

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
    std::cout << "Parse output:" << spr->getParseResult() << std::endl;
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

BOOST_AUTO_TEST_CASE(CountQuery2) {
    std::string stmt = "SELECT count(*) from Source;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(spr->getHasSubChunks());
    BOOST_CHECK(spr->getHasAggregate());
}


BOOST_AUTO_TEST_SUITE_END()
////////////////////////////////////////////////////////////////////////
// Case01
////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(Case01Parse, ParserFixture)

BOOST_AUTO_TEST_CASE(Case01_0002) {
    std::string stmt = "SELECT * FROM Object WHERE objectId = 430213989000;";
    std::string expected = "SELECT * FROM LSST.%$#Object%$# WHERE objectId=430213989000;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK_EQUAL(spr->getParseResult(), expected);
    // FIXME: Impl something in spr to inspect for objectid
    //  (or other indexing!)
}

BOOST_AUTO_TEST_CASE(Case01_0012) {
    std::string stmt = "SELECT sce.filterId, sce.filterName "
        "FROM   Science_Ccd_Exposure AS sce WHERE  (sce.visit = 887404831) "
        "AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%');";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(!spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    // should parse okay as a full-scan of sce, non-partitioned.
    // Optional parens may be confusing the parser.
}

BOOST_AUTO_TEST_CASE(Case01_1012) {
    std::string stmt = "SELECT objectId, iE1_SG, ABS(iE1_SG) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ABS(iE1_SG);";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
}

BOOST_AUTO_TEST_CASE(Case01_1013) {
    std::string stmt = "SELECT objectId, ROUND(iE1_SG, 3), ROUND(ABS(iE1_SG), 3) FROM Object WHERE iE1_SG between -0.1 and 0.1 ORDER BY ROUND(ABS(iE1_SG), 3);";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
}

// Syntax not supported currently.
BOOST_AUTO_TEST_CASE(Case01_1030) {
    std::string stmt = "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) "
        "FROM   Source "
        "JOIN   Object USING(objectId) JOIN   Filter USING(filterId) "
        "WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' "
        "ORDER BY objectId, taiMidPoint ASC;";
    SqlParseRunner::Ptr spr = getRunner(stmt);
    testStmt2(spr);
    BOOST_CHECK(spr->getHasChunks());
    BOOST_CHECK(!spr->getHasSubChunks());
    BOOST_CHECK(!spr->getHasAggregate()); // Not really "aggregation"
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

BOOST_AUTO_TEST_SUITE_END()

#endif
// SELECT o1.id as o1id,o2.id as o2id,
//        LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
//  AS dist FROM Object AS o1, Object AS o2 
//  WHERE ABS(o1.decl-o2.decl) < 0.001 
//      AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.001 
//      AND o1.id != o2.id;

#if 0
int main(int, char**) {
    //    tryAutoSubstitute();
    tryNnSubstitute();
    //tryTriple();
    //    tryAggregate();
    return 0;
}
#endif
