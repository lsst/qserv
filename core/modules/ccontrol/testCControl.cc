// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

// System headers
#include <array>
#include <string>
#include <unistd.h>

// Boost unit test header
#define BOOST_TEST_MODULE CControl_1
#include "boost/test/included/unit_test.hpp"

#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

// Qserv headers
#include "ccontrol/A4UserQueryFactory.h"
#include "ccontrol/UserQueryType.h"
#include "ccontrol/UserQueryFactory.h"
#include "query/SelectStmt.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)

static const std::vector< std::string > QUERIES = {
      "SELECT objectId, ra_PS FROM Object WHERE objectId=386937898687249"
    , "SELECT COUNT ( * ) AS OBJ_COUNT FROM Object WHERE qserv_areaspec_box ( 0.1 , - 6 , 4 , 6 ) AND scisql_fluxToAbMag ( zFlux_PS ) BETWEEN 20 AND 24 AND scisql_fluxToAbMag ( gFlux_PS ) - scisql_fluxToAbMag ( rFlux_PS ) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag ( iFlux_PS ) - scisql_fluxToAbMag ( zFlux_PS ) BETWEEN 0.1 AND 1.0;"
    , "SELECT objectId,iauId,ra_PS,ra_PS_Sigma,decl_PS,decl_PS_Sigma,radecl_PS_Cov,htmId20,ra_SG,ra_SG_Sigma,decl_SG,decl_SG_Sigma, radecl_SG_Cov,raRange,declRange,muRa_PS,muRa_PS_Sigma,muDecl_PS,muDecl_PS_Sigma,muRaDecl_PS_Cov,parallax_PS, parallax_PS_Sigma,canonicalFilterId,extendedness,varProb,earliestObsTime,latestObsTime,meanObsTime,flags,uNumObs, uExtendedness,uVarProb,uRaOffset_PS,uRaOffset_PS_Sigma,uDeclOffset_PS,uDeclOffset_PS_Sigma,uRaDeclOffset_PS_Cov, uRaOffset_SG,uRaOffset_SG_Sigma,uDeclOffset_SG,uDeclOffset_SG_Sigma,uRaDeclOffset_SG_Cov,uLnL_PS,uLnL_SG,uFlux_PS, uFlux_PS_Sigma,uFlux_ESG,uFlux_ESG_Sigma,uFlux_Gaussian,uFlux_Gaussian_Sigma,uTimescale,uEarliestObsTime,uLatestObsTime, uSersicN_SG,uSersicN_SG_Sigma,uE1_SG,uE1_SG_Sigma,uE2_SG,uE2_SG_Sigma,uRadius_SG,uRadius_SG_Sigma,uFlags,gNumObs, gExtendedness,gVarProb,gRaOffset_PS,gRaOffset_PS_Sigma,gDeclOffset_PS,gDeclOffset_PS_Sigma,gRaDeclOffset_PS_Cov, gRaOffset_SG,gRaOffset_SG_Sigma,gDeclOffset_SG,gDeclOffset_SG_Sigma,gRaDeclOffset_SG_Cov,gLnL_PS,gLnL_SG,gFlux_PS, gFlux_PS_Sigma,gFlux_ESG,gFlux_ESG_Sigma,gFlux_Gaussian,gFlux_Gaussian_Sigma,gTimescale,gEarliestObsTime, gLatestObsTime,gSersicN_SG,gSersicN_SG_Sigma,gE1_SG,gE1_SG_Sigma,gE2_SG,gE2_SG_Sigma,gRadius_SG,gRadius_SG_Sigma, gFlags,rNumObs,rExtendedness,rVarProb,rRaOffset_PS,rRaOffset_PS_Sigma,rDeclOffset_PS,rDeclOffset_PS_Sigma, rRaDeclOffset_PS_Cov,rRaOffset_SG,rRaOffset_SG_Sigma,rDeclOffset_SG,rDeclOffset_SG_Sigma,rRaDeclOffset_SG_Cov,rLnL_PS, rLnL_SG,rFlux_PS,rFlux_PS_Sigma,rFlux_ESG,rFlux_ESG_Sigma,rFlux_Gaussian,rFlux_Gaussian_Sigma,rTimescale, rEarliestObsTime,rLatestObsTime,rSersicN_SG,rSersicN_SG_Sigma,rE1_SG,rE1_SG_Sigma,rE2_SG,rE2_SG_Sigma,rRadius_SG, rRadius_SG_Sigma,rFlags,iNumObs,iExtendedness,iVarProb,iRaOffset_PS,iRaOffset_PS_Sigma,iDeclOffset_PS, iDeclOffset_PS_Sigma,iRaDeclOffset_PS_Cov,iRaOffset_SG,iRaOffset_SG_Sigma,iDeclOffset_SG,iDeclOffset_SG_Sigma, iRaDeclOffset_SG_Cov,iLnL_PS,iLnL_SG,iFlux_PS,iFlux_PS_Sigma,iFlux_ESG,iFlux_ESG_Sigma,iFlux_Gaussian, iFlux_Gaussian_Sigma,iTimescale,iEarliestObsTime,iLatestObsTime,iSersicN_SG,iSersicN_SG_Sigma,iE1_SG,iE1_SG_Sigma, iE2_SG,iE2_SG_Sigma,iRadius_SG,iRadius_SG_Sigma,iFlags,zNumObs,zExtendedness,zVarProb,zRaOffset_PS,zRaOffset_PS_Sigma, zDeclOffset_PS,zDeclOffset_PS_Sigma,zRaDeclOffset_PS_Cov,zRaOffset_SG,zRaOffset_SG_Sigma,zDeclOffset_SG, zDeclOffset_SG_Sigma,zRaDeclOffset_SG_Cov,zLnL_PS,zLnL_SG,zFlux_PS,zFlux_PS_Sigma,zFlux_ESG,zFlux_ESG_Sigma, zFlux_Gaussian,zFlux_Gaussian_Sigma,zTimescale,zEarliestObsTime,zLatestObsTime,zSersicN_SG,zSersicN_SG_Sigma,zE1_SG, zE1_SG_Sigma,zE2_SG,zE2_SG_Sigma,zRadius_SG,zRadius_SG_Sigma,zFlags,yNumObs,yExtendedness,yVarProb,yRaOffset_PS, yRaOffset_PS_Sigma,yDeclOffset_PS,yDeclOffset_PS_Sigma,yRaDeclOffset_PS_Cov,yRaOffset_SG,yRaOffset_SG_Sigma, yDeclOffset_SG,yDeclOffset_SG_Sigma,yRaDeclOffset_SG_Cov,yLnL_PS,yLnL_SG,yFlux_PS,yFlux_PS_Sigma,yFlux_ESG, yFlux_ESG_Sigma,yFlux_Gaussian,yFlux_Gaussian_Sigma,yTimescale,yEarliestObsTime,yLatestObsTime,ySersicN_SG, ySersicN_SG_Sigma,yE1_SG,yE1_SG_Sigma,yE2_SG,yE2_SG_Sigma,yRadius_SG,yRadius_SG_Sigma,yFlags FROM Object WHERE  objectId = 430213989148129"
//     "SELECT ra_Ps, decl_PS FROM Object WHERE objectId IN (390034570102582, 396210733076852, 393126946553816, 390030275138483)"
};

BOOST_AUTO_TEST_CASE_EXPECTED_FAILURES( antlr_compare, 1 )

BOOST_DATA_TEST_CASE(antlr_compare, QUERIES, query) {
    auto a4SelectStatement = ccontrol::a4NewUserQuery(query);
    BOOST_REQUIRE(a4SelectStatement != nullptr);
    std::ostringstream a4QueryStr;
    a4QueryStr << a4SelectStatement->getQueryTemplate();

    auto a2SelectStatement = ccontrol::UserQueryFactory::antlr2NewSelectStmt(query);
    BOOST_REQUIRE(a2SelectStatement != nullptr);
    std::ostringstream a2QueryStr;
    a2QueryStr << a2SelectStatement->getQueryTemplate();
    if (a4QueryStr.str() != a2QueryStr.str()) {
        BOOST_TEST_MESSAGE("antlr4 selectStmt does not match antlr2 selectStmt...");
        BOOST_TEST_MESSAGE("antlr2 selectStmt structure:" << a2SelectStatement);
        BOOST_TEST_MESSAGE("antlr4 selectStmt structure:" << a4SelectStatement);
    }
    BOOST_REQUIRE_EQUAL(a4QueryStr.str(), a2QueryStr.str());
}


BOOST_AUTO_TEST_CASE(testUserQueryType) {
    using lsst::qserv::ccontrol::UserQueryType;

    BOOST_CHECK(UserQueryType::isSelect("SELECT 1"));
    BOOST_CHECK(UserQueryType::isSelect("SELECT\t1"));
    BOOST_CHECK(UserQueryType::isSelect("SELECT\n\r1"));

    BOOST_CHECK(UserQueryType::isSelect("select 1"));
    BOOST_CHECK(UserQueryType::isSelect("SeLeCt 1"));

    BOOST_CHECK(not UserQueryType::isSelect("unselect X"));
    BOOST_CHECK(not UserQueryType::isSelect("DROP SELECT;"));

    std::string stripped;
    BOOST_CHECK(UserQueryType::isSubmit("SUBMIT SELECT", stripped));
    BOOST_CHECK_EQUAL("SELECT", stripped);
    BOOST_CHECK(UserQueryType::isSubmit("submit\tselect  ", stripped));
    BOOST_CHECK_EQUAL("select  ", stripped);
    BOOST_CHECK(UserQueryType::isSubmit("SubMiT \n SelEcT", stripped));
    BOOST_CHECK_EQUAL("SelEcT", stripped);
    BOOST_CHECK(not UserQueryType::isSubmit("submit", stripped));
    BOOST_CHECK(not UserQueryType::isSubmit("submit ", stripped));
    BOOST_CHECK(not UserQueryType::isSubmit("unsubmit select", stripped));
    BOOST_CHECK(not UserQueryType::isSubmit("submitting select", stripped));

    struct {
        const char* query;
        const char* db;
        const char* table;
    } drop_table_ok[] = {
        {"DROP TABLE DB.TABLE", "DB", "TABLE"},
        {"DROP TABLE DB.TABLE;", "DB", "TABLE"},
        {"DROP TABLE DB.TABLE ;", "DB", "TABLE"},
        {"DROP TABLE `DB`.`TABLE` ", "DB", "TABLE"},
        {"DROP TABLE \"DB\".\"TABLE\"", "DB", "TABLE"},
        {"DROP TABLE TABLE", "", "TABLE"},
        {"DROP TABLE `TABLE`", "", "TABLE"},
        {"DROP TABLE \"TABLE\"", "", "TABLE"},
        {"drop\ttable\nDB.TABLE ;", "DB", "TABLE"}
    };

    for (auto test: drop_table_ok) {
        std::string db, table;
        BOOST_CHECK(UserQueryType::isDropTable(test.query, db, table));
        BOOST_CHECK_EQUAL(db, test.db);
        BOOST_CHECK_EQUAL(table, test.table);
    }

    const char* drop_table_fail[] = {
        "DROP DATABASE DB",
        "DROP TABLE",
        "DROP TABLE TABLE; DROP IT;",
        "DROP TABLE 'DB'.'TABLE'",
        "DROP TABLE db%.TABLE",
        "UNDROP TABLE X"
    };
    for (auto test: drop_table_fail) {
        std::string db, table;
        BOOST_CHECK(not UserQueryType::isDropTable(test, db, table));
    }

    struct {
        const char* query;
        const char* db;
    } drop_db_ok[] = {
        {"DROP DATABASE DB", "DB"},
        {"DROP SCHEMA DB ", "DB"},
        {"DROP DATABASE DB;", "DB"},
        {"DROP SCHEMA DB ; ", "DB"},
        {"DROP DATABASE `DB` ", "DB"},
        {"DROP SCHEMA \"DB\"", "DB"},
        {"drop\tdatabase\nd_b ;", "d_b"}
    };
    for (auto test: drop_db_ok) {
        std::string db;
        BOOST_CHECK(UserQueryType::isDropDb(test.query, db));
        BOOST_CHECK_EQUAL(db, test.db);
    }

    const char* drop_db_fail[] = {
        "DROP TABLE DB",
        "DROP DB",
        "DROP DATABASE",
        "DROP DATABASE DB;;",
        "DROP SCHEMA DB; DROP IT;",
        "DROP SCHEMA DB.TABLE",
        "DROP SCHEMA 'DB'",
        "DROP DATABASE db%",
        "UNDROP DATABASE X",
        "UN DROP DATABASE X"
    };
    for (auto test: drop_db_fail) {
        std::string db;
        BOOST_CHECK(not UserQueryType::isDropDb(test, db));
    }

    struct {
        const char* query;
        const char* db;
    } flush_empty_ok[] = {
        {"FLUSH QSERV_CHUNKS_CACHE", ""},
        {"FLUSH QSERV_CHUNKS_CACHE\t ", ""},
        {"FLUSH QSERV_CHUNKS_CACHE;", ""},
        {"FLUSH QSERV_CHUNKS_CACHE ; ", ""},
        {"FLUSH QSERV_CHUNKS_CACHE FOR DB", "DB"},
        {"FLUSH QSERV_CHUNKS_CACHE FOR `DB`", "DB"},
        {"FLUSH QSERV_CHUNKS_CACHE FOR \"DB\"", "DB"},
        {"FLUSH QSERV_CHUNKS_CACHE FOR DB ; ", "DB"},
        {"flush qserv_chunks_cache for `d_b`", "d_b"},
        {"flush\nqserv_chunks_CACHE\tfor \t d_b", "d_b"},
    };
    for (auto test: flush_empty_ok) {
        std::string db;
        BOOST_CHECK(UserQueryType::isFlushChunksCache(test.query, db));
        BOOST_CHECK_EQUAL(db, test.db);
    }

    const char* flush_empty_fail[] = {
        "FLUSH QSERV CHUNKS CACHE",
        "UNFLUSH QSERV_CHUNKS_CACHE",
        "FLUSH QSERV_CHUNKS_CACHE DB",
        "FLUSH QSERV_CHUNKS_CACHE FOR",
        "FLUSH QSERV_CHUNKS_CACHE FROM DB",
        "FLUSH QSERV_CHUNKS_CACHE FOR DB.TABLE",
    };
    for (auto test: flush_empty_fail) {
        std::string db;
        BOOST_CHECK(not UserQueryType::isFlushChunksCache(test, db));
    }

    const char* show_proclist_ok[] = {
        "SHOW PROCESSLIST",
        "show processlist",
        "show    PROCESSLIST",
    };
    for (auto test: show_proclist_ok) {
        bool full;
        BOOST_CHECK(UserQueryType::isShowProcessList(test, full));
        BOOST_CHECK(not full);
    }

    const char* show_full_proclist_ok[] = {
        "SHOW FULL PROCESSLIST",
        "show full   processlist",
        "show FULL PROCESSLIST",
    };
    for (auto test: show_full_proclist_ok) {
        bool full;
        BOOST_CHECK(UserQueryType::isShowProcessList(test, full));
        BOOST_CHECK(full);
    }

    const char* show_proclist_fail[] = {
        "show PROCESS",
        "SHOW PROCESS LIST",
        "show fullprocesslist",
        "show full process list",
    };
    for (auto test: show_proclist_fail) {
        bool full;
        BOOST_CHECK(not UserQueryType::isShowProcessList(test, full));
    }

    struct {
        const char* db;
        const char* table;
    } proclist_table_ok[] = {
        {"INFORMATION_SCHEMA", "PROCESSLIST"},
        {"information_schema", "processlist"},
        {"Information_Schema", "ProcessList"},
    };
    for (auto test: proclist_table_ok) {
        BOOST_CHECK(UserQueryType::isProcessListTable(test.db, test.table));
    }

    struct {
        const char* db;
        const char* table;
    } proclist_table_fail[] = {
        {"INFORMATIONSCHEMA", "PROCESSLIST"},
        {"information_schema", "process_list"},
        {"Information Schema", "Process List"},
    };
    for (auto test: proclist_table_fail) {
        BOOST_CHECK(not UserQueryType::isProcessListTable(test.db, test.table));
    }

    struct {
        const char* query;
        int id;
    } kill_query_ok[] = {
        {"KILL 100", 100},
        {"KilL 101  ", 101},
        {"kill   102  ", 102},
        {"KILL QUERY 100", 100},
        {"kill\tquery   100   ", 100},
        {"KILL CONNECTION 100", 100},
        {"KILL \t CONNECTION   100  ", 100},
    };
    for (auto test: kill_query_ok) {
        int threadId;
        BOOST_CHECK(UserQueryType::isKill(test.query, threadId));
        BOOST_CHECK_EQUAL(threadId, test.id);
    }

    const char* kill_query_fail[] = {
        "NOT KILL 100",
        "KILL SESSION 100 ",
        "KILL QID100",
        "KILL 100Q ",
        "KILL QUIERY=100 ",
    };
    for (auto test: kill_query_fail) {
        int threadId;
        BOOST_CHECK(not UserQueryType::isKill(test, threadId));
    }

    struct {
        const char* query;
        QueryId id;
    } cancel_ok[] = {
        {"CANCEL 100", 100},
        {"CAnCeL 101  ", 101},
        {"cancel \t  102  ", 102},
    };
    for (auto test: cancel_ok) {
        QueryId queryId;
        BOOST_CHECK(UserQueryType::isCancel(test.query, queryId));
        BOOST_CHECK_EQUAL(queryId, test.id);
    }

    const char* cancel_fail[] = {
        "NOT CANCLE 100",
        "CANCEL QUERY 100 ",
        "CANCEL q100",
        "cancel 100Q ",
        "cancel QUIERY=100 ",
    };
    for (auto test: cancel_fail) {
        QueryId queryId;
        BOOST_CHECK(not UserQueryType::isCancel(test, queryId));
    }

}

BOOST_AUTO_TEST_SUITE_END()
