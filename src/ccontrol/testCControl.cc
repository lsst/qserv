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
#include <memory>
#include <string>
#include <unistd.h>

// Boost unit test header
#define BOOST_TEST_MODULE CControl_1
#include "boost/test/unit_test.hpp"
#include <boost/test/data/test_case.hpp>

// Qserv headers
#include "ccontrol/UserQueryType.h"
#include "parser/ParseException.h"
#include "qproc/QuerySession.h"
#include "query/AndTerm.h"
#include "query/BetweenPredicate.h"
#include "query/BoolFactor.h"
#include "query/BoolTerm.h"
#include "query/CompPredicate.h"
#include "query/InPredicate.h"
#include "query/LikePredicate.h"
#include "query/OrTerm.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)

struct ParseErrorQueryInfo {
    ParseErrorQueryInfo(std::string const& q, std::string const& m) : query(q), errorMessage(m) {}

    std::string query;
    std::string errorMessage;
};

std::ostream& operator<<(std::ostream& os, ParseErrorQueryInfo const& i) {
    os << "ParseErrorQueryInfo(" << i.query << ", " << i.errorMessage << ")";
    return os;
}

static const std::vector<ParseErrorQueryInfo> PARSE_ERROR_QUERIES = {
        // "UNION JOIN" is not expected to parse.
        ParseErrorQueryInfo(
                "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 UNION JOIN Source s2 WHERE s1.bar = s2.bar;",
                "ParseException:Failed to instantiate query: \"SELECT s1.foo, s2.foo AS s2_foo FROM Source "
                "s1 UNION "
                "JOIN Source s2 WHERE s1.bar = s2.bar;\""),

        // The qserv manual says:
        // "Expressions/functions in ORDER BY clauses are not allowed
        // In SQL92 ORDER BY is limited to actual table columns, thus expressions or functions in ORDER BY are
        // rejected. This is true for Qserv too.
        ParseErrorQueryInfo("SELECT objectId, iE1_SG, ABS(iE1_SG) FROM Object WHERE iE1_SG between -0.1 and "
                            "0.1 ORDER BY ABS(iE1_SG)",
                            "ParseException:Error parsing query, near \"ABS(iE1_SG)\", qserv does not "
                            "support functions in ORDER BY."),

        ParseErrorQueryInfo("SELECT foo from Filter f limit 5 garbage query !#$%!#$",
                            "ParseException:Failed to instantiate query: \"SELECT foo from Filter f limit 5 "
                            "garbage query !#$%!#$\""),

        ParseErrorQueryInfo("SELECT foo from Filter f limit 5 garbage query !#$%!#$",
                            "ParseException:Failed to instantiate query: \"SELECT foo from Filter f limit 5 "
                            "garbage query !#$%!#$\""),

        ParseErrorQueryInfo("SELECT foo from Filter f limit 5; garbage query !#$%!#$",
                            "ParseException:Failed to instantiate query: \"SELECT foo from Filter f limit 5; "
                            "garbage query !#$%!#$\""),

        ParseErrorQueryInfo(
                "SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), _chunkId FROM Object GROUP BY _chunkId;",
                "ParseException:Error parsing query, near \"_chunkId\", Identifiers in Qserv may not start "
                "with an underscore."),

        ParseErrorQueryInfo(
                "LECT sce.filterName,sce.field "
                "FROM LSST.Science_Ccd_Exposure AS sce "
                "WHERE sce.field=535 AND sce.camcol LIKE '%' ",
                "ParseException:Failed to instantiate query: \"LECT sce.filterName,sce.field "
                "FROM LSST.Science_Ccd_Exposure AS sce WHERE sce.field=535 AND sce.camcol LIKE '%' \""),

        // per testQueryAnaGeneral: CASE in column spec is illegal.
        ParseErrorQueryInfo(
                "SELECT  COUNT(*) AS totalCount, "
                "SUM(CASE WHEN (typeId=3) THEN 1 ELSE 0 END) AS galaxyCount "
                "FROM Object WHERE rFlux_PS > 10;",
                "ParseException:qserv can not parse query, near \"CASE WHEN (typeId=3) THEN 1 ELSE 0 END\""),
};

BOOST_DATA_TEST_CASE(expected_parse_error, PARSE_ERROR_QUERIES, queryInfo) {
    auto querySession = qproc::QuerySession();
    auto selectStmt = querySession.parseQuery(queryInfo.query);
    BOOST_REQUIRE_EQUAL(selectStmt, nullptr);
    BOOST_REQUIRE_EQUAL(querySession.getError(), queryInfo.errorMessage);
}

BOOST_AUTO_TEST_CASE(testSimpleCountStar) {
    using lsst::qserv::ccontrol::UserQueryType;
    auto querySession = qproc::QuerySession();
    std::string spelling;

    // test identifying a simple COUNT(*) query - only COUNT(*) and a FROM statement.
    auto selectStmt = querySession.parseQuery("SELECT COUNT(*) FROM mydb.mytable");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == true);
    BOOST_REQUIRE_EQUAL(spelling, "COUNT");
    // test lower case
    selectStmt = querySession.parseQuery("select count(*) from mydb.mytable");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == true);
    BOOST_REQUIRE_EQUAL(spelling, "count");
    // test mixed case
    selectStmt = querySession.parseQuery("select cOuNt(*) from mydb.mytable");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == true);
    BOOST_REQUIRE_EQUAL(spelling, "cOuNt");
    // LIMIT should be allowed, as it does not change the meaning of the query.
    selectStmt = querySession.parseQuery("SELECT COUNT(*) FROM mydb.mytable LIMIT 5");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == true);
    BOOST_REQUIRE_EQUAL(spelling, "COUNT");

    // any WHERE, ORDER BY, GROUP BY, OR HAVING disqalifies the statement from being a simple COUNT(*)
    selectStmt = querySession.parseQuery("SELECT COUNT(*) FROM mydb.mytable WHERE foo = bar");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == false);
    selectStmt = querySession.parseQuery("SELECT COUNT(*) FROM mydb.mytable ORDER BY foo");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == false);
    selectStmt = querySession.parseQuery("SELECT COUNT(*) FROM mydb.mytable GROUP BY foo");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == false);
    selectStmt = querySession.parseQuery("SELECT COUNT(*) FROM mydb.mytable HAVING foo > 42");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == false);
    // without COUNT(*) disqualifies
    selectStmt = querySession.parseQuery("SELECT foo FROM mydb.mytable");
    BOOST_TEST(UserQueryType::isSimpleCountStar(selectStmt, spelling) == false);
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

    const char* show_proclist_ok[] = {
            "SHOW PROCESSLIST",
            "show processlist",
            "show    PROCESSLIST",
    };
    for (auto test : show_proclist_ok) {
        bool full;
        BOOST_CHECK(UserQueryType::isShowProcessList(test, full));
        BOOST_CHECK(not full);
    }

    const char* show_full_proclist_ok[] = {
            "SHOW FULL PROCESSLIST",
            "show full   processlist",
            "show FULL PROCESSLIST",
    };
    for (auto test : show_full_proclist_ok) {
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
    for (auto test : show_proclist_fail) {
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
    for (auto test : proclist_table_ok) {
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
    for (auto test : proclist_table_fail) {
        BOOST_CHECK(not UserQueryType::isProcessListTable(test.db, test.table));
    }

    struct {
        const char* db;
        const char* table;
    } queries_table_ok[] = {{"INFORMATION_SCHEMA", "QUERIES"},
                            {"information_schema", "queries"},
                            {"Information_Schema", "Queries"}};
    for (auto test : queries_table_ok) {
        BOOST_CHECK(UserQueryType::isQueriesTable(test.db, test.table));
    }

    struct {
        const char* db;
        const char* table;
    } queries_table_fail[] = {{"INFORMATIONSCHEMA", "QUERIES"}, {"information_schema", "query"}};
    for (auto test : queries_table_fail) {
        BOOST_CHECK(not UserQueryType::isQueriesTable(test.db, test.table));
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
    for (auto test : kill_query_ok) {
        int threadId;
        BOOST_CHECK(UserQueryType::isKill(test.query, threadId));
        BOOST_CHECK_EQUAL(threadId, test.id);
    }

    const char* kill_query_fail[] = {
            "NOT KILL 100", "KILL SESSION 100 ", "KILL QID100", "KILL 100Q ", "KILL QUIERY=100 ",
    };
    for (auto test : kill_query_fail) {
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
    for (auto test : cancel_ok) {
        QueryId queryId;
        BOOST_CHECK(UserQueryType::isCancel(test.query, queryId));
        BOOST_CHECK_EQUAL(queryId, test.id);
    }

    const char* cancel_fail[] = {
            "NOT CANCLE 100", "CANCEL QUERY 100 ", "CANCEL q100", "cancel 100Q ", "cancel QUIERY=100 ",
    };
    for (auto test : cancel_fail) {
        QueryId queryId;
        BOOST_CHECK(not UserQueryType::isCancel(test, queryId));
    }
}

BOOST_AUTO_TEST_SUITE_END()
