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
#include <string>
#include <unistd.h>

// Boost unit test header
#define BOOST_TEST_MODULE CControl_1
#include "boost/test/included/unit_test.hpp"

// Qserv headers
#include "ccontrol/A4UserQueryFactory.h"
#include "ccontrol/UserQueryType.h"
#include "ccontrol/UserQueryFactory.h"
#include "query/SelectStmt.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(testAntlr4SelectStatement) {
    std::string query = "SELECT objectId, ra_PS FROM Object WHERE objectId=386937898687249";
    std::shared_ptr<query::SelectStmt> selectStatement = ccontrol::a4NewUserQuery(query);
    BOOST_REQUIRE(selectStatement != nullptr);
    std::ostringstream queryStr;
    queryStr << *selectStatement;
    std::string expectedQueryStr = "SELECT objectId, ra_PS, (FIXME) FROM Table(.Object),  WHERE objectId=386937898687249 ";
    BOOST_REQUIRE_EQUAL(queryStr.str(), expectedQueryStr);
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
