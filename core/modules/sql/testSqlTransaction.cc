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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "SqlTransaction.h"

// System headers
#include <iostream>
#include <string>
#include <unistd.h> // for getpass

// Third-party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"
#include "sql/SqlResults.h"

// Local headers

// Boost unit test header
#define BOOST_TEST_MODULE SqlTransaction_1
#include "boost/test/included/unit_test.hpp"

using lsst::qserv::mysql::MySqlConfig;
using namespace lsst::qserv::sql;

#define DB_NAME "testSqlTransX675sdrt"
#define TABLE_NAME "TEST123"
#define FULL_TABLE_NAME DB_NAME "." TABLE_NAME

namespace {


struct TestDBGuard {
    TestDBGuard() {
        sqlConfig.hostname = "";
        sqlConfig.port = 0;
        sqlConfig.username = "root";
        sqlConfig.password = getpass("Enter mysql root password: ");
        std::cout << "Enter mysql socket: ";
        std::cin >> sqlConfig.socket;
        sqlConfig.dbName = DB_NAME;

        // need config without database name
        MySqlConfig sqlConfigLocal = sqlConfig;
        sqlConfigLocal.dbName = "";
        SqlConnection sqlConn(sqlConfigLocal);

        SqlErrorObject errObj;

        // create database
        sqlConn.createDb(DB_NAME, errObj);
    }

    ~TestDBGuard() {
        SqlConnection sqlConn(sqlConfig);
        SqlErrorObject errObj;
        sqlConn.dropDb(sqlConfig.dbName, errObj);
    }

    MySqlConfig sqlConfig;
};

}

struct PerTestFixture {
    PerTestFixture() : sqlConn(testDB.sqlConfig) {

        // create table (must be InnoDB)
        std::string query = "CREATE TABLE " FULL_TABLE_NAME " (X INT, Y INT) ENGINE=InnoDB";
        SqlErrorObject errObj;
        sqlConn.runQuery(query, errObj);
    }
    ~PerTestFixture() {
        // drop table
        std::string query = "DROP TABLE " FULL_TABLE_NAME;
        SqlErrorObject errObj;
        sqlConn.runQuery(query, errObj);
    }

    static TestDBGuard testDB;
    SqlConnection sqlConn;
};

TestDBGuard PerTestFixture::testDB;

BOOST_FIXTURE_TEST_SUITE(SqlTransactionTestSuite, PerTestFixture)

BOOST_AUTO_TEST_CASE(riiaTest) {

    // do transactions riia-style, they automatically abort

    for (int i = 0; i != 3; ++ i){
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string i_str = boost::lexical_cast<std::string>(i);
        std::string j_str = boost::lexical_cast<std::string>(i*100);
        std::string query = "INSERT INTO " FULL_TABLE_NAME " (X, Y) VALUES(" + i_str + ", " + j_str + ")";
        BOOST_CHECK(sqlConn.runQuery(query, errObj));

        BOOST_CHECK(trans.isActive());
    }

    {
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string query = "SELECT COUNT(*) FROM " FULL_TABLE_NAME;
        SqlResults sqlRes;
        BOOST_CHECK(sqlConn.runQuery(query, sqlRes, errObj));
        std::vector<std::string> rows;
        BOOST_CHECK(sqlRes.extractFirstColumn(rows, errObj));
        BOOST_CHECK_EQUAL(rows.size(), 1U);
        BOOST_CHECK_EQUAL(rows[0], "0");
    }

}

BOOST_AUTO_TEST_CASE(commitTest) {

    // explicit commit

    for (int i = 0; i != 3; ++ i){
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string i_str = boost::lexical_cast<std::string>(i);
        std::string j_str = boost::lexical_cast<std::string>(i*100);
        std::string query = "INSERT INTO " FULL_TABLE_NAME " (X, Y) VALUES(" + i_str + ", " + j_str + ")";
        BOOST_CHECK(sqlConn.runQuery(query, errObj));

        BOOST_CHECK(trans.isActive());
        BOOST_CHECK(trans.commit(errObj));
        BOOST_CHECK(not trans.isActive());
    }

    {
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string query = "SELECT COUNT(*) FROM " FULL_TABLE_NAME;
        SqlResults sqlRes;
        BOOST_CHECK(sqlConn.runQuery(query, sqlRes, errObj));
        std::vector<std::string> rows;
        BOOST_CHECK(sqlRes.extractFirstColumn(rows, errObj));
        BOOST_CHECK_EQUAL(rows.size(), 1U);
        BOOST_CHECK_EQUAL(rows[0], "3");
    }

}

BOOST_AUTO_TEST_CASE(abortTest) {

    // explicit abort

    for (int i = 0; i != 3; ++ i){
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string i_str = boost::lexical_cast<std::string>(i);
        std::string j_str = boost::lexical_cast<std::string>(i*100);
        std::string query = "INSERT INTO " FULL_TABLE_NAME " (X, Y) VALUES(" + i_str + ", " + j_str + ")";
        BOOST_CHECK(sqlConn.runQuery(query, errObj));

        BOOST_CHECK(trans.isActive());
        BOOST_CHECK(trans.abort(errObj));
        BOOST_CHECK(not trans.isActive());
    }

    {
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string query = "SELECT COUNT(*) FROM " FULL_TABLE_NAME;
        SqlResults sqlRes;
        BOOST_CHECK(sqlConn.runQuery(query, sqlRes, errObj));
        std::vector<std::string> rows;
        BOOST_CHECK(sqlRes.extractFirstColumn(rows, errObj));
        BOOST_CHECK_EQUAL(rows.size(), 1U);
        BOOST_CHECK_EQUAL(rows[0], "0");
    }

}

BOOST_AUTO_TEST_CASE(mixedTest) {

    // explicit abort/commit

    for (int i = 0; i != 10; ++ i){
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string i_str = boost::lexical_cast<std::string>(i);
        std::string j_str = boost::lexical_cast<std::string>(i*100);
        std::string query = "INSERT INTO " FULL_TABLE_NAME " (X, Y) VALUES(" + i_str + ", " + j_str + ")";
        BOOST_CHECK(sqlConn.runQuery(query, errObj));

        BOOST_CHECK(trans.isActive());
        if (i % 2) {
            BOOST_CHECK(trans.commit(errObj));
        } else {
            BOOST_CHECK(trans.abort(errObj));
        }
        BOOST_CHECK(not trans.isActive());
    }

    {
        SqlErrorObject errObj;

        SqlTransaction trans(sqlConn, errObj);
        BOOST_CHECK(not errObj.isSet());

        std::string query = "SELECT COUNT(*) FROM " FULL_TABLE_NAME;
        SqlResults sqlRes;
        BOOST_CHECK(sqlConn.runQuery(query, sqlRes, errObj));
        std::vector<std::string> rows;
        BOOST_CHECK(sqlRes.extractFirstColumn(rows, errObj));
        BOOST_CHECK_EQUAL(rows.size(), 1U);
        BOOST_CHECK_EQUAL(rows[0], "5");
    }

}

BOOST_AUTO_TEST_SUITE_END()
