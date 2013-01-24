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
#define BOOST_TEST_MODULE SqlConnection_1
#include "boost/test/included/unit_test.hpp"

#include "SqlConnection.hh"

#include <iostream>
#include <string>
#include <unistd.h> // for getpass

namespace test = boost::test_tools;
namespace qsrv = lsst::qserv;

namespace {

inline std::string makeCreateTable(std::string const& t) {
    std::stringstream ss;
    ss <<  "CREATE TABLE " << t << " (o1 int)";
    return ss.str();
}
inline std::string makeShowTables(std::string const& dbName=std::string()) {
    std::stringstream ss;
    ss <<  "SHOW TABLES ";
    if(!dbName.empty()) { ss << "IN " << dbName; }
    return ss.str();
}
class createIntTable {
public:
    createIntTable(qsrv::SqlConnection* sqlConn_, 
                   qsrv::SqlErrorObject& errObj_) 
        : sqlConn(sqlConn_), errObj(errObj_) {}


    void operator()(std::string const& s) {
        if ( !sqlConn->runQuery(makeCreateTable(s), errObj) ) {
            BOOST_FAIL(errObj.printErrMsg());
        }
    }
    qsrv::SqlConnection* sqlConn;
    qsrv::SqlErrorObject& errObj;
};
} // anonymous namespace

struct PerTestFixture {
    PerTestFixture() {
        if ( sqlConfig.username.empty() ) {
            sqlConfig.hostname = "";
            sqlConfig.dbName = "";
            sqlConfig.port = 0;
            std::cout << "Enter mysql user name: ";
            std::cin >> sqlConfig.username;
            sqlConfig.password = getpass("Enter mysql password: ");
            std::cout << "Enter mysql socket: ";
            std::cin >> sqlConfig.socket;
        }
        sqlConn.reset(new qsrv::SqlConnection(sqlConfig));
    }
    ~PerTestFixture () {}
    boost::shared_ptr<qsrv::SqlConnection> sqlConn;
    static qsrv::SqlConfig sqlConfig;
};
qsrv::SqlConfig PerTestFixture::sqlConfig;

BOOST_FIXTURE_TEST_SUITE(SqlConnectionTestSuite, PerTestFixture)

BOOST_AUTO_TEST_CASE(CreateAndDropDb) {
    std::string dbN = "one_xysdfed34d";
    qsrv::SqlErrorObject errObj;

    // this database should not exist
    BOOST_CHECK_EQUAL(sqlConn->dbExists(dbN, errObj), false);
    // create it now
    if ( !sqlConn->createDb(dbN, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // this database should exist now
    if ( !sqlConn->dbExists(dbN, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }       
    // drop it
    if ( !sqlConn->dropDb(dbN, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }            
    // this database should not exist now
    BOOST_CHECK_EQUAL(sqlConn->dbExists(dbN, errObj), false);
}

BOOST_AUTO_TEST_CASE(TableExists) {
    std::string dbN1 = "one_xysdfed34d";
    std::string dbN2 = "two_xysdfed34d";
    std::string tNa = "object_a";
    std::string tNb = "object_b";
    std::stringstream ss;
    qsrv::SqlErrorObject errObj;

    // create 2 dbs
    if ( !sqlConn->createDb(dbN1, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    if ( !sqlConn->createDb(dbN2, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // select db to use
    if ( !sqlConn->selectDb(dbN1, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // check if table exists in default db
    BOOST_CHECK_EQUAL(sqlConn->tableExists(tNa, errObj), false);
    // check if table exists in dbN1
    BOOST_CHECK_EQUAL(sqlConn->tableExists(tNa, errObj), false);
    // check if table exists in dbN2
    BOOST_CHECK_EQUAL(sqlConn->tableExists(tNa, errObj, dbN2), false);
    // create table (in dbN1)
    ss.str(""); ss <<  "CREATE TABLE " << tNa << " (i int)";
    if ( !sqlConn->runQuery(ss.str(), errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // check if table tN exists in default db (it should)
    if ( !sqlConn->tableExists(tNa, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // check if table tN exists in dbN1 (it should)
    if ( !sqlConn->tableExists(tNa, errObj, dbN1) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // check if table tN exists in dbN2 (it should NOT)
    BOOST_CHECK_EQUAL(sqlConn->tableExists(tNa, errObj, dbN2), false);
    // drop dbs
    if (!sqlConn->dropDb(dbN1, errObj)) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    if (!sqlConn->dropDb(dbN2, errObj)) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // check if table tN exists in dbN2 (it should not)
    BOOST_CHECK_EQUAL(sqlConn->tableExists(tNa, errObj, dbN2), false);
}

BOOST_AUTO_TEST_CASE(ListTables) {
    std::string dbN = "one_xysdfed34d";
    std::string tList[] = {
        "object_1", "object_2", "object_3", 
        "source_1", "source_2" };
    int const tListLen = 5;
    qsrv::SqlErrorObject errObj;
    std::vector<std::string> v;

    // create db and select it as default
    if ( !sqlConn->createDbAndSelect(dbN, errObj) ) { 
        BOOST_FAIL(errObj.printErrMsg());
    }

    // create tables
    std::for_each(tList, tList + tListLen, 
                  createIntTable(sqlConn.get(), errObj));

    // try creating exiting table, should fail
    if ( sqlConn->runQuery(makeCreateTable(tList[0]), errObj) ) {
        BOOST_FAIL("Creating table " + makeCreateTable(tList[0])
                   +" should fail, but it didn't. Received this: "
                   + errObj.printErrMsg());
    }
    // list all tables, should get 5
    if ( !sqlConn->listTables(v, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    BOOST_CHECK_EQUAL(v.size(), 5);

    // list "object" tables, should get 3
    if ( !sqlConn->listTables(v, errObj, "object_") ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    BOOST_CHECK_EQUAL(v.size(), 3);

    // list "source" tables, should get 2
    if ( !sqlConn->listTables(v, errObj, "source_") ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    BOOST_CHECK_EQUAL(v.size(), 2);

    // list nonExisting tables, should get 0
    if ( !sqlConn->listTables(v, errObj, "whatever") ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    BOOST_CHECK_EQUAL(v.size(), 0);
    // drop db
    if ( !sqlConn->dropDb(dbN, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
}

BOOST_AUTO_TEST_CASE(UnbufferedQuery) {
    sqlConn.reset(new qsrv::SqlConnection(sqlConfig, true));
    // Setup for "list tables"
    std::string dbN = "one_xysdfed34d";
    std::string tList[] = {
        "object_1", "object_2", "object_3", 
        "source_1", "source_2" };
    int const tListLen = 5;
    qsrv::SqlErrorObject errObj;
    std::vector<std::string> v;

    // create db and select it as default
    if ( !sqlConn->createDbAndSelect(dbN, errObj) ) { 
        BOOST_FAIL(errObj.printErrMsg());
    }

    // create tables
    std::for_each(tList, tList + tListLen, 
                  createIntTable(sqlConn.get(), errObj));

    boost::shared_ptr<qsrv::SqlResultIter> ri;
    ri = sqlConn->getQueryIter(makeShowTables());
    int i=0;
    for(; !ri->done(); ++*ri, ++i) { // Assume mysql is order-preserving.
        std::string column0 = (**ri)[0];
        BOOST_CHECK_EQUAL(tList[i], column0);
        BOOST_CHECK(i < tListLen);
    }
    BOOST_CHECK_EQUAL(i, tListLen);

    // drop db
    if ( !sqlConn->dropDb(dbN, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
}


BOOST_AUTO_TEST_SUITE_END()
