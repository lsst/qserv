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
#define BOOST_TEST_MODULE Metadata_1
#include "boost/test/included/unit_test.hpp"

#include "lsst/qserv/worker/Metadata.h"

#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/SqlErrorObject.hh"

#include <iostream>
#include <string>
#include <unistd.h> // for getpass

namespace test = boost::test_tools;
namespace qsrv = lsst::qserv;

using lsst::qserv::SqlConnection;
using lsst::qserv::SqlErrorObject;
using lsst::qserv::SqlResults;

struct PerTestFixture {
    PerTestFixture() {
        static qsrv::SqlConfig sqlConfig;

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
        sqlConn = new qsrv::SqlConnection(sqlConfig);
    }
    ~PerTestFixture () {
        delete sqlConn;
        sqlConn = 0;
    }
    qsrv::SqlConnection* sqlConn;
};

BOOST_FIXTURE_TEST_SUITE(MetadataTestSuite, PerTestFixture)

BOOST_AUTO_TEST_CASE(Registrations) {
    std::string workerId = "dummy123";
    lsst::qserv::worker::Metadata m(workerId);
    qsrv::SqlErrorObject errObj;
    qsrv::SqlErrorObject errObjDummy;

    std::string dbN1 = "rplante_PT1_2_u_pt12prod_im2000_";
    std::string pts1 = "Object, Source";
    if ( !m.registerQservedDb(dbN1, pts1, *sqlConn, errObj) ) {
        sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL(errObj.printErrMsg());
    }
    std::string dbN2 = "rplante_PT1_2_u_pt12prod_im3000_";
    std::string pts2 = "Object, Source , ForcedSource";
    std::string ptsBad = "Object, Source , ForcedSource, ";
    if ( m.registerQservedDb(dbN2, ptsBad, *sqlConn, errObj) ) {
        sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL("This should fail because of extra ','");
    }
    if ( !m.registerQservedDb(dbN2, pts2, *sqlConn, errObj) ) {
        sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL(errObj.printErrMsg());
    }
    if ( m.registerQservedDb(dbN2, pts2, *sqlConn, errObj) ) {
        sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL("This should fail (already registered)");
    }
    sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
}

BOOST_AUTO_TEST_CASE(PathCreate) {
    std::string workerId = "dummy123";
    lsst::qserv::worker::Metadata m(workerId);
    qsrv::SqlErrorObject errObj;

    std::string dbN1 = "qservTestSuite_aa";
    std::string pts1 = "Object, Source";

    qsrv::SqlErrorObject errObjDummy;
    sqlConn->dropDb(dbN1, errObjDummy);
    sqlConn->createDbAndSelect(dbN1, errObjDummy);
    sqlConn->runQuery("create table Object_1234 (i int)", errObjDummy);
    sqlConn->runQuery("create table Object_1235 (i int)", errObjDummy);
    sqlConn->runQuery("create table Source_1234 (i int)", errObjDummy);
    sqlConn->runQuery("create table Source_1235 (i int)", errObjDummy);
    sqlConn->runQuery("create table Exposure_99 (i int)", errObjDummy);
    
    if ( !m.registerQservedDb(dbN1, pts1, *sqlConn, errObj) ) {
        sqlConn->dropDb(dbN1, errObjDummy);
        sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL(errObj.printErrMsg());
    }
    if ( !m.createExportDirs("/query2", *sqlConn, errObj)) {
        sqlConn->dropDb(dbN1, errObjDummy);
        sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL(errObj.printErrMsg());
    }
    sqlConn->dropDb(dbN1, errObjDummy);
    sqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
}

BOOST_AUTO_TEST_SUITE_END()
