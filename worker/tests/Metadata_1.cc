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
        std::string fName = getenv("HOME");
        fName += "/.qmwadm";
        qmsConnCfg.initFromFile(fName, "qmsHost", "qmsPort", "qmsUser",
                                "qmsPass", "qmsDb", "", true);
        qmwConnCfg.initFromFile(fName, "", "", "qmwUser", 
                                "qmwPass", "", "qmwMySqlSocket", true);
        qmsConnCfg.dbName = "qms_" + qmsConnCfg.dbName;
        qmwSqlConn = new qsrv::SqlConnection(qmwConnCfg);
        qmsConnCfg.printSelf("qms");
        qmwConnCfg.printSelf("qmw");
    }
    ~PerTestFixture () {
        delete qmwSqlConn;
        qmwSqlConn = 0;
    }
    qsrv::SqlConnection* qmwSqlConn;
    qsrv::SqlConfig qmsConnCfg;
    qsrv::SqlConfig qmwConnCfg;
};

BOOST_FIXTURE_TEST_SUITE(MetadataTestSuite, PerTestFixture)

BOOST_AUTO_TEST_CASE(Registrations) {
    lsst::qserv::worker::Metadata m(qmsConnCfg);
    qsrv::SqlErrorObject errObj;
    qsrv::SqlErrorObject errObjDummy;

    // start clean
    m.destroyWorkerMetadata(*qmwSqlConn, errObj);
    errObj.reset();
    
    // register db, legitimate
    std::string dbN1 = "rplante_PT1_2_u_pt12prod_im2000";
    std::string baseDir1 = "/u1/lsst/qserv/worker/exportDir";
    if ( !m.registerQservedDb(dbN1, baseDir1, *qmwSqlConn, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    /*
    // try to register db, bad path
    std::string dbN2 = "rplante_PT1_2_u_pt12prod_im3000";
    std::string baseDir2 = "/u124/nonExist/exportDir";
    if ( m.registerQservedDb(dbN2, baseDir2, *qmwSqlConn, errObj) ) {
        BOOST_FAIL("This should fail because of bad baseDir");
    }
    // try to register already registered db
    if ( m.registerQservedDb(dbN1, baseDir1, *qmwSqlConn, errObj) ) {
        BOOST_FAIL("This should fail (already registered)");
    }
    // clean up
    if ( !m.destroyWorkerMetadata(*qmwSqlConn, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    */
}

/*
BOOST_AUTO_TEST_CASE(PathCreate) {
    std::string workerId = "dummy123";
    lsst::qserv::worker::Metadata m(workerId);
    qsrv::SqlErrorObject errObj;

    std::string dbN1 = "qservTestSuite_aa";
    std::string pts1 = "Object, Source";

    qsrv::SqlErrorObject errObjDummy;
    qmwSqlConn->dropDb(dbN1, errObjDummy);
    qmwSqlConn->createDbAndSelect(dbN1, errObjDummy);
    qmwSqlConn->runQuery("create table Object_1234 (i int)", errObjDummy);
    qmwSqlConn->runQuery("create table Object_1235 (i int)", errObjDummy);
    qmwSqlConn->runQuery("create table Source_1234 (i int)", errObjDummy);
    qmwSqlConn->runQuery("create table Source_1235 (i int)", errObjDummy);
    qmwSqlConn->runQuery("create table Exposure_99 (i int)", errObjDummy);
    
    if ( !m.registerQservedDb(dbN1, pts1, *qmwSqlConn, errObj) ) {
        qmwSqlConn->dropDb(dbN1, errObjDummy);
        qmwSqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL(errObj.printErrMsg());
    }
    std::vector<std::string> exportPaths;
    if ( !m.generateExportPaths("/u1/lsst/qserv/worker/exportDir", 
                                *qmwSqlConn, errObj, exportPaths)) {
        qmwSqlConn->dropDb(dbN1, errObjDummy);
        qmwSqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
        BOOST_FAIL(errObj.printErrMsg());
    }
    int i, s = exportPaths.size();
    for (i=0; i<s ; i++) {
        std::cout << "got: " << exportPaths[i] << std::endl;
    }
    qmwSqlConn->dropDb(dbN1, errObjDummy);
    qmwSqlConn->dropDb("qserv_worker_meta_"+workerId, errObjDummy);
}
*/

BOOST_AUTO_TEST_SUITE_END()
