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
    // start clean
    m.destroyWorkerMetadata(*qmwSqlConn, errObj);
    errObj.reset();    
    // register db 1
    std::string dbN1 = "Summer2012";
    std::string baseDirGood = "/u1/lsst/qserv/worker/exportDir";
    if ( !m.registerQservedDb(dbN1, baseDirGood, *qmwSqlConn, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // try to register already registered db (should return error)
    if ( m.registerQservedDb(dbN1, baseDirGood, *qmwSqlConn, errObj) ) {
        BOOST_FAIL("This should fail (already registered)");
    } else {
        errObj.reset();
    }
    // register db 2
    std::string dbN2 = "Winter2013";
    if ( !m.registerQservedDb(dbN2, baseDirGood, *qmwSqlConn, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // show metadata
    if ( !m.showMetadata(*qmwSqlConn, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // unregister already registered db
    std::string dbPathToDestroy;
    if ( !m.unregisterQservedDb(dbN1, dbPathToDestroy, *qmwSqlConn, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
    // unregister non-existing db (should return error)
    if ( m.unregisterQservedDb(dbN1, dbPathToDestroy, *qmwSqlConn, errObj) ) {
        BOOST_FAIL("This should fail (nothing to unregister)");
    } else {
        errObj.reset();
    }
    // clean up
    if ( !m.destroyWorkerMetadata(*qmwSqlConn, errObj) ) {
        BOOST_FAIL(errObj.printErrMsg());
    }
}

BOOST_AUTO_TEST_CASE(PathCreate) {
    lsst::qserv::worker::Metadata m(qmsConnCfg);
    qsrv::SqlErrorObject errObj;
    // start clean
    m.destroyWorkerMetadata(*qmwSqlConn, errObj);
    errObj.reset();    

    // register db 1
    std::string dbN = "Summer2012";
    std::string baseDir = "/u1/lsst/qserv/worker/exportDir";
    if ( !m.registerQservedDb(dbN, baseDir, *qmwSqlConn, errObj) ) {
        m.destroyWorkerMetadata(*qmwSqlConn, errObj);
        BOOST_FAIL(errObj.printErrMsg());
    }
    qmwSqlConn->createDbAndSelect("qmw_"+dbN, errObj);
    qmwSqlConn->runQuery("create table Object_1234 (i int)", errObj);
    qmwSqlConn->runQuery("create table Object_1235 (i int)", errObj);
    qmwSqlConn->runQuery("create table Source_1234 (i int)", errObj);
    qmwSqlConn->runQuery("create table Source_1235 (i int)", errObj);
    qmwSqlConn->runQuery("create table Exposure_99 (i int)", errObj);
    
    /*std::vector<std::string> exportPaths;
    if ( !m.generateExportPaths(*qmwSqlConn, errObj, exportPaths)) {
        qmwSqlConn->dropDb("qmw_"+dbN, errObj);
        m.destroyWorkerMetadata(*qmwSqlConn, errObj);
        BOOST_FAIL(errObj.printErrMsg());
    }
    int i, s = exportPaths.size();
    for (i=0; i<s ; i++) {
        std::cout << "got: " << exportPaths[i] << std::endl;
    }
    */
    // final cleanup
    qmwSqlConn->dropDb("qmw_"+dbN, errObj);
    m.destroyWorkerMetadata(*qmwSqlConn, errObj);
}

BOOST_AUTO_TEST_SUITE_END()
