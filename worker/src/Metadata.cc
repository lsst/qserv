/* 
 * LSST Data Management System
 * Copyright 2008 - 2012 LSST Corporation.
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

#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/worker/Metadata.h"

#include "mysql/mysql.h"

using lsst::qserv::SqlConfig;
using lsst::qserv::SqlConnection;

namespace qWorker = lsst::qserv::worker;

// to check if table exists:
// SELECT COUNT(*) FROM information_schema.tables 
// WHERE table_schema = 'dbName' AND table_name = 'tableName'"


qWorker::Metadata::Metadata(int workerId) 
    : _metadataDbName("qserv_worker_meta_" + workerId) {

    SqlConfig sc;
    sc.hostname = "";
    sc.username = "dummy";
    sc.password = "";
    sc.dbName = "";
    sc.port = 0;
    sc.socket = "dummy";

    _sqlConn.reset(new SqlConnection(sc, false));
}

/// called ones for each new database that this worker should serve
/// partitionedTables is a comma separated list of tables that are
/// partitioned.
bool
qWorker::Metadata::registerQservedDb(std::string const& dbName,
                                     std::string const& partitionedTables) {

    if ( !_sqlConn->dbExists(_metadataDbName, _errObj) ) {
        if ( !_sqlConn->createDb(_metadataDbName, _errObj) ) {
            return false;
        }
    }
    if ( !_sqlConn->selectDb(_metadataDbName, _errObj) ) {
        return false;
    }
    // check if table "Dbs" exists, create if it does not
    if ( !_sqlConn->tableExists("Dbs", _errObj)) {
        //runQuery("CREATE TABLE Dbs (hash INT, dbName VARCHAR, partitionedTables VARCHAR)");
    } else {
        // check if db is not already registered
        //runQuery("SELECT COUNT(*) FROM 'Dbs' WHERE dbName='" + dbName + "'");
        // fail if the result is not equal 1
    }
    
    // register the new database this worker should serve
    int hash = 123; //computeHash(dbName);
    //runQuery("INSERT INTO Dbs(hash,dbName) VALUES("+hash+","
    //         +dbName+","+partitionedTables+")");

    return true; // success
}

/// creates directories: <baseDir>/<dbName><chunkNumber>
/// for every chunk in every database served
bool 
qWorker::Metadata::createExportDirs(std::string const& baseDir) {
    if ( !_sqlConn->dbExists(_metadataDbName, _errObj) ) {
        return false;
    }
    if (! _sqlConn->selectDb(_metadataDbName, _errObj) ) {
        return false;
    }
    /*dbsArray = runQuery("SELECT dbName FROM Dbs");
    for (dInfo in dbsArray) {
        dbName = dInfo[0];
        partitionedTables = dInfo[1];
        status = createExportDirsForDb(baseDir, dbName, partitionedTables);
        if ( 0 != status ) {
            return status;
        }
        }*/
    return true; // success
}

bool
qWorker::Metadata::createExportDirsForDb(std::string const& baseDir,
                                         std::string const& dbName,
                                         std::string const& partitionedTables){
    if ( ! _sqlConn->selectDb(dbName, _errObj) ) {
        return false;
    }
    /*for (each tableName in partitionedTables) {
        chunkIds = runQuery("SELECT table_name " 
                            + "FROM information_schema.tables " 
                            + "WHERE table_schema = '" + d 
                            + " AND table_name LIKE '" + tableName + "_%'");
        for ( each chunkId in chunkIds) {
            buildPath(baseDir, dbName, chunkId);
        }
    }*/
    return true; // success
}
