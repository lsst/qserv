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

#include "lsst/qserv/worker/Metadata.h"

#include "mysql/mysql.h"

namespace qWorker = lsst::qserv::worker;

// to check if table exists:
// SELECT COUNT(*) FROM information_schema.tables 
// WHERE table_schema = 'dbName' AND table_name = 'tableName'"


qWorker::Metadata::Metadata(int workerId) :
    _metadataDbName("qserv_worker_meta_" + _workerId) {
};

/// called ones for each new database that this worker should serve
/// partitionedTables is a comma separated list of tables that are
/// partitioned.
/// Returns 0 on success
int
qWorker::Metadata::registerQservedDb(std::string const& dbName,
                                     std::string const& partitionedTables) {
    int status = 0;
    if ( !dbExists(_metadataDbName) ) {
        status = createDb(_metadataDbName);
        if ( 0 != status ) {
            return status;
        }
    }
    status = connectToDb(_metadataDbName);
    if ( 0 != status ) {
        return status;
    }
    // check if table "Dbs" exists, create if it does not
    if ( !tableExists("Dbs")) {
        runQuery("CREATE TABLE Dbs (hash INT, dbName VARCHAR, partitionedTables VARCHAR)");
    } else {
        // check if db is not already registered
        runQuery("SELECT COUNT(*) FROM 'Dbs' WHERE dbName='" + dbName + "'");
        // fail if the result is not equal 1
    }
    
    // register the new database this worker should serve
    int hash = computeHash(dbName);
    runQuery("INSERT INTO Dbs(hash,dbName) VALUES("+hash+","
             +dbName+","+partitionedTables+")");

    return 0; // success
};

/// creates directories: <baseDir>/<dbName><chunkNumber>
/// for every chunk in every database served
int 
qWorker::Metadata::createExportDirs(std::string const& baseDir) {
    if ( !dbExists(_metadataDbName) ) {
        // print error here
        return -1;
    }
    int status = connectToDb(_metadataDbName);
    if ( 0 != status ) {
        // print error here
        return status;
    }
    dbsArray = runQuery("SELECT dbName FROM Dbs");
    for (dInfo in dbsArray) {
        dbName = dInfo[0];
        partitionedTables = dInfo[1];
        status = createExportDirsForDb(baseDir, dbName, partitionedTables);
        if ( 0 != status ) {
            return status;
        }
    }
    return 0; // success
}

int 
qWorker::Metadata::createExportDirsForDb(std::string const& baseDir,
                                         std::string const& dbName,
                                         std::string const& partitionedTables){
    int status = connectToDb(dbName);
    if (0 != status) {
        // fail, served db should exist
        return -2;
    }
    for (each tableName in partitionedTables) {
        chunkIds = runQuery("SELECT table_name " 
                            + "FROM information_schema.tables " 
                            + "WHERE table_schema = '" + d 
                            + " AND table_name LIKE '" + tableName + "_%'");
        for ( each chunkId in chunkIds) {
            buildPath(baseDir, dbName, chunkId);
        }
    }
    return 0; // success
}
