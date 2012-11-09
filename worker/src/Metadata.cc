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

#include "lsst/qserv/QservPath.hh"
#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/SqlErrorObject.hh"
#include "lsst/qserv/worker/Metadata.h"
#include "lsst/qserv/worker/Base.h"

#include <iostream>
#include <sstream>
#include <string>

namespace qWorker = lsst::qserv::worker;

using lsst::qserv::SqlConnection;
using lsst::qserv::SqlErrorObject;
using lsst::qserv::SqlResults;
using qWorker::hashQuery;

using std::cout;
using std::endl;
using std::string;
using std::stringstream;

// Constant. Long term, this should be defined differently
const int DUMMYEMPTYCHUNKID = 1234567890;


qWorker::Metadata::Metadata(SqlConfig const& qmsConnCfg) {
    _qmsConnCfg = new SqlConfig(qmsConnCfg);
    _workerMetadataDbName = qmsConnCfg.dbName;
    if ( 0 != _workerMetadataDbName.compare(0, 4, "qms_")) {
        stringstream s;
        s << "Unexpected qms metadata database name: '"
          << _workerMetadataDbName << "', should start with 'qms_'";
        throw s.str();
    }
    _workerMetadataDbName[2] = 'w'; // 'qms_' --> 'qmw_'
}

qWorker::Metadata::~Metadata() {
    delete _qmsConnCfg;
}

bool
qWorker::Metadata::installMeta(std::string const& exportDir,
                               SqlConnection& sqlConn,
                               SqlErrorObject& errObj) {
    // create the metadata db and select it
    if (!sqlConn.createDbAndSelect(_workerMetadataDbName, errObj, true)) {
        return false;
    }
    // Create internal tables
    std::string sql = "CREATE TABLE Dbs (dbId INT NOT NULL PRIMARY KEY, dbName VARCHAR(255) NOT NULL, dbUuid VARCHAR(255) NOT NULL)";
    if (!sqlConn.runQuery(sql, errObj)) {
        return errObj.addErrMsg(string("installMeta failed.") +
                                "Sql command was: " + sql);
    }
    sql = "CREATE TABLE Internals (exportDir VARCHAR(255) NOT NULL)";
    if (!sqlConn.runQuery(sql, errObj)) {
        return errObj.addErrMsg(string("installMeta failed. ") +
                                "Sql command was: " + sql);
    }
    sql = "INSERT INTO Internals(exportDir) VALUES('" + exportDir + "')";
    if (!sqlConn.runQuery(sql, errObj)) {
        return errObj.addErrMsg(string("installMeta failed. ") +
                                "Sql command was: " + sql);
    }
    return true;
}

bool
qWorker::Metadata::destroyMeta(SqlConnection& sqlConn, 
                               SqlErrorObject& errObj) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        errObj.addErrMsg("No metadata found.");
        return false;
    }
    return sqlConn.dropDb(_workerMetadataDbName, errObj);
    // FIXME: remove export paths
}

/// called ones for each new database that this worker should serve
bool
qWorker::Metadata::registerQservedDb(std::string const& dbName,
                                     SqlConnection& sqlConn,
                                     SqlErrorObject& errObj) {
    // create the metadata db if it does not exist and select it
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return errObj.addErrMsg("Metadata is not initialized.");
    }
    if ( isRegistered(dbName, sqlConn, errObj) ) {
        std::stringstream s;
        s << "Database '" << dbName 
          << "' is already registered on this worker.";
        std::cout << s.str() << std::endl;
        return errObj.addErrMsg(s.str());
    }
    int dbId = 0;
    std::string dbUuid = "";
    if (!getDbInfoFromQms(dbName, dbId, dbUuid, errObj)) {
        return errObj.addErrMsg(std::string("Database '" + dbName + 
                                            "' is not registered in qms"));
    }       
    std::stringstream sql;
    sql << "INSERT INTO Dbs(dbId, dbName, dbUuid) VALUES (" << dbId 
        << ",'" << dbName << "','" << dbUuid << "')";
    return sqlConn.runQuery(sql.str(), errObj);
}

bool
qWorker::Metadata::unregisterQservedDb(std::string const& dbName,
                                       std::string& dbPathToDestroy,
                                       SqlConnection& sqlConn,
                                       SqlErrorObject& errObj) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return errObj.addErrMsg("Failed to connect to metadata db");
    }
    if ( !isRegistered(dbName, sqlConn, errObj) ) {
        return errObj.addErrMsg("Db " + dbName + " is not registered.");
    }
    std::stringstream sql;
    sql << "DELETE FROM Dbs WHERE dbName='" << dbName << "'";
    if ( !sqlConn.runQuery(sql.str(), errObj) ) {
        return false;
    }
    std::string exportDir = "dummy"; // FIXME!!!
    
    QservPath p;
    p.setAsCquery(dbName);
    std::stringstream ss;
    ss << exportDir << "/" << p.path();
    dbPathToDestroy = ss.str();
    return true;
}

bool
qWorker::Metadata::showMetadata(SqlConnection& sqlConn,
                                SqlErrorObject& errObj) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        cout << "No metadata found." << endl;
        return true;
    }
    std::string sql = "SELECT exportDir FROM Internals";
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to execute: " + sql);
    }
    std::string exportDir;
    if (!results.extractFirstValue(exportDir, errObj)) {
        return errObj.addErrMsg("Failed to fetch exportDir from metadata.");
    }
    sql = "SELECT dbId, dbName, dbUuid FROM Dbs";
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to execute: " + sql);
    }
    std::vector<std::string> col1;
    std::vector<std::string> col2;
    std::vector<std::string> col3;
    if (!results.extractFirst3Columns(col1, col2, col3, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    if (col1.size() == 0 ) {
        cout << "No databases registered in qserv metadata." << endl;
        return true;
    }
    cout << "export directory is: " << exportDir << endl;
    cout << "Databases registered in qserv metadata:" << endl;
    int i, s = col1.size();
    for (i=0; i<s ; i++) {
        cout << i+1 <<")  db:      " << col2[i] << "\n"
             << "    id:      " << col1[i] << "\n"
             << "    dbUuid:  " << col3[i] << endl;
    }
    return true;
}

/// generates export directory paths for every chunk in every database served
bool 
qWorker::Metadata::generateExportPaths(SqlConnection& sqlConn,
                                       SqlErrorObject& errObj,
                                       std::vector<std::string>& exportPaths) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return false;
    }
    std::string sql = "SELECT dbName FROM Dbs";
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to execute: " + sql);
    }
    std::vector<std::string> dbs;
    if (!results.extractFirstColumn(dbs, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    int i, s = dbs.size();
    for (i=0; i<s ; i++) {
        std::string dbName = dbs[i];
        /*
        std::string tableList = pts[i];
        if (!generateExportPathsForDb(exportDir, dbName, tableList, 
                                      sqlConn, errObj, exportPaths)) {
            std::stringstream ss;
            ss << "Failed to create export dir for exportDir="
               << exportDir << ", dbName=" << dbName << ", tableList=" 
               << tableList << std::endl;
            return errObj.addErrMsg(ss.str());
        }
        */
    }
    //return true;
    return false;
}

bool
qWorker::Metadata::generateExportPathsForDb(
                                   std::string const& exportDir,
                                   std::string const& dbName,
                                   SqlConnection& sqlConn,
                                   SqlErrorObject& errObj,
                                   std::vector<std::string>& exportPaths) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return false;
    }
    if ( !isRegistered(dbName, sqlConn, errObj) ) {
        return errObj.addErrMsg("Database: " + dbName + 
                                " is not registered in qserv metadata.");
    }
    /*
    std::string sql = "SELECT partitionedTables FROM Dbs WHERE dbName='"
                      + dbName + "'";
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Database: " + dbName + 
                                " not registered in qserv metadata.");
    }
    std::string pTables;
    if (!results.extractFirstValue(pTables, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    return generateExportPathsForDb(exportDir, dbName, pTables, 
                                    sqlConn, errObj, exportPaths);
    */
    return false;
}

bool
qWorker::Metadata::generateExportPathsForDb(
                             std::string const& exportDir,
                             std::string const& dbName,
                             std::vector<std::string const> const& pTables,
                             SqlConnection& sqlConn,
                             SqlErrorObject& errObj,
                             std::vector<std::string>& exportPaths) {
    /*
    int i, s = pTables.size();
    for (i=0 ; i<s ; i++) {
        std::vector<std::string> t;
        if (!sqlConn.listTables(t, errObj, pTables[i]+"_", dbName)) {
            std::stringstream ss;
            ss << "Failed to list tables for db=" << dbName
               << ", prefix=" << pTables[i] << "\n";
            return errObj.addErrMsg(ss.str());
        }
        int j, s2 = t.size();
        if ( s2 == 0 ) {
            std::stringstream ss;
            ss << "WARNING: no partitioned tables with prefix '"
               << pTables[i] << "_' found in the database '"
               << dbName << "'. Did you forget to load the data?\n";
            std::cout << ss.str() << std::endl;
            // FIXME: is this an error?
            //return errObj.addErrMsg(ss.str());
        }        
        for (j=0; j<s2 ; j++) {
            addChunk(extractChunkNo(t[j]), exportDir, dbName, exportPaths);
        }
    } // end foreach t in pTables
    // Always create dummy chunk export regardless of tables. (#2048)
    addChunk(DUMMYEMPTYCHUNKID, exportDir, dbName, exportPaths);
    return true;
    */
    return false;
}

void
qWorker::Metadata::addChunk(int chunkNo, 
                            std::string const& exportDir,
                            std::string const& dbName,
                            std::vector<std::string>& exportPaths) {
    QservPath p;
    p.setAsCquery(dbName, chunkNo);
    std::stringstream ss;
    ss << exportDir << "/" << p.path() << std::ends;
    exportPaths.push_back(ss.str());
}

int
qWorker::Metadata::extractChunkNo(std::string const& str) {
    int s = str.size();
    std::string::size_type cursor = str.find_last_of('_', s);
    if ( cursor < 1 ) {
        return -1; // negative indicates an error    
    }
    int num;
    std::stringstream csm(str.substr(cursor+1, s));
    csm >> num;
    return num;
}

bool
qWorker::Metadata::isRegistered(std::string const& dbName,
                                SqlConnection& sqlConn,
                                SqlErrorObject& errObj) {
    std::string sql = "SELECT COUNT(*) FROM Dbs WHERE dbName='"+dbName+"'";
    
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return false;
    }
    std::string s;
    if (!results.extractFirstValue(s, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    return s[0] == '1';
}

bool
qWorker::Metadata::getDbInfoFromQms(std::string const& dbName,
                                    int& dbId, 
                                    std::string& dbUuid, 
                                    SqlErrorObject& errObj) {
    // FIXME: todo: contact qms and retrieve dbId and dbUuid for the
    // database called 'dbName'
    static int nextId = 100;
    dbId = ++nextId;
    dbUuid = "db-uuid-for-" + dbName;
    return true;
}
