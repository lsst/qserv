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
using std::vector;

// Constant. Long term, this should be defined differently
const int DUMMYEMPTYCHUNKID = 1234567890;

// ****************************************************************************
// ***** Metadata constructor(s) and destructor
// ****************************************************************************
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

// ****************************************************************************
// ***** installMeta
// ****************************************************************************
bool
qWorker::Metadata::installMeta(string const& exportBaseDir,
                               SqlConnection& sqlConn,
                               SqlErrorObject& errObj) {
    // create the metadata db and select it
    if (!sqlConn.createDbAndSelect(_workerMetadataDbName, errObj, true)) {
        return false;
    }
    // Create internal tables
    string sql = "CREATE TABLE Dbs (dbId INT NOT NULL PRIMARY KEY, dbName VARCHAR(255) NOT NULL, dbUuid VARCHAR(255) NOT NULL)";
    if (!sqlConn.runQuery(sql, errObj)) {
        return errObj.addErrMsg(string("installMeta failed.") +
                                "Sql command was: " + sql);
    }
    sql = "CREATE TABLE Internals (exportBaseDir VARCHAR(255) NOT NULL)";
    if (!sqlConn.runQuery(sql, errObj)) {
        return errObj.addErrMsg(string("installMeta failed. ") +
                                "Sql command was: " + sql);
    }
    sql = "INSERT INTO Internals(exportBaseDir) VALUES('"+exportBaseDir+"')";
    if (!sqlConn.runQuery(sql, errObj)) {
        return errObj.addErrMsg(string("installMeta failed. ") +
                                "Sql command was: " + sql);
    }
    return true;
}

// ****************************************************************************
// ***** destroyMeta
// ****************************************************************************
/// Destroys all metadata for a given worker
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

// ****************************************************************************
// ***** registerQservedDb
// ****************************************************************************
/// called ones for each new database that this worker should serve
bool
qWorker::Metadata::registerQservedDb(string const& dbName,
                                     SqlConnection& sqlConn,
                                     SqlErrorObject& errObj) {
    // create the metadata db if it does not exist and select it
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return errObj.addErrMsg("Metadata is not initialized.");
    }
    if ( isRegistered(dbName, sqlConn, errObj) ) {
        stringstream s;
        s << "Database '" << dbName 
          << "' is already registered on this worker.";
        cout << s.str() << endl;
        return errObj.addErrMsg(s.str());
    }
    int dbId = 0;
    string dbUuid = "";
    if (!getDbInfoFromQms(dbName, dbId, dbUuid, errObj)) {
        return errObj.addErrMsg(string("Database '" + dbName + 
                                            "' is not registered in qms"));
    }       
    stringstream sql;
    sql << "INSERT INTO Dbs(dbId, dbName, dbUuid) VALUES (" << dbId 
        << ",'" << dbName << "','" << dbUuid << "')";
    return sqlConn.runQuery(sql.str(), errObj);
}

// ****************************************************************************
// ***** unregisterQservedDb
// ****************************************************************************
bool
qWorker::Metadata::unregisterQservedDb(string const& dbName,
                                       string& dbPathToDestroy,
                                       SqlConnection& sqlConn,
                                       SqlErrorObject& errObj) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return errObj.addErrMsg("Failed to connect to metadata db");
    }
    if ( !isRegistered(dbName, sqlConn, errObj) ) {
        return errObj.addErrMsg("Db " + dbName + " is not registered.");
    }
    stringstream sql;
    sql << "DELETE FROM Dbs WHERE dbName='" << dbName << "'";
    if ( !sqlConn.runQuery(sql.str(), errObj) ) {
        return false;
    }
    string exportBaseDir;
    if ( !getExportBaseDir(exportBaseDir, sqlConn, errObj) ) {
        return false;
    }
    QservPath p;
    p.setAsCquery(dbName);
    stringstream ss;
    ss << exportBaseDir << "/" << p.path();
    dbPathToDestroy = ss.str();
    return true;
}

// ****************************************************************************
// ***** showMetadata
// ****************************************************************************
/// Simply prints all metadata to cout, useful for debugging.
bool
qWorker::Metadata::showMetadata(SqlConnection& sqlConn,
                                SqlErrorObject& errObj) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        cout << "No metadata found." << endl;
        return true;
    }
    // get/print exportBaseDir
    string exportBaseDir;
    if ( !getExportBaseDir(exportBaseDir, sqlConn, errObj) ) {
        return false;
    }
    cout << "Export base directory is: " << exportBaseDir << endl;
    // get/print info about all registered dbs
    vector<string> dbIds, dbNames, dbUuids;
    if (dbIds.size() == 0 ) {
        cout << "No databases registered in qserv metadata." << endl;
        return true;
    }
    cout << "Databases registered in qserv metadata:" << endl;
    int i, s = dbIds.size();
    for (i=0; i<s ; i++) {
        cout << i+1 <<")  db:      " << dbNames[i] << "\n"
             << "    id:      " << dbIds[i] << "\n"
             << "    dbUuid:  " << dbUuids[i] << endl;
    }
    return true;
}

// ****************************************************************************
// ***** generateExportPaths
// ****************************************************************************
/// generates export paths for every chunk in every database served
bool 
qWorker::Metadata::generateExportPaths(SqlConnection& sqlConn,
                                       SqlErrorObject& errObj,
                                       vector<string>& exportPaths) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return false;
    }
    string sql = "SELECT dbName FROM Dbs";
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to execute: " + sql);
    }
    vector<string> dbs;
    if (!results.extractFirstColumn(dbs, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    string exportBaseDir;
    if ( !getExportBaseDir(exportBaseDir, sqlConn, errObj) ) {
        return false;
    }
    int i, s = dbs.size();
    for (i=0; i<s ; i++) {
        string dbName = dbs[i];
        if (!generateExportPathsForDb(exportBaseDir, dbName, sqlConn, errObj,
                                      exportPaths)) {
            stringstream ss;
            ss << "Failed to create export paths. ExportBaseDir="
               << exportBaseDir << ", dbName=" << dbName << std::endl;
            return errObj.addErrMsg(ss.str());
        }
    }
    return true;
}

// ****************************************************************************
// ***** generateExportPathsForDb (public, exportBaseDir not passed)
// ****************************************************************************
bool
qWorker::Metadata::generateExportPathsForDb(
                                   string const& dbName,
                                   SqlConnection& sqlConn,
                                   SqlErrorObject& errObj,
                                   vector<string>& exportPaths) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return false;
    }
    string exportBaseDir;
    if ( !getExportBaseDir(exportBaseDir, sqlConn, errObj) ) {
        return false;
    }
    return generateExportPathsForDb(exportBaseDir, dbName, 
                                    sqlConn, errObj, exportPaths);
}

// ****************************************************************************
// ***** generateExportPathsForDb (private, exportBaseDir passed)
// ****************************************************************************
bool
qWorker::Metadata::generateExportPathsForDb(
                                   string const& exportBaseDir,
                                   string const& dbName,
                                   SqlConnection& sqlConn,
                                   SqlErrorObject& errObj,
                                   vector<string>& exportPaths) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return false;
    }
    if ( !isRegistered(dbName, sqlConn, errObj) ) {
        return errObj.addErrMsg("Database: " + dbName + " is not registered "
                                "in qserv worker metadata.");
    }
    vector<string> partTables;
    if ( !getPartTablesFromQms(dbName, partTables, errObj) ) {
        return false;
    }
    int i, s = partTables.size();
    for (i=0 ; i<s ; i++) {
        vector<string> t;
        if (!sqlConn.listTables(t, errObj, partTables[i]+"_", dbName)) {
            stringstream ss;
            ss << "Failed to list tables for db=" << dbName
               << ", prefix=" << partTables[i] << "\n";
            return errObj.addErrMsg(ss.str());
        }
        int j, s2 = t.size();
        if ( s2 == 0 ) {
            stringstream ss;
            ss << "WARNING: no partitioned tables with prefix '"
               << partTables[i] << "_' found in the database '"
               << dbName << "'. Did you forget to load the data?\n";
            cout << ss.str() << endl;
            // FIXME: is this an error?
            //return errObj.addErrMsg(ss.str());
        }        
        for (j=0; j<s2 ; j++) {
            addChunk(extractChunkNo(t[j]), exportBaseDir, dbName, exportPaths);
        }
    } // end foreach t in partTables
    // Always create dummy chunk export regardless of tables. (#2048)
    addChunk(DUMMYEMPTYCHUNKID, exportBaseDir, dbName, exportPaths);
    return true;
}

// ****************************************************************************
// ***** addChunk
// ****************************************************************************
void
qWorker::Metadata::addChunk(int chunkNo, 
                            string const& exportBaseDir,
                            string const& dbName,
                            vector<string>& exportPaths) {
    QservPath p;
    p.setAsCquery(dbName, chunkNo);
    stringstream ss;
    ss << exportBaseDir << "/" << p.path() << std::ends;
    exportPaths.push_back(ss.str());
}

// ****************************************************************************
// ***** extractChunkNo
// ****************************************************************************
int
qWorker::Metadata::extractChunkNo(string const& str) {
    int s = str.size();
    string::size_type cursor = str.find_last_of('_', s);
    if ( cursor < 1 ) {
        return -1; // negative indicates an error    
    }
    int num;
    stringstream csm(str.substr(cursor+1, s));
    csm >> num;
    return num;
}

// ****************************************************************************
// ***** isRegistered
// ****************************************************************************
bool
qWorker::Metadata::isRegistered(string const& dbName,
                                SqlConnection& sqlConn,
                                SqlErrorObject& errObj) {
    string sql = "SELECT COUNT(*) FROM Dbs WHERE dbName='"+dbName+"'";
    
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return false;
    }
    string s;
    if (!results.extractFirstValue(s, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    return s[0] == '1';
}

// ****************************************************************************
// ***** getExportBaseDir
// ****************************************************************************
/// Gets exportBaseDir for this worker.
bool
qWorker::Metadata::getExportBaseDir(string& exportBaseDir,
                                    SqlConnection& sqlConn,
                                    SqlErrorObject& errObj) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return false;
    }
    string sql = "SELECT exportBaseDir FROM Internals";
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to fetch exportBasetDir.");
    }
    if (!results.extractFirstValue(exportBaseDir, errObj)) {
        return errObj.addErrMsg("Failed to fetch exportBaseDir.");
    }
    return true;
}

// ****************************************************************************
// ***** getInfoAboutAllDbs
// ****************************************************************************
/// Gets all info about all qserved databases for this worker.
bool
qWorker::Metadata::getInfoAboutAllDbs(vector<string>& dbIds,
                                      vector<string>& dbNames,
                                      vector<string>& dbUuids,
                                      SqlConnection& sqlConn,
                                      SqlErrorObject& errObj) {
    if (!sqlConn.selectDb(_workerMetadataDbName, errObj)) {
        return false;
    }
    SqlResults results;
    string sql("SELECT dbId, dbName, dbUuid FROM Dbs");
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to execute: " + sql);
    }
    if (!results.extractFirst3Columns(dbIds, dbNames, dbUuids, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    return true;
}

// ****************************************************************************
// ***** getDbList
// ****************************************************************************
/// Gets the list of all qserved databases for the current worker.
/// Assumes that we are already connected to metadata db.
bool
qWorker::Metadata::getDbList(vector<string>& dbNames,
                             SqlConnection& sqlConn,
                             SqlErrorObject& errObj) {
    vector<string> v1, v2;
    return getInfoAboutAllDbs(v1, dbNames, v2, sqlConn, errObj);
}
    
// ****************************************************************************
// ***** getDbInfoFromQms
// ****************************************************************************
bool
qWorker::Metadata::getDbInfoFromQms(string const& dbName,
                                    int& dbId, 
                                    string& dbUuid, 
                                    SqlErrorObject& errObj) {
    // FIXME: todo: contact qms and retrieve dbId and dbUuid for the
    // database called 'dbName'
    static int nextId = 100;
    dbId = ++nextId;
    dbUuid = "db-uuid-for-" + dbName;
    return true;
}

// ****************************************************************************
// ***** getPartTablesFromQms
// ****************************************************************************
bool 
qWorker::Metadata::getPartTablesFromQms(string const& dbName,
                                        vector<string>& partTables,
                                        SqlErrorObject& errObj) {
    // FIXME: todo: contact qms and retrieve partTables for dbName
    partTables.push_back("Object");
    return true;
}
