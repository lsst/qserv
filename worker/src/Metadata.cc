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
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/Metadata.h"
#include "lsst/qserv/worker/QservPathStructure.h"

#include <iostream>
#include <sstream>
#include <string>

namespace qWorker = lsst::qserv::worker;

using std::cout;
using std::endl;
using std::string;
using std::stringstream;
using std::vector;

// Constant. Long term, this should be defined differently
const int DUMMYEMPTYCHUNKID = 1234567890;

// ****************************************************************************
// ***** Metadata constructor(s), init, destructor, getError
// ****************************************************************************
qWorker::Metadata::Metadata() : _qmsConnCfg(0) {
}

bool
qWorker::Metadata::init(SqlConfig const& qmsConnCfg, 
                        SqlConfig const& qmwConnCfg) {
    _qmsConnCfg = new SqlConfig(qmsConnCfg);
    _workerMetadataDbName = qmsConnCfg.dbName;
    if ( 0 != _workerMetadataDbName.compare(0, 4, "qms_")) {
        std::stringstream ss;
        ss << "Unexpected qms metadata database name: '"
           << _workerMetadataDbName << "', should start with 'qms_'";
        return _errObj.addErrMsg(ss.str());
    }
    _workerMetadataDbName[2] = 'w'; // 'qms_' --> 'qmw_'
    _sqlConn.init(qmwConnCfg);
    return true;
}

qWorker::Metadata::~Metadata() {
    delete _qmsConnCfg;
}

string
qWorker::Metadata::getLastError() const {
    return _errObj.errMsg();
}

// ****************************************************************************
// ***** installMeta
// ****************************************************************************
bool
qWorker::Metadata::installMeta(string const& exportBaseDir) {
    // create the metadata db and select it
    if (!_sqlConn.createDbAndSelect(_workerMetadataDbName, _errObj, true)) {
        return false;
    }
    // Create internal tables
    string sql = "CREATE TABLE Dbs (dbId INT NOT NULL PRIMARY KEY, dbName VARCHAR(255) NOT NULL, dbUuid VARCHAR(255) NOT NULL)";
    if (!_sqlConn.runQuery(sql, _errObj)) {
        return _errObj.addErrMsg(string("installMeta failed.") +
                                "Sql command was: " + sql);
    }
    sql = "CREATE TABLE Internals (exportBaseDir VARCHAR(255) NOT NULL)";
    if (!_sqlConn.runQuery(sql, _errObj)) {
        return _errObj.addErrMsg(string("installMeta failed. ") +
                                "Sql command was: " + sql);
    }
    sql = "INSERT INTO Internals(exportBaseDir) VALUES('"+exportBaseDir+"')";
    if (!_sqlConn.runQuery(sql, _errObj)) {
        return _errObj.addErrMsg(string("installMeta failed. ") +
                                "Sql command was: " + sql);
    }
    return true;
}

// ****************************************************************************
// ***** destroyMeta
// ****************************************************************************
/// Destroys all metadata for a given worker
bool
qWorker::Metadata::destroyMeta() {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        _errObj.addErrMsg("No metadata found.");
        return false;
    }
    if (!_destroyExportPathWithPrefix()) {
        return false;
    }       
    return _sqlConn.dropDb(_workerMetadataDbName, _errObj);
}

// ****************************************************************************
// ***** registerQservedDb
// ****************************************************************************
/// called ones for each new database that this worker should serve
bool
qWorker::Metadata::registerQservedDb(string const& dbName) {
    // create the metadata db if it does not exist and select it
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        return _errObj.addErrMsg("Metadata is not initialized.");
    }
    if ( _isRegistered(dbName) ) {
        stringstream ss;
        ss << "Database '" << dbName 
           << "' is already registered on this worker.";
        return _errObj.addErrMsg(ss.str());
    }
    int dbId = 0;
    string dbUuid = "";
    if (!_getDbInfoFromQms(dbName, dbId, dbUuid)) {
        return _errObj.addErrMsg(string("Database '" + dbName + 
                                       "' is not registered in qms"));
    }       
    stringstream sql;
    sql << "INSERT INTO Dbs(dbId, dbName, dbUuid) VALUES (" << dbId 
        << ",'" << dbName << "','" << dbUuid << "')";
    return _sqlConn.runQuery(sql.str(), _errObj);
}

// ****************************************************************************
// ***** unregisterQservedDb
// ****************************************************************************
bool
qWorker::Metadata::unregisterQservedDb(string const& dbName) {
    if (!_unregisterQservedDb(dbName)) {
        return false;
    }
    return _destroyExportPath4Db(dbName);
}

// ****************************************************************************
// ***** createExportPaths
// ****************************************************************************
/// If dbName is not set, it creates paths for all databases
bool 
qWorker::Metadata::createExportPaths(std::string const& dbName) {
    vector<string> exportPaths;
    if (dbName == "") {
        if ( !_generateExportPaths(exportPaths) ) {
            return false;
        }
    } else {
        if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
            return false;
        }
        string exportBaseDir;
        if ( !_getExportBaseDir(exportBaseDir) ) {
            return false;
        }
        if (!_generateExportPathsForDb(exportBaseDir, dbName, exportPaths)) {
            return false;
        }
    }
    QservPathStructure p;
    if ( !p.insert(exportPaths) ) {
        return _errObj.addErrMsg(string("Failed to insert export paths."));
    }
    if ( !p.persist() ) {
        return _errObj.addErrMsg(string("Failed to persist export paths."));
    }
    return true;
}

// ****************************************************************************
// ***** _getExportPathWithPrefix
// ****************************************************************************
/// Sets the "thePath" to something like <exportBasePath>/q
bool
qWorker::Metadata::_getExportPathWithPrefix(string& thePath) {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        return _errObj.addErrMsg("Failed to connect to metadata db");
    }
    string exportBaseDir;
    if ( !_getExportBaseDir(exportBaseDir) ) {
        return false;
    }
    thePath = exportBaseDir + "/" + QservPath::prefix(QservPath::CQUERY);
    return true;
}        

// ****************************************************************************
// ***** showMetadata
// ****************************************************************************
/// Simply prints all metadata to cout, useful for debugging.
bool
qWorker::Metadata::showMetadata() {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        cout << "No metadata found." << endl;
        return true;
    }
    // get/print exportBaseDir
    string exportBaseDir;
    if ( !_getExportBaseDir(exportBaseDir) ) {
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
// ***** _generateExportPaths
// ****************************************************************************
/// generates export paths for every chunk in every database served
bool 
qWorker::Metadata::_generateExportPaths(vector<string>& exportPaths) {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        return false;
    }
    string sql = "SELECT dbName FROM Dbs";
    SqlResults results;
    if (!_sqlConn.runQuery(sql, results, _errObj)) {
        return _errObj.addErrMsg("Failed to execute: " + sql);
    }
    vector<string> dbs;
    if (!results.extractFirstColumn(dbs, _errObj)) {
        return _errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    string exportBaseDir;
    if ( !_getExportBaseDir(exportBaseDir) ) {
        return false;
    }
    int i, s = dbs.size();
    for (i=0; i<s ; i++) {
        string dbName = dbs[i];
        if (!_generateExportPathsForDb(exportBaseDir, dbName, exportPaths)) {
            stringstream ss;
            ss << "Failed to create export paths. ExportBaseDir="
               << exportBaseDir << ", dbName=" << dbName << std::endl;
            return _errObj.addErrMsg(ss.str());
        }
    }
    return true;
}

// ****************************************************************************
// ***** _generateExportPathsForDb
// ****************************************************************************
bool
qWorker::Metadata::_generateExportPathsForDb(string const& exportBaseDir,
                                             string const& dbName,
                                             vector<string>& exportPaths) {
    vector<TableChunks> allChunks;
    if (!_getTableChunksForDb(dbName, allChunks)) {
        return false;
    }
    vector<TableChunks>::const_iterator oneT;
    for (oneT=allChunks.begin(); oneT!=allChunks.end(); ++oneT) {
        if (0 == oneT->_chunksInDb.size()) {
            stringstream ss;
            ss << "WARNING: no partitioned tables with prefix '"
               << oneT->_tableName << "_' found in the database '"
               << dbName << "'. Did you forget to load the data?\n";
            cout << ss.str() << endl;
        }
        vector<string>::const_iterator cItr;
        for (cItr=oneT->_chunksInDb.begin(); 
             cItr!=oneT->_chunksInDb.end(); 
             ++cItr) {
            _addChunk(_extractChunkNo(*cItr), exportBaseDir, dbName,
                      exportPaths);
        }
    } // end foreach t in partTables
    // Always create dummy chunk export regardless of tables. (#2048)
    _addChunk(DUMMYEMPTYCHUNKID, exportBaseDir, dbName, exportPaths);
    return true;
}

// ****************************************************************************
// ***** _getTableChunksForDb
// ****************************************************************************
/// Retrieves from the database list of all chunks for a given table.
/// The format of retrieved info: vector of tuples: 
/// <tableName, <vector of chunk names> >
bool
qWorker::Metadata::_getTableChunksForDb(string const& dbName,
                                        vector<TableChunks>& allChunks) {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        return false;
    }
    if ( !_isRegistered(dbName) ) {
        return _errObj.addErrMsg("Database: " + dbName + " is not registered "
                                "in qserv worker metadata.");
    }
    vector<string> partTables;
    if ( !_getPartTablesFromQms(dbName, partTables) ) {
        return false;
    }
    vector<string>::const_iterator itr;
    for (itr=partTables.begin(); itr!=partTables.end(); ++itr) {
        TableChunks tc;
        tc._tableName = *itr;
        if (!_sqlConn.listTables(tc._chunksInDb, _errObj, 
                                tc._tableName + "_", dbName)) {
            stringstream ss;
            ss << "Failed to list tables for db=" << dbName
               << ", prefix=" << *itr << "\n";
            allChunks.clear();
            return _errObj.addErrMsg(ss.str());
        }
        allChunks.push_back(tc);
    }
    return true;
}

// ****************************************************************************
// ***** addChunk
// ****************************************************************************
void
qWorker::Metadata::_addChunk(int chunkNo, 
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
// ***** _unregisterQservedDb
// ****************************************************************************
bool
qWorker::Metadata::_unregisterQservedDb(string const& dbName) {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        return _errObj.addErrMsg("Failed to connect to metadata db");
    }
    if ( !_isRegistered(dbName) ) {
        return _errObj.addErrMsg("Db " + dbName + " is not registered.");
    }
    stringstream sql;
    sql << "DELETE FROM Dbs WHERE dbName='" << dbName << "'";
    if ( !_sqlConn.runQuery(sql.str(), _errObj) ) {
        return false;
    }
    return true;
}

// ****************************************************************************
// ***** _destroyExportPathWithPrefix
// ****************************************************************************
/// Unconditionally destroys export path for given qms, without checking if it
/// matches the chunks in the database
bool
qWorker::Metadata::_destroyExportPathWithPrefix() {
    string p2d;
    if (!_getExportPathWithPrefix(p2d)) {
        return false;
    }
    QservPathStructure::destroy(p2d);
    return true;
}

// ****************************************************************************
// ***** _destroyExportPath4Db
// ****************************************************************************
bool
qWorker::Metadata::_destroyExportPath4Db(string const& dbName) {
    string exportBaseDir;
    if ( !_getExportBaseDir(exportBaseDir) ) {
        return false;
    }
    QservPath p;
    p.setAsCquery(dbName);
    stringstream ss;
    ss << exportBaseDir << "/" << p.path();
    QservPathStructure::destroy(ss.str());
    return true;
}

// ****************************************************************************
// ***** extractChunkNo
// ****************************************************************************
int
qWorker::Metadata::_extractChunkNo(string const& str) {
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
// ***** _isRegistered
// ****************************************************************************
bool
qWorker::Metadata::_isRegistered(string const& dbName) {
    string sql = "SELECT COUNT(*) FROM Dbs WHERE dbName='" + dbName + "'";
    
    SqlResults results;
    if (!_sqlConn.runQuery(sql, results, _errObj)) {
        return false;
    }
    string s;
    if (!results.extractFirstValue(s, _errObj)) {
        return _errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    return s[0] == '1';
}

// ****************************************************************************
// ***** _getExportBaseDir
// ****************************************************************************
/// Gets exportBaseDir for this worker.
bool
qWorker::Metadata::_getExportBaseDir(string& exportBaseDir) {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        return false;
    }
    string sql = "SELECT exportBaseDir FROM Internals";
    SqlResults results;
    if (!_sqlConn.runQuery(sql, results, _errObj)) {
        return _errObj.addErrMsg("Failed to fetch exportBasetDir.");
    }
    if (!results.extractFirstValue(exportBaseDir, _errObj)) {
        return _errObj.addErrMsg("Failed to fetch exportBaseDir.");
    }
    return true;
}

// ****************************************************************************
// ***** _getInfoAboutAllDbs
// ****************************************************************************
/// Gets all info about all qserved databases for this worker.
bool
qWorker::Metadata::_getInfoAboutAllDbs(vector<string>& dbIds,
                                       vector<string>& dbNames,
                                       vector<string>& dbUuids) {
    if (!_sqlConn.selectDb(_workerMetadataDbName, _errObj)) {
        return false;
    }
    SqlResults results;
    string sql("SELECT dbId, dbName, dbUuid FROM Dbs");
    if (!_sqlConn.runQuery(sql, results, _errObj)) {
        return _errObj.addErrMsg("Failed to execute: " + sql);
    }
    if (!results.extractFirst3Columns(dbIds, dbNames, dbUuids, _errObj)) {
        return _errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    return true;
}

// ****************************************************************************
// ***** getDbList
// ****************************************************************************
/// Gets the list of all qserved databases for the current worker.
/// Assumes that we are already connected to metadata db.
bool
qWorker::Metadata::getDbList(vector<string>& dbNames) {
    vector<string> v1, v2;
    return _getInfoAboutAllDbs(v1, dbNames, v2);
}
    
// ****************************************************************************
// ***** _getDbInfoFromQms
// ****************************************************************************
bool
qWorker::Metadata::_getDbInfoFromQms(string const& dbName,
                                     int& dbId, 
                                     string& dbUuid) {
    // FIXME: todo: contact qms and retrieve dbId and dbUuid for the
    // database called 'dbName'
    static int nextId = 100;
    dbId = ++nextId;
    dbUuid = "db-uuid-for-" + dbName;
    return true;
}

// ****************************************************************************
// ***** _getPartTablesFromQms
// ****************************************************************************
bool 
qWorker::Metadata::_getPartTablesFromQms(string const& dbName,
                                         vector<string>& partTables) {
    // FIXME: todo: contact qms and retrieve partTables for dbName
    partTables.push_back("Object");
    return true;
}
