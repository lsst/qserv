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

namespace qWorker = lsst::qserv::worker;

using lsst::qserv::SqlConnection;
using lsst::qserv::SqlErrorObject;
using lsst::qserv::SqlResults;
using qWorker::hashQuery;

qWorker::Metadata::Metadata(std::string const& workerId) 
    : _metadataDbName("qserv_worker_meta_" + workerId) {
}

/// called ones for each new database that this worker should serve
/// partitionedTables is a comma separated list of tables that are
/// partitioned.
bool
qWorker::Metadata::registerQservedDb(std::string const& dbName,
                                     std::string const& partitionedTables,
                                     SqlConnection& sqlConn,
                                     SqlErrorObject& errObj) {
    std::string _partitionedTables = partitionedTables;
    if (!prepPartitionedTables(_partitionedTables, errObj)) {
        return false;
    }
    
    // create the metadata db if it does not exist and select it
    if (!sqlConn.createDbAndSelect(_metadataDbName, errObj, false)) {
        return errObj.addErrMsg("Failed to register qserved db " + dbName);
    }
    // check if table "Dbs" exists, create if it does not
    if (!sqlConn.tableExists("Dbs", errObj)) {
        std::string sql = 
            "CREATE TABLE Dbs (dbName VARCHAR(255), "
            "                  partitionedTables TEXT)"; // comma separated list
        if (!sqlConn.runQuery(sql, errObj)) {
            return errObj.addErrMsg(
                       std::string("Failed to register qserved db. ")
                       + "Sql command was: " + sql);
        }
    } else {
        // check if db is not already registered
        std::string sql = "SELECT COUNT(*) FROM Dbs WHERE dbName='"+dbName+"'";
        SqlResults results;
        if (!sqlConn.runQuery(sql, results, errObj)) {
            return false;
        }
        char n;
        if (!results.extractFirstValue(n, errObj)) {
            return errObj.addErrMsg("Failed to receive results from: " + sql);
        }
        if (n == '1') {
            return errObj.addErrMsg("Db " + dbName + " is already registered");
        }
    }
    std::stringstream sql;
    sql << "INSERT INTO Dbs(dbName, partitionedTables) "
        << "VALUES ('" << dbName << "', " 
        << "'" << _partitionedTables << "'" << ")" << std::endl;
    return sqlConn.runQuery(sql.str(), errObj);
}

/// generates export directory paths for every chunk in every database served
bool 
qWorker::Metadata::generateExportPaths(std::string const& baseDir,
                                       SqlConnection& sqlConn,
                                       SqlErrorObject& errObj,
                                       std::vector<std::string>& exportPaths) {
    if (!sqlConn.selectDb(_metadataDbName, errObj)) {
        return false;
    }
    std::string sql = "SELECT dbName, partitionedTables FROM Dbs";
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to execute: " + sql);
    }
    std::vector<std::string> dbs;
    std::vector<std::string> pts; // each string = comma separated list
    if (!results.extractFirst2Columns(dbs, pts, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    int i, s = dbs.size();
    for (i=0; i<s ; i++) {
        std::string dbName = dbs[i];
        std::string tableList = pts[i];
        if (!generateExportPathsForDb(baseDir, dbName, tableList, 
                                      sqlConn, errObj, exportPaths)) {
            std::stringstream ss;
            ss << "Failed to create export dir for baseDir="
               << baseDir << ", dbName=" << dbName << ", tableList=" 
               << tableList << std::endl;
            return errObj.addErrMsg(ss.str());
        }
    }
    return true;
}

bool
qWorker::Metadata::generateExportPathsForDb(
                                   std::string const& baseDir,
                                   std::string const& dbName,
                                   std::string const& tableList,
                                   SqlConnection& sqlConn,
                                   SqlErrorObject& errObj,
                                   std::vector<std::string>& exportPaths) {
    std::vector<std::string> pTables = tokenizeString(tableList);

    int i, s = pTables.size();
    for (i=0 ; i<s ; i++) {
        std::vector<std::string> t;
        if (!sqlConn.listTables(t, errObj, pTables[i]+"_", dbName)) {
            std::stringstream ss;
            ss << "Failed to list tables for db=" << dbName
               << ", prefix=" << pTables[i] << std::endl;
            return errObj.addErrMsg(ss.str());
        }
        int j, s2 = t.size();
        for (j=0; j<s2 ; j++) {
            int chunkNo = extractChunkNo(t[j]);
            QservPath p;
            p.setAsCquery(dbName, chunkNo);
            std::stringstream ss;
            ss << baseDir << "/" << p.path() << std::ends;
            exportPaths.push_back(ss.str());
        }
    }
    return true;
}

bool
qWorker::Metadata::prepPartitionedTables(std::string& strIn,
                                         SqlErrorObject& errObj) {
    std::string strOut;

    // remove extra spaces
    int i, s = strIn.size();
    for (i=0; i<s; i++) {
        char c = strIn[i];
        if (c==' ' || c=='\t') {
            continue;
        } else {
            strOut += c;
        }
    }
    // check if does not end with ','
    s = strOut.size();
    if (s > 1) {
        if ( strOut[strOut.size()-1] == ',' ) {
            std::stringstream ss;
            ss << "PartitionedTables list can't end with ','. "
               << "Full string was: '" << strIn << "'" << std::endl;
            return errObj.addErrMsg(ss.str());
        }
    }
    strIn = strOut;
    return true;
}

std::vector<std::string>
qWorker::Metadata::tokenizeString(std::string const& str) {
    std::vector<std::string> v;
    std::string token;
    int i, s = str.size();
    for (i=0; i<s; i++) {
        char c = str[i];
        if (c==' ' || c=='\t') {
            continue;
        } else if (c== ',') {
            v.push_back(token);
            token.clear();
        } else {
            token += c;
        }
    }
    v.push_back(token);
    return v;
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
