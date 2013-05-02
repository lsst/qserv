// -*- LSST-C++ -*-

/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/**
  * @file MetadataCache.cc
  *
  * @brief Transient metadata structure for qserv. 
  *
  * @Author Jacek Becla, SLAC
  */

#include "lsst/qserv/master/MetadataCache.h"

namespace qMaster = lsst::qserv::master;

/** Constructs object representing a non-partitioned database.
  */
qMaster::MetadataCache::DbInfo::DbInfo() :
    _isPartitioned(false),
    _nStripes(-1),
    _nSubStripes(-1),
    _defOverlapF(-1),
    _defOverlapNN(-1) {
}

/** Constructs object representing a partitioned database
  * which use spherical partitioning mode.
  *
  * @param nStripes number of stripes
  * @param nSubStripes number of sub-stripes
  * @param defOverlapF default overlap for 'fuzziness'
  * @param defOverlapNN default overlap for 'near-neighbor'-type queries
  */
qMaster::MetadataCache::DbInfo::DbInfo(int nStripes, int nSubStripes,
                                       float defOverlapF, float defOverlapNN) :
    _isPartitioned(true),
    _nStripes(nStripes),
    _nSubStripes(nSubStripes),
    _defOverlapF(defOverlapF),
    _defOverlapNN(defOverlapNN) {
}

/** Adds information about a non-partitioned table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::DbInfo::addTable(std::string const& tbName, const TableInfo& tbInfo) {
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tbName);
    if (itr != _tables.end()) {
        return MetadataCache::STATUS_ERR_TABLE_EXISTS;
    }
    _tables.insert(std::pair<std::string, TableInfo> (tbName, tbInfo));
    return MetadataCache::STATUS_OK;
}

/** Checks if a given table is registered in the qserv metadata.
  *
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::DbInfo::checkIfContainsTable(std::string const& tableName) const {
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tableName);
    return itr != _tables.end();
}

/** Checks if a given table is chunked
  *
  * @param tableName table name
  *
  * @return returns true or false (false if the table does not exist)
  */
bool
qMaster::MetadataCache::DbInfo::checkIfTableIsChunked(std::string const& tableName) const {
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tableName);
    if (itr == _tables.end()) {
        return false;
    }
    return itr->second.getIsPartitioned();
}

/** Checks if a given table is subchunked
  *
  * @param tableName table name
  *
  * @return returns true or false (false if the table does not exist)
  */
bool
qMaster::MetadataCache::DbInfo::checkIfTableIsSubChunked(std::string const& tableName) const {
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tableName);
    if (itr == _tables.end()) {
        return false;
    }
    // why 2? See meta/python/lsst/qserv/meta/metaImpl.py, 
    // schema for PS_Tb_sphBox, explaination of bits for logicalPart
    return 2 == itr->second.getLogicalPart();
}

/** Gets chunked tables
  *
  * @return returns a vector of table names that are chunked
  */
std::vector<std::string>
qMaster::MetadataCache::DbInfo::getChunkedTables() const {
    std::vector<std::string> v;
    std::map<std::string, qMaster::MetadataCache::TableInfo>::const_iterator itr;
    for (itr=_tables.begin() ; itr!=_tables.end(); ++itr) {
        if (checkIfTableIsChunked(itr->first)) {
            v.push_back(itr->first);
        }
    }
    return v;
}

/** Gets a list of subchunked tables
  *
  * @return returns a vector of table names that are subchunked
  */
std::vector<std::string>
qMaster::MetadataCache::DbInfo::getSubChunkedTables() const {
    std::vector<std::string> v;
    std::map<std::string, qMaster::MetadataCache::TableInfo>::const_iterator itr;
    for (itr=_tables.begin() ; itr!=_tables.end(); ++itr) {
        if (checkIfTableIsSubChunked(itr->first)) {
            v.push_back(itr->first);
        }
    }
    return v;
}

/** Gets names of partition columns (ra, decl, objectId) for a given table.
  *
  * @return returns a 3-element vector with column names: ra, decl, objectId
  */
std::vector<std::string>
qMaster::MetadataCache::DbInfo::getPartitionCols(std::string const& tableName) const {
    std::vector<std::string> v;
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tableName);
    if (itr == _tables.end()) {
        v.push_back("");
        v.push_back("");
        v.push_back("");
        return v;
    }
    v.push_back(itr->second.getPhiCol());
    v.push_back(itr->second.getThetaCol());
    v.push_back(itr->second.getObjIdCol());
    return v;
}

/** Constructs object representing a non-partitioned table.
  */
qMaster::MetadataCache::TableInfo::TableInfo() :
    _isPartitioned(false),
    _overlap(-1),
    _phiCol("invalid"),
    _thetaCol("invalid"),
    _objIdCol("invalid"),
    _phiColNo(-1),
    _thetaColNo(-1),
    _objIdColNo(-1),
    _logicalPart(-1),
    _physChunking(-1) {
}

/** Constructs object representing a partitioned table
  * which use spherical partitioning mode.
  *
  * @param overlap used for this table (overwrites overlaps from dbInfo)
  * @param phiCol name of the phi column (right ascention)
  * @param thetaCol name of the theta column (declination)
  * @param objIdCol name of the objectId column
  * @param phiColNo position of the phi column in the table, counting from zero
  * @param thetaColNo position of the theta column in the table, counting from zero
  * @param objIdColNo position of the objectId column in the table, counting from zero
  * @param logicalPart definition how the table is partitioned logically
  * @param physChunking definition how the table is chunked physically
  */
qMaster::MetadataCache::TableInfo::TableInfo(float overlap, 
                                             std::string const& phiCol,
                                             std::string const& thetaCol,
                                             std::string const& objIdCol,
                                             int phiColNo,
                                             int thetaColNo,
                                             int objIdColNo,
                                             int logicalPart,
                                             int physChunking) :
    _isPartitioned(true),
    _overlap(overlap),
    _phiCol(phiCol),
    _thetaCol(thetaCol),
    _objIdCol(objIdCol),
    _phiColNo(phiColNo),
    _thetaColNo(thetaColNo),
    _objIdColNo(objIdColNo),
    _logicalPart(logicalPart),
    _physChunking(physChunking) {
}

/** Adds database information for a non-partitioned database.
  *
  * @param dbName database name
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addDbInfoNonPartitioned(std::string const& dbName) {
    if (checkIfContainsDb(dbName)) {
        return MetadataCache::STATUS_ERR_DB_EXISTS;
    }
    boost::lock_guard<boost::mutex> m(_mutex);
    _dbs.insert(std::pair<std::string, DbInfo> (dbName, DbInfo()));
    return MetadataCache::STATUS_OK;
}

/** Adds database information for a partitioned database,
  * which use spherical partitioning mode.
  *
  * @param dbName database name
  * @param nStripes number of stripes
  * @param nSubStripes number of sub-stripes
  * @param defOverlapF default overlap for 'fuzziness'
  * @param defOverlapNN default overlap for 'near-neighbor'-type queries
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addDbInfoPartitionedSphBox(std::string const& dbName,
                                                   int nStripes,
                                                   int nSubStripes,
                                                   float defOverlapF,
                                                   float defOverlapNN) {
    if (checkIfContainsDb(dbName)) {
        return MetadataCache::STATUS_ERR_DB_EXISTS;
    }
    DbInfo dbInfo(nStripes, nSubStripes, defOverlapF, defOverlapNN);
    boost::lock_guard<boost::mutex> m(_mutex);
    _dbs.insert(std::pair<std::string, DbInfo> (dbName, dbInfo));
    return MetadataCache::STATUS_OK;
}

/** Adds table information for a non-partitioned table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addTbInfoNonPartitioned(std::string const& dbName,
                                                std::string const& tbName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return MetadataCache::STATUS_ERR_DB_DOES_NOT_EXIST;
    }
    const qMaster::MetadataCache::TableInfo tInfo;
    return itr->second.addTable(tbName, tInfo);
}

/** Adds database information for a partitioned table,
  * which use spherical partitioning mode.
  *
  * @param dbName database name
  * @param tableName table name
  * @param overlap used for this table (overwrites overlaps from dbInfo)
  * @param phiCol name of the phi column (right ascention)
  * @param thetaCol name of the theta column (declination)
  * @param objIdCol name of the objId column
  * @param phiColNo position of the phi column in the table, counting from zero
  * @param thetaColNo position of the theta column in the table, counting from zero
  * @param objIdColNo position of the objId column in the table, counting from zero
  * @param logicalPart definition how the table is partitioned logically
  * @param physChunking definition how the table is chunked physically
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addTbInfoPartitionedSphBox(std::string const& dbName,
                                                   std::string const& tbName,
                                                   float overlap,
                                                   std::string const& phiCol,
                                                   std::string const& thetaCol,
                                                   std::string const& objIdCol,
                                                   int phiColNo,
                                                   int thetaColNo,
                                                   int objIdColNo,
                                                   int logicalPart,
                                                   int physChunking) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return MetadataCache::STATUS_ERR_DB_DOES_NOT_EXIST;
    }
    const qMaster::MetadataCache::TableInfo tInfo(
                          overlap, phiCol, thetaCol, objIdCol, phiColNo, 
                          thetaColNo, objIdColNo, logicalPart, physChunking);
    return itr->second.addTable(tbName, tInfo);
}

/** Checks if a given database is registered in the qserv metadata.
  *
  * @param dbName database name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::checkIfContainsDb(std::string const& dbName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    return itr != _dbs.end();
}

/** Checks if a given table is registered in the qserv metadata.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::checkIfContainsTable(std::string const& dbName,
                                             std::string const& tableName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return false;
    }
    return itr->second.checkIfContainsTable(tableName);
}

/** Checks if a given table is chunked
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::checkIfTableIsChunked(std::string const& dbName,
                                              std::string const& tableName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return false;
    }
    return itr->second.checkIfTableIsChunked(tableName);
}

/** Checks if a given table is subchunked
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::checkIfTableIsSubChunked(std::string const& dbName,
                                                 std::string const& tableName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return false;
    }
    return itr->second.checkIfTableIsSubChunked(tableName);
}

/** Gets allowed databases (database that are configured for qserv)
  *
  * @return returns a vector of database names that are configured for qserv
  */
std::vector<std::string>
qMaster::MetadataCache::getAllowedDbs() {
    std::vector<std::string> v;
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr;
    for (itr=_dbs.begin() ; itr!=_dbs.end(); ++itr) {
        v.push_back(itr->first);
    }
    return v;
}

/** Gets chunked tables
  *
  * @param dbName database name
  *
  * @return returns a vector of table names that are chunked
  */
std::vector<std::string>
qMaster::MetadataCache::getChunkedTables(std::string const& dbName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        std::vector<std::string> v;
        return v;
    }
    return itr->second.getChunkedTables();
}

/** Gets subchunked tables
  *
  * @param dbName database name
  *
  * @return returns a vector of table names that are subchunked
  */
std::vector<std::string>
qMaster::MetadataCache::getSubChunkedTables(std::string const& dbName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        std::vector<std::string> v;
        return v;
    }
    return itr->second.getSubChunkedTables();
}

/** Gets names of partition columns (ra, decl, objectId) for a given database/table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns a 3-element vector with column names: ra, decl, objectId
  */
std::vector<std::string>
qMaster::MetadataCache::getPartitionCols(std::string const& dbName,
                                         std::string const& tableName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        std::vector<std::string> v;
        v.push_back("");
        v.push_back("");
        v.push_back("");
        return v;
    }
    return itr->second.getPartitionCols(tableName);
}

/** Prints the contents of the qserv metadata cache. This is
  * handy for debugging.
  */
void
qMaster::MetadataCache::printSelf() {
    std::cout << "\n\nMetadata Cache in C++:" << std::endl;
    std::map<std::string, DbInfo>::const_iterator itr;
    boost::lock_guard<boost::mutex> m(_mutex);
    for (itr=_dbs.begin() ; itr!=_dbs.end() ; ++itr) {
        std::cout << "db: " << itr->first << ": " << itr->second << "\n";
    }
    std::cout << std::endl;
}

/** Operator<< for printing DbInfo object
  *
  * @param s the output stream
  * @param dbInfo DbInfo object
  *
  * @return returns the output stream
  */
std::ostream &
qMaster::operator<<(std::ostream &s, const qMaster::MetadataCache::DbInfo &dbInfo) {
    if (dbInfo.getIsPartitioned()) {
        s << "is partitioned (nStripes=" << dbInfo.getNStripes()
          << ", nSubStripes=" << dbInfo.getNSubStripes()
          << ", defOvF=" << dbInfo.getDefOverlapF()
          << ", defOvNN=" << dbInfo.getDefOverlapNN() << ").\n";
    } else {
        s << "is not partitioned.\n";
    }
    s << "  Tables:\n";
    std::map<std::string, qMaster::MetadataCache::TableInfo>::const_iterator itr;
    for (itr=dbInfo._tables.begin() ; itr!=dbInfo._tables.end(); ++itr) {
        s << "   " << itr->first << ": " << itr->second << "\n";
    }
    return s;
}

/** Operator<< for printing TableInfo object
  *
  * @param s the output stream
  * @param tableInfo TableInfo object
  *
  * @return returns the output stream
  */
std::ostream &
qMaster::operator<<(std::ostream &s, const qMaster::MetadataCache::TableInfo &tableInfo) {
    if (tableInfo.getIsPartitioned()) {
        s << "is partitioned (overlap=" << tableInfo.getOverlap()
          << ", phiCol=" << tableInfo.getPhiCol()
          << ", thetaCol=" << tableInfo.getThetaCol()
          << ", objIdCol=" << tableInfo.getObjIdCol()
          << ", phiColNo=" << tableInfo.getPhiColNo()
          << ", thetaColNo=" << tableInfo.getThetaColNo()
          << ", objIdColNo=" << tableInfo.getObjIdColNo()
          << ", logPart=" << tableInfo.getLogicalPart()
          << ", physChunking=" << tableInfo.getPhysChunking() << ").\n";
    } else {
        s << "is not partitioned.\n";
    }
    return s;
}
