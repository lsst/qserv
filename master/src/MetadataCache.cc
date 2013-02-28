// -*- LSST-C++ -*-

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

#include "lsst/qserv/master/MetadataCache.h"

namespace qMaster = lsst::qserv::master;

qMaster::MetadataCache::DbInfo::DbInfo() :
    _isPartitioned(false),
    _nStripes(-1),
    _nSubStripes(-1),
    _defOverlapF(-1),
    _defOverlapNN(-1) {
}

qMaster::MetadataCache::DbInfo::DbInfo(int nStripes, int nSubStripes,
                                       float defOverlapF, float defOverlapNN) :
    _isPartitioned(true),
    _nStripes(nStripes),
    _nSubStripes(nSubStripes),
    _defOverlapF(defOverlapF),
    _defOverlapNN(defOverlapNN) {
}

int
qMaster::MetadataCache::DbInfo::addTable(std::string const& tbName, const TableInfo& tbInfo) {
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tbName);
    if (itr != _tables.end()) {
        return -2; // the table is already there
    }
    _tables.insert(std::pair<std::string, TableInfo> (tbName, tbInfo));
    return 0;
}

qMaster::MetadataCache::TableInfo::TableInfo() :
    _isPartitioned(false),
    _overlap(-1),
    _phiCol("invalid"),
    _thetaCol("invalid"),
    _phiColNo(-1),
    _thetaColNo(-1),
    _logicalPart(-1),
    _physChunking(-1) {
}

qMaster::MetadataCache::TableInfo::TableInfo(float overlap, 
                                             std::string const& phiCol,
                                             std::string const& thetaCol,
                                             int phiColNo,
                                             int thetaColNo,
                                             int logicalPart,
                                             int physChunking) :
    _isPartitioned(true),
    _overlap(overlap),
    _phiCol(phiCol),
    _thetaCol(thetaCol),
    _phiColNo(phiColNo),
    _thetaColNo(thetaColNo),
    _logicalPart(logicalPart),
    _physChunking(physChunking) {
}

int
qMaster::MetadataCache::addDbInfoNonPartitioned(std::string const& dbName) {
    if (containsDb(dbName)) {
        return -1; // the dbInfo already exists
    }
    _dbs.insert(std::pair<std::string, DbInfo> (dbName, DbInfo()));
    return 0; // success
}

int
qMaster::MetadataCache::addDbInfoPartitionedSphBox(std::string const& dbName,
                                                   int nStripes,
                                                   int nSubStripes,
                                                   float defOverlapF,
                                                   float defOverlapNN) {
    if (containsDb(dbName)) {
        return -1; // the dbInfo already exists
    }
    DbInfo dbInfo(nStripes, nSubStripes, defOverlapF, defOverlapNN);
    _dbs.insert(std::pair<std::string, DbInfo> (dbName, dbInfo));
    return 0; // success
}

int
qMaster::MetadataCache::addTbInfoNonPartitioned(std::string const& dbName,
                                                std::string const& tbName) {

    std::map<std::string, DbInfo>::iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return -1; // the dbInfo does not exist
    }
    const qMaster::MetadataCache::TableInfo tInfo;
    return itr->second.addTable(tbName, tInfo);
}

int
qMaster::MetadataCache::addTbInfoPartitionedSphBox(std::string const& dbName, 
                                                   std::string const& tbName,
                                                   float overlap, 
                                                   std::string const& phiCol, 
                                                   std::string const& thetaCol, 
                                                   int phiColNo, 
                                                   int thetaColNo, 
                                                   int logicalPart, 
                                                   int physChunking) {
    std::map<std::string, DbInfo>::iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return -1; // the dbInfo does not exist
    }
    const qMaster::MetadataCache::TableInfo tInfo(
                          overlap, phiCol, thetaCol, phiColNo, 
                          thetaColNo, logicalPart, physChunking);
    return itr->second.addTable(tbName, tInfo);
}

void
qMaster::MetadataCache::resetSelf() {
    _dbs.clear();
}

void
qMaster::MetadataCache::printSelf() const {
    std::cout << "\n\nMetadata Cache in C++:" << std::endl;
    std::map<std::string, DbInfo>::const_iterator itr;
    for (itr=_dbs.begin() ; itr!= _dbs.end() ; ++itr) {
        std::cout << "db: " << itr->first << ": " << itr->second << "\n";
    }
    std::cout << std::endl;
}

bool 
qMaster::MetadataCache::containsDb(std::string const& dbName) const {
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    return itr != _dbs.end();
}

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
    s << "  Tables:";
    std::map<std::string, qMaster::MetadataCache::TableInfo>::const_iterator itr;
    for (itr=dbInfo._tables.begin() ; itr!= dbInfo._tables.end(); ++itr) {
        s << "   " << itr->first << ": " << itr->second << "\n";
    }
    return s;
}

std::ostream &
qMaster::operator<<(std::ostream &s, const qMaster::MetadataCache::TableInfo &tableInfo) {
    if (tableInfo.getIsPartitioned()) {
        s << "is partitioned (overlap=" << tableInfo.getOverlap()
          << ", phiCol=" << tableInfo.getPhiCol()
          << ", thetaCol=" << tableInfo.getThetaCol()
          << ", phiColNo=" << tableInfo.getPhiColNo()
          << ", thetaColNo=" << tableInfo.getThetaColNo()
          << ", logPart=" << tableInfo.getLogicalPart()
          << ", physChunking=" << tableInfo.getPhysChunking() << ").\n";
    } else {
        s << "is not partitioned.\n";
    }
    return s;
}
