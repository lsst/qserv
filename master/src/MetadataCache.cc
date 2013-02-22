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
    _nStripes(0),
    _nSubStripes(0),
    _defOverlapF(0),
    _defOverlapNN(0) {
}

qMaster::MetadataCache::DbInfo::DbInfo(int nStripes, int nSubStripes,
                                       float defOverlapF, float defOverlapNN) :
    _isPartitioned(true),
    _nStripes(nStripes),
    _nSubStripes(nSubStripes),
    _defOverlapF(defOverlapF),
    _defOverlapNN(defOverlapNN) {
}

qMaster::MetadataCache::TableInfo::TableInfo() :
    _isPartitioned(false),
    _overlap(0),
    _phiCol(""),
    _thetaCol(""),
    _logicalPart(0),
    _physPart(0) {
}

qMaster::MetadataCache::TableInfo::TableInfo(float overlap, 
                                             std::string const& phiCol,
                                             std::string const& thetaCol,
                                             int logicalPart,
                                             int physPart) :
    _isPartitioned(true),
    _overlap(overlap),
    _phiCol(phiCol),
    _thetaCol(thetaCol),
    _logicalPart(logicalPart),
    _physPart(physPart) {
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
qMaster::MetadataCache::addDbInfoPartitioned(std::string const& dbName,
                                             int nStripes,
                                             int nSubStripes,
                                             float defOverlapF,
                                             float defOverlapNN) {
    if (containsDb(dbName)) {
        return -1; // the dbInfo already exists
    }
    DbInfo dbInfo = DbInfo(nStripes, nSubStripes, defOverlapF, defOverlapNN);
    _dbs.insert(std::pair<std::string, DbInfo> (dbName, dbInfo));
    return 0; // success
}

void
qMaster::MetadataCache::printSelf() const {
    std::map<std::string, DbInfo>::const_iterator itr;
    for ( ; itr!= _dbs.end() ; ++itr) {
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
    for ( ; itr!= dbInfo._tables.end(); ++itr) {
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
          << ", logPart=" << tableInfo.getLogicalPart()
          << ", physPart=" << tableInfo.getPhysPart() << ").\n";
    } else {
        s << "is not partitioned.\n";
    }
    return s;
}
