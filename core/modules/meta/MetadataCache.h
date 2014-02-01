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
  * @file MetadataCache.h
  *
  * @brief Transient metadata structure for qserv.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_META_METADATACACHE_H
#define LSST_QSERV_META_METADATACACHE_H

// Standard
#include <iostream>
#include <map>
#include <string>
#include <vector>

// Boost
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>     // for mutex

namespace lsst {
namespace qserv {
namespace master {

/** The class manages transient structure that contains metadata
 * information fetched from the qserv metadata server and used by qserv.
 */
class MetadataCache {
public:
    class DbInfo; // Forward.
    enum { STATUS_OK = 0,
           STATUS_ERR_DB_DOES_NOT_EXIST = -1,
           STATUS_ERR_DB_EXISTS = -2,
           STATUS_ERR_TABLE_EXISTS = -3
    };

    typedef boost::shared_ptr<MetadataCache> Ptr;

    // modifiers
    int addDbInfoNonPartitioned(std::string const&);
    int addDbInfoPartitionedSphBox(std::string const&, int, int, float, float);
    int addTbInfoNonPartitioned(std::string const&, std::string const&);
    int addTbInfoPartitionedSphBox(std::string const&, std::string const&,
                                   float, std::string const&, std::string const&, std::string const&,
                                   int, int, int, int, int);
    // accessors (they all lock a mutex, thus can't be const)
    bool checkIfContainsDb(std::string const&);
    bool checkIfContainsTable(std::string const&, std::string const&);
    bool checkIfTableIsChunked(std::string const&, std::string const&);
    bool checkIfTableIsSubChunked(std::string const&, std::string const&);
    std::vector<std::string> getAllowedDbs();
    std::vector<std::string> getChunkedTables(std::string const&);
    std::vector<std::string> getSubChunkedTables(std::string const&);
    std::vector<std::string> getPartitionCols(std::string const&, std::string const&);
    long getChunkLevel(std::string const& db, std::string const& table);
    std::string getKeyColumn(std::string const& db, std::string const& table);
    DbInfo getDbInfo(std::string const& dbName);

    void printSelf();

    /** The class TableInfo encapsulates metadata information about single table.
      */
    class TableInfo {
    public:
        // constructors
        TableInfo();
        TableInfo(float, std::string const&, std::string const&, std::string const&,
                  int, int, int, int, int);
        // accessors
        bool getIsPartitioned() const { return _isPartitioned; }
        float getOverlap() const { return _overlap; }
        std::string getPhiCol() const { return _phiCol; }
        std::string getThetaCol() const { return _thetaCol; }
        std::string getObjIdCol() const { return _objIdCol; }
        int getPhiColNo() const { return _phiColNo; }
        int getThetaColNo() const { return _thetaColNo; }
        int getObjIdColNo() const { return _objIdColNo; }
        long getLogicalPart() const { return _logicalPart; }
        long getPhysChunking() const { return _physChunking; }
    private:
        const bool _isPartitioned;
        const float _overlap;        // invalid for non partitioned tables
        const std::string _phiCol;   // invalid for non partitioned tables
        const std::string _thetaCol; // invalid for non partitioned tables
        const std::string _objIdCol; // invalid for non partitioned tables
        const int _phiColNo;         // invalid for non partitioned tables
        const int _thetaColNo;       // invalid for non partitioned tables
        const int _objIdColNo;       // invalid for non partitioned tables
        const long _logicalPart;     // invalid for non partitioned tables
        const long _physChunking;    // invalid for non partitioned tables
        // friendship
        friend std::ostream& operator<<(std::ostream&, const TableInfo&);
    };

    /** The class DbInfo encapsulates metadata information about a single database.
      */
    class DbInfo {
    public:
        // constructors
        DbInfo();
        DbInfo(int, int, float, float);
        // modifiers
        int addTable(std::string const&, const TableInfo&);
        // accessors
        bool getIsPartitioned() const { return _isPartitioned; }
        int getNStripes() const { return _nStripes; }
        int getNSubStripes() const { return _nSubStripes; }
        float getDefOverlapF() const { return _defOverlapF; }
        float getDefOverlapNN() const { return _defOverlapNN; }
        bool checkIfContainsTable(std::string const&) const;
        bool checkIfTableIsChunked(std::string const&) const;
        bool checkIfTableIsSubChunked(std::string const&) const;
        int getChunkLevel(std::string const& table) const;
        std::vector<std::string> getChunkedTables() const;
        std::vector<std::string> getSubChunkedTables() const;
        std::vector<std::string> getPartitionCols(std::string const&) const;
        std::string getKeyColumn(std::string const&) const;

    private:
        const bool _isPartitioned;
        const int _nStripes;         // invalid for non partitioned tables
        const int _nSubStripes;      // invalid for non partitioned tables
        const float _defOverlapF;    // invalid for non partitioned tables
        const float _defOverlapNN;   // invalid for non partitioned tables
        std::map<std::string, TableInfo> _tables;
        // friendship
        friend std::ostream& operator<<(std::ostream&, const DbInfo&);
    };

private:
    std::map<std::string, DbInfo> _dbs;
    boost::mutex _mutex; // guards the map "_dbs"
};

}}} // namespace lsst::qserv::meta

#endif // LSST_QSERV_META_METADATAMANAGER_H
