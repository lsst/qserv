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


#ifndef LSST_QSERV_META_METADATACACHE_H
#define LSST_QSERV_META_METADATACACHE_H

// Standard
#include <iostream>
#include <string>
#include <map>

// Boost
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {
namespace master {

/// The class manages transient structure that contains metadata
/// information fetched from the qserv metadata server.
class MetadataCache {
public:
    typedef boost::shared_ptr<MetadataCache> Ptr;
    int addDbInfoNonPartitioned(std::string const&);
    int addDbInfoPartitioned(std::string const&, int, int, float, float);
    void printSelf() const;

    class TableInfo {
    public:
        // constructors
        TableInfo();
        TableInfo(float, std::string const&, std::string const&, int, int);
        // accessors
        bool getIsPartitioned() const { return _isPartitioned; }
        float getOverlap() const { return _overlap; }
        std::string getPhiCol() const { return _phiCol; }
        std::string getThetaCol() const { return _thetaCol; }
        int getLogicalPart() const { return _logicalPart; }
        int getPhysPart() const { return _physPart; }
    private:
        const bool _isPartitioned;
        const float _overlap;        // invalid for non partitioned tables
        const std::string _phiCol;   // invalid for non partitioned tables
        const std::string _thetaCol; // invalid for non partitioned tables
        const int _logicalPart;      // invalid for non partitioned tables
        const int _physPart;         // invalid for non partitioned tables
        // friendship
        friend std::ostream& operator<<(std::ostream&, const TableInfo&);
    };

    class DbInfo {        
    public:
        // constructors
        DbInfo();
        DbInfo(int, int, float, float);
        // accessors
        bool getIsPartitioned() const { return _isPartitioned; }
        int getNStripes() const { return _nStripes; }
        int getNSubStripes() const { return _nSubStripes; }
        int getDefOverlapF() const { return _defOverlapF; }
        int getDefOverlapNN() const { return _defOverlapNN; }
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
    bool containsDb(std::string const&) const;

private:
    std::map<std::string, DbInfo> _dbs;
};

}}} // namespace lsst::qserv::meta

#endif // LSST_QSERV_META_METADATAMANAGER_H
