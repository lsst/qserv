// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_TABLEREFCHECKER_H
#define LSST_QSERV_MASTER_TABLEREFCHECKER_H
#include <set>
#include <map>
#include <deque>
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {
namespace master {

class TableRefChecker {
public:
    class DbInfo;
    typedef boost::shared_ptr<DbInfo> DbInfoPtr;

    typedef std::map<std::string, boost::shared_ptr<DbInfo const> > Info;
    typedef boost::shared_ptr<Info> InfoPtr;
    typedef boost::shared_ptr<Info const> InfoConstPtr;

    typedef std::pair<std::string, std::string> RefPair;
    typedef std::deque<RefPair> RefPairDeque;

    // Should be able to construct info from qserv metadata
    explicit TableRefChecker(InfoConstPtr info=InfoPtr());
    
    void markTableRef(std::string const& db, std::string const& table);

    void resetTransient();
    
    bool getHasChunks() const;
    bool getHasSubChunks() const;
    

private:
    void _setDefaultInfo();
    void _computeChunking() const;

    boost::shared_ptr<Info const> _info;

    RefPairDeque _refs;
    mutable bool _computed;
    mutable bool _hasChunks;
    mutable bool _hasSubChunks;        
};


// Information on chunked/subchunked tables in a db.
class TableRefChecker::DbInfo {
public:
    InfoPtr getDefault();
    
    std::set<std::string> chunked; // chunked tables
    std::set<std::string> subchunked; // subchunked tables
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_TABLEREFCHECKER_H
