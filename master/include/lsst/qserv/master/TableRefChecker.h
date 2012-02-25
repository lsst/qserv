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
#include <string>
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/common.h"

namespace lsst {
namespace qserv {
namespace master {

class TableRefChecker {
public:
    typedef boost::shared_ptr<TableRefChecker> Ptr;
    typedef boost::shared_ptr<TableRefChecker const> ConstPtr;

    class DbInfo;
    typedef boost::shared_ptr<DbInfo> DbInfoPtr;

    typedef std::pair<std::string, std::string> RefPair;
    typedef std::map<std::string, boost::shared_ptr<DbInfo> > Info;
    typedef boost::shared_ptr<Info> InfoPtr;
    typedef boost::shared_ptr<Info const> InfoConstPtr;

    // Should be able to construct info from qserv metadata
    explicit TableRefChecker(InfoPtr info=InfoPtr());
    void importDbWhitelist(StringList const& wlist);

    bool isChunked(std::string const& db, std::string const& table) const;
    bool isSubChunked(std::string const& db, std::string const& table) const;
    bool isDbAllowed(std::string const& db) const;
private:
    void _setDefaultInfo();

    boost::shared_ptr<Info> _info;
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
