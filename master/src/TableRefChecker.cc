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
#include "lsst/qserv/master/TableRefChecker.h"
#include <string>
#include <iostream> // debugging
namespace qMaster =  lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {
    bool infoHasEntry(qMaster::TableRefChecker::Info const& info, 
                      qMaster::TableRefChecker::RefPair const& rp, 
                      bool& isSubChunk) {
        typedef qMaster::TableRefChecker::Info Info;

        Info::const_iterator dbIter = info.find(rp.first);
        if(dbIter == info.end()) { return false; }
        assert(dbIter->second.get()); // DbInfo ptr shouldn't be null.
        
        std::set<std::string> const& chunked = dbIter->second->chunked;
        std::set<std::string> const& subchunked = dbIter->second->subchunked;

        if(chunked.end() != chunked.find(rp.second)) { return true; }
        if(subchunked.end() != subchunked.find(rp.second)) { 
            isSubChunk = true;
            return true; 
        }
        return false;
    }
    inline void addDefaultTables(qMaster::TableRefChecker::DbInfo& dbinfo) {
        dbinfo.chunked.insert("Source");
        dbinfo.subchunked.insert("Object");
    }

} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// class TableRefChecker (public)
////////////////////////////////////////////////////////////////////////
qMaster::TableRefChecker::TableRefChecker(InfoPtr info) 
    : _info(info) {
    if(!_info.get()) {
        _setDefaultInfo();
    }
}

void 
qMaster::TableRefChecker::importDbWhitelist(qMaster::StringList const& wlist) {
    // replace existing db list. 
    StringList::const_iterator i;
    for(i=wlist.begin(); i != wlist.end(); ++i) {
        DbInfoPtr dbi = (*_info)[*i];
        if(!dbi.get()) { 
            dbi.reset(new DbInfo());
            (*_info)[*i] = dbi;
        }
        addDefaultTables(*dbi);
    }
}

bool qMaster::TableRefChecker::isChunked(std::string const& db, 
                                         std::string const& table) const {
    bool isSc;
    return infoHasEntry(*_info, RefPair(db, table), isSc);
}

bool qMaster::TableRefChecker::isSubChunked(std::string const& db, 
                                            std::string const& table) const {
    bool isSc;
    if(infoHasEntry(*_info, RefPair(db, table), isSc)) {
        return isSc;
    } else return false;
}

bool qMaster::TableRefChecker::isDbAllowed(std::string const& db) const {
    return _info->find(db) != _info->end();
}

/////////////////////////////////////////////////////////////////////////
// class TableRefChecker (private)
////////////////////////////////////////////////////////////////////////
void qMaster::TableRefChecker::_setDefaultInfo() {
    InfoPtr info(new Info());
    DbInfoPtr lsstDefault(new DbInfo());
    std::string lsst("LSST");
    addDefaultTables(*lsstDefault);
    
    (*info)[lsst] = lsstDefault;
    _info = info;
}

