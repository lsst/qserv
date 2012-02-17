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

} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// class TableRefChecker (public)
////////////////////////////////////////////////////////////////////////
qMaster::TableRefChecker::TableRefChecker(InfoConstPtr info) 
    : _info(info) {
    resetTransient();
    if(!_info.get()) {
        _setDefaultInfo();
    }
}
void qMaster::TableRefChecker::markTableRef(std::string const& db, 
                                            std::string const& table) {
    _refs.push_back(RefPair(db, table));
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

void qMaster::TableRefChecker::resetTransient() {
    _computed = false;
    _refs.clear(); 
}
    
bool qMaster::TableRefChecker::getHasChunks() const {
    if(!_computed) _computeChunking();
    return _hasChunks;
}

bool qMaster::TableRefChecker::getHasSubChunks() const {
    if(!_computed) _computeChunking();
    return _hasSubChunks;
}

qMaster::TableRefChecker::RefPairDeque 
qMaster::TableRefChecker::getSpatialTableRefs() const {
    RefPairDeque spatials;
    for(RefPairDeque::const_iterator i=_refs.begin();
        i != _refs.end();
        ++i) {
        bool isSc;
        if(infoHasEntry(*_info, *i, isSc)) {
            spatials.push_back(*i);
        }
    }
    return spatials;
}

/////////////////////////////////////////////////////////////////////////
// class TableRefChecker (private)
////////////////////////////////////////////////////////////////////////
void qMaster::TableRefChecker::_setDefaultInfo() {
    InfoPtr info(new Info());
    DbInfoPtr lsstDefault(new DbInfo());
    std::string lsst("LSST");
    lsstDefault->chunked.insert("Source");
    lsstDefault->subchunked.insert("Object");
    
    (*info)[lsst] = lsstDefault;
    _info = info;
}

void qMaster::TableRefChecker::_computeChunking() const {
    _hasChunks = false;
    _hasSubChunks = false;
    // C: chunked
    // SC: subchunked
    // T: unpartitioned
    // Tables involved | hasC | hasSc
    // C               | yes  | no
    // SC              | yes  | no
    // C1 C2           | yes  | no
    // C SC            | yes  | yes
    // SC SC           | yes  | yes
    // SC T            | yes  | no*
    //
    // * : For now. Optimization may be possible.  May rewrite query to 
    // add subchunk qualifier?
    // FIXME: Better to return ref table list with annotations that
    // can be modified more intelligently based on the query type?
    //
    // Some efficiency can be gained by performing this computation in-place as
    // db/table pairs are marked, but batching up the check allows for multiple
    // passes in case they are necessary.

    // If any chunked/subchunked table is involved, 
    for(RefPairDeque::const_iterator i=_refs.begin();
        i != _refs.end();
        ++i) {
        bool isSc = false;
        if(infoHasEntry(*_info, *i, isSc)) {
            if(_hasChunks && isSc) { _hasSubChunks = true; }
            _hasChunks = true;
        }        
    }
    _computed = true;
}

