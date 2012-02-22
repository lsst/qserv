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
#include "lsst/qserv/master/TableNamer.h"
#include "lsst/qserv/master/TableRefChecker.h"
#include <string>
#include <iostream> // debugging
namespace qMaster =  lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {

} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// class TableNamer (public)
////////////////////////////////////////////////////////////////////////
qMaster::TableNamer::TableNamer(qMaster::TableRefChecker const& checker) 
    : _checker(checker) {
    resetTransient();
}

void qMaster::TableNamer::_acceptAlias(std::string const& logical,
                                            std::string const& physical) {
    char const nameSep = '.';
    std::string::size_type split = physical.find(nameSep);
    if(split == std::string::npos) {
        // No db specified, use default.
        _refs.push_back(AliasedRef(logical, _defaultDb, physical));
    } else {
        _refs.push_back(AliasedRef(logical, physical.substr(0,split),
                                   physical.substr(split + 1)));
    }
    std::cout << "Marking alias: " << logical << std::endl;
}



void qMaster::TableNamer::resetTransient() {
    _computed = false;
    _refs.clear(); 
}
    
bool qMaster::TableNamer::getHasChunks() const {
    if(!_computed) _computeChunking();
    return _hasChunks;
}

bool qMaster::TableNamer::getHasSubChunks() const {
    if(!_computed) _computeChunking();
    return _hasSubChunks;
}
#if 0
qMaster::TableNamer::RefPairDeque 
qMaster::TableNamer::getSpatialTableRefs() const {
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
#endif
/////////////////////////////////////////////////////////////////////////
// class TableNamer (private)
////////////////////////////////////////////////////////////////////////

void qMaster::TableNamer::_computeChunking() const {
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
    bool canSubChunk = false;
    for(RefDeque::const_iterator i=_refs.begin();
        i != _refs.end();
        ++i) {
        if(_checker.isChunked(i->db, i->table)) {
            if(canSubChunk) {
                _hasSubChunks = true;
            }
            bool subC = _checker.isSubChunked(i->db, i->table);
            if(_hasChunks && subC) {
                _hasSubChunks = true;
            }
            _hasChunks = true;
            canSubChunk |= subC;
        }
    }
    _computed = true;
}

