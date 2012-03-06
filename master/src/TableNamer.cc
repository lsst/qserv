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
#include "lsst/qserv/master/parseHandlers.h"
#include <string>
#include <sstream>
#include <iostream> // debugging
namespace qMaster =  lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {
    // alpha chars at ends force spacing when tree walking.
    std::string const mungeDelim("z$%z"); 
    char const dbTableSep = '.';
} // anonymous namespace

#define TRYFUNC 1
////////////////////////////////////////////////////////////////////////
// class AliasedRef (ostream)
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, 
                         qMaster::TableNamer::AliasedRef const& ar) {
    os << ar.alias << "->\"" << ar.magic << '"' 
       << ar.db << '.' << ar.table;
    return os;
}

////////////////////////////////////////////////////////////////////////
// class TableNamer::AliasFunc
////////////////////////////////////////////////////////////////////////

class qMaster::TableNamer::AliasFunc : public qMaster::TableAliasFunc {
public:
    typedef boost::shared_ptr<AliasFunc> Ptr;
    AliasFunc(TableNamer& tn) :_tn(tn) {}
    virtual void operator()(TableAliasInfo& i) {
        // First, compute info.
        TableNamer::AliasedRef ar(_tn._computeAliasedRef(i.alias, i.table));
#ifdef TRYFUNC
        _updateMagic(ar);
        // Replace physical table with munged name
        // Set bounds to "AS" or <alias> if they exist.
        if(i.asN.get()) i.tableN->setNextSibling(i.asN);
        else if(i.aliasN.get()) i.tableN->setNextSibling(i.aliasN);
        else collapseToSingle(i.tableN);

        i.tableN->setText(ar.magic);
        // Add ref.
        _tn._refs.push_back(ar);
#endif
    }
private:
    void _updateMagic(TableNamer::AliasedRef& ar) {
        std::stringstream ss;
        // Munge the name, only for chunked tables.
        if(_tn._checker.isChunked(ar.db, ar.table)) {
            ss << mungeDelim 
               << ar.db << ar.table << _tn._refs.size()
               << mungeDelim;
        } else {
            ss << ar.db << dbTableSep << ar.table;
        }
            ar.magic = ss.str();
    }
    TableNamer& _tn;

};

////////////////////////////////////////////////////////////////////////
// class TableNamer (public)
////////////////////////////////////////////////////////////////////////
qMaster::TableNamer::TableNamer(qMaster::TableRefChecker const& checker) 
    : _checker(checker) {
    resetTransient();
}

void qMaster::TableNamer::_acceptAlias(std::string const& logical,
                                       std::string const& physical) {
#ifndef TRYFUNC
    _refs.push_back(_computeAliasedRef(logical, physical));
    //std::cout << "Marking alias: " << logical << std::endl;
#endif
}


qMaster::TableNamer::AliasedRef
qMaster::TableNamer::_computeAliasedRef(std::string const& logical,
                                        std::string const& physical) {
    char const nameSep = '.';
    std::string::size_type split = physical.find(nameSep);
    if(split == std::string::npos) {
        // No db specified, use default.
        return AliasedRef(logical, _defaultDb, physical);
    } else {
        return AliasedRef(logical, physical.substr(0,split),
                          physical.substr(split + 1));
    }
}

void qMaster::TableNamer::resetTransient() {
    _computed = false;
    _refs.clear(); 
}

boost::shared_ptr<qMaster::TableAliasFunc> 
qMaster::TableNamer::getTableAliasFunc() {
    return TableAliasFunc::Ptr(new AliasFunc(*this));
}

bool qMaster::TableNamer::getHasChunks() const {
    if(!_computed) _computeChunking();
    return _hasChunks;
}

bool qMaster::TableNamer::getHasSubChunks() const {
    if(!_computed) _computeChunking();
    return _hasSubChunks;
}

qMaster::StringList qMaster::TableNamer::getBadDbs() const {
    StringList result;
    // Filter my dbs from table refs through refchecker.
    for(RefDeque::const_iterator i=_refs.begin();
        i != _refs.end();
        ++i) {
        if(!_checker.isDbAllowed(i->db)) {
            result.push_back(i->db);
        }
    }
    return result;
}

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
    // C SC            | yes  | no*
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
            // if(canSubChunk) {
            //     _hasSubChunks = true;
            // }
            bool subC = _checker.isSubChunked(i->db, i->table);
            if(canSubChunk && subC) {
                _hasSubChunks = true;
            }
            _hasChunks = true;
            canSubChunk |= subC;
        }
    }
    _computed = true;
}

