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
// ColumnRefList.cc houses the implementation of a ColumnRefList, a
// container for column refs parsed from a sql statement.

#include "lsst/qserv/master/ColumnRefList.h"
#include "lsst/qserv/master/ColumnRef.h"

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
}


boost::shared_ptr<qMaster::ColumnRef const>
qMaster::ColumnRefList::getRef(antlr::RefAST r) { 
    if(_refs.find(r) == _refs.end()) {
        std::cout << "couldn't find " << tokenText(r) << " in";
        printRefs(); 
    } 
    assert(_refs.find(r) != _refs.end()); 
    return _refs[r];
}
    
void qMaster::ColumnRefList::printRefs() const { 
    std::cout << "Printing select refs." << std::endl;
    typedef RefMap::const_iterator Citer; 
    Citer end = _refs.end();

    for(Citer i=_refs.begin(); i != end; ++i) {
        ColumnRef const& cr(*(i->second));
        std::cout << "\t\"" << cr.db << "\".\"" 
                  << cr.table << "\".\"" 
                  << cr.column << "\"" 
                  << std::endl; 
    }
}

