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
#include "lsst/qserv/master/TableRemapper.h"

// Std
#include <map>
#include <sstream>

// Pkg
#include "lsst/qserv/master/TableNamer.h"
#include "lsst/qserv/master/TableRefChecker.h"

// namespace modifiers
namespace qMaster = lsst::qserv::master;
typedef std::map<std::string, int> IntMap;

namespace { // anonymous 
    const char sep='#';

    template <class M>
    int mergeGet(M const& m, 
                    std::string const& one, std::string const& two, 
                    int def) {
        return qMaster::getFromMap(m, one + sep + two, def);
    }
    template <class M>
    void mergeSet(M& m, 
                 std::string const& one, std::string const& two, 
                  int val) {
        m[one + sep + two] = val;
    }
}


qMaster::TableRemapper::TableRemapper(TableNamer const& tn,
                                      TableRefChecker const& checker,
                                      std::string const& delim) 
    : _tableNamer(tn), _checker(checker), _delim(delim) {
}

qMaster::StringMap qMaster::TableRemapper::TableRemapper::getBaseMap() {
    typedef TableNamer::RefDeque RefDeque;
    RefDeque const& rd = _tableNamer.getRefs();
    IntMap m;
    StringMap sm;
    bool subC = _tableNamer.getHasSubChunks();
    for(RefDeque::const_iterator i=rd.begin(); i != rd.end(); ++i) {
        // For now, map back to original naming scheme.
        if(subC && _checker.isSubChunked(i->db, i->table)) {
            int num = mergeGet(m, i->db, i->table, 0);
            mergeSet(m, i->db, i->table, ++num);
            std::stringstream ss;
            ss << i->db << "." << _delim << i->table << "_sc" << num
               << _delim;
            sm[i->magic] = ss.str();
        }
    }
    return sm;
}
 
qMaster::StringMap qMaster::TableRemapper::TableRemapper::getOverlapMap() {    
    StringMap sm;
    return sm;
}

