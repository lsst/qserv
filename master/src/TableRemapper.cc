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
// See TableRemapper.h
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

std::string stripDelim(std::string const& src,std::string const& delim) {
    std::string::size_type p1 = src.find(delim);
    assert(p1 != std::string::npos);
    p1 += delim.size();
    std::string::size_type p2 = src.find(delim, p1);
    assert(p2 != std::string::npos);
    return src.substr(p1, p2-p1);
}

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

// Generate substitution names for basic tables 
class BaseGenerator {
public:
    BaseGenerator(std::string const& delim) : _delim(delim) {}
    
    virtual std::ostream& writeSubChunkName(std::ostream& os, 
                                            std::string const& db,
                                            std::string const& table) {
        int num = mergeGet(_m, db, table, 0);
        mergeSet(_m, db, table, ++num);
        os << db << "." << _delim << table << "_sc" << num
           << _delim;
        return os;
    }

    std::ostream& writeChunkName(std::ostream& os, 
                                 std::string const& db,
                                 std::string const& table) {
        return os << db << "." << _delim << table << _delim;
    } 

    std::ostream& writePlainName(std::ostream& os, 
                                 std::string const& db,
                                 std::string const& table) {
        return os << db << "." << table;
    }
    std::string const _delim;
    IntMap _m;
};

// Generate substitution names for tables with overlap
class OverlapGenerator : public BaseGenerator {
public:
    OverlapGenerator(std::string const& delim) 
        : BaseGenerator(delim) {}

    virtual std::ostream& writeSubChunkName(std::ostream& os, 
                                            std::string const& db,
                                            std::string const& table) {
        int num = mergeGet(_m, db, table, 0);
        mergeSet(_m, db, table, ++num);
        os << db << "." << _delim << table;
        if(num == 1) os << "_sc" << num;
        else os << "_sfo";
        os << _delim;
        return os;
    }
};
} // anonymous namespace


qMaster::TableRemapper::TableRemapper(TableNamer const& tn,
                                      TableRefChecker const& checker,
                                      std::string const& delim) 
    : _tableNamer(tn), _checker(checker), _delim(delim) {
}

qMaster::StringMap qMaster::TableRemapper::TableRemapper::getMap(bool overlap) {
    typedef TableNamer::RefDeque RefDeque;
    RefDeque const& rd = _tableNamer.getRefs();
    IntMap m;
    StringMap sm;
    std::stringstream ss;
    bool subC = _tableNamer.getHasSubChunks();

    boost::shared_ptr<BaseGenerator> g;
    if(overlap) g.reset(new OverlapGenerator(_delim));
    else g.reset(new BaseGenerator(_delim));

    for(RefDeque::const_iterator i=rd.begin(); i != rd.end(); ++i) {
        // For now, map back to original naming scheme.
        std::string db = i->db;
        std::string table = i->table;
        if(subC && _checker.isSubChunked(db, table)) {
            g->writeSubChunkName(ss, db, table);
        } else if(_checker.isChunked(db, table)) {
            g->writeChunkName(ss, db, table);
        } else {
            g->writePlainName(ss, db, table);
        }
        sm[i->magic] = ss.str();
        ss.str(std::string()); // clear
    }
    return sm;
}

qMaster::StringMap qMaster::TableRemapper::TableRemapper::getPatchMap() {
    StringMap baseMap = getMap(); // magic --> "normal"
    StringMap overlapMap = getMap(true); // magic -> "overlap"
    StringMap newMap;

    for(StringMap::const_iterator i=baseMap.begin(); i != baseMap.end(); ++i) {
        // create mapping for "normal" -> "overlap"
        std::string key = i->second;
        std::string val = overlapMap[i->first];
        if(key != val) {
            // remove delimiters
            key = stripDelim(key, _delim);
            val = stripDelim(val, _delim);
            newMap[key] = val;
        }
    }
    return newMap;
}
