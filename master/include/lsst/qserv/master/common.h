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
/* 
 * @file
 * 
 * @brief Common utilty functions for lsst::qserv::master
 *
 */
#ifndef LSST_QSERV_MASTER_COMMON_H
#define LSST_QSERV_MASTER_COMMON_H

#include <list>
#include <map>
#include <string>
#include <iostream>

namespace lsst {
namespace qserv {
namespace master {

typedef std::map<std::string, std::string> StringMap;
typedef std::map<std::string, StringMap> StringMapMap;
typedef std::list<std::pair<std::string, std::string> > StringPairList;
typedef std::list<std::string> StringList;

template <class Map>
typename Map::mapped_type const& getFromMap(Map const& m, 
                                            typename Map::key_type const& key,
                                            typename Map::mapped_type const& defValue) {
    typename Map::const_iterator i = m.find(key);
    if(i == m.end()) {
        return defValue;
    } else {
        return i->second;
    }
}

template <class Map, class Func>
void forEachMapped(Map const& m, Func& f) {
    typename Map::const_iterator b = m.begin();
    typename Map::const_iterator e = m.end();
    typename Map::const_iterator i;
    for(i = b; i != e; ++i) {
        f(i->second);
    }
}


template <class Map, class Func>
void forEachFirst(Map const& m, Func& f) {
    typename Map::const_iterator b = m.begin();
    typename Map::const_iterator e = m.end();
    typename Map::const_iterator i;
    for(i = b; i != e; ++i) {
        f(i->first);
    }
}

template <class Map, class Func, class Filter>
void forEachFirst(Map const& m, Func& f, Filter& filter) {
    typename Map::const_iterator b = m.begin();
    typename Map::const_iterator e = m.end();
    typename Map::const_iterator i;
    for(i = b; i != e; ++i) {
        if(filter(*i)) {
            f(i->first);
        }
    }
}

template <class C>
std::ostream& printList(std::ostream &os, char const* label, C const& c) {
    typename C::const_iterator i; 
    os << label << ": ";
    for(i = c.begin(); i != c.end(); ++i) {
        os << **i << ", ";
    }
    return os;
}


}}} // namesapce lsst::qserv::master
#endif // LSST_QSERV_MASTER_COMMON_H
