// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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
 * @brief Common utilty functions for lsst::qserv::master. Only std C++ dependencies allowed.
 *
 */
#ifndef LSST_QSERV_UTIL_COMMON_H
#define LSST_QSERV_UTIL_COMMON_H

// System headers
#include <iostream>
#include <map>
#include <sstream>
#include <string>


namespace lsst {
namespace qserv {
namespace util {

template <class Map>
typename Map::mapped_type const&
getFromMap(Map const& m,
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

template <typename C>
std::string prettyCharList(C const& c) {
    std::ostringstream os;
    os << "[";
    for(auto i = c.begin(); i != c.end();) {
        int val = *i;
        os << val ;
        ++i;
        if (i == c.end()) {
            break;
        }
        os << ", ";
    }
    os << "]";
    return os.str();
}

/** Return a string showing the 'edge' of a list as integers.
 * c is the container, edge is how many elements to include from
 * the begging and end of the list.
 */
template <typename C>
std::string prettyCharList(C const& c, uint edge) {
    std::ostringstream os;
    auto sz = c.size();
    os << "[";
    auto j = sz;
    for(j = 0; j < sz && j < edge; ++j) {
        auto val = static_cast<int>(c[j]);
        os << "[" << j << "]=" << val;
        if (j < sz - 1) {
            os << ", ";
        }
    }
    if (j < sz) {
        if (j < sz - edge) {
            os << "..., ";
            j = sz - edge;
        }
    }
    for(;j < sz; ++j) {
        int val = c[j];
        os << "[" << j << "]=" << val;
        if (j < sz - 1) {
            os << ", ";
        }
    }
    os << "]";
    return os.str();
}

template <typename C>
std::string prettyCharBuf(C* c, uint bufLen, uint edge) {
    std::ostringstream os;
    os << "[";
    uint j;
    for(j = 0; j < bufLen && j < edge; ++j) {
        auto val = static_cast<int>(c[j]);
        os << "[" << j << "]=" << val;
        if (j < bufLen - 1) {
            os << ", ";
        }
    }
    if (j < bufLen) {
        if (j < bufLen - edge) {
            os << "..., ";
            j = bufLen - edge;
        }
    }
    for(;j < bufLen; ++j) {
        int val = c[j];
        os << "[" << j << "]=" << val;
        if (j < bufLen -1) {
            os << ", ";
        }
    }
    os << "]";
    return os.str();
}


}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_COMMON_H
