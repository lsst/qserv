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
 
#ifndef LSST_QSERV_MASTER_STRINGUTIL_H
#define LSST_QSERV_MASTER_STRINGUTIL_H

#include <sstream>
#include <map>

namespace lsst {
namespace qserv {
namespace master {

class strToDoubleFunc {
public:
    double operator()(std::string const& s) { 
        char const* start = s.c_str();
        char const* eptr;
        // Cast away const. strtod won't write any chars anyway.
        double d = std::strtod(start, const_cast<char**>(&eptr));
        if(s.size() != static_cast<std::string::size_type>(eptr-start)) {
            std::stringstream s;
            s << "Exception converting string to double ("
              << s << ")";
            throw s.str();
        }
        return d;
    }
};

template <typename T>
class passFunc {
public:
    T const& operator()(T const& t) const {
        return t;
    }
};

// Tokenize a string delimited by ',' and place it into a container, 
// transforming it if desired.
template <class Container, class Transform>
Container& tokenizeInto(std::string const& s, 
                        std::string const& delimiter,
                        Container& c, 
                        Transform transform) {
        std::string::size_type pos = 0;
        std::string::size_type lastPos = 0;
        lastPos = s.find_first_not_of(delimiter, 0);
        while(std::string::npos != lastPos) {
            pos = s.find_first_of(delimiter, lastPos);
            std::string token(s, lastPos, pos-lastPos);
            c.push_back(transform(token));
            if(std::string::npos == pos) {
                break;
            } else {
                lastPos = s.find_first_not_of(delimiter, pos);
            }
        }
        return c;
}

template <class Container>
std::map<typename Container::value_type,int>& fillMapFromKeys(Container const& c,
                                                            std::map<typename Container::value_type, int>& e) {
    
    typedef std::map<typename Container::value_type,int> Map;
    typename Container::const_iterator i;
    typename Container::const_iterator ie = c.end();
    e.clear();
    for(i = c.begin(); i != ie; ++i) {
        e[*i] = 1;
    }
    return e;
}


template <typename Target>
class coercePrint {
public:
    coercePrint(std::ostream& o_, const char* d_) 
        : o(o_), d(d_), first(true) {}
    template<typename T>
    void operator()(T const& t) { 
            if(!first) { o << d; }
            else { first = false; }
            o << (Target)t;
    }
    std::ostream& o;
    char const* d;
    bool first;
};

}}} // namesapce lsst::qserv::master
#endif // LSST_QSERV_MASTER_STRINGUTIL_H
