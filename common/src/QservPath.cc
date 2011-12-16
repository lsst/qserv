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

#include "QservPath.hh"
#include <iostream>
#include <sstream>
namespace qsrv = lsst::qserv;

qsrv::QservPath::QservPath(std::string const& path) {
    _setFromPath(path);
}
std::string qsrv::QservPath::path() const {
    std::stringstream ss;
    ss << _pathSep << prefix(_requestType);
    if(_requestType == CQUERY) {
        ss << _pathSep << _db << _pathSep << _chunk;
    } else {
    }
    return ss.str();
}
    
std::string qsrv::QservPath::prefix(RequestType const& r) const {
    switch(r) {
    case UNKNOWN:
        return "UNKNOWN";
    case CQUERY:
        return "q";
    default:
        return "GARBAGE";
    }
}

    void qsrv::QservPath::_setFromPath(std::string const& path) {
        std::string::size_type cursor = 0;
        std::string::size_type next = 0;
        next = path.find_first_of(_pathSep, cursor);
        std::cout << "token 1:" << path.substr(cursor, next-cursor)
                  << std::endl;
        cursor = next + 1;
        next = path.find_first_of(_pathSep, cursor);
        std::cout << "token 2:" << path.substr(cursor, next-cursor)
                  << std::endl;
        cursor = next + 1;
        next = path.find_first_of(_pathSep, cursor);
        std::cout << "token 3:" << path.substr(cursor, next-cursor)
                  << std::endl;        
    }
