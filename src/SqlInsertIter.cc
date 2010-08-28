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

#include "lsst/qserv/master/SqlInsertIter.h"
#include <iostream>

namespace qMaster = lsst::qserv::master;

namespace {

// Helpers to make regex's
boost::regex makeLockInsertRegex(std::string const& tableName) {
    return boost::regex("LOCK TABLES `?" + tableName + "`? WRITE;"
                            "(.*?)(INSERT INTO[^;]*?;)*(.*?)"
                        "UNLOCK TABLES;");
}
    
boost::regex makeInsertRegex(std::string const& tableName) {
    return boost::regex("(INSERT INTO `?" + tableName + 
                        "`? [^;]+?;)");// [~;]*?;)");
}

boost::regex makeNullInsertRegex(std::string const& tableName) {
    return boost::regex("(INSERT INTO `?" + tableName + 
                        "`? +VALUES ?[(]NULL(,NULL)*[)];)");
}

// Helpful debugging
void printInserts(char const* buf, off_t bufSize, 
                  std::string const& tableName)  {
    for(qMaster::SqlInsertIter i(buf, bufSize, tableName); !i.isDone(); 
        ++i) {
        std::cout << "Sql[" << tableName << "]: " 
                  << (void*)i->first << "  --->  " 
                  << (void*)i->second << "  "
                  << *i;
        if(i.isNullInsert()) {
            std::cout << "Null match" << std::endl;
        } else { std::cout << std::endl; }
    }
}


} // anonymous namespace

qMaster::SqlInsertIter::SqlInsertIter(char const* buf, off_t bufSize, 
                                    std::string const& tableName)  {
    _init(buf, bufSize, tableName);
}

void qMaster::SqlInsertIter::_init(char const* buf, off_t bufSize, 
                                 std::string const& tableName) {
    assert(buf < (buf+bufSize));
    _blockExpr = makeLockInsertRegex(tableName);
    _blockFound = boost::regex_search(buf, buf+bufSize, 
                                      _blockMatch, _blockExpr);
    if(_blockFound) {
        _insExpr = makeInsertRegex(tableName);
        _nullExpr = makeNullInsertRegex(tableName);
        _iter = Iter(_blockMatch[3].first, _blockMatch[3].second,
                     _insExpr);            
    }        
}

bool qMaster::SqlInsertIter::isNullInsert() const {
    // Avoid constructing a string > 1MB just to check for null.
    if(_iter->length() > (1<<20)) return false;
    return boost::regex_match(_iter->str(), _nullExpr);
}
