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
// See TableRef.h
#include "lsst/qserv/master/TableRef.h"
#include <sstream>

namespace qMaster = lsst::qserv::master;

qMaster::TableRef::TableRef(std::string const& db_,
                            std::string const& table_,
                            std::string const& alias_) 
    : db(db_), table(table_), alias(alias_) {
}

std::string 
qMaster::TableRef::getMungedName(std::string const& delimiter) const {
    if(alias != _empty) {
        return alias;
    }
    std::stringstream ss;
    const char sep[] = ".";

    ss << db << sep;
    if(chunkLevel > 0) {
        ss << delimiter << table << delimiter;
    } else {
        ss << table;
    }
    return ss.str();
}
