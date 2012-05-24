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
// TableRef is an abstract value class containing a specification of a table
// whose only behavior is the ability to generate a munged table name, generally
// for use with substitution logic.
#ifndef LSST_QSERV_MASTER_TABLEREF_H
#define LSST_QSERV_MASTER_TABLEREF_H
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {
namespace master {

class TableRef {
public:
    typedef boost::shared_ptr<TableRef> Ptr;
    TableRef(std::string const& db_,
             std::string const& table_,
             std::string const& alias_=_empty);
    
    std::string getMungedName(std::string const& delimiter) const; 
    
    // Fields
    std::string db;
    std::string table;
    std::string alias;
    int chunkLevel; // 0 = none, 1=chunked, 2=subchunked
    
    // static
    static const std::string _empty;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_TABLEREF_H
