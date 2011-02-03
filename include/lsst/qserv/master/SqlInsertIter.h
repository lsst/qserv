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

// SqlInsertIter.h: 
// class SqlInsertIter -- A class that finds INSERT statements in 
// mysqldump output and iterates over them.
//
 
#ifndef LSST_QSERV_MASTER_SQLINSERTITER_H
#define LSST_QSERV_MASTER_SQLINSERTITER_H


// Boost
#include <boost/regex.hpp>

// Pkg
#include "lsst/qserv/master/PacketIter.h"

namespace lsst {
namespace qserv {
namespace master {

class SqlInsertIter {
public:
    typedef char const* BufIter;
    typedef boost::regex_iterator<BufIter> Iter;
    typedef boost::match_results<BufIter> Match;
    typedef boost::sub_match<BufIter> Value;

    SqlInsertIter() {}

    /// Constructor.  Buffer must be valid over this object's lifetime.
    SqlInsertIter(char const* buf, off_t bufSize, 
                  std::string const& tableName, bool allowNull); 
    SqlInsertIter(PacketIter::Ptr p,
                  std::string const& tableName, bool allowNull); 
    
    // Dereference
    const Value& operator*() const { return (*_iter)[0]; }
    const Value* operator->() const { return &(*_iter)[0]; }

    // Increment
    SqlInsertIter& operator++();

    // Const accessors:
    bool operator==(SqlInsertIter const& rhs) const {
        return _iter == rhs._iter;
    }
    bool isDone() const { return _iter ==  Iter(); }
    bool isMatch() const { return _blockFound; }
    bool isNullInsert() const;

private:
    SqlInsertIter operator++(int); // Disable: this isn't safe.

    void _init(char const* buf, off_t bufSize, std::string const& tableName);
    void _increment();

    bool _allowNull;
    Iter _iter;
    Match _blockMatch;
    bool _blockFound;
    boost::regex _blockExpr;
    boost::regex _insExpr;
    boost::regex _nullExpr;
    PacketIter::Ptr _pacIterP;
};

}}} // lsst::qserv::master

#endif // LSST_QSERV_MASTER_SQLINSERTITER_H
