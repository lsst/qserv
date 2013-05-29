// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_PARSEALIASMAP_H
#define LSST_QSERV_MASTER_PARSEALIASMAP_H
/**
  * @file ParseAliasMap.h
  *
  * @brief ParseAliasAliasMap contains meta-information on column aliases
  * declared in the select list of a SQL select statement. 
  *
  * @author Daniel L. Wang, SLAC
  */

// Standard
#include <map>

namespace lsst { namespace qserv { namespace master {

/// class ParseAliasMap - maintain mappings for aliasing in SQL statements
/// in the parse node domain.
class ParseAliasMap {
public:
    typedef boost::shared_ptr<ParseAliasMap> Ptr;
    typedef boost::shared_ptr<ParseAliasMap const> Cptr;

    typedef std::map<antlr::RefAST, antlr::RefAST> Map; // Aliases are unique
    // Although in SQL, a table may have multiple aliases, each
    // alias declaration has its own parse nodes, so reverse-lookups
    // to aliases are still unique: each table expression node only
    // has one alias. For example, in:
    // SELECT o1.*, o2.id, from Object o1, Object o2;
    // The first "Object" node points only at o1, and the second
    // "Object" node points only at o2.  In the parse tree domain,
    // "Object" is not merged.
    
    typedef Map::const_iterator Miter;
    ParseAliasMap() {}

    void addAlias(antlr::RefAST alias, antlr::RefAST target) {
        _map[alias] = target;
        _rMap[target] = alias;
    }
    inline antlr::RefAST get(antlr::RefAST alias) const {
        return _get(_map, alias);
    }
    inline antlr::RefAST getAlias(antlr::RefAST target) const {
        return _get(_rMap, target);
    }
private:
    friend std::ostream& operator<<(std::ostream& os, ParseAliasMap const& m);

    inline antlr::RefAST _get(Map const& m, antlr::RefAST k) const {
        Miter i = m.find(k);
        if(i != m.end()) return i->second;
        else return antlr::RefAST();        
    }
    Map _map;
    Map _rMap;
};

// cryptically implemented in SelectFactory.cc for now.
std::ostream& operator<<(std::ostream& os, ParseAliasMap const& m);

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_PARSEALIASMAP_H
