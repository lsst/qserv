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

#include "lsst/qserv/master/parseHandlers.h"
// namespace modifiers
namespace qMaster = lsst::qserv::master;


// ColumnAliasHandler is bolted to the SQL parser, where it gets called for
// each aliasing instance.
class qMaster::AliasMgr::ColumnAliasHandler : public VoidTwoRefFunc {
public: 
    
    ColumnAliasHandler(AliasMgr& am) : _am(am) {}
    virtual ~ColumnAliasHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        using lsst::qserv::master::getLastSibling;
        if(b.get()) {
            _am._columnAliasNodeMap[a] = NodeBound(b, getLastSibling(a));
        }
        _am._columnAliasNodes.push_back(NodeBound(a, getLastSibling(a)));
        // Save column ref for pass/fixup computation, 
        // regardless of alias.
    }
private:
    AliasMgr& _am;
}; // class AliasMgr::ColumnAliasHandler

// TableAliasHandler is bolted to the SQL parser, where it gets called for
// each table aliasing instance.
class qMaster::AliasMgr::TableAliasHandler : public VoidFourRefFunc {
public: 
    
    TableAliasHandler(AliasMgr& am) : _am(am) {}
    virtual ~TableAliasHandler() {}
    virtual void operator()(antlr::RefAST table, 
                            antlr::RefAST subQuery,
                            antlr::RefAST as,
                            antlr::RefAST alias)  {
        using lsst::qserv::master::getLastSibling;
        std::string logicalName;
        std::string physicalName;
        antlr::RefAST tableBound;

        if(subQuery.get()) {
            std::cout << "ERROR!! Unexpected subquery alias in query. " 
                      << subQuery->getText();
            return; // Refuse to process.
        }
        assert(table.get());
        if(alias.get()) {
            antlr::RefAST bound = table;
            logicalName = walkTreeString(alias);
            if(as.get()) {
                tableBound = as;
            } else {
                tableBound = alias;
            }
            while(bound->getNextSibling() != tableBound) {
                bound = bound->getNextSibling(); 
            }
            physicalName = walkBoundedTreeString(table, bound);
        } else {
            physicalName = walkTreeString(table);
            logicalName = physicalName;
        }
        _am.addTableAlias(logicalName, physicalName);
    }
private:
    AliasMgr& _am;
}; // class AliasMgr::ColumnAliasHandler


////////////////////////////////////////////////////////////////////////
// AliasMgr
////////////////////////////////////////////////////////////////////////
boost::shared_ptr<VoidTwoRefFunc> qMaster::AliasMgr::getColumnAliasHandler() {
    return boost::shared_ptr<VoidTwoRefFunc>(new ColumnAliasHandler(*this));
}
boost::shared_ptr<VoidFourRefFunc> qMaster::AliasMgr::getTableAliasHandler() {
    return boost::shared_ptr<VoidFourRefFunc>(new TableAliasHandler(*this));
}

void qMaster::AliasMgr::addTableAlias(std::string const& alias, 
                                      std::string const& tName) {
    _tableMap[alias] = tName;
    _tableAliases.push_back(StringMap::value_type(alias,tName));
}
