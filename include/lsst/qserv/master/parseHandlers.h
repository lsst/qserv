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

// parseHandlers.h: parse action handlers that aren't separated into
// their own file.
//
// class ColumnAliasHandler -- Remembers table and column aliases in effect.
// NodeBound, NodeList typedefs
//
 
#ifndef LSST_QSERV_MASTER_PARSEHANDLERS_H
#define LSST_QSERV_MASTER_PARSEHANDLERS_H

// C++ 
#include <deque>
#include <map>

// Boost
#include <boost/shared_ptr.hpp>

// ANTLR
//#include "antlr/AST.hpp"
//#include "antlr/CommonAST.hpp"
#include "antlr/ASTRefCount.hpp"

// Package:
#include "lsst/qserv/master/parserBase.h" 
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/common.h"

namespace lsst {
namespace qserv {
namespace master {

typedef std::pair<antlr::RefAST, antlr::RefAST> NodeBound;
typedef std::deque<NodeBound> NodeList;
typedef NodeList::const_iterator NodeListConstIter;

class AliasMgr {
public:
    typedef std::map<antlr::RefAST, NodeBound> NodeMap;
    typedef NodeMap::const_iterator MapConstIter;
    typedef NodeMap::iterator MapIter;

    class ColumnAliasHandler;
    friend class ColumnAliasHandler;
    class TableAliasHandler;
    friend class TableAliasHandler;

    boost::shared_ptr<VoidTwoRefFunc> getColumnAliasHandler();
    boost::shared_ptr<VoidFourRefFunc> getTableAliasHandler();
    
    // Manipulation
    // Retrieved by aggregation post-processing.
    NodeMap const& getInvAliases() const { return _columnAliasNodeMap; }
    // Activated by SelectListHandler
    NodeList getColumnNodeListCopy() { return _columnAliasNodes; }
    void resetColumnNodeList() { _columnAliasNodes.clear(); }

private:
    // Invoked by child handlers.
    void addTableAlias(std::string const& tName, std::string const& alias);

    NodeMap _columnAliasNodeMap;
    NodeList _columnAliasNodes;
    StringMap _tableMap;
};

}}} // lsst::qserv::master
#endif // LSST_QSERV_MASTER_PARSEHANDLERS_H
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             
