// -*- LSST-C++ -*-
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
// SelectStmt contains extracted information about a particular parsed
// SQL select statement. It is not responsible for performing
// verification, validation, or other processing that requires
// persistent or run-time state.
#ifndef LSST_QSERV_MASTER_SELECTSTMT_H
#define LSST_QSERV_MASTER_SELECTSTMT_H

// Standard
#include <list>

// Boost
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/QueryTemplate.h"

// Forward
class SqlSQL2Parser;

namespace lsst {
namespace qserv {
namespace master {
// Forward
class SelectList;
class FromList;
class WhereClause;
class OrderByClause;
class GroupByClause;
class HavingClause;
class ColumnAliasMap;

/// class SelectStmt - a container for SQL SELECT statement info.
class SelectStmt  {
public:
    typedef boost::shared_ptr<SelectStmt> Ptr;
    typedef boost::shared_ptr<SelectStmt const> Cptr;
    typedef std::list<std::string> StringList; // placeholder
    
    SelectStmt();

    void diagnose(); // for debugging

    boost::shared_ptr<WhereClause const> getWhere() const;
    QueryTemplate getTemplate() const;
    boost::shared_ptr<SelectStmt> copyDeep() const;
    boost::shared_ptr<SelectStmt> copyMerge() const;    
    boost::shared_ptr<SelectStmt> copySyntax() const;
    
    void fillEmpty(); // Add placeholders for NULL parts

    SelectList const& getSelectList() const { return *_selectList; }
    SelectList& getSelectList() { return *_selectList; }

    FromList const& getFromList() const { return *_fromList; }
    FromList& getFromList() { return *_fromList; }
    
    bool hasWhereClause() { return _whereClause.get(); }
    WhereClause const& getWhereClause() const { return *_whereClause; }
    WhereClause& getWhereClause() { return *_whereClause; }
    
    int getLimit() const { return _limit; }
    bool hasOrderBy() { return _orderBy.get(); }
    OrderByClause const& getOrderBy() const { return *_orderBy; }
    OrderByClause& getOrderBy() { return *_orderBy; }

 private: // public for now.
    // Declarations
    friend class SelectFactory;

    // Helpers
    void _print();
    void _generate();
    
    // Fields
    boost::shared_ptr<FromList> _fromList; // Data sources
    boost::shared_ptr<SelectList> _selectList; // Desired columns
    boost::shared_ptr<WhereClause> _whereClause; // Filtering conditions (WHERE)
    boost::shared_ptr<OrderByClause> _orderBy; // Ordering
    boost::shared_ptr<GroupByClause> _groupBy; // Aggr. grouping
    boost::shared_ptr<HavingClause> _having; // Aggr. grouping
    
    int  _limit; // result limit
    boost::shared_ptr<ColumnAliasMap> _columnAliasMap;
    StringList OutputMods; // Output modifiers (order, grouping,
                           // sort, limit
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_SELECTSTMT_H
