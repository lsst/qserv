// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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

#ifndef LSST_QSERV_QUERY_SELECTSTMT_H
#define LSST_QSERV_QUERY_SELECTSTMT_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "global/stringTypes.h"
#include "query/QueryTemplate.h"

// Forward declarations
class SqlSQL2Parser;
namespace lsst {
namespace qserv {
namespace parser {
    class SelectFactory;
}
namespace query {
    class SelectList;
    class FromList;
    class WhereClause;
    class OrderByClause;
    class GroupByClause;
    class HavingClause;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {

// SelectStmt contains extracted information about a particular parsed
// SQL select statement. It is not responsible for performing
// verification, validation, or other processing that requires
// persistent or run-time state.
class SelectStmt  {
public:
    typedef boost::shared_ptr<SelectStmt> Ptr;
    typedef boost::shared_ptr<SelectStmt const> Cptr;

    SelectStmt();

    std::string diagnose(); // for debugging

    boost::shared_ptr<WhereClause const> getWhere() const;
    QueryTemplate getTemplate() const;
    QueryTemplate getPostTemplate() const;
    boost::shared_ptr<SelectStmt> clone() const;
    boost::shared_ptr<SelectStmt> copyMerge() const;
    boost::shared_ptr<SelectStmt> copySyntax() const;

    bool getDistinct() const { return _hasDistinct; }
    void setDistinct(bool d) { _hasDistinct = d; }

    SelectList const& getSelectList() const { return *_selectList; }
    SelectList& getSelectList() { return *_selectList; }
    void setSelectList(boost::shared_ptr<SelectList> s) { _selectList = s; }

    FromList const& getFromList() const { return *_fromList; }
    FromList& getFromList() { return *_fromList; }
    void setFromList(boost::shared_ptr<FromList> f) { _fromList = f; }
    void setFromListAsTable(std::string const& t);

    bool hasWhereClause() const { return static_cast<bool>(_whereClause); }
    WhereClause const& getWhereClause() const { return *_whereClause; }
    WhereClause& getWhereClause() { return *_whereClause; }
    void setWhereClause(boost::shared_ptr<WhereClause> w) { _whereClause = w; }

    int getLimit() const { return _limit; }
    void setLimit(int limit) { _limit = limit; }

    bool hasOrderBy() const { return static_cast<bool>(_orderBy); }
    OrderByClause const& getOrderBy() const { return *_orderBy; }
    OrderByClause& getOrderBy() { return *_orderBy; }
    void setOrderBy(boost::shared_ptr<OrderByClause> o) { _orderBy = o; }

    bool hasGroupBy() const { return static_cast<bool>(_groupBy); }
    GroupByClause const& getGroupBy() const { return *_groupBy; }
    GroupByClause& getGroupBy() { return *_groupBy; }
    void setGroupBy(boost::shared_ptr<GroupByClause> g) { _groupBy = g; }

    bool hasHaving() const { return static_cast<bool>(_having); }
    HavingClause const& getHaving() const { return *_having; }
    HavingClause& getHaving() { return *_having; }
    void setHaving(boost::shared_ptr<HavingClause> h) { _having = h; }

 private:
    // Declarations
    friend class parser::SelectFactory;

    // Helpers
    void _print();
    std::string _generateDbg();

    // Fields
    boost::shared_ptr<FromList> _fromList; // Data sources
    boost::shared_ptr<SelectList> _selectList; // Desired columns
    boost::shared_ptr<WhereClause> _whereClause; // Filtering conditions (WHERE)
    boost::shared_ptr<OrderByClause> _orderBy; // Ordering
    boost::shared_ptr<GroupByClause> _groupBy; // Aggr. grouping
    boost::shared_ptr<HavingClause> _having; // Aggr. grouping

    bool _hasDistinct; ///< SELECT DISTINCT (consider merging with ALL)

    int  _limit; // result limit
    StringVector OutputMods; // Output modifiers (order, grouping,
                           // sort, limit
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_SELECTSTMT_H
