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
#include <memory>

// Local headers
#include "global/constants.h"
#include "global/stringTypes.h"
#include "query/OrderByClause.h"
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
    typedef std::shared_ptr<SelectStmt> Ptr;
    typedef std::shared_ptr<SelectStmt const> Cptr;

    SelectStmt();

    std::shared_ptr<WhereClause const> getWhere() const;
    QueryTemplate getQueryTemplate() const;
    QueryTemplate getPostTemplate() const;
    std::shared_ptr<SelectStmt> clone() const;

    /**
     *  @brief Create a merge statement for current object
     *
     *  Starting from a shallow copy, copy only the pieces that matter for the merge clause.
     *  SQL doesn't guarantee result order so ORDER BY clause must be executed on mysql-proxy
     *  during result retrieval and not during merging
     *
     * @return: A proposal for merge statement, which will be finalized by query plugins
     */
    std::shared_ptr<SelectStmt> copyMerge() const;

    bool getDistinct() const { return _hasDistinct; }
    void setDistinct(bool d) { _hasDistinct = d; }

    SelectList const& getSelectList() const { return *_selectList; }
    SelectList& getSelectList() { return *_selectList; }
    void setSelectList(std::shared_ptr<SelectList> s) { _selectList = s; }

    FromList const& getFromList() const { return *_fromList; }
    FromList& getFromList() { return *_fromList; }
    void setFromList(std::shared_ptr<FromList> f) { _fromList = f; }
    void setFromListAsTable(std::string const& t);

    bool hasWhereClause() const { return static_cast<bool>(_whereClause); }
    WhereClause const& getWhereClause() const { return *_whereClause; }
    WhereClause& getWhereClause() { return *_whereClause; }
    void setWhereClause(std::shared_ptr<WhereClause> w) { _whereClause = w; }

    /**
     * @brief Get LIMIT value in LIMIT clause for a SQL query
     *
     * @return LIMIT value, lsst::qserv::NOTSET if not specified
     *
     * @see lsst::qserv::parser::ModFactory::getLimit()
     */
    int getLimit() const { return _limit; }

    /**
     * @brief Indicate existence of a LIMIT clause
     *
     * @return: true if LIMIT clause exists, else false
     */
    bool hasLimit() const { return _limit != lsst::qserv::NOTSET; }

    bool hasOrderBy() const { return static_cast<bool>(_orderBy); }
    OrderByClause const& getOrderBy() const { return *_orderBy; }
    OrderByClause& getOrderBy() { return *_orderBy; }
    void setOrderBy(std::shared_ptr<OrderByClause> o) { _orderBy = o; }

    bool hasGroupBy() const { return static_cast<bool>(_groupBy); }
    GroupByClause const& getGroupBy() const { return *_groupBy; }
    GroupByClause& getGroupBy() { return *_groupBy; }
    void setGroupBy(std::shared_ptr<GroupByClause> g) { _groupBy = g; }

    bool hasHaving() const { return static_cast<bool>(_having); }
    HavingClause const& getHaving() const { return *_having; }
    HavingClause& getHaving() { return *_having; }
    void setHaving(std::shared_ptr<HavingClause> h) { _having = h; }

    // Helpers, for debugging

    /** Return a string representation of the object
     *
     * Used only for debugging, or logging
     * Use getQueryTemplate().toString() to get the actual SQL query
     *
     * @return a string representation of the object
     */
    std::string toString();

    /** Output operator for SelectStmt
     *
     *  Used only for debugging, or logging
     *  Use getQueryTemplate() output operator to get the actual SQL query
     *
     *  @param os: std::ostream which will contain object output
     *  @param selectStmt: SelectStmt to output
     *
     *  @return std::ostream containing selectStmt output
     *
     *  @warning this function always add a trailing whitespace
     */
    friend std::ostream& operator<<(std::ostream& os, SelectStmt const& selectStmt);

 private:
    // Declarations
    friend class parser::SelectFactory;

    // Fields
    std::shared_ptr<FromList> _fromList; // Data sources
    std::shared_ptr<SelectList> _selectList; // Desired columns
    std::shared_ptr<WhereClause> _whereClause; // Filtering conditions (WHERE)
    std::shared_ptr<OrderByClause> _orderBy; // Ordering
    std::shared_ptr<GroupByClause> _groupBy; // Aggr. grouping
    std::shared_ptr<HavingClause> _having; // Aggr. grouping

    bool _hasDistinct; ///< SELECT DISTINCT (consider merging with ALL)

    int  _limit; // result limit
    StringVector OutputMods; // Output modifiers (order, grouping,
                             // sort, limit
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_SELECTSTMT_H
