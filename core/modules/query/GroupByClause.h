// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_GROUPBYCLAUSE_H
#define LSST_QSERV_QUERY_GROUPBYCLAUSE_H
/**
  * @file
  *
  * @brief GroupByClause is a representation of a group-by clause element.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <deque>
#include <string>

// Third-party headers
#include "boost/make_shared.hpp"
#include "boost/shared_ptr.hpp"

// Local headers
#include "query/types.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace parser {
    class ModFactory;
}

namespace query {

// Forward declarations
class QueryTemplate;

// GroupByTerm is a element of a GroupByClause
class GroupByTerm {
public:
    class render;
    friend class render;

    GroupByTerm() {}
    ~GroupByTerm() {}

    ValueExprPtr& getExpr() { return _expr; }
    std::string getCollate() const { return _collate; }
    GroupByTerm cloneValue() const;

    GroupByTerm& operator=(GroupByTerm const& gb);

private:
    friend std::ostream& operator<<(std::ostream& os, GroupByTerm const& gb);
    friend class parser::ModFactory;

    ValueExprPtr _expr;
    std::string _collate;
};

/// GroupByClause is a parsed GROUP BY ... element.
class GroupByClause {
public:
    typedef boost::shared_ptr<GroupByClause> Ptr;
    typedef std::deque<GroupByTerm> List;

    GroupByClause() : _terms(boost::make_shared<List>()) {}
    ~GroupByClause() {}

    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<GroupByClause> clone() const;
    boost::shared_ptr<GroupByClause> copySyntax();

    void findValueExprs(ValueExprPtrVector& list);

private:
    friend std::ostream& operator<<(std::ostream& os, GroupByClause const& gc);
    friend class parser::ModFactory;

    void _addTerm(GroupByTerm const& t) { _terms->push_back(t); }
    boost::shared_ptr<List> _terms;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_GROUPBYCLAUSE_H

