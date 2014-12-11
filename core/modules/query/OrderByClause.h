// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
#ifndef LSST_QSERV_QUERY_ORDERBYCLAUSE_H
#define LSST_QSERV_QUERY_ORDERBYCLAUSE_H
/**
  * @file
  *
  * @brief OrderByClause is a representation of a SQL ORDER BY clause.  It
  * consists of OrderByTerm objects.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <deque>
#include <list>
#include <string>

// Third party headers
#include "boost/shared_ptr.hpp"
#include "boost/make_shared.hpp"

namespace lsst {
namespace qserv {

namespace parser {
    // Forward
    class ModFactory;
}

namespace query {

// Forward declarations
class QueryTemplate;
class ValueExpr;

typedef boost::shared_ptr<ValueExpr> ValueExprPtr;
typedef std::list<ValueExprPtr> ValueExprList;

/// OrderByTerm is an element of an OrderByClause
class OrderByTerm {
public:
    enum Order {DEFAULT, ASC, DESC};
    class render;

    OrderByTerm() {}
    OrderByTerm(boost::shared_ptr<ValueExpr> val,
                Order _order,
                std::string _collate);

    ~OrderByTerm() {}

    boost::shared_ptr<ValueExpr>& getExpr() { return _expr; }
    Order getOrder() const;
    std::string getCollate() const;
    void renderTo(QueryTemplate& qt) const;

private:
    friend std::ostream& operator<<(std::ostream& os, OrderByTerm const& ob);
    friend class render;
    friend class parser::ModFactory;

    boost::shared_ptr<ValueExpr> _expr;
    Order _order;
    std::string _collate;
};

/// OrderByClause is a parsed SQL ORDER BY ... clause
class OrderByClause {
public:
    typedef boost::shared_ptr<OrderByClause> Ptr;
    typedef std::deque<OrderByTerm> List;

    OrderByClause() : _terms(boost::make_shared<List>()) {}
    ~OrderByClause() {}

    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<OrderByClause> clone() const;
    boost::shared_ptr<OrderByClause> copySyntax();

    void findValueExprs(ValueExprList& list);
private:
    friend std::ostream& operator<<(std::ostream& os, OrderByClause const& oc);
    friend class parser::ModFactory;

    void _addTerm(OrderByTerm const& t) {_terms->push_back(t); }
    boost::shared_ptr<List> _terms;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_ORDERBYCLAUSE_H

