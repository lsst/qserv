// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 LSST Corporation.
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
#include <memory>
#include <string>

// Local headers
#include "query/typedefs.h"

namespace lsst {
namespace qserv {

namespace parser {
    // Forward
    class ModFactory;
}

namespace query {

// Forward declarations
class QueryTemplate;

/// OrderByTerm is an element of an OrderByClause
class OrderByTerm {
public:
    enum Order {DEFAULT, ASC, DESC};
    class render;

    OrderByTerm() : _order(DEFAULT) {}
    OrderByTerm(std::shared_ptr<ValueExpr> val,
                Order _order,
                std::string _collate);

    ~OrderByTerm() {}

    std::string sqlFragment() const;
    std::shared_ptr<ValueExpr>& getExpr() { return _expr; }
    std::shared_ptr<ValueExpr> const& getExpr() const { return _expr; }
    Order getOrder() const;
    void renderTo(QueryTemplate& qt) const;

private:
    friend std::ostream& operator<<(std::ostream& os, OrderByTerm const& ob);
    friend class render;
    friend class parser::ModFactory;

    std::shared_ptr<ValueExpr> _expr;
    Order _order;
    std::string _collate;
};

/// OrderByClause is a parsed SQL ORDER BY ... clause
class OrderByClause {
public:
    typedef std::shared_ptr<OrderByClause> Ptr;
    typedef std::vector<OrderByTerm> OrderByTermVector;

    OrderByClause() : _terms(std::make_shared<OrderByTermVector>()) {}
    ~OrderByClause() {}

    std::string sqlFragment() const;
    void renderTo(QueryTemplate& qt) const;
    std::shared_ptr<OrderByClause> clone() const;
    std::shared_ptr<OrderByClause> copySyntax();
    std::shared_ptr<OrderByTermVector> getTerms() { return _terms; }

    void findValueExprs(ValueExprPtrVector& list);
private:
    friend std::ostream& operator<<(std::ostream& os, OrderByClause const& oc);
    friend class parser::ModFactory;

    void _addTerm(OrderByTerm const& t) {_terms->push_back(t); }
    std::shared_ptr<OrderByTermVector> _terms;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_ORDERBYCLAUSE_H

