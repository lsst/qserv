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
/**
  * @file
  *
  * @brief OrderByClause is a representation of a SQL ORDER BY clause.  It
  * consists of OrderByTerm objects.
  *
  * @author Daniel L. Wang, SLAC
  */


#ifndef LSST_QSERV_QUERY_ORDERBYCLAUSE_H
#define LSST_QSERV_QUERY_ORDERBYCLAUSE_H


// System headers
#include <deque>
#include <memory>
#include <string>

// Local headers
#include "query/typedefs.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// OrderByTerm is an element of an OrderByClause
class OrderByTerm {
public:
    enum Order {DEFAULT, ASC, DESC};
    class render;

    OrderByTerm() : _order(DEFAULT) {}
    OrderByTerm(std::shared_ptr<ValueExpr> val,
                Order order=DEFAULT,
                std::string collate=std::string())
    : _expr(val)
    , _order(order)
    , _collate(collate)
    {}

    OrderByTerm(OrderByTerm const& rhs);

    ~OrderByTerm() {}

    std::string sqlFragment() const;
    std::shared_ptr<ValueExpr>& getExpr() { return _expr; }
    std::shared_ptr<ValueExpr> const& getExpr() const { return _expr; }
    void setExpr(std::shared_ptr<ValueExpr> const& expr) { _expr = expr; }
    Order getOrder() const;
    void renderTo(QueryTemplate& qt) const;

    bool operator==(const OrderByTerm& rhs) const;

private:
    friend std::ostream& operator<<(std::ostream& os, OrderByTerm const& ob);
    friend class render;

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

    OrderByClause(OrderByClause const& rhs);

    // Construct an OrderByClause that owns the given vector of OrderByTerm.
    OrderByClause(std::shared_ptr<OrderByTermVector> const& terms) : _terms(terms) {}

    ~OrderByClause() {}

    std::string sqlFragment() const;
    void renderTo(QueryTemplate& qt) const;
    std::shared_ptr<OrderByClause> clone() const;
    std::shared_ptr<OrderByClause> copySyntax();
    std::shared_ptr<OrderByTermVector> getTerms() const { return _terms; }
    void addTerm(const OrderByTerm& term) { _terms->push_back(term); }

    void findValueExprs(ValueExprPtrVector& list) const;
    void findValueExprRefs(ValueExprPtrRefVector& list);

    bool operator==(const OrderByClause& rhs) const;

private:
    friend std::ostream& operator<<(std::ostream& os, OrderByClause const& oc);
    friend std::ostream& operator<<(std::ostream& os, OrderByClause const* oc);

    void _addTerm(OrderByTerm const& t) {_terms->push_back(t); }
    std::shared_ptr<std::vector<OrderByTerm>> _terms;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_ORDERBYCLAUSE_H
