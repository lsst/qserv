// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
// ValueFactor is a term in a ValueExpr's "term (term_op term)*" phrase
#ifndef LSST_QSERV_MASTER_VALUEFACTOR_H
#define LSST_QSERV_MASTER_VALUEFACTOR_H
/**
  * @file ValueFactor.h
  *
  * @author Daniel L. Wang, SLAC
  */
#include <list>
#include <string>
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/ColumnRef.h"

namespace lsst {
namespace qserv {
namespace master {
// Forward
class QueryTemplate;
class FuncExpr;
class ValueExpr; // To support nested expressions.

class ValueFactor;
typedef boost::shared_ptr<ValueFactor> ValueFactorPtr;
typedef std::list<ValueFactorPtr> ValueFactorList;

/// ValueFactor is some kind of value that can exist in a column. It can be
/// logical (i.e. a column name) or physical (a constant number or value).
class ValueFactor {
public:
    enum Type { COLUMNREF, FUNCTION, AGGFUNC, STAR, CONST, EXPR };

    // May need non-const, otherwise, need new construction
    boost::shared_ptr<ColumnRef const> getColumnRef() const { return _columnRef; }
    boost::shared_ptr<ColumnRef> getColumnRef() { return _columnRef; }
    boost::shared_ptr<FuncExpr const> getFuncExpr() const { return _funcExpr; }
    boost::shared_ptr<FuncExpr> getFuncExpr() { return _funcExpr; }
    boost::shared_ptr<ValueExpr const> getExpr() const { return _valueExpr; }
    boost::shared_ptr<ValueExpr> getExpr() { return _valueExpr; }
    Type getType() const { return _type; }

    std::string const& getAlias() const { return _alias; }
    void setAlias(std::string const& a) { _alias = a; }
    // TableStar is used for CONST literals as well.
    std::string const& getTableStar() const { return _tableStar; }
    void setTableStar(std::string const& a) { _tableStar = a; }

    void findColumnRefs(ColumnRef::List& list);

    ValueFactorPtr clone() const;

    static ValueFactorPtr newColumnRefFactor(boost::shared_ptr<ColumnRef const> cr);
    static ValueFactorPtr newStarFactor(std::string const& table);
    static ValueFactorPtr newAggFactor(boost::shared_ptr<FuncExpr> fe);
    static ValueFactorPtr newFuncFactor(boost::shared_ptr<FuncExpr> fe);
    static ValueFactorPtr newConstFactor(std::string const& alnum);
    static ValueFactorPtr newExprFactor(boost::shared_ptr<ValueExpr> ve);

    friend std::ostream& operator<<(std::ostream& os, ValueFactor const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueFactor const* ve);

    class render;
    friend class render;
private:
    Type _type;
    boost::shared_ptr<ColumnRef> _columnRef;
    boost::shared_ptr<FuncExpr> _funcExpr;
    boost::shared_ptr<ValueExpr> _valueExpr;
    std::string _alias;
    std::string _tableStar; // Reused as const val (no tablestar)
};

class ValueFactor::render : public std::unary_function<ValueFactor, void> {
public:
    render(QueryTemplate& qt) : _qt(qt) {}
    void operator()(ValueFactor const& ve);
    void operator()(ValueFactor const* vep) {
        if(vep) (*this)(*vep); }
    void operator()(boost::shared_ptr<ValueFactor> const& vep) {
        (*this)(vep.get()); }
    QueryTemplate& _qt;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_VALUEFACTOR_H
