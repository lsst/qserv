// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_VALUEFACTOR_H
#define LSST_QSERV_QUERY_VALUEFACTOR_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>
#include <string>

// Local headers
#include "query/ColumnRef.h"

namespace lsst {
namespace qserv {
namespace query {

// Forward
class QueryTemplate;
class FuncExpr;
class ValueExpr; // To support nested expressions.

class ValueFactor;
typedef std::shared_ptr<ValueFactor> ValueFactorPtr;

/// ValueFactor is some kind of value that can exist in a column. It can be
/// logical (i.e. a column name) or physical (a constant number or value).
class ValueFactor {
public:
    enum Type { COLUMNREF, FUNCTION, AGGFUNC, STAR, CONST, EXPR };
    static std::string getTypeString(Type t) {
        switch(t) {
        case COLUMNREF: return "COLUMNREF";
        case FUNCTION:  return "FUNCTION";
        case AGGFUNC:   return "AGGFUNC";
        case STAR:      return "STAR";
        case CONST:     return "CONST";
        case EXPR:      return "EXPR";
        }
        return "unknown";
    }

    // May need non-const, otherwise, need new construction
    std::shared_ptr<ColumnRef const> getColumnRef() const { return _columnRef; }
    std::shared_ptr<ColumnRef> getColumnRef() { return _columnRef; }
    std::shared_ptr<FuncExpr const> getFuncExpr() const { return _funcExpr; }
    std::shared_ptr<FuncExpr> getFuncExpr() { return _funcExpr; }
    std::shared_ptr<ValueExpr const> getExpr() const { return _valueExpr; }
    std::shared_ptr<ValueExpr> getExpr() { return _valueExpr; }
    Type getType() const { return _type; }

    std::string const& getAlias() const { return _alias; }
    void setAlias(std::string const& a) { _alias = a; }
    // TableStar is used for CONST literals as well.
    std::string const& getTableStar() const { return _tableStar; }
    void setTableStar(std::string const& a) { _tableStar = a; }

    void findColumnRefs(ColumnRef::Vector& vector) const;

    ValueFactorPtr clone() const;

    static ValueFactorPtr newColumnRefFactor(std::shared_ptr<ColumnRef const> cr);
    static ValueFactorPtr newStarFactor(std::string const& table);
    static ValueFactorPtr newAggFactor(std::shared_ptr<FuncExpr> fe);
    static ValueFactorPtr newFuncFactor(std::shared_ptr<FuncExpr> fe);
    static ValueFactorPtr newConstFactor(std::string const& alnum);
    static ValueFactorPtr newExprFactor(std::shared_ptr<ValueExpr> ve);

    friend std::ostream& operator<<(std::ostream& os, ValueFactor const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueFactor const* ve);

    class render;
    friend class render;

    std::ostream& dbgPrint(std::ostream& os) const;

private:
    Type _type;
    std::shared_ptr<ColumnRef> _columnRef;
    std::shared_ptr<FuncExpr> _funcExpr;
    std::shared_ptr<ValueExpr> _valueExpr;
    std::string _alias;
    std::string _tableStar; // Reused as const val (no tablestar)
};


class ValueFactor::render {
public:
    render(QueryTemplate& qt) : _qt(qt) {}
    void applyToQT(ValueFactor const& ve);
    void applyToQT(ValueFactor const* vep) {
        if(vep) applyToQT(*vep);
    }
    void applyToQT(std::shared_ptr<ValueFactor> const& vep) {
        applyToQT(vep.get());
    }
    QueryTemplate& _qt;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_VALUEFACTOR_H
