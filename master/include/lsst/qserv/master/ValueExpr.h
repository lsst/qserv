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
#ifndef LSST_QSERV_MASTER_VALUEEXPR_H
#define LSST_QSERV_MASTER_VALUEEXPR_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */
#include <iostream>
#include <list>
#include <sstream>
#include <string>
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/ColumnRef.h"

namespace lsst {
namespace qserv {
namespace master {
// Forward
class QueryTemplate;

class ValueExpr;
typedef boost::shared_ptr<ValueExpr> ValueExprPtr;
typedef std::list<ValueExprPtr> ValueExprList;
class ValueFactor;
/// ValueExpr is a general value expression in a SQL statement. It is allowed to
/// have an alias and a single level of ValueFactors joined by arithmetic
/// operators. No nesting is allowed yet.
class ValueExpr {
public:
    ValueExpr();
    enum Op {NONE=200, UNKNOWN, PLUS, MINUS, MULTIPLY, DIVIDE};
    struct FactorOp {
        boost::shared_ptr<ValueFactor> factor;
        Op op;
    };
    typedef std::list<FactorOp> FactorOpList;
    friend std::ostream& operator<<(std::ostream& os, FactorOp const& fo);

    std::string const& getAlias() const { return _alias; }
    void setAlias(std::string const& a) { _alias = a; }

    /// @return a list of ValueFactor-Op
    FactorOpList& getFactorOps() { return _factorOps; }
    /// @return a const list of ValueFactor-Op
    FactorOpList const& getFactorOps() const { return _factorOps; }

    boost::shared_ptr<ColumnRef> castAsColumnRef() const;
    std::string castAsLiteral() const;
    template<typename T>
    T castAsType(T const& defaultValue) const;

    void findColumnRefs(ColumnRef::List& list);

    ValueExprPtr clone() const;
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const* ve);

    static ValueExprPtr newSimple(boost::shared_ptr<ValueFactor> vt);

    friend class ValueExprFactory;
    class render;
    friend class render;
private:
    std::string _alias;
    std::list<FactorOp> _factorOps;
};
/// A helper functor for rendering to QueryTemplates
class ValueExpr::render : public std::unary_function<ValueExpr, void> {
public:
    render(QueryTemplate& qt, bool needsComma)
        : _qt(qt), _needsComma(needsComma), _count(0) {}
    void operator()(ValueExpr const& ve);
    void operator()(ValueExpr const* vep) {
        if(vep) (*this)(*vep); }
    void operator()(boost::shared_ptr<ValueExpr> const& vep) {
        (*this)(vep.get()); }
    QueryTemplate& _qt;
    bool _needsComma;
    int _count;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_VALUEEXPR_H
