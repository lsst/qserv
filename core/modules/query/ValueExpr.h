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
#ifndef LSST_QSERV_QUERY_VALUEEXPR_H
#define LSST_QSERV_QUERY_VALUEEXPR_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "query/ColumnRef.h"
#include "query/typedefs.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace parser {
    class ValueExprFactory;
}
namespace query {
    class QueryTemplate;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {

class ValueFactor;
/// ValueExpr is a general value expression in a SQL statement. It is allowed to
/// have an alias and a single level of ValueFactors joined by arithmetic
/// operators. No nesting is allowed yet.
class ValueExpr {
public:
    ValueExpr();
    enum Op {NONE=200, UNKNOWN, PLUS, MINUS, MULTIPLY, DIVIDE};
    struct FactorOp {
        explicit FactorOp(boost::shared_ptr<ValueFactor> factor_, Op op_=NONE)
            : factor(factor_), op(op_) {}
        FactorOp() {}
        boost::shared_ptr<ValueFactor> factor;
        Op op;
    };
    typedef std::vector<FactorOp> FactorOpVector;
    friend std::ostream& operator<<(std::ostream& os, FactorOp const& fo);

    std::string const& getAlias() const { return _alias; }
    void setAlias(std::string const& a) { _alias = a; }

    /// @return a list of ValueFactor-Op
    FactorOpVector& getFactorOps() { return _factorOps; }
    /// @return a const list of ValueFactor-Op
    FactorOpVector const& getFactorOps() const { return _factorOps; }

    boost::shared_ptr<ColumnRef> copyAsColumnRef() const;
    std::string copyAsLiteral() const;
    template<typename T>
    T copyAsType(T const& defaultValue) const;

    void findColumnRefs(ColumnRef::Vector& vector);

    bool isStar() const;
    bool isFactor() const;
    boost::shared_ptr<ValueFactor const> getFactor() const;

    // Convenience checkers
    bool isColumnRef() const;

    ValueExprPtr clone() const;
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const* ve);

    static ValueExprPtr newSimple(boost::shared_ptr<ValueFactor> vt);

    friend class parser::ValueExprFactory;
    class render;
    friend class render;
private:
    std::string _alias;
    FactorOpVector _factorOps;
};
/// A helper functor for rendering to QueryTemplates
class ValueExpr::render : public std::unary_function<ValueExpr, void> {
public:
    render(QueryTemplate& qt, bool needsComma, bool isProtected=false)
        : _qt(qt),
          _needsComma(needsComma),
          _isProtected(isProtected),
          _count(0)
        {}
    void operator()(ValueExpr const& ve);
    void operator()(ValueExpr const* vep) {
        if(vep) (*this)(*vep); }
    void operator()(boost::shared_ptr<ValueExpr> const& vep) {
        (*this)(vep.get()); }
    QueryTemplate& _qt;
    bool _needsComma;
    bool _isProtected;
    int _count;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_VALUEEXPR_H
