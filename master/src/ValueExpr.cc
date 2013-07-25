/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
  * @file ValueExpr.cc
  *
  * @brief ValueExpr implementat. A ValueExpr is an object
  * object containing elements of a SQL value expresssion (construct
  * that evaluates to a [non-boolean] SQL primitive value).
  *
  * ValueExpr elements are formed as 'term (op term)*' .
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/ValueExpr.h"
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <boost/make_shared.hpp>
#include "lsst/qserv/master/ValueFactor.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/FuncExpr.h"

namespace lsst {
namespace qserv {
namespace master {

std::ostream&
output(std::ostream& os, ValueExprList const& vel) {
    std::copy(vel.begin(), vel.end(),
              std::ostream_iterator<ValueExprPtr>(os, ";"));
    return os;
}
void
renderList(QueryTemplate& qt, ValueExprList const& vel) {
    std::for_each(vel.begin(), vel.end(), ValueExpr::render(qt, true));
}

////////////////////////////////////////////////////////////////////////
// ValueExpr::FactorOp
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, ValueExpr::FactorOp const& fo) {
    os << "FACT:" << fo.factor << " OP:";
    char const* opStr;
    switch(fo.op) {
    case ValueExpr::NONE: opStr = "NONE"; break;
    case ValueExpr::UNKNOWN: opStr = "UNKNOWN"; break;
    case ValueExpr::PLUS: opStr = "PLUS"; break;
    case ValueExpr::MINUS: opStr = "MINUS"; break;
    case ValueExpr::MULTIPLY: opStr = "MULTIPLY"; break;
    case ValueExpr::DIVIDE: opStr = "DIVIDE"; break;
    default: opStr = "broken"; break;
    }
    os << opStr;
    return os;
}

////////////////////////////////////////////////////////////////////////
// ValueExpr statics
////////////////////////////////////////////////////////////////////////
ValueExprPtr ValueExpr::newSimple(boost::shared_ptr<ValueFactor> vt)  {
    if(!vt) {
        throw std::invalid_argument("Unexpected NULL ValueFactor");
    }
    boost::shared_ptr<ValueExpr> ve(new ValueExpr);
    FactorOp t = {vt, NONE};
    ve->_factorOps.push_back(t);
    return ve;
}
////////////////////////////////////////////////////////////////////////
// ValueExpr
////////////////////////////////////////////////////////////////////////
ValueExpr::ValueExpr() {
}

boost::shared_ptr<ColumnRef> ValueExpr::copyAsColumnRef() const {
    boost::shared_ptr<ColumnRef> cr;
    if(_factorOps.size() != 1) { return cr; } // Empty or Not a single ColumnRef
    boost::shared_ptr<ValueFactor> factor = _factorOps.front().factor;
    assert(factor);
    cr.reset(new ColumnRef(*factor->getColumnRef()));
    return cr;
}

std::string ValueExpr::copyAsLiteral() const{
    std::string s;
    // Make sure there is only one factor.
    if(_factorOps.empty() || (_factorOps.size() > 1)) { return s; }

    boost::shared_ptr<ValueFactor> factor = _factorOps.front().factor;
    assert(factor);
    if(factor->getType() != ValueFactor::CONST) { return s; }
    return factor->getTableStar();
}

template<typename T>
T ValueExpr::copyAsType(T const& defaultValue) const {
    std::string literal = copyAsLiteral();
    std::istringstream is(literal);
    T value;
    is >> value;
    std::ostringstream os;
    os << value;
    if (os.str() != literal) {
        return defaultValue;
    }
    return value;
}
template<>
float ValueExpr::copyAsType<float>(float const& defaultValue) const;
template<>
double ValueExpr::copyAsType<double>(double const& defaultValue) const;

template int ValueExpr::copyAsType<int>(int const&) const;


void ValueExpr::findColumnRefs(ColumnRef::List& list) {
    for(FactorOpList::iterator i=_factorOps.begin();
        i != _factorOps.end(); ++i) {
        assert(i->factor); // FactorOps should never have null ValueFactors
        i->factor->findColumnRefs(list);
    }
}

ValueExprPtr ValueExpr::clone() const {
    // First, make a shallow copy
    ValueExprPtr expr(new ValueExpr(*this));
    FactorOpList::iterator ti = expr->_factorOps.begin();
    for(FactorOpList::const_iterator i=_factorOps.begin();
        i != _factorOps.end(); ++i, ++ti) {
        // Deep copy (clone) each factor.
        ti->factor = i->factor->clone();
    }
    return expr;
}


std::ostream& operator<<(std::ostream& os, ValueExpr const& ve) {
    // Reuse QueryTemplate-based rendering
    QueryTemplate qt;
    ValueExpr::render render(qt, false);
    render(ve);
    os << qt.dbgStr();
    return os;
}

std::ostream& operator<<(std::ostream& os, ValueExpr const* ve) {
    if(!ve) return os << "<NULL>";
    return os << *ve;
}
////////////////////////////////////////////////////////////////////////
// ValueExpr::render
////////////////////////////////////////////////////////////////////////
void ValueExpr::render::operator()(ValueExpr const& ve) {
    if(_needsComma && _count++ > 0) { _qt.append(","); }
    ValueFactor::render render(_qt);
    if(ve._factorOps.size() > 1) { // Need opening parenthesis
        _qt.append("(");
    }
    for(FactorOpList::const_iterator i=ve._factorOps.begin();
        i != ve._factorOps.end(); ++i) {
        render(i->factor);
        switch(i->op) {
        case ValueExpr::NONE: break;
        case ValueExpr::UNKNOWN: _qt.append("<UNKNOWN_OP>"); break;
        case ValueExpr::PLUS: _qt.append("+"); break;
        case ValueExpr::MINUS: _qt.append("-"); break;
        case ValueExpr::MULTIPLY: _qt.append("*"); break;
        case ValueExpr::DIVIDE: _qt.append("/"); break;
        default:
            std::ostringstream ss;
            ss << "Corruption: bad _op in ValueExpr optype=" << i->op;
            // FIXME: Make sure this never happens.
            throw ss.str();
        }
    }
    if(ve._factorOps.size() > 1) { // Need closing parenthesis
        _qt.append(")");
    }
    if(!ve._alias.empty()) { _qt.append("AS"); _qt.append(ve._alias); }
}

}}} // lsst::qserv::master
