// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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
  * @brief ValueExpr implementat. A ValueExpr is an object
  * object containing elements of a SQL value expresssion (construct
  * that evaluates to a [non-boolean] SQL primitive value).
  *
  * ValueExpr elements are formed as 'term (op term)*' .
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/ValueExpr.h"

// System headers
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>

// Qserv headers
#include "qana/CheckAggregation.h"
#include "query/FuncExpr.h"
#include "query/QueryTemplate.h"
#include "query/ValueFactor.h"

namespace lsst {
namespace qserv {
namespace query {

std::ostream&
output(std::ostream& os, ValueExprPtrVector const& vel) {
    std::copy(vel.begin(), vel.end(),
              std::ostream_iterator<ValueExprPtr>(os, ";"));
    return os;
}
void
renderList(QueryTemplate& qt, ValueExprPtrVector const& vel) {
    std::for_each(vel.begin(), vel.end(), ValueExpr::render(qt, true, true));
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
ValueExprPtr ValueExpr::newSimple(std::shared_ptr<ValueFactor> vt)  {
    if (!vt) {
        throw std::invalid_argument("Unexpected NULL ValueFactor");
    }
    std::shared_ptr<ValueExpr> ve = std::make_shared<ValueExpr>();
    FactorOp t(vt, NONE);
    ve->_factorOps.push_back(t);
    return ve;
}
////////////////////////////////////////////////////////////////////////
// ValueExpr
////////////////////////////////////////////////////////////////////////
ValueExpr::ValueExpr() {
}

/** Print a string representation of the object
 *
 * @param out
 * @return void
 */
void ValueExpr::dbgPrint(std::ostream& os) {
    ColumnRef::Vector cList;
    os << "ValueExpr:\t\"" << *this << "\"\n";
    os << "\tAlias:\t\"" << this->_alias << "\"\n";
    os << "\tIsColumnRef:\t" << this->isColumnRef() << "\n";
    os << "\tIsFactor:\t" << this->isFactor() << "\n";
    os << "\tIsStar:\t" << this->isStar() << "\n";
    this->findColumnRefs(cList);
    typedef ColumnRef::Vector::const_iterator ColIter;
    for(ColIter i=cList.begin(), e=cList.end(); i != e; ++i) {
        os << "\tColumnRef:\t" << *i << "\n";
    }
    bool hasAgg = false;
    qana::CheckAggregation ca(hasAgg);
    for(FactorOpVector::const_iterator i=this->_factorOps.begin(),
            e=this->_factorOps.end(); i != e; ++i) {
            ca(*i);
            os << "\tFactorOp:\t\"" << *i << "\", hasAgg: " << hasAgg << "\n";
    }
}

std::shared_ptr<ColumnRef> ValueExpr::copyAsColumnRef() const {
    std::shared_ptr<ColumnRef> cr;
    if (_factorOps.size() != 1) { return cr; } // Empty or Not a single ColumnRef
    std::shared_ptr<ValueFactor> factor = _factorOps.front().factor;
    assert(factor);
    cr = factor->getColumnRef();
    if (cr) {
        cr = std::make_shared<ColumnRef>(*cr);  // Make a copy
    }
    return cr;
}

std::string ValueExpr::copyAsLiteral() const{
    std::string s;
    // Make sure there is only one factor.
    if (_factorOps.empty() || (_factorOps.size() > 1)) { return s; }

    std::shared_ptr<ValueFactor> factor = _factorOps.front().factor;
    assert(factor);
    if (factor->getType() != ValueFactor::CONST) { return s; }
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


void ValueExpr::findColumnRefs(ColumnRef::Vector& vector) {
    for(FactorOpVector::iterator i=_factorOps.begin();
        i != _factorOps.end(); ++i) {
        assert(i->factor); // FactorOps should never have null ValueFactors
        i->factor->findColumnRefs(vector);
    }
}

/** Check if the current ValueExpr contains an aggregation function.
 *  This function assume the ValueExpr was part of a SelectList
 * @return true if the object contains an aggregation function
 */
bool ValueExpr::hasAggregation() const {
    bool hasAgg = false;
    qana::CheckAggregation ca(hasAgg);
    std::for_each(_factorOps.begin(), _factorOps.end(), ca);
    return hasAgg;
}

ColumnRef::Ptr ValueExpr::getColumnRef() const {
    if (_factorOps.size() != 1) {
        return nullptr;
    }
    ValueFactorPtr const& vf = _factorOps.front().factor;
    if (!vf) {
        return nullptr;
    }
    return vf->getColumnRef();
}

/// @return true if holding a single ValueFactor, and that factor is a *
bool ValueExpr::isStar() const {
    if (!_factorOps.empty() && _factorOps.size() == 1) {
        std::shared_ptr<ValueFactor const> vf = getFactor();
        if (!vf) {
            throw std::invalid_argument("ValueExpr::isStar null ValueFactor");
        }
        return vf->getType() == ValueFactor::STAR;
    }
    return false;
}

/// @return true if holding a single ValueFactor
bool ValueExpr::isFactor() const {
    return _factorOps.size() == 1;
}
/// @return first ValueFactorPtr held. Useful when isFactor() == true
std::shared_ptr<ValueFactor const> ValueExpr::getFactor() const {
    if (_factorOps.empty()) {
        throw std::logic_error("ValueExpr::getFactor no factors");
    }
    return _factorOps.front().factor;
}

/// @return true if holding a single ValueFactor
bool ValueExpr::isColumnRef() const {
    if (_factorOps.size() == 1) {
        ValueFactor const& factor = *_factorOps.front().factor;
        if (factor.getType() == ValueFactor::COLUMNREF) {
            return true;
        }
    }
    return false;
}

ValueExprPtr ValueExpr::clone() const {
    // First, make a shallow copy
    ValueExprPtr expr = std::make_shared<ValueExpr>(*this);
    FactorOpVector::iterator ti = expr->_factorOps.begin();
    for(FactorOpVector::const_iterator i=_factorOps.begin();
        i != _factorOps.end(); ++i, ++ti) {
        // Deep copy (clone) each factor.
        ti->factor = i->factor->clone();
    }
    return expr;
}

/** Return a string representation of the object
 *
 * @return a string representation of the object
 */
std::string ValueExpr::toString() const {
    std::ostringstream oss;

    oss << *this;
    return oss.str();
}

std::ostream& operator<<(std::ostream& os, ValueExpr const& ve) {
    // Reuse QueryTemplate-based rendering
    QueryTemplate qt;
    ValueExpr::render render(qt, false);
    render(ve);
    os << qt;
    return os;
}

std::ostream& operator<<(std::ostream& os, ValueExpr const* ve) {
    if (!ve) return os << "<NULL>";
    return os << *ve;
}
////////////////////////////////////////////////////////////////////////
// ValueExpr::render
////////////////////////////////////////////////////////////////////////
void ValueExpr::render::operator()(ValueExpr const& ve) {
    if (_needsComma && _count++ > 0) { _qt.append(","); }
    ValueFactor::render render(_qt);
    bool needsClose = false;
    if (!_isProtected && ve._factorOps.size() > 1) { // Need opening parenthesis
        _qt.append("(");
        needsClose = true;
    }
    for(FactorOpVector::const_iterator i=ve._factorOps.begin();
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
    if (needsClose) { // Need closing parenthesis
        _qt.append(")");
    }
    if (!ve._alias.empty()) { _qt.append("AS"); _qt.append(ve._alias); }
}

// Miscellaneous
struct _copyValueExpr {
    ValueExprPtr operator()(ValueExprPtr const& p) {
        return p->clone();
    }
};
void cloneValueExprPtrVector(ValueExprPtrVector& dest,
                             ValueExprPtrVector const& src) {
    dest.resize(src.size()); // Presize destination
    std::transform(src.begin(), src.end(),
                   dest.begin(),
                   _copyValueExpr());
}
}}} // namespace lsst::qserv::query
