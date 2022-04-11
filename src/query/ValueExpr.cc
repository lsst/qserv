// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2017 AURA/LSST.
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
#include <cctype>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/CheckAggregation.h"
#include "query/FuncExpr.h"
#include "query/QueryTemplate.h"
#include "query/SubsetHelper.h"
#include "query/ValueFactor.h"
#include "util/IterableFormatter.h"
#include "util/PointerCompare.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.query.ValueExpr");
}

namespace lsst::qserv::query {

std::ostream& output(std::ostream& os, ValueExprPtrVector const& vel) {
    std::copy(vel.begin(), vel.end(), std::ostream_iterator<ValueExprPtr>(os, ";"));
    return os;
}

void renderList(QueryTemplate& qt, ValueExprPtrVector const& vel) {
    ValueExpr::render rend(qt, true, true);
    for (auto& v : vel) {
        rend.applyToQT(v);
    }
}

////////////////////////////////////////////////////////////////////////
// ValueExpr::FactorOp
////////////////////////////////////////////////////////////////////////

std::ostream& operator<<(std::ostream& os, const ValueExpr::FactorOp& factorOp) {
    os << "FactorOp(";
    os << factorOp.factor;
    switch (factorOp.op) {
        case ValueExpr::NONE:
            os << ", query::ValueExpr::NONE";
            break;
        case ValueExpr::UNKNOWN:
            os << ", query::ValueExpr::UNKNOWN";
            break;
        case ValueExpr::PLUS:
            os << ", query::ValueExpr::PLUS";
            break;
        case ValueExpr::MINUS:
            os << ", query::ValueExpr::MINUS";
            break;
        case ValueExpr::MULTIPLY:
            os << ", query::ValueExpr::MULTIPLY";
            break;
        case ValueExpr::DIVIDE:
            os << ", query::ValueExpr::DIVIDE";
            break;
        case ValueExpr::DIV:
            os << ", query::ValueExpr::DIV";
            break;
        case ValueExpr::MODULO:
            os << ", query::ValueExpr::MODULO";
            break;
        case ValueExpr::MOD:
            os << ", query::ValueExpr::MOD";
            break;
        case ValueExpr::BIT_SHIFT_LEFT:
            os << ", query::ValueExpr::BIT_SHIFT_LEFT";
            break;
        case ValueExpr::BIT_SHIFT_RIGHT:
            os << ", query::ValueExpr::BIT_SHIFT_RIGHT";
            break;
        case ValueExpr::BIT_AND:
            os << ", query::ValueExpr::BIT_AND";
            break;
        case ValueExpr::BIT_OR:
            os << ", query::ValueExpr::BIT_OR";
            break;
        case ValueExpr::BIT_XOR:
            os << ", query::ValueExpr::BIT_XOR";
            break;
        default:
            os << ",  !!unhandled!!";
            break;
    }
    os << ")";
    return os;
}

bool ValueExpr::FactorOp::operator==(const FactorOp& rhs) const {
    return util::ptrCompare<ValueFactor>(factor, rhs.factor) && op == rhs.op;
}

bool ValueExpr::FactorOp::isSubsetOf(FactorOp const& rhs) const {
    if (op != rhs.op) return false;
    return factor->isSubsetOf(*rhs.factor);
}

////////////////////////////////////////////////////////////////////////
// ValueExpr statics
////////////////////////////////////////////////////////////////////////
ValueExprPtr ValueExpr::newSimple(std::shared_ptr<ValueFactor> vt) {
    if (!vt) {
        throw std::invalid_argument("Unexpected NULL ValueFactor");
    }
    std::shared_ptr<ValueExpr> ve = std::make_shared<ValueExpr>();
    FactorOp t(vt, NONE);
    ve->_factorOps.push_back(t);
    return ve;
}

ValueExprPtr ValueExpr::newSimple(std::shared_ptr<ColumnRef> columnRef) {
    if (nullptr == columnRef) {
        throw std::invalid_argument("Unexpected NULL ColumnRef");
    }
    auto valueFactor = std::make_shared<ValueFactor>(columnRef);
    return newSimple(valueFactor);
}

ValueExprPtr ValueExpr::newColumnExpr(std::string const& db, std::string const& table,
                                      std::string const& alias, std::string const& column) {
    return newSimple(query::ValueFactor::newColumnRefFactor(
            std::make_shared<query::ColumnRef>(db, table, alias, column)));
}

ValueExprPtr ValueExpr::newColumnExpr(std::string const& column) {
    return newSimple(query::ValueFactor::newColumnRefFactor(std::make_shared<query::ColumnRef>(column)));
}

////////////////////////////////////////////////////////////////////////
// ValueExpr
////////////////////////////////////////////////////////////////////////
ValueExpr::ValueExpr() {}

ValueExpr::ValueExpr(FactorOpVector factorOpVec) : _factorOps(factorOpVec) {}

void ValueExpr::addValueFactor(std::shared_ptr<query::ValueFactor> valueFactor) {
    query::ValueExpr::FactorOp factorOp;
    factorOp.factor = valueFactor;
    _factorOps.push_back(factorOp);
}

bool ValueExpr::addOp(query::ValueExpr::Op op) {
    if (_factorOps.empty()) {
        return false;
    }
    _factorOps.back().op = op;
    return true;
}

void ValueExpr::setAlias(std::string const& alias) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set alias:" << alias);
    _alias = alias;
}

std::shared_ptr<ColumnRef> ValueExpr::copyAsColumnRef() const {
    std::shared_ptr<ColumnRef> cr;
    if (_factorOps.size() != 1) {
        return cr;
    }  // Empty or Not a single ColumnRef
    std::shared_ptr<ValueFactor> factor = _factorOps.front().factor;
    assert(factor);
    cr = factor->getColumnRef();
    if (cr) {
        cr = std::make_shared<ColumnRef>(*cr);  // Make a copy
    }
    return cr;
}

void ValueExpr::findColumnRefs(ColumnRef::Vector& vector) const {
    for (const auto& factorOp : _factorOps) {
        assert(factorOp.factor);  // FactorOps should never have null ValueFactors
        factorOp.factor->findColumnRefs(vector);
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

bool ValueExpr::isCountStar(std::string* spelling) const {
    if (_factorOps.empty() or _factorOps.size() != 1) {
        return false;
    }
    std::shared_ptr<ValueFactor const> vf = getFactor();
    if (not vf) {
        throw std::invalid_argument("ValueExpr::isCountStar null ValueFactor");
    }
    if (vf->getType() != ValueFactor::AGGFUNC) {
        return false;
    }
    auto aggFunc = vf->getFuncExpr();
    if (not aggFunc) {
        throw std::logic_error("ValueExpr::isCountStar null FuncExpr in AGGFUNC");
    }
    auto name = aggFunc->getName();
    auto uppercaseName = name;
    std::transform(uppercaseName.begin(), uppercaseName.end(), uppercaseName.begin(), ::toupper);
    if (uppercaseName != "COUNT") {
        return false;
    }
    if (spelling != nullptr) {
        *spelling = name;
    }
    auto& valueExprParams = aggFunc->getParams();
    if (valueExprParams.size() != 1) {
        return false;
    }
    if (not valueExprParams[0]->isStar()) {
        return false;
    }
    return true;
}

/// @return true if holding a single ValueFactor
bool ValueExpr::isFactor() const { return _factorOps.size() == 1; }

/// @return first ValueFactorPtr held. Useful when isFactor() == true
std::shared_ptr<ValueFactor const> ValueExpr::getFactor() const {
    if (_factorOps.empty()) {
        throw std::logic_error("ValueExpr::getFactor no factors");
    }
    return _factorOps.front().factor;
}

std::shared_ptr<ValueFactor> ValueExpr::getFactor() {
    if (_factorOps.empty()) {
        throw std::logic_error("ValueExpr::getFactor no factors");
    }
    return _factorOps.front().factor;
}

std::string ValueExpr::getConstVal() const {
    if (_factorOps.size() == 1) {
        ValueFactor const& factor = *_factorOps.front().factor;
        if (factor.getType() == ValueFactor::CONST) {
            return factor.getConstVal();
        }
    }
    return std::string();
}

std::shared_ptr<FuncExpr const> ValueExpr::getFunction() const {
    if (_factorOps.size() == 1) {
        ValueFactor const& factor = *_factorOps.front().factor;
        if (factor.getType() == ValueFactor::FUNCTION) {
            return factor.getFuncExpr();
        }
    }
    return nullptr;
}

bool ValueExpr::isColumnRef() const {
    if (_factorOps.size() == 1) {
        ValueFactor const& factor = *_factorOps.front().factor;
        if (factor.getType() == ValueFactor::COLUMNREF) {
            return true;
        }
    }
    return false;
}

bool ValueExpr::isFunction() const { return getFunction() != nullptr; }

bool ValueExpr::isConstVal() const { return _factorOps.size() == 1 && _factorOps[0].factor->isConstVal(); }

ValueExprPtr ValueExpr::clone() const {
    // First, make a shallow copy
    ValueExprPtr expr = std::make_shared<ValueExpr>(*this);
    FactorOpVector::iterator ti = expr->_factorOps.begin();
    for (FactorOpVector::const_iterator i = _factorOps.begin(); i != _factorOps.end(); ++i, ++ti) {
        // Deep copy (clone) each factor.
        ti->factor = i->factor->clone();
    }
    return expr;
}

bool ValueExpr::isSubsetOf(ValueExpr const& valueExpr) const {
    if (not _alias.empty() && _alias != valueExpr._alias) return false;
    // A ValueExpr may have been built as a ColumnRef with no table, or table alias, only the column defined,
    // in which case this column might be the alias of another ValueExpr. (For example in the following:
    // "SELECT AVG(foo) as f ORDER BY f", the ValueExpr for "f" in the OrderBy clause f will be constructed
    // as a column ref like so: ColumnRef("", "", "f"), and in this case that ValueExpr is a subset of the
    // ValueExpr in the SelectList.

    if (isColumnRef() && getColumnRef()->isColumnOnly() && getColumnRef()->getColumn() == valueExpr._alias) {
        return true;
    }
    return query::isSubsetOf(_factorOps, valueExpr._factorOps);
}

std::string ValueExpr::sqlFragment(QueryTemplate::SetAliasMode aliasMode) const {
    // Reuse QueryTemplate-based rendering
    QueryTemplate qt(aliasMode);
    ValueExpr::render render(qt, false);
    render.applyToQT(this);
    return boost::lexical_cast<std::string>(qt);
}

std::string ValueExpr::sqlFragmentNoQuotes(QueryTemplate::SetAliasMode aliasMode) const {
    QueryTemplate qt(aliasMode);
    qt.setQuoteIdentifiers(false);
    ValueExpr::render render(qt, false);
    render.applyToQT(this);
    return boost::lexical_cast<std::string>(qt);
}

std::ostream& operator<<(std::ostream& os, ValueExpr const& ve) {
    os << "ValueExpr(";
    os << "\"" << ve._alias << "\", ";
    os << util::printable(ve._factorOps, "", "");
    os << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, ValueExpr const* ve) {
    if (!ve) return os << "nullptr";
    return os << *ve;
}

////////////////////////////////////////////////////////////////////////
// ValueExpr::render
////////////////////////////////////////////////////////////////////////
void ValueExpr::render::applyToQT(ValueExpr const& ve) {
    if (_needsComma && _count++ > 0) {
        _qt.append(",");
    }
    if (_qt.getValueExprAliasMode() == QueryTemplate::USE && ve.hasAlias()) {
        _qt.appendIdentifier(ve._alias);
        return;
    }
    auto previousAliasMode = _qt.getAliasMode();

    // If we are defining this ValueExpr's alias, we still must USE the alias of any contained ValueFactor.
    // If the mode is DONT_USE then we can leave it as is.
    if (_qt.getAliasMode() == QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS) {
        _qt.setAliasMode(QueryTemplate::USE_ALIAS);
    }

    ValueFactor::render render(_qt);

    bool needsClose = false;
    if (!_isProtected && ve._factorOps.size() > 1) {  // Need opening parenthesis
        _qt.append("(");
        needsClose = true;
    }
    for (FactorOpVector::const_iterator i = ve._factorOps.begin(); i != ve._factorOps.end(); ++i) {
        render.applyToQT(i->factor);
        switch (i->op) {
            case ValueExpr::NONE:
                break;
            case ValueExpr::UNKNOWN:
                _qt.append("<UNKNOWN_OP>");
                break;
            case ValueExpr::PLUS:
                _qt.append("+");
                break;
            case ValueExpr::MINUS:
                _qt.append("-");
                break;
            case ValueExpr::MULTIPLY:
                _qt.append("*");
                break;
            case ValueExpr::DIVIDE:
                _qt.append("/");
                break;
            case ValueExpr::DIV:
                _qt.append("DIV");
                break;
            case ValueExpr::MODULO:
                _qt.append("%");
                break;
            case ValueExpr::MOD:
                _qt.append("MOD");
                break;
            case ValueExpr::BIT_SHIFT_LEFT:
                _qt.append("<<");
                break;
            case ValueExpr::BIT_SHIFT_RIGHT:
                _qt.append(">>");
                break;
            case ValueExpr::BIT_AND:
                _qt.append("&");
                break;
            case ValueExpr::BIT_OR:
                _qt.append("|");
                break;
            case ValueExpr::BIT_XOR:
                _qt.append("^");
                break;
            default:
                std::ostringstream ss;
                ss << "Corruption: bad _op in ValueExpr optype=" << i->op;
                // FIXME: Make sure this never happens.
                throw ss.str();
        }
    }
    if (needsClose) {  // Need closing parenthesis
        _qt.append(")");
    }
    _qt.setAliasMode(previousAliasMode);
    if (_qt.getValueExprAliasMode() == QueryTemplate::DEFINE) {
        if (!ve._alias.empty()) {
            _qt.append("AS");
            _qt.appendIdentifier(ve._alias);
        }
    }
}

bool ValueExpr::operator==(const ValueExpr& rhs) const { return (_alias == rhs._alias && compareValue(rhs)); }

bool ValueExpr::compareValue(const ValueExpr& rhs) const { return _factorOps == rhs._factorOps; }

// Miscellaneous
struct _copyValueExpr {
    ValueExprPtr operator()(ValueExprPtr const& p) { return p->clone(); }
};

void cloneValueExprPtrVector(ValueExprPtrVector& dest, ValueExprPtrVector const& src) {
    dest.resize(src.size());  // Presize destination
    std::transform(src.begin(), src.end(), dest.begin(), _copyValueExpr());
}

}  // namespace lsst::qserv::query
