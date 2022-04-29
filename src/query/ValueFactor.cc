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
 * @brief ValueFactor can be thought of the "ValueExpr" portion of a
 * ValueExpr. A ValueFactor is an element that evaluates to a
 * non-boolean value. ValueExprs bundle ValueFactors together with
 * conjunctions and allow tagging with an aliases. ValueFactors do
 * not have aliases. Value factor is a concept borrowed from the
 * SQL92 grammer.
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "query/ValueFactor.h"

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>

// Third-party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "query/ColumnRef.h"
#include "query/FuncExpr.h"
#include "query/QueryTemplate.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"

namespace lsst { namespace qserv { namespace query {

ValueFactor::ValueFactor(std::shared_ptr<ColumnRef> const& columnRef)
        : _type(COLUMNREF), _columnRef(columnRef) {}

ValueFactor::ValueFactor(std::string const& constVal) : _type(CONST), _constVal(constVal) {
    auto&& removeFrom = std::find_if(_constVal.rbegin(), _constVal.rend(), [](unsigned char c) {
                            return !std::isspace(c);
                        }).base();
    _constVal.erase(removeFrom, _constVal.end());
}

ValueFactor::ValueFactor(std::shared_ptr<FuncExpr> const& funcExpr) : _type(FUNCTION), _funcExpr(funcExpr) {}

ValueFactorPtr ValueFactor::newColumnRefFactor(std::shared_ptr<ColumnRef const> cr) {
    return std::make_shared<ValueFactor>(std::make_shared<ColumnRef>(*cr));
}

ValueFactorPtr ValueFactor::newStarFactor(std::string const& table) {
    ValueFactorPtr term = std::make_shared<ValueFactor>();
    term->_type = STAR;
    if (!table.empty()) {
        term->_tableStar = std::make_shared<TableRef>("", table, "");
    }
    return term;
}

ValueFactorPtr ValueFactor::newFuncFactor(std::shared_ptr<FuncExpr> const& fe) {
    ValueFactorPtr term = std::make_shared<ValueFactor>();
    term->_type = FUNCTION;
    term->_funcExpr = fe;
    return term;
}

ValueFactorPtr ValueFactor::newAggFactor(std::shared_ptr<FuncExpr> const& fe) {
    ValueFactorPtr term = std::make_shared<ValueFactor>();
    term->_type = AGGFUNC;
    term->_funcExpr = fe;
    return term;
}

ValueFactorPtr ValueFactor::newConstFactor(std::string const& alnum) {
    return std::make_shared<ValueFactor>(alnum);
}

ValueFactorPtr ValueFactor::newExprFactor(std::shared_ptr<ValueExpr> const& ve) {
    ValueFactorPtr factor = std::make_shared<ValueFactor>();
    factor->_type = EXPR;
    factor->_valueExpr = ve;
    return factor;
}

void ValueFactor::findColumnRefs(ColumnRef::Vector& vector) const {
    switch (_type) {
        case COLUMNREF:
            vector.push_back(_columnRef);
            break;
        case FUNCTION:
        case AGGFUNC:
            _funcExpr->findColumnRefs(vector);
            break;
        case STAR:
            break;
        case CONST:
            break;
        case EXPR:
            _valueExpr->findColumnRefs(vector);
            break;
        default:
            break;
    }
}

ValueFactorPtr ValueFactor::clone() const {
    ValueFactorPtr expr = std::make_shared<ValueFactor>(*this);
    // Clone refs.
    if (_columnRef.get()) {
        expr->_columnRef = std::make_shared<ColumnRef>(*_columnRef);
    }
    if (_funcExpr.get()) {
        expr->_funcExpr = _funcExpr->clone();
    }
    if (_valueExpr.get()) {
        expr->_valueExpr = _valueExpr->clone();
    }
    return expr;
}

std::ostream& operator<<(std::ostream& os, ValueFactor const& ve) {
    os << "ValueFactor(";
    if (ve._columnRef != nullptr) {
        os << ve._columnRef;
    } else if (ve._funcExpr != nullptr) {
        if (ValueFactor::AGGFUNC == ve._type) {
            os << "query::ValueFactor::AGGFUNC, ";
        } else if (ValueFactor::FUNCTION == ve._type) {
            os << "query::ValueFactor::FUNCTION, ";
        }
        os << ve._funcExpr;
    } else if (ve._valueExpr != nullptr) {
        os << ve._valueExpr;
    } else if (ve._type == ValueFactor::STAR) {
        os << "STAR";
        if (nullptr != ve._tableStar) {
            os << ",  << ve._tableStar << ";
        }
    } else {
        os << "\"" << ve._constVal << "\"";
    }
    os << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, ValueFactor const* ve) {
    if (!ve) return os << "nullptr";
    return os << *ve;
}

void ValueFactor::render::applyToQT(ValueFactor const& ve) {
    switch (ve._type) {
        case ValueFactor::COLUMNREF:
            ve._columnRef->renderTo(_qt);
            break;
        case ValueFactor::FUNCTION:
            ve._funcExpr->renderTo(_qt);
            break;
        case ValueFactor::AGGFUNC:
            ve._funcExpr->renderTo(_qt);
            break;
        case ValueFactor::STAR:
            if (nullptr != ve._tableStar) {
                QueryTemplate qt(_qt.getAliasMode());
                TableRef::render render(qt);
                render.applyToQT(ve._tableStar);
                _qt.append(boost::lexical_cast<std::string>(qt) + ".*");
            } else {
                _qt.append("*");
            }
            break;
        case ValueFactor::CONST:
            _qt.append(ve._constVal);
            break;
        case ValueFactor::EXPR: {
            ValueExpr::render r(_qt, false);
            r.applyToQT(ve._valueExpr);
        } break;
        default:
            break;
    }
}

bool ValueFactor::operator==(const ValueFactor& rhs) const {
    return (_type == rhs._type && util::ptrCompare<ColumnRef>(_columnRef, rhs._columnRef) &&
            util::ptrCompare<FuncExpr>(_funcExpr, rhs._funcExpr) &&
            util::ptrCompare<ValueExpr>(_valueExpr, rhs._valueExpr) && _constVal == rhs._constVal);
}

bool ValueFactor::isSubsetOf(ValueFactor const& rhs) const {
    if (_type != rhs._type) return false;
    switch (_type) {
        default:
            throw std::logic_error("unhandled factor op type");
        case NONE:
            return true;
        case COLUMNREF:
            return _columnRef->isSubsetOf(rhs._columnRef);
        case FUNCTION:
            return _funcExpr->isSubsetOf(*rhs._funcExpr);
        case AGGFUNC:
            return _funcExpr->isSubsetOf(*rhs._funcExpr);
        case STAR:
            if (nullptr == _tableStar && nullptr == rhs._tableStar) return true;
            if (nullptr != _tableStar && nullptr != rhs._tableStar)
                return _tableStar->isSubsetOf(*rhs._tableStar);
            else
                return false;
        case CONST:
            return _constVal == rhs._constVal;
        case EXPR:
            return _valueExpr->isSubsetOf(*rhs._valueExpr);
    }
}

void ValueFactor::set(std::shared_ptr<ValueExpr> const& valueExpr) {
    _reset();
    _valueExpr = valueExpr;
    _type = EXPR;
}

void ValueFactor::setStar(std::shared_ptr<TableRef> const& tableRef) {
    _reset();
    _tableStar = tableRef;
    _type = STAR;
}

void ValueFactor::_reset() {
    _type = NONE;
    _columnRef.reset();
    _funcExpr.reset();
    _valueExpr.reset();
    _constVal.clear();
}

}}}  // namespace lsst::qserv::query
