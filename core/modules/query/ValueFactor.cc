// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#include "query/ValueFactor.h"

// System headers
#include <iostream>
#include <iterator>
#include <sstream>

// Local headers
#include "query/ColumnRef.h"
#include "query/FuncExpr.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {

ValueFactorPtr ValueFactor::newColumnRefFactor(boost::shared_ptr<ColumnRef const> cr) {
    ValueFactorPtr term = boost::make_shared<ValueFactor>();
    term->_type = COLUMNREF;
    term->_columnRef = boost::make_shared<ColumnRef>(*cr);
    return term;
}

ValueFactorPtr ValueFactor::newStarFactor(std::string const& table) {
    ValueFactorPtr term = boost::make_shared<ValueFactor>();
    term->_type = STAR;
    if(!table.empty()) {
        term->_tableStar = table;
    }
    return term;
}
ValueFactorPtr ValueFactor::newFuncFactor(boost::shared_ptr<FuncExpr> fe) {
    ValueFactorPtr term = boost::make_shared<ValueFactor>();
    term->_type = FUNCTION;
    term->_funcExpr = fe;
    return term;
}

ValueFactorPtr ValueFactor::newAggFactor(boost::shared_ptr<FuncExpr> fe) {
    ValueFactorPtr term = boost::make_shared<ValueFactor>();
    term->_type = AGGFUNC;
    term->_funcExpr = fe;
    return term;
}

ValueFactorPtr
ValueFactor::newConstFactor(std::string const& alnum) {
    ValueFactorPtr term = boost::make_shared<ValueFactor>();
    term->_type = CONST;
    term->_tableStar = alnum;
    return term;
}

ValueFactorPtr
ValueFactor::newExprFactor(boost::shared_ptr<ValueExpr> ve) {
    ValueFactorPtr factor = boost::make_shared<ValueFactor>();
    factor->_type = EXPR;
    factor->_valueExpr = ve;
    return factor;
}

void ValueFactor::findColumnRefs(ColumnRef::List& list) {
    switch(_type) {
    case COLUMNREF:
        list.push_back(_columnRef);
        break;
    case FUNCTION:
    case AGGFUNC:
        _funcExpr->findColumnRefs(list);
        break;
    case STAR:
    case CONST:
        break;
    case EXPR:
        _valueExpr->findColumnRefs(list);
        break;
    default: break;
    }
}

ValueFactorPtr ValueFactor::clone() const{
    ValueFactorPtr expr = boost::make_shared<ValueFactor>(*this);
    // Clone refs.
    // FIXME: not sure these are deep copies.
    if(_columnRef.get()) {
        expr->_columnRef = boost::make_shared<ColumnRef>(*_columnRef);
    }
    if(_funcExpr.get()) {
        expr->_funcExpr = boost::make_shared<FuncExpr>(*_funcExpr);
    }
    if(_valueExpr.get()) {
        expr->_funcExpr = boost::make_shared<FuncExpr>(*_funcExpr);
    }
    return expr;
}

std::ostream& operator<<(std::ostream& os, ValueFactor const& ve) {
    switch(ve._type) {
    case ValueFactor::COLUMNREF: os << "CREF: " << *(ve._columnRef); break;
    case ValueFactor::FUNCTION: os << "FUNC: " << *(ve._funcExpr); break;
    case ValueFactor::AGGFUNC: os << "AGGFUNC: " << *(ve._funcExpr); break;
    case ValueFactor::STAR:
        os << "<";
        if(!ve._tableStar.empty()) os << ve._tableStar << ".";
        os << "*>";
        break;
    case ValueFactor::CONST:
        os << "CONST: " << ve._tableStar;
        break;
    case ValueFactor::EXPR: os << "EXPR: " << *(ve._valueExpr); break;
    default: os << "UnknownFactor"; break;
    }
    if(!ve._alias.empty()) { os << " [" << ve._alias << "]"; }
    return os;
}

std::ostream& operator<<(std::ostream& os, ValueFactor const* ve) {
    if(!ve) return os << "<NULL>";
    return os << *ve;
}

void ValueFactor::render::operator()(ValueFactor const& ve) {
    switch(ve._type) {
    case ValueFactor::COLUMNREF: ve._columnRef->renderTo(_qt); break;
    case ValueFactor::FUNCTION: ve._funcExpr->renderTo(_qt); break;
    case ValueFactor::AGGFUNC: ve._funcExpr->renderTo(_qt); break;
    case ValueFactor::STAR:
        if(!ve._tableStar.empty()) {
            _qt.append(ColumnRef("",ve._tableStar, "*"));
        } else {
            _qt.append("*");
        }
        break;
    case ValueFactor::CONST: _qt.append(ve._tableStar); break;
    case ValueFactor::EXPR:
        { ValueExpr::render r(_qt, false);
            r(ve._valueExpr);
        }
        break;
    default: break;
    }
    if(!ve._alias.empty()) { _qt.append("AS"); _qt.append(ve._alias); }
}

}}} // namespace lsst::qserv::query
